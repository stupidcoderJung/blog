#!/usr/bin/env python3
"""Single-writer checkpoint journals with completion sentinels.

Each worker owns exactly one journal.  A coordinator accepts a journal only
after validating the worker's DONE sentinel.  The hot path never scans the
shared directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, payload: bytes, durable: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        if durable:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(tmp, path)
    if durable:
        fsync_directory(path.parent)


def append_bytes(path: Path, payload: bytes, durable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        if durable:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def worker_directory(root: Path, worker_id: int) -> Path:
    return root / f"worker-{worker_id:05d}"


@dataclass(frozen=True)
class JournalSummary:
    worker_id: int
    events: int
    sha256: str
    last_sequence: int
    journal_path: str


class WorkerJournal:
    def __init__(
        self,
        root: Path,
        worker_id: int,
        *,
        flush_every: int = 100,
        durable_flush: bool = False,
    ) -> None:
        if flush_every <= 0:
            raise ValueError("flush_every must be positive")
        self.worker_id = worker_id
        self.directory = worker_directory(root, worker_id)
        self.journal_path = self.directory / "events.jsonl"
        self.done_path = self.directory / "DONE.json"
        self.flush_every = flush_every
        self.durable_flush = durable_flush
        self._buffer: list[bytes] = []
        self._sequence = 0
        self._events = 0
        self._digest = hashlib.sha256()
        self.directory.mkdir(parents=True, exist_ok=True)

        if self.done_path.exists():
            raise RuntimeError(f"completed journal is immutable: {self.done_path}")
        if self.journal_path.exists():
            self._restore_existing_journal()

    def _restore_existing_journal(self) -> None:
        with self.journal_path.open("rb") as file:
            for raw_line in file:
                event = json.loads(raw_line)
                self._digest.update(raw_line)
                self._events += 1
                self._sequence = max(self._sequence, int(event["sequence"]))

    def append(self, record_id: str, status: str = "ok") -> None:
        self._sequence += 1
        event = {
            "sequence": self._sequence,
            "record_id": record_id,
            "status": status,
            "worker_id": self.worker_id,
        }
        raw = (
            json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        self._buffer.append(raw)
        if len(self._buffer) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        payload = b"".join(self._buffer)
        append_bytes(self.journal_path, payload, durable=self.durable_flush)
        for raw in self._buffer:
            self._digest.update(raw)
            self._events += 1
        self._buffer.clear()

    def mark_done(self) -> JournalSummary:
        self.flush()
        if not self.journal_path.exists():
            atomic_write_bytes(
                self.journal_path,
                b"",
                durable=self.durable_flush,
            )
        if self.durable_flush and self.journal_path.exists():
            descriptor = os.open(self.journal_path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

        summary = JournalSummary(
            worker_id=self.worker_id,
            events=self._events,
            sha256=self._digest.hexdigest(),
            last_sequence=self._sequence,
            journal_path=self.journal_path.name,
        )
        atomic_write_bytes(
            self.done_path,
            (json.dumps(asdict_summary(summary), separators=(",", ":")) + "\n").encode(
                "utf-8"
            ),
            durable=self.durable_flush,
        )
        return summary


def asdict_summary(summary: JournalSummary) -> dict[str, object]:
    return {
        "worker_id": summary.worker_id,
        "events": summary.events,
        "sha256": summary.sha256,
        "last_sequence": summary.last_sequence,
        "journal_path": summary.journal_path,
    }


def validate_completed_journal(root: Path, worker_id: int) -> JournalSummary:
    directory = worker_directory(root, worker_id)
    done_path = directory / "DONE.json"
    with done_path.open("r", encoding="utf-8") as file:
        expected = json.load(file)

    journal_path = directory / expected["journal_path"]
    digest = hashlib.sha256()
    events = 0
    last_sequence = 0
    with journal_path.open("rb") as file:
        for raw_line in file:
            event = json.loads(raw_line)
            if int(event["worker_id"]) != worker_id:
                raise ValueError(f"wrong worker id in {journal_path}")
            sequence = int(event["sequence"])
            if sequence != last_sequence + 1:
                raise ValueError(f"non-contiguous sequence in {journal_path}")
            last_sequence = sequence
            events += 1
            digest.update(raw_line)

    actual = JournalSummary(
        worker_id=worker_id,
        events=events,
        sha256=digest.hexdigest(),
        last_sequence=last_sequence,
        journal_path=journal_path.name,
    )
    if asdict_summary(actual) != expected:
        raise ValueError(
            f"journal does not match sentinel: expected={expected}, "
            f"actual={asdict_summary(actual)}"
        )
    return actual


def merge_completed(
    root: Path,
    worker_ids: Iterable[int],
    *,
    require_all: bool = True,
    durable: bool = True,
) -> dict[str, object]:
    summaries: list[JournalSummary] = []
    merged_records: set[str] = set()
    missing: list[int] = []

    for worker_id in worker_ids:
        done_path = worker_directory(root, worker_id) / "DONE.json"
        try:
            summary = validate_completed_journal(root, worker_id)
        except FileNotFoundError:
            missing.append(worker_id)
            continue
        summaries.append(summary)
        journal_path = worker_directory(root, worker_id) / summary.journal_path
        with journal_path.open("r", encoding="utf-8") as file:
            for line in file:
                event = json.loads(line)
                if event["status"] == "ok":
                    merged_records.add(str(event["record_id"]))

    if require_all and missing:
        raise RuntimeError(f"workers without valid completion sentinel: {missing}")

    manifest = {
        "schema_version": 1,
        "workers": [asdict_summary(summary) for summary in summaries],
        "missing_workers": missing,
        "completed_records": sorted(merged_records),
        "completed_record_count": len(merged_records),
    }
    atomic_write_bytes(
        root / "completed-manifest.json",
        (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        durable=durable,
    )
    return manifest


def run_demo(
    root: Path, workers: int, records: int, incomplete_worker: int | None
) -> None:
    journals = [
        WorkerJournal(root, worker_id, flush_every=37) for worker_id in range(workers)
    ]
    for index in range(records):
        worker_id = index % workers
        journals[worker_id].append(f"record-{index:08d}")

    for journal in journals:
        if journal.worker_id == incomplete_worker:
            journal.flush()
            continue
        journal.mark_done()

    manifest = merge_completed(
        root,
        range(workers),
        require_all=incomplete_worker is None,
        durable=False,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo = subparsers.add_parser("demo")
    demo.add_argument("--root", type=Path, required=True)
    demo.add_argument("--workers", type=int, default=4)
    demo.add_argument("--records", type=int, default=1000)
    demo.add_argument("--incomplete-worker", type=int)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "demo":
        run_demo(args.root, args.workers, args.records, args.incomplete_worker)
    else:
        raise AssertionError(args.command)


if __name__ == "__main__":
    main()
