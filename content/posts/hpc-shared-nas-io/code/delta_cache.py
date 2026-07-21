#!/usr/bin/env python3
"""Worker-local SQLite cache with immutable shared delta chunks.

The SQLite database is never opened over the shared filesystem.  A worker
publishes immutable JSONL chunks and an atomically replaced small manifest.
Peers open known manifest paths directly and import only unseen chunks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import threading
from dataclasses import asdict, dataclass
from pathlib import Path

from checkpoint_protocol import atomic_write_bytes


@dataclass(frozen=True)
class Chunk:
    filename: str
    first_sequence: int
    last_sequence: int
    records: int
    sha256: str


class DeltaCache:
    def __init__(self, root: Path, worker_id: int, peers: int) -> None:
        self.root = root
        self.worker_id = worker_id
        self.peers = peers
        self.local_dir = root / "local" / f"worker-{worker_id:05d}"
        self.shared_dir = root / "shared" / f"worker-{worker_id:05d}"
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self._db_lock = threading.RLock()
        self._export_lock = threading.Lock()
        self.connection = sqlite3.connect(
            self.local_dir / "cache.sqlite",
            check_same_thread=False,
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS entries (
                cache_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                origin_worker INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS own_delta (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS imported_chunks (
                peer_worker INTEGER NOT NULL,
                filename TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                PRIMARY KEY (peer_worker, filename)
            );
            CREATE TABLE IF NOT EXISTS state (
                state_key TEXT PRIMARY KEY,
                state_value TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def close(self) -> None:
        with self._db_lock:
            self.connection.close()

    def put(self, cache_key: str, payload: dict[str, object]) -> bool:
        encoded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        with self._db_lock:
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO entries(cache_key, payload_json, origin_worker) "
                "VALUES (?, ?, ?)",
                (cache_key, encoded, self.worker_id),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                self.connection.execute(
                    "INSERT INTO own_delta(cache_key, payload_json) VALUES (?, ?)",
                    (cache_key, encoded),
                )
                self.connection.commit()
        return inserted

    def get(self, cache_key: str) -> dict[str, object] | None:
        with self._db_lock:
            row = self.connection.execute(
                "SELECT payload_json FROM entries WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def _state_int(self, key: str, default: int = 0) -> int:
        with self._db_lock:
            row = self.connection.execute(
                "SELECT state_value FROM state WHERE state_key = ?", (key,)
            ).fetchone()
        return int(row[0]) if row else default

    def _set_state_int(self, key: str, value: int) -> None:
        with self._db_lock:
            self.connection.execute(
                "INSERT INTO state(state_key, state_value) VALUES (?, ?) "
                "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
                (key, str(value)),
            )
            self.connection.commit()

    def _manifest_path(self, worker_id: int | None = None) -> Path:
        owner = self.worker_id if worker_id is None else worker_id
        return self.root / "shared" / f"worker-{owner:05d}" / "latest.json"

    def _load_owner_manifest(self) -> dict[str, object]:
        path = self._manifest_path()
        try:
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError:
            return {
                "schema_version": 1,
                "worker_id": self.worker_id,
                "revision": 0,
                "chunks": [],
            }

    def export_pending(self, limit: int = 1000) -> Chunk | None:
        with self._export_lock:
            return self._export_pending_serial(limit)

    def _export_pending_serial(self, limit: int) -> Chunk | None:
        with self._db_lock:
            exported = self._state_int("exported_sequence")
            rows = self.connection.execute(
                "SELECT sequence, cache_key, payload_json "
                "FROM own_delta WHERE sequence > ? ORDER BY sequence LIMIT ?",
                (exported, limit),
            ).fetchall()
        if not rows:
            return None

        lines = []
        for sequence, cache_key, payload_json in rows:
            lines.append(
                json.dumps(
                    {
                        "sequence": sequence,
                        "cache_key": cache_key,
                        "payload": json.loads(payload_json),
                        "origin_worker": self.worker_id,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
        payload = "".join(lines).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        first_sequence = int(rows[0][0])
        last_sequence = int(rows[-1][0])
        filename = (
            f"chunk-{first_sequence:012d}-{last_sequence:012d}-" f"{digest[:16]}.jsonl"
        )

        with tempfile.TemporaryDirectory(prefix="delta-cache-") as temporary:
            local_chunk = Path(temporary) / filename
            local_chunk.write_bytes(payload)
            shared_tmp = self.shared_dir / f".{filename}.{os.getpid()}.tmp"
            shutil.copyfile(local_chunk, shared_tmp)
            descriptor = os.open(shared_tmp, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(shared_tmp, self.shared_dir / filename)

        chunk = Chunk(
            filename=filename,
            first_sequence=first_sequence,
            last_sequence=last_sequence,
            records=len(rows),
            sha256=digest,
        )
        manifest = self._load_owner_manifest()
        encoded_chunk = asdict(chunk)
        existing = {
            raw_chunk["filename"]: raw_chunk for raw_chunk in manifest["chunks"]
        }.get(chunk.filename)
        if existing is None:
            manifest["revision"] = int(manifest["revision"]) + 1
            manifest["chunks"].append(encoded_chunk)
            atomic_write_bytes(
                self._manifest_path(),
                (
                    json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n"
                ).encode("utf-8"),
                durable=True,
            )
        elif existing != encoded_chunk:
            raise ValueError(
                f"chunk filename collision with different metadata: {chunk.filename}"
            )
        self._set_state_int("exported_sequence", last_sequence)
        return chunk

    def import_peer(self, peer_worker: int) -> dict[str, int]:
        if peer_worker == self.worker_id:
            return {"imported": 0, "conflicts": 0, "chunks": 0}
        manifest_path = self._manifest_path(peer_worker)
        try:
            with manifest_path.open("r", encoding="utf-8") as file:
                manifest = json.load(file)
        except FileNotFoundError:
            return {"imported": 0, "conflicts": 0, "chunks": 0}

        imported = 0
        conflicts = 0
        processed_chunks = 0
        peer_dir = manifest_path.parent
        for raw_chunk in manifest["chunks"]:
            chunk = Chunk(**raw_chunk)
            with self._db_lock:
                seen = self.connection.execute(
                    "SELECT 1 FROM imported_chunks "
                    "WHERE peer_worker = ? AND filename = ?",
                    (peer_worker, chunk.filename),
                ).fetchone()
            if seen:
                continue

            payload = (peer_dir / chunk.filename).read_bytes()
            actual_digest = hashlib.sha256(payload).hexdigest()
            if actual_digest != chunk.sha256:
                raise ValueError(f"checksum mismatch: {peer_dir / chunk.filename}")

            events = [json.loads(raw_line) for raw_line in payload.splitlines()]
            with self._db_lock:
                seen = self.connection.execute(
                    "SELECT 1 FROM imported_chunks "
                    "WHERE peer_worker = ? AND filename = ?",
                    (peer_worker, chunk.filename),
                ).fetchone()
                if seen:
                    continue

                for event in events:
                    encoded = json.dumps(
                        event["payload"],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    existing = self.connection.execute(
                        "SELECT payload_json FROM entries WHERE cache_key = ?",
                        (event["cache_key"],),
                    ).fetchone()
                    if existing:
                        if existing[0] != encoded:
                            conflicts += 1
                        continue
                    self.connection.execute(
                        "INSERT INTO entries(cache_key, payload_json, origin_worker) "
                        "VALUES (?, ?, ?)",
                        (event["cache_key"], encoded, peer_worker),
                    )
                    imported += 1

                self.connection.execute(
                    "INSERT INTO imported_chunks(peer_worker, filename, sha256) "
                    "VALUES (?, ?, ?)",
                    (peer_worker, chunk.filename, chunk.sha256),
                )
                self.connection.commit()
            processed_chunks += 1
        return {
            "imported": imported,
            "conflicts": conflicts,
            "chunks": processed_chunks,
        }

    def import_all_peers(self) -> dict[int, dict[str, int]]:
        return {
            peer: self.import_peer(peer)
            for peer in range(self.peers)
            if peer != self.worker_id
        }


def run_demo(root: Path) -> None:
    worker_zero = DeltaCache(root, worker_id=0, peers=2)
    worker_one = DeltaCache(root, worker_id=1, peers=2)
    try:
        worker_zero.put("image:a", {"segments": ["a0"], "version": 1})
        worker_zero.put("image:shared", {"segments": ["same"], "version": 1})
        worker_one.put("image:b", {"segments": ["b0"], "version": 1})
        worker_one.put("image:shared", {"segments": ["different"], "version": 2})
        chunk_zero = worker_zero.export_pending()
        chunk_one = worker_one.export_pending()
        import_zero = worker_zero.import_peer(1)
        import_one = worker_one.import_peer(0)
        result = {
            "worker_0_chunk": asdict(chunk_zero) if chunk_zero else None,
            "worker_1_chunk": asdict(chunk_one) if chunk_one else None,
            "worker_0_import": import_zero,
            "worker_1_import": import_one,
            "worker_0_image_b": worker_zero.get("image:b"),
            "worker_1_image_a": worker_one.get("image:a"),
            "conflict_is_observable": (
                import_zero["conflicts"] == 1 and import_one["conflicts"] == 1
            ),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        worker_zero.close()
        worker_one.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo = subparsers.add_parser("demo")
    demo.add_argument("--root", type=Path, required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "demo":
        run_demo(args.root)
    else:
        raise AssertionError(args.command)


if __name__ == "__main__":
    main()
