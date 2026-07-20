#!/usr/bin/env python3
"""Reproducible metadata-I/O microbenchmark for a shared filesystem.

The fixture deliberately uses many small record directories.  Four access
strategies answer the same question: "load meta.json for this record".

This is educational code.  Run it only in a disposable test directory.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable


IMAGE_NAMES = ("image-00.bin", "image-01.bin")


def record_id(index: int) -> str:
    return f"{index:012d}"


def shard_path(root: Path, rid: str) -> Path:
    """Map an ID to a deterministic three-level fan-out."""
    if len(rid) < 6:
        raise ValueError("record id must have at least six characters")
    return root / "records" / rid[-2:] / rid[-4:-2] / rid[-6:-4] / rid


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as file:
        file.write(text)
        file.flush()
        os.fsync(file.fileno())
    os.replace(tmp, path)


def prepare_fixture(root: Path, records: int, payload_bytes: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    manifest_lines: list[str] = []
    payload = b"x" * payload_bytes

    for index in range(records):
        rid = record_id(index)
        directory = shard_path(root, rid)
        directory.mkdir(parents=True, exist_ok=True)
        meta = {
            "record_id": rid,
            "images": list(IMAGE_NAMES),
            "group": index % 17,
            "version": 1,
        }
        atomic_write_text(
            directory / "meta.json",
            json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
        )
        for name in IMAGE_NAMES:
            image_path = directory / name
            if not image_path.exists():
                image_path.write_bytes(payload)
        manifest_lines.append(f"{rid}\t{directory / 'meta.json'}\n")

    atomic_write_text(root / "manifest.tsv", "".join(manifest_lines))
    print(
        json.dumps(
            {
                "event": "fixture_prepared",
                "root": str(root),
                "records": records,
                "payload_bytes_per_image": payload_bytes,
            },
            ensure_ascii=False,
        )
    )


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def direct_open(root: Path, rid: str) -> dict:
    return read_json(shard_path(root, rid) / "meta.json")


def exists_then_open(root: Path, rid: str) -> dict:
    path = shard_path(root, rid) / "meta.json"
    if not path.exists():
        raise FileNotFoundError(path)
    return read_json(path)


def scandir_then_open(root: Path, rid: str) -> dict:
    directory = shard_path(root, rid)
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.name == "meta.json" and entry.is_file():
                return read_json(Path(entry.path))
    raise FileNotFoundError(directory / "meta.json")


def load_manifest(path: Path) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            rid, raw_path = line.rstrip("\n").split("\t", maxsplit=1)
            mapping[rid] = Path(raw_path)
    return mapping


@dataclass(frozen=True)
class MethodResult:
    method: str
    samples: int
    failures: int
    elapsed_seconds: float
    operations_per_second: float
    mean_ms: float
    p50_ms: float
    p95_ms: float
    max_ms: float


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * ratio)))
    return ordered[rank]


def run_method(
    name: str,
    reader: Callable[[str], dict],
    ids: Iterable[str],
) -> MethodResult:
    latencies_ms: list[float] = []
    failures = 0
    started = time.perf_counter()

    for rid in ids:
        item_started = time.perf_counter_ns()
        try:
            value = reader(rid)
            if value.get("record_id") != rid:
                raise ValueError(f"record mismatch for {rid}")
        except (OSError, ValueError, json.JSONDecodeError):
            failures += 1
        latencies_ms.append((time.perf_counter_ns() - item_started) / 1_000_000)

    elapsed = time.perf_counter() - started
    samples = len(latencies_ms)
    return MethodResult(
        method=name,
        samples=samples,
        failures=failures,
        elapsed_seconds=elapsed,
        operations_per_second=samples / elapsed if elapsed else 0.0,
        mean_ms=statistics.fmean(latencies_ms) if latencies_ms else 0.0,
        p50_ms=percentile(latencies_ms, 0.50),
        p95_ms=percentile(latencies_ms, 0.95),
        max_ms=max(latencies_ms, default=0.0),
    )


def benchmark(root: Path, records: int, rounds: int, seed: int) -> None:
    ids = [record_id(index) for index in range(records)]
    manifest = load_manifest(root / "manifest.tsv")
    methods: list[tuple[str, Callable[[str], dict]]] = [
        ("direct_open", lambda rid: direct_open(root, rid)),
        ("exists_then_open", lambda rid: exists_then_open(root, rid)),
        ("scandir_then_open", lambda rid: scandir_then_open(root, rid)),
        ("manifest_direct_open", lambda rid: read_json(manifest[rid])),
    ]

    all_results: list[MethodResult] = []
    for round_index in range(rounds):
        random.Random(seed + round_index).shuffle(ids)
        rotated = (
            methods[round_index % len(methods) :]
            + methods[: round_index % len(methods)]
        )
        for name, reader in rotated:
            result = run_method(name, reader, ids)
            all_results.append(result)
            print(
                json.dumps(
                    {
                        "event": "benchmark_result",
                        "round": round_index + 1,
                        **asdict(result),
                    },
                    ensure_ascii=False,
                )
            )

    grouped: dict[str, list[MethodResult]] = {}
    for result in all_results:
        grouped.setdefault(result.method, []).append(result)

    baseline = statistics.fmean(
        result.operations_per_second for result in grouped["direct_open"]
    )
    summary = []
    for name, results in grouped.items():
        throughput = statistics.fmean(item.operations_per_second for item in results)
        summary.append(
            {
                "method": name,
                "rounds": len(results),
                "mean_operations_per_second": throughput,
                "relative_to_direct_open": throughput / baseline if baseline else 0.0,
                "mean_p95_ms": statistics.fmean(item.p95_ms for item in results),
                "total_failures": sum(item.failures for item in results),
            }
        )
    print(
        json.dumps(
            {"event": "benchmark_summary", "methods": summary}, ensure_ascii=False
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="create synthetic records")
    prepare.add_argument("--root", type=Path, required=True)
    prepare.add_argument("--records", type=int, default=3000)
    prepare.add_argument("--payload-bytes", type=int, default=1024)

    bench = subparsers.add_parser("benchmark", help="run access-method benchmark")
    bench.add_argument("--root", type=Path, required=True)
    bench.add_argument("--records", type=int, default=3000)
    bench.add_argument("--rounds", type=int, default=3)
    bench.add_argument("--seed", type=int, default=20260720)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "prepare":
        prepare_fixture(args.root, args.records, args.payload_bytes)
    elif args.command == "benchmark":
        benchmark(args.root, args.records, args.rounds, args.seed)
    else:
        raise AssertionError(args.command)


if __name__ == "__main__":
    main()
