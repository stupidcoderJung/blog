#!/usr/bin/env python3
"""Compare blocking file I/O in a coroutine with asyncio.to_thread().

An optional injected delay makes the event-loop effect reproducible on a local
filesystem.  On a real shared filesystem use zero injected delay first.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from nas_io_lab import direct_open, record_id, shard_path


def blocking_read(root: Path, rid: str, injected_latency_seconds: float) -> dict:
    value = direct_open(root, rid)
    if injected_latency_seconds:
        time.sleep(injected_latency_seconds)
    return value


def blocking_atomic_result_write(
    root: Path,
    rid: str,
    value: dict,
    injected_latency_seconds: float,
) -> None:
    output = shard_path(root, rid) / "result.json"
    tmp = output.with_name(f".{output.name}.{os.getpid()}.{id(value)}.tmp")
    with tmp.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, separators=(",", ":"))
        file.flush()
    os.replace(tmp, output)
    if injected_latency_seconds:
        time.sleep(injected_latency_seconds)


@dataclass(frozen=True)
class RunResult:
    mode: str
    records: int
    concurrency: int
    elapsed_seconds: float
    records_per_second: float
    heartbeat_samples: int
    heartbeat_p95_gap_ms: float
    heartbeat_max_gap_ms: float


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * ratio))
    return ordered[index]


async def heartbeat(stop: asyncio.Event, interval_seconds: float) -> list[float]:
    gaps_ms: list[float] = []
    previous = time.perf_counter()
    while not stop.is_set():
        await asyncio.sleep(interval_seconds)
        now = time.perf_counter()
        gaps_ms.append((now - previous) * 1000)
        previous = now
    return gaps_ms


async def process_one(
    *,
    mode: str,
    root: Path,
    rid: str,
    io_latency_seconds: float,
    service_latency_seconds: float,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        if mode == "blocking":
            meta = blocking_read(root, rid, io_latency_seconds)
        elif mode == "offloaded":
            meta = await asyncio.to_thread(blocking_read, root, rid, io_latency_seconds)
        else:
            raise ValueError(f"unknown mode: {mode}")

        await asyncio.sleep(service_latency_seconds)
        result = {
            "record_id": meta["record_id"],
            "status": "ok",
            "source_version": meta["version"],
        }

        if mode == "blocking":
            blocking_atomic_result_write(root, rid, result, io_latency_seconds)
        else:
            await asyncio.to_thread(
                blocking_atomic_result_write,
                root,
                rid,
                result,
                io_latency_seconds,
            )


async def run_once(
    *,
    mode: str,
    root: Path,
    records: int,
    concurrency: int,
    io_threads: int,
    io_latency_seconds: float,
    service_latency_seconds: float,
) -> RunResult:
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=io_threads, thread_name_prefix="nas-io")
    loop.set_default_executor(executor)
    semaphore = asyncio.Semaphore(concurrency)
    stop = asyncio.Event()
    heartbeat_task = asyncio.create_task(heartbeat(stop, 0.01))
    started = time.perf_counter()

    try:
        async with asyncio.TaskGroup() as group:
            for index in range(records):
                group.create_task(
                    process_one(
                        mode=mode,
                        root=root,
                        rid=record_id(index),
                        io_latency_seconds=io_latency_seconds,
                        service_latency_seconds=service_latency_seconds,
                        semaphore=semaphore,
                    )
                )
    finally:
        elapsed = time.perf_counter() - started
        stop.set()
        gaps = await heartbeat_task
        executor.shutdown(wait=True)

    return RunResult(
        mode=mode,
        records=records,
        concurrency=concurrency,
        elapsed_seconds=elapsed,
        records_per_second=records / elapsed if elapsed else 0.0,
        heartbeat_samples=len(gaps),
        heartbeat_p95_gap_ms=percentile(gaps, 0.95),
        heartbeat_max_gap_ms=max(gaps, default=0.0),
    )


async def async_main(args: argparse.Namespace) -> None:
    modes = ["blocking", "offloaded"] if args.compare else [args.mode]
    results: list[RunResult] = []
    for mode in modes:
        result = await run_once(
            mode=mode,
            root=args.root,
            records=args.records,
            concurrency=args.concurrency,
            io_threads=args.io_threads,
            io_latency_seconds=args.injected_io_latency_ms / 1000,
            service_latency_seconds=args.service_latency_ms / 1000,
        )
        results.append(result)
        print(json.dumps(asdict(result), ensure_ascii=False))

    if len(results) == 2:
        blocking, offloaded = results
        print(
            json.dumps(
                {
                    "event": "comparison",
                    "throughput_speedup": (
                        offloaded.records_per_second / blocking.records_per_second
                    ),
                    "heartbeat_max_gap_reduction": (
                        blocking.heartbeat_max_gap_ms / offloaded.heartbeat_max_gap_ms
                        if offloaded.heartbeat_max_gap_ms
                        else None
                    ),
                },
                ensure_ascii=False,
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--io-threads", type=int, default=32)
    parser.add_argument("--injected-io-latency-ms", type=float, default=0.0)
    parser.add_argument("--service-latency-ms", type=float, default=2.0)
    parser.add_argument(
        "--mode", choices=("blocking", "offloaded"), default="offloaded"
    )
    parser.add_argument("--compare", action="store_true")
    return parser


def main() -> None:
    asyncio.run(async_main(build_parser().parse_args()))


if __name__ == "__main__":
    main()
