from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from async_pipeline import run_once
from checkpoint_protocol import (
    WorkerJournal,
    merge_completed,
    validate_completed_journal,
)
from delta_cache import DeltaCache
from nas_io_lab import (
    direct_open,
    exists_then_open,
    prepare_fixture,
    record_id,
    scandir_then_open,
    shard_path,
)


class FixtureTests(unittest.TestCase):
    def test_shard_path_is_deterministic(self) -> None:
        root = Path("/fixture")
        self.assertEqual(
            shard_path(root, "000000123456"),
            root / "records" / "56" / "34" / "12" / "000000123456",
        )

    def test_all_access_methods_return_same_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepare_fixture(root, records=3, payload_bytes=8)
            rid = record_id(2)
            expected = direct_open(root, rid)
            self.assertEqual(exists_then_open(root, rid), expected)
            self.assertEqual(scandir_then_open(root, rid), expected)


class AsyncPipelineTests(unittest.TestCase):
    def test_offloaded_mode_preserves_event_loop_and_is_faster(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepare_fixture(root, records=48, payload_bytes=8)

            async def compare() -> tuple[object, object]:
                blocking = await run_once(
                    mode="blocking",
                    root=root,
                    records=48,
                    concurrency=16,
                    io_threads=16,
                    io_latency_seconds=0.003,
                    service_latency_seconds=0.001,
                )
                offloaded = await run_once(
                    mode="offloaded",
                    root=root,
                    records=48,
                    concurrency=16,
                    io_threads=16,
                    io_latency_seconds=0.003,
                    service_latency_seconds=0.001,
                )
                return blocking, offloaded

            blocking, offloaded = asyncio.run(compare())
            self.assertGreater(
                offloaded.records_per_second,
                blocking.records_per_second * 2,
            )
            self.assertLess(
                offloaded.heartbeat_max_gap_ms,
                blocking.heartbeat_max_gap_ms,
            )


class CheckpointTests(unittest.TestCase):
    def test_empty_worker_can_publish_a_valid_completion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            journal = WorkerJournal(root, 0)
            journal.mark_done()
            summary = validate_completed_journal(root, 0)
            self.assertEqual(summary.events, 0)
            self.assertEqual(summary.last_sequence, 0)

    def test_only_completed_worker_is_merged(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            completed = WorkerJournal(root, 0, flush_every=2)
            incomplete = WorkerJournal(root, 1, flush_every=2)
            for index in range(5):
                completed.append(f"a-{index}")
                incomplete.append(f"b-{index}")
            completed.mark_done()
            incomplete.flush()

            summary = validate_completed_journal(root, 0)
            self.assertEqual(summary.events, 5)
            manifest = merge_completed(
                root,
                [0, 1],
                require_all=False,
                durable=False,
            )
            self.assertEqual(manifest["completed_record_count"], 5)
            self.assertEqual(manifest["missing_workers"], [1])

    def test_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            journal = WorkerJournal(root, 0)
            journal.append("record-a")
            summary = journal.mark_done()
            path = root / "worker-00000" / summary.journal_path
            with path.open("a", encoding="utf-8") as file:
                file.write(
                    '{"sequence":2,"record_id":"tampered","status":"ok","worker_id":0}\n'
                )
            with self.assertRaises(ValueError):
                validate_completed_journal(root, 0)


class DeltaCacheTests(unittest.TestCase):
    def test_export_can_be_offloaded_to_a_worker_thread(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cache = DeltaCache(Path(temporary), worker_id=0, peers=1)
            try:
                cache.put("a", {"value": 1})

                async def export():
                    return await asyncio.to_thread(cache.export_pending)

                chunk = asyncio.run(export())
                self.assertIsNotNone(chunk)
                self.assertEqual(chunk.records, 1)
            finally:
                cache.close()

    def test_repeated_chunk_publication_does_not_duplicate_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = DeltaCache(root, worker_id=0, peers=1)
            try:
                cache.put("a", {"value": 1})
                first = cache.export_pending()
                cache._set_state_int("exported_sequence", 0)
                second = cache.export_pending()
                self.assertEqual(first, second)

                manifest_path = root / "shared" / "worker-00000" / "latest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                self.assertEqual(len(manifest["chunks"]), 1)
            finally:
                cache.close()

    def test_round_trip_is_idempotent_and_conflict_is_visible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            zero = DeltaCache(root, worker_id=0, peers=2)
            one = DeltaCache(root, worker_id=1, peers=2)
            try:
                zero.put("a", {"value": 1})
                zero.put("shared", {"value": "from-zero"})
                one.put("b", {"value": 2})
                one.put("shared", {"value": "from-one"})
                zero.export_pending()
                one.export_pending()

                first_zero = zero.import_peer(1)
                first_one = one.import_peer(0)
                self.assertEqual(first_zero["imported"], 1)
                self.assertEqual(first_zero["conflicts"], 1)
                self.assertEqual(first_one["imported"], 1)
                self.assertEqual(first_one["conflicts"], 1)
                self.assertEqual(zero.get("b"), {"value": 2})
                self.assertEqual(one.get("a"), {"value": 1})

                second_zero = zero.import_peer(1)
                self.assertEqual(
                    second_zero, {"imported": 0, "conflicts": 0, "chunks": 0}
                )
            finally:
                zero.close()
                one.close()

    def test_corrupt_chunk_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            zero = DeltaCache(root, worker_id=0, peers=2)
            one = DeltaCache(root, worker_id=1, peers=2)
            try:
                zero.put("a", {"value": 1})
                chunk = zero.export_pending()
                self.assertIsNotNone(chunk)
                chunk_path = root / "shared" / "worker-00000" / chunk.filename
                chunk_path.write_text("corrupted\n", encoding="utf-8")
                with self.assertRaises(ValueError):
                    one.import_peer(0)
            finally:
                zero.close()
                one.close()


if __name__ == "__main__":
    unittest.main()
