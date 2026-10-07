import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import chess

from stockfish_batch import checkpoints as checkpoint_module
from stockfish_batch.checkpoints import BatchCheckpoints, encode
from stockfish_batch.config import Config
from stockfish_batch.storage import Storage
from stockfish_batch.worker import run
from test_worker import Factory


class UploadModeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "input.jsonl"
        self.source.write_bytes(b"".join(encode({"id": str(i), "fen": chess.STARTING_FEN}) for i in range(7)))
        self.config = Config(str(self.source), str(self.root / "result"), workers=3, hash_mb=16,
                             multipv=1, upload_mode="background", batch_size=3, upload_queue_size=2)

    async def execute(self, config=None, factory=None, **kwargs):
        return await run(config or self.config, engine_factory=factory or Factory(), engine_sha="test-engine",
                         progress=kwargs.pop("progress", lambda *a, **k: None), **kwargs)

    async def test_remainder_batch_and_idempotent_resume(self):
        first = await self.execute()
        files = sorted((self.root / "result/batches").glob("*.json"))
        self.assertEqual([len(json.loads(p.read_text())["records"]) for p in files], [3, 3, 1])
        before = {p: p.read_bytes() for p in (self.root / "result").rglob("*.json")}
        factory = Factory()
        self.assertEqual(await self.execute(factory=factory), first)
        self.assertEqual(factory.calls, [])
        self.assertEqual(before, {p: p.read_bytes() for p in (self.root / "result").rglob("*.json")})

    async def test_all_modes_have_identical_chess_results_and_expected_file_counts(self):
        digests = []
        for mode, size in (("blocking", 1), ("background", 1), ("background", 3), ("background", 500)):
            config = replace(self.config, output=str(self.root / f"{mode}-{size}"), upload_mode=mode, batch_size=size)
            events = []
            manifest = await self.execute(config, progress=lambda e, **f: events.append((e, f)), measure=True)
            self.assertEqual(len(manifest["records"]), 7)
            self.assertEqual(len({entry["object"] for entry in manifest["records"]}), (7 + size - 1) // size)
            metrics = events[-1][1]["metrics"]
            self.assertGreater(metrics["worker_seconds"], 0)
            self.assertLessEqual(metrics["upload_queue_peak"], 2)
            digests.append(metrics["semantic_sha256"])
        self.assertEqual(len(set(digests)), 1)

    async def test_engines_continue_during_upload_and_queue_is_bounded(self):
        entered, release = threading.Event(), threading.Event()

        class SlowStorage(Storage):
            def create(self, uri, data):
                if "/positions/" in uri:
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("Test did not release upload")
                return super().create(uri, data)

        config = replace(self.config, batch_size=1)
        factory = Factory(delay=0.001)
        task = asyncio.create_task(self.execute(config, factory, storage=SlowStorage()))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 5))
            await asyncio.sleep(0.05)
            self.assertGreater(len(factory.calls), config.workers)
            self.assertLessEqual(len(factory.calls), config.workers + config.upload_queue_size + 1)
        finally:
            release.set()
            await task

    async def test_failed_batch_upload_does_not_mark_results_saved(self):
        class BrokenStorage(Storage):
            def create(self, uri, data):
                if "/batches/" in uri:
                    raise PermissionError("upload failed")
                return super().create(uri, data)
        events = []
        with self.assertRaises(ExceptionGroup):
            await self.execute(storage=BrokenStorage(), progress=lambda e, **f: events.append(e))
        self.assertNotIn("saved", events)
        self.assertNotIn("complete", events)
        self.assertFalse((self.root / "result/manifest.json").exists())

    async def test_compact_pipeline_validates_new_results_once_and_logs_per_batch(self):
        events = []
        with patch.object(checkpoint_module, "validate_result", wraps=checkpoint_module.validate_result) as validate:
            await self.execute(replace(self.config, compact_results=True),
                               progress=lambda event, **fields: events.append((event, fields)))
            self.assertEqual(validate.call_count, 7)
        saved = [fields for event, fields in events if event == "saved"]
        self.assertEqual(len(saved), 3)
        self.assertEqual(sum(event["batch_positions"] for event in saved), 7)
        self.assertEqual(saved[-1]["saved"], 7)
        with patch.object(checkpoint_module, "validate_result", wraps=checkpoint_module.validate_result) as validate:
            await self.execute(replace(self.config, compact_results=True))
            self.assertEqual(validate.call_count, 7)  # Recovery still fully validates every FEN.

    async def test_whole_batch_preparation_does_not_stop_collecting_new_results(self):
        entered, release = threading.Event(), threading.Event()
        original = BatchCheckpoints.prepare_batch

        def slow_prepare(writer, index, items):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("Test did not release batch preparation")
            return original(writer, index, items)

        self.source.write_bytes(b"".join(encode({"id": str(i), "fen": chess.STARTING_FEN}) for i in range(100)))
        config = replace(self.config, max_positions=100)
        factory = Factory(delay=0.001)
        with patch.object(BatchCheckpoints, "prepare_batch", slow_prepare):
            task = asyncio.create_task(self.execute(config, factory))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 5))
                await asyncio.sleep(0.08)
                # Two complete batches can queue while another is validated;
                # backpressure still prevents analyzing the whole input.
                self.assertGreaterEqual(len(factory.calls), 9)
                self.assertLess(len(factory.calls), 100)
            finally:
                release.set()
                await task

    async def test_preparation_continues_during_batch_upload_with_bounded_queues(self):
        entered, release = threading.Event(), threading.Event()
        class SlowStorage(Storage):
            def create(self, uri, raw):
                if "/batches/" in uri:
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("Test did not release upload")
                return super().create(uri, raw)
        self.source.write_bytes(b"".join(encode({"id": str(i), "fen": chess.STARTING_FEN}) for i in range(100)))
        factory, events = Factory(delay=0.001), []
        task = asyncio.create_task(self.execute(replace(self.config, max_positions=100), factory,
            storage=SlowStorage(), progress=lambda event, **fields: events.append((event, fields))))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 5))
            await asyncio.sleep(0.08)
            self.assertGreaterEqual(len(factory.calls), 12)
            self.assertLess(len(factory.calls), 100)
            self.assertNotIn("saved", [event for event, _ in events])
        finally:
            release.set()
            await task
        metrics = events[-1][1]["metrics"]["result_pipeline"]
        self.assertEqual(metrics["prepared_batch_queue_peak"], 2)
        self.assertLessEqual(metrics["raw_batch_queue_peak"], 2)

    def test_prepared_payload_is_immutable_and_bound_to_writer(self):
        from test_worker import result_for
        row = {"id": "0", "fen": chess.STARTING_FEN}
        contract = {"fingerprint": "a" * 64, "settings": {"multipv": 1}}
        writer = BatchCheckpoints(Storage(), str(self.root / "prepared"), contract, [row], 3)
        result = result_for(row)
        prepared = writer.prepare_batch(0, {"0": (result, 1)})
        result["analysis"][0]["pv"] = ["e2e5"]
        foreign = BatchCheckpoints(Storage(), str(self.root / "foreign"), contract, [row], 3)
        with self.assertRaises(ValueError):
            foreign.commit_prepared_batch(prepared)
        saved = writer.commit_prepared_batch(prepared)
        self.assertEqual(saved["0"]["engine_result"]["analysis"][0]["pv"], ["e2e4"])
        self.assertEqual(writer.load_batch(0), saved)

    async def test_prepared_batches_cannot_skip_validation_of_conflict_winner(self):
        class CorruptWinner(Storage):
            def create(self, uri, raw):
                if "/batches/" in uri:
                    value = json.loads(raw)
                    value["records"][0]["result_sha256"] = "0" * 64
                    super().create(uri, encode(value))
                    return False
                return super().create(uri, raw)

        with self.assertRaises(ExceptionGroup) as caught:
            await self.execute(storage=CorruptWinner())
        self.assertIn("checksum", str(caught.exception.exceptions[0]))
        self.assertFalse((self.root / "result/manifest.json").exists())

    async def test_bad_new_result_never_reaches_batch_storage(self):
        from test_worker import FakeEngine, result_for
        class BadEngine(FakeEngine):
            async def analyse(self, row):
                value = result_for(row)
                value["analysis"][0]["pv"] = ["e2e5"]
                return value
        with self.assertRaises(ExceptionGroup):
            await self.execute(factory=lambda config: BadEngine(Factory(), config))
        self.assertFalse(list((self.root / "result").glob("batches/*.json")))

    async def test_interrupt_then_resume_skips_whole_committed_batches(self):
        config = replace(self.config, workers=1)
        committed = asyncio.Event()
        factory = Factory(delay=0.05)
        task = asyncio.create_task(self.execute(config, factory,
            progress=lambda e, **f: committed.set() if e == "saved" else None))
        try:
            await asyncio.wait_for(committed.wait(), 5)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        before = {p: p.read_bytes() for p in (self.root / "result/batches").glob("*.json")}
        saved_ids = {r["id"] for raw in before.values() for r in json.loads(raw)["records"]}
        self.assertGreaterEqual(len(saved_ids), 3)
        self.assertFalse((self.root / "result/manifest.json").exists())
        resumed = Factory()
        await self.execute(config, resumed)
        self.assertEqual(set(resumed.calls), {str(i) for i in range(7)} - saved_ids)
        self.assertTrue(all(p.read_bytes() == raw for p, raw in before.items()))

    async def test_batch_corruption_and_changed_batch_size_fail_before_engines(self):
        await self.execute()
        factory = Factory()
        with self.assertRaises(ValueError):
            await self.execute(replace(self.config, batch_size=2), factory)
        self.assertFalse(factory.calls)
        path = self.root / "result/batches/000000.json"
        value = json.loads(path.read_text())
        value["records"].pop()
        path.write_bytes(encode(value))
        with self.assertRaises(ValueError):
            await self.execute(factory=factory)
        self.assertFalse(factory.calls)

    async def test_concurrent_attempts_do_not_overwrite_batches(self):
        one, two = await asyncio.gather(self.execute(), self.execute())
        self.assertEqual(one, two)
        self.assertEqual(len(list((self.root / "result/batches").glob("*.json"))), 3)

    def test_invalid_batch_configuration(self):
        for updates in ({"batch_size": 501}, {"upload_queue_size": 0}, {"upload_mode": "other"},
                        {"upload_mode": "blocking", "batch_size": 3}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                replace(self.config, **updates).validate()


if __name__ == "__main__":
    unittest.main()
