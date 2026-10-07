import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import chess

from stockfish_batch.checkpoints import encode
from stockfish_batch.config import Config
from stockfish_batch.storage import Storage
from stockfish_batch.worker import run
from stockfish_batch.watchdog import AnalysisStalled


def result_for(row):
    return {"fen": row["fen"], "is_valid": True, "analysis": [
        {"multipv": 1, "score": 12, "pv": ["e2e4"], "nodes": 100, "wdl": [20, 975, 5]}
    ]}


class FakeEngine:
    def __init__(self, factory, config):
        self.factory = factory

    async def __aenter__(self):
        self.factory.opened += 1
        return self

    async def __aexit__(self, *_):
        self.factory.closed += 1

    async def analyse(self, row):
        self.factory.calls.append(row["id"])
        self.factory.active += 1
        self.factory.peak = max(self.factory.peak, self.factory.active)
        try:
            await asyncio.sleep(self.factory.delay)
            if row["id"] == self.factory.fail_id:
                raise RuntimeError("simulated engine failure")
            return result_for(row)
        finally:
            self.factory.active -= 1


class Factory:
    def __init__(self, fail_id=None, delay=0.005):
        self.fail_id, self.delay = fail_id, delay
        self.opened = self.closed = self.active = self.peak = 0
        self.calls = []

    def __call__(self, config):
        return FakeEngine(self, config)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_fens_keep_worker_alive_before_a_batch_is_uploaded(self):
        self.config = replace(self.config, workers=1, run_timeout=0, stall_timeout=1,
                              upload_mode="background", batch_size=500)
        events = []
        manifest = await self.execute(Factory(delay=0.25),
                                      progress=lambda event, **fields: events.append((event, fields)))
        self.assertEqual(manifest["position_count"], 7)
        self.assertGreater(events[-1][1]["wall_seconds"], 1)

    async def test_stalled_worker_exits_without_a_success_manifest(self):
        self.config = replace(self.config, run_timeout=0, stall_timeout=1)
        factory = Factory(delay=5)
        # Both inactivity deadlines are one second. Depending on scheduling,
        # either the overall-progress timeout or the TaskGroup's completed-FEN
        # timeout can fire first; both must terminate and close every engine.
        with self.assertRaises((TimeoutError, ExceptionGroup)) as raised:
            await self.execute(factory)
        if isinstance(raised.exception, ExceptionGroup):
            timeouts, unexpected = raised.exception.split(TimeoutError)
            self.assertIsNotNone(timeouts)
            self.assertIsNone(unexpected)
        self.assertEqual(factory.opened, factory.closed)
        self.assertFalse((self.output / "manifest.json").exists())

    async def test_engine_progress_cannot_hide_no_completed_fens(self):
        class ProgressEngine(FakeEngine):
            async def analyse(self, row):
                for _ in range(8):
                    await asyncio.sleep(0.2)
                    self.progress_callback()
                return result_for(row)

        factory = Factory()
        self.config = replace(self.config, workers=7, run_timeout=0, stall_timeout=1)
        with self.assertRaises(ExceptionGroup) as raised:
            await self.execute(lambda config: ProgressEngine(factory, config))
        self.assertTrue(raised.exception.subgroup(AnalysisStalled))
        self.assertEqual(factory.opened, factory.closed)
        self.assertFalse((self.output / "manifest.json").exists())

    async def test_final_upload_drain_is_not_an_analysis_stall(self):
        import time
        class SlowStorage(Storage):
            def create(self, uri, raw):
                if "/positions/" in uri:
                    time.sleep(0.2)
                return super().create(uri, raw)
        self.config = replace(self.config, run_timeout=0, stall_timeout=1, upload_mode="background")
        manifest = await self.execute(Factory(delay=0), storage=SlowStorage())
        self.assertEqual(manifest["position_count"], 7)

    async def test_slow_checkpoint_recovery_has_its_own_progress_window(self):
        import time
        await self.execute()
        class SlowReads(Storage):
            def read(self, uri, limit=2 * 1024 * 1024):
                if "/positions/" in uri:
                    time.sleep(0.2)
                return super().read(uri, limit)
        self.config = replace(self.config, run_timeout=0, stall_timeout=1)
        factory = Factory()
        manifest = await self.execute(factory, storage=SlowReads())
        self.assertEqual(manifest["position_count"], 7)
        self.assertEqual(factory.calls, [])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input = self.root / "input.jsonl"
        self.input.write_bytes(b"".join(encode({"id": str(i), "fen": chess.STARTING_FEN}) for i in range(7)))
        self.output = self.root / "results"
        self.config = Config(str(self.input), str(self.output), workers=3, hash_mb=16, multipv=1)

    async def execute(self, factory=None, **kwargs):
        return await run(self.config, engine_factory=factory or Factory(), engine_sha="test-engine-sha",
                         progress=kwargs.pop("progress", lambda *args, **fields: None), **kwargs)

    async def test_parallel_persistent_engines_and_idempotent_resume(self):
        factory = Factory()
        events = []
        manifest = await self.execute(factory, progress=lambda event, **fields: events.append((event, fields)))
        self.assertEqual(manifest["position_count"], 7)
        self.assertEqual(factory.opened, 3)
        self.assertEqual(factory.closed, 3)
        self.assertEqual(factory.peak, 3)
        self.assertCountEqual(factory.calls, [str(i) for i in range(7)])
        self.assertEqual([fields["saved"] for event, fields in events if event == "saved"], list(range(1, 8)))
        original = {path: path.read_bytes() for path in self.output.rglob("*.json")}
        resumed = Factory()
        self.assertEqual(await self.execute(resumed), manifest)
        self.assertEqual(resumed.opened, 0)
        self.assertEqual(original, {path: path.read_bytes() for path in self.output.rglob("*.json")})

    async def test_partial_failure_resumes_only_missing_positions(self):
        self.config = replace(self.config, workers=1)
        first = Factory(fail_id="3")
        with self.assertRaises(ExceptionGroup):
            await self.execute(first)
        self.assertEqual(first.opened, first.closed)
        self.assertFalse((self.output / "manifest.json").exists())
        self.assertEqual(len(list((self.output / "positions").glob("*.json"))), 3)
        second = Factory()
        await self.execute(second)
        self.assertEqual(second.calls, ["3", "4", "5", "6"])

    async def test_cancel_then_resume(self):
        self.config = replace(self.config, workers=1)
        first = Factory(delay=0.05)
        committed = asyncio.Event()
        task = asyncio.create_task(self.execute(first, progress=lambda event, **fields: committed.set() if event == "saved" else None))
        try:
            await asyncio.wait_for(committed.wait(), 5)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(first.closed, first.opened)
        self.assertFalse((self.output / "manifest.json").exists())
        second = Factory()
        await self.execute(second)
        self.assertNotIn("0", second.calls)

    async def test_timeout_closes_engines_without_completion(self):
        self.config = replace(self.config, run_timeout=1, position_timeout=1)
        factory = Factory(delay=5)
        with self.assertRaises(TimeoutError):
            await self.execute(factory)
        self.assertEqual(factory.opened, factory.closed)
        self.assertFalse((self.output / "manifest.json").exists())

    async def test_corrupt_checkpoint_fails_before_starting_engines(self):
        await self.execute()
        target = self.output / "positions/0.json"
        value = json.loads(target.read_bytes())
        value["engine_result"]["analysis"][0]["score"] += 1
        target.write_bytes(encode(value))
        factory = Factory()
        with self.assertRaisesRegex(ValueError, "checksum"):
            await self.execute(factory)
        self.assertEqual(factory.opened, 0)

    async def test_settings_mismatch_refused(self):
        await self.execute()
        self.config = replace(self.config, nodes=200)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            await self.execute()

    async def test_invalid_input_has_no_outputs(self):
        self.input.write_text('{"id":"bad","fen":"not a fen"}\n')
        with self.assertRaises(ValueError):
            await self.execute()
        self.assertFalse(self.output.exists())

    async def test_upload_error_never_counts_as_saved_or_complete(self):
        class BrokenStorage(Storage):
            def create(self, uri, data):
                if "/positions/" in uri:
                    raise PermissionError("simulated upload denial")
                return super().create(uri, data)
        events = []
        factory = Factory()
        with self.assertRaises(ExceptionGroup):
            await self.execute(factory, storage=BrokenStorage(), progress=lambda event, **fields: events.append(event))
        self.assertNotIn("saved", events)
        self.assertNotIn("complete", events)
        self.assertFalse((self.output / "manifest.json").exists())
        self.assertEqual(factory.opened, factory.closed)

    async def test_concurrent_attempts_do_not_overwrite(self):
        manifests = await asyncio.gather(self.execute(), self.execute())
        self.assertEqual(manifests[0], manifests[1])
        self.assertEqual(len(list((self.output / "positions").glob("*.json"))), 7)

    async def test_conflicting_completion_manifest_is_not_reported_successful(self):
        await self.execute()
        target = self.output / "manifest.json"
        target.write_text('{"status":"complete","position_count":1000}')
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            await self.execute()


if __name__ == "__main__":
    unittest.main()
