import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import chess
from stockfish_batch.checkpoints import BatchCheckpoints, encode
from stockfish_batch.config import Config
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import Storage
from stockfish_batch.worker import run
from test_worker import Factory, FakeEngine, result_for


class SingleVMTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "input.jsonl"
        self.config = Config(str(self.source), str(self.root / "output"), workers=4, hash_mb=16,
            multipv=1, upload_mode="background", batch_size=500, compact_results=True, max_positions=2082)

    async def execute(self, count, factory=None, storage=None):
        self.source.write_bytes(b"".join(encode({"id": str(i), "fen": chess.STARTING_FEN}) for i in range(count)))
        return await run(self.config, engine_factory=factory or Factory(delay=0),
            storage=storage, engine_sha="fake", progress=lambda *a, **kw: None)

    async def test_entire_2082_fens_use_four_persistent_engines_and_compact_references(self):
        factory = Factory(delay=0)
        manifest = await self.execute(2082, factory)
        self.assertEqual((factory.opened, factory.closed), (4, 4))
        self.assertEqual(manifest["position_count"], 2082)
        files = sorted((self.root / "output/batches").glob("*.json"))
        self.assertEqual([len(json.loads(p.read_bytes())["records"]) for p in files], [500, 500, 500, 500, 82])
        report = json.loads((self.root / "output/performance.json").read_bytes())
        contract = json.loads((self.root / "output/contract.json").read_bytes())
        validate_performance(report, contract, 4)
        self.assertEqual(sum(w["positions"] for w in report["workers"]), 2082)
        self.assertEqual(len(report["workers"]), 4)
        # Checkpoints and first reporting attempt's summary are immutable on recovery.
        original = (self.root / "output/performance.json").read_bytes()
        factory = Factory()
        self.assertEqual(await self.execute(2082, factory), manifest)
        self.assertEqual(factory.calls, [])
        self.assertEqual(original, (self.root / "output/performance.json").read_bytes())
        refs = {r["id"]: r for r in manifest["records"]}
        self.assertTrue(all(set(r) == {"id", "object", "sha256"} for r in refs.values()))

    async def test_fast_engines_take_more_fens_instead_of_waiting_for_a_slow_engine(self):
        counts = []
        class Engine(FakeEngine):
            async def __aenter__(self):
                self.index = len(counts); counts.append(0)
                return self
            async def analyse(self, row):
                await asyncio.sleep(0.07 if self.index == 0 else 0.001)
                counts[self.index] += 1
                return result_for(row)
        factory = Factory()
        await self.execute(80, lambda cfg: Engine(factory, cfg))
        self.assertEqual(sum(counts), 80)
        self.assertTrue(all(n > 0 for n in counts))
        self.assertGreater(min(counts[1:]), counts[0])

    async def test_failed_summary_upload_does_not_report_success_and_retry_needs_no_reanalysis(self):
        class Broken(Storage):
            def create(self, uri, raw):
                if uri.endswith("performance.json"): raise PermissionError("test report failure")
                return super().create(uri, raw)
        with self.assertRaises(PermissionError): await self.execute(8, storage=Broken())
        factory = Factory()
        await self.execute(8, factory)
        self.assertEqual(factory.calls, [])
        report = json.loads((self.root / "output/performance.json").read_bytes())
        self.assertEqual(report["resumed"], 8)

    def test_large_manifest_can_be_verified_on_retry(self):
        rows = [{"id": f"{i:064x}", "fen": chess.STARTING_FEN} for i in range(20000)]
        cp = BatchCheckpoints(Storage(), str(self.root / "large"), {"fingerprint": "abc"}, rows, 500)
        refs = {r["id"]: cp.reference(r, {"result_sha256": "a" * 64}) for r in rows}
        manifest = cp.finish_references(rows, refs)
        self.assertGreater((self.root / "large/manifest.json").stat().st_size, 2 * 1024 * 1024)
        self.assertEqual(cp.finish_references(rows, refs), manifest)

    def test_large_jobs_require_bounded_result_memory(self):
        self.assertEqual(replace(self.config, max_positions=200000).validate().max_positions, 200000)
        with self.assertRaises(ValueError): replace(self.config, compact_results=False).validate()
        with self.assertRaises(ValueError): replace(self.config, max_positions=200001).validate()
