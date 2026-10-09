"""Opt-in real HTTP-handler/cloud-engine parity; run in the local engine image."""
import json
import os
from pathlib import Path
import unittest


@unittest.skipUnless(os.environ.get("STOCKFISH_BACKEND_PARITY") == "1", "opt-in local/cloud real-engine parity")
class BackendParityTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_handler_and_cloud_worker_match_independent_of_order(self):
        from routers.analysis import analyze_fens_endpoint, AnalysisRequest, engine_pool
        from stockfish_batch.config import Config
        from stockfish_batch.engine import Engine
        rows = [json.loads(line) for line in (Path(__file__).resolve().parents[1] / "examples/input.jsonl").read_text().splitlines()]
        # Real DB position keys: retain counters only for the explicitly supplied
        # full-FEN case. Include terminal/special-move positions from the fixture.
        rows = [{**row, "fen": " ".join(row["fen"].split()[:4])} for row in rows]
        def stable(value):
            if isinstance(value, dict):
                return {k: stable(v) for k, v in value.items() if k not in {"time", "nps", "hashfull", "cpuload"}}
            if isinstance(value, list): return [stable(v) for v in value]
            return value
        await engine_pool.initialize()
        try:
            local = await analyze_fens_endpoint(AnalysisRequest(fens=[row["fen"] for row in rows]))
            cfg = Config("unused-input", "unused-output", workers=1, threads=1, hash_mb=256,
                         nodes=100000, multipv=4, memory_mib=1024, stall_timeout=300, run_timeout=0)
            async with Engine(cfg) as engine:
                cloud = {row["id"]: await engine.analyse(row) for row in reversed(rows)}
            for row, result in zip(rows, local):
                self.assertEqual(stable(result), stable(cloud[row["id"]]), row["id"])
        finally:
            await engine_pool.shutdown()


if __name__ == "__main__": unittest.main()
