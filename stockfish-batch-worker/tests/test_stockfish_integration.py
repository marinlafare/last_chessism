"""Opt-in, small real-engine test. No network, Google credentials, or production data."""
import json
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import unittest

from stockfish_batch.config import Config
from stockfish_batch.checkpoints import encode, parse_input, semantic_digest
from stockfish_batch.worker import run


@unittest.skipUnless(os.environ.get("STOCKFISH_INTEGRATION") == "1", "opt-in real Stockfish test")
class RealStockfishTests(unittest.IsolatedAsyncioTestCase):
    async def test_compact_single_vm_reports_all_four_workers_and_resumes(self):
        from stockfish_batch.performance import validate_performance
        with tempfile.TemporaryDirectory() as directory:
            config = Config(input=str(Path(__file__).resolve().parents[1] / "examples/input.jsonl"),
                output=directory, workers=4, hash_mb=16, memory_mib=2048,
                nodes=2000, upload_mode="background", batch_size=500, compact_results=True,
                run_timeout=0, stall_timeout=10)
            manifest = await run(config, progress=lambda *a, **k: None)
            report = json.loads((Path(directory) / "performance.json").read_bytes())
            contract = json.loads((Path(directory) / "contract.json").read_bytes())
            validate_performance(report, contract, 4)
            self.assertEqual(len(report["workers"]), 4)
            self.assertEqual(sum(w["positions"] for w in report["workers"]), 20)
            self.assertEqual(await run(config, progress=lambda *a, **k: None), manifest)

    async def test_four_field_keys_match_full_fens_and_resume_without_reanalysis(self):
        raw = (Path(__file__).resolve().parents[1] / "examples/input.jsonl").read_bytes()
        full_rows = parse_input(raw, 20)
        short_rows = [{**row, "fen": " ".join(row["fen"].split()[:4])} for row in full_rows]
        # Equivalent full FENs use the local service's default counters 0/1.
        full_rows = [{**row, "fen": row["fen"] + " 0 1"} for row in short_rows]
        digests = []
        with tempfile.TemporaryDirectory() as directory:
            for kind, rows in (("full", full_rows), ("short", short_rows)):
                input_path = Path(directory) / f"{kind}.jsonl"
                input_path.write_bytes(b"".join(encode(row) for row in rows))
                output = Path(directory) / kind
                config = Config(input=str(input_path), output=str(output), workers=1,
                    hash_mb=16, memory_mib=1024, nodes=2000, multipv=4,
                    run_timeout=0, stall_timeout=10, upload_mode="background", batch_size=500)
                events = []
                progress = lambda event, **fields: events.append((event, fields))
                manifest = await run(config, progress=progress)
                records = json.loads((output / "batches/000000.json").read_bytes())["records"]
                self.assertEqual([r["engine_result"]["fen"] for r in records], [row["fen"] for row in rows])
                # Compare chess outcomes, ignoring FEN identity and timing counters.
                for record, short in zip(records, short_rows):
                    record["engine_result"]["fen"] = short["fen"]
                digests.append(semantic_digest(short_rows, {r["id"]: r for r in records}))
                snapshots = {p.name: p.read_bytes() for p in output.rglob("*.json")}
                self.assertEqual(await run(config, progress=progress), manifest)
                self.assertEqual(events[-1][1]["resumed"], 20)
                self.assertEqual(events[-1][1]["analyzed_this_attempt"], 0)
                self.assertEqual(snapshots, {p.name: p.read_bytes() for p in output.rglob("*.json")})
        self.assertEqual(digests[0], digests[1])

    async def test_real_engine_results_match_in_all_upload_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Config(input=str(Path(__file__).resolve().parents[1] / "examples/input.jsonl"),
                            output=directory, workers=1, hash_mb=16, memory_mib=1024,
                            nodes=2000, position_timeout=10, run_timeout=60)
            digests = []
            for mode, size in (("blocking", 1), ("background", 1), ("background", 7), ("background", 500)):
                events = []
                await run(replace(config, output=str(Path(directory) / f"{mode}-{size}"),
                                  upload_mode=mode, batch_size=size),
                          progress=lambda e, **f: events.append((e, f)))
                digests.append(events[-1][1]["metrics"]["semantic_sha256"])
            self.assertEqual(len(set(digests)), 1)
            events = []
            await run(replace(config, output=str(Path(directory) / "progress-watchdog"),
                              run_timeout=0, stall_timeout=10, upload_mode="background", batch_size=500),
                      progress=lambda event, **fields: events.append((event, fields)))
            self.assertEqual(events[-1][1]["metrics"]["semantic_sha256"], digests[0])

    async def test_twenty_positions_and_full_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Config(
                input=str(Path(__file__).resolve().parents[1] / "examples/input.jsonl"), output=directory,
                engine=os.environ.get("STOCKFISH_PATH", "/usr/local/bin/stockfish"),
                workers=1, threads=1, hash_mb=16, memory_mib=1024,
                nodes=2000, multipv=4, position_timeout=10, run_timeout=60,
            )
            events = []
            progress = lambda event, **fields: events.append((event, fields))
            first = await run(config, progress=progress)
            self.assertEqual(first["position_count"], 20)
            self.assertEqual(events[-1][0], "complete")
            snapshots = {p.name: p.read_bytes() for p in Path(directory).rglob("*.json")}
            second = await run(config, progress=progress)
            self.assertEqual(first, second)
            self.assertEqual(events[-1][1]["resumed"], 20)
            self.assertEqual(events[-1][1]["analyzed_this_attempt"], 0)
            self.assertEqual(snapshots, {p.name: p.read_bytes() for p in Path(directory).rglob("*.json")})
            black_mated = json.loads((Path(directory) / "positions/black-mated.json").read_text())
            self.assertEqual(black_mated["engine_result"]["analysis"]["score"], 10000)


if __name__ == "__main__":
    unittest.main()
