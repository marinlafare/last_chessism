from pathlib import Path
from copy import deepcopy
import json
import subprocess
import sys
import unittest
from unittest.mock import patch

import chess

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmark_fen_batches.benchmark_dataset import select
from benchmark_fen_batches.render_benchmark import LEGACY_IMAGE, render
from benchmark_fen_batches.benchmark import MODES
from stockfish_batch.config import Config
import benchmark_fen_batches.benchmark as runner
import benchmark_fen_batches.benchmark_audit as auditor


class BenchmarkRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, *, mismatch=False, resumed=0, reuse=False):
        class FakeStorage:
            def __init__(self):
                self.saved = {}
            def read(self, *args):
                return b"fixture"
            def create(self, uri, data):
                if reuse:
                    return False
                self.saved[uri] = json.loads(data)
                return True
        storage = FakeStorage()
        calls = []
        rows = [{"id": str(i), "fen": str(i)} for i in range(1000)]
        async def fake_run(config, *, progress, measure):
            calls.append(config)
            self.assertTrue(measure)
            progress("complete", resumed=resumed, saved=1000, manifest=config.output + "/manifest.json",
                     metrics={"semantic_sha256": str(len(calls)) if mismatch else "same", "fen_per_second": len(calls)})
            return {"position_count": 1000}
        with patch.object(runner, "Storage", return_value=storage), patch.object(runner, "parse_input", return_value=rows), \
             patch.object(runner.chess, "Board") as board, patch.object(runner, "run", side_effect=fake_run), \
             patch.object(runner, "emit"):
            board.return_value.is_game_over.return_value = False
            try:
                report = await runner.benchmark("input", "output")
            except ValueError:
                self.assertNotIn("output/benchmark-report.json", storage.saved)
                if reuse:
                    self.assertFalse(calls)
                raise
        return report, storage, calls

    async def test_four_fresh_sequential_modes_and_complete_report(self):
        report, storage, calls = await self.exercise()
        self.assertEqual([c.batch_size for c in calls], [1, 1, 100, 500])
        self.assertEqual(len({c.output for c in calls}), 4)
        self.assertEqual(report["winner"], "d-background-500")
        self.assertEqual(len(storage.saved), 6)  # Start marker, four metrics, report.

    async def test_mismatch_resumed_trial_and_reused_prefix_never_report_success(self):
        for options in ({"mismatch": True}, {"resumed": 1}, {"reuse": True}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                await self.exercise(**options)


class BenchmarkAuditTests(unittest.TestCase):
    def test_rejects_different_job_or_relaxed_cost_bounds(self):
        job = render(auditor.IMAGE, auditor.INPUT, auditor.PREFIX)
        job.update(uid=auditor.UID, status={"state": "RUNNING"})
        with patch.object(auditor, "save"), patch.object(auditor, "command", return_value=job):
            self.assertEqual(auditor.status("gcloud"), job)
        for key in ("uid", "maxRetryCount", "maxRunDuration", "machineType"):
            bad = deepcopy(job)
            if key == "uid":
                bad[key] = "wrong"
            elif key == "machineType":
                bad["allocationPolicy"]["instances"][0]["policy"][key] = "n2d-standard-8"
            else:
                bad["taskGroups"][0]["taskSpec"][key] = 1 if key == "maxRetryCount" else "7200s"
            with self.subTest(key=key), patch.object(auditor, "command", return_value=bad), \
                 patch.object(auditor, "save"), self.assertRaises(ValueError):
                auditor.status("gcloud")

    def test_cleanup_requires_both_vm_and_disk_empty_twice(self):
        with patch.object(auditor, "command", return_value=[]) as command, \
             patch.object(auditor.time, "sleep"), patch.object(auditor, "save"):
            self.assertTrue(auditor.cleanup("gcloud"))
            self.assertEqual(command.call_count, 4)
        with patch.object(auditor, "command", side_effect=[[], [{"name": "still-here"}]]), \
             self.assertRaises(ValueError):
            auditor.cleanup("gcloud")


class BenchmarkTests(unittest.TestCase):
    def test_relocated_scripts_have_direct_and_module_help_without_side_effects(self):
        for name in ("benchmark", "benchmark_dataset", "render_benchmark", "benchmark_audit"):
            for args, cwd in (([str(ROOT / "benchmark_fen_batches" / (name + ".py")), "--help"], ROOT.parent),
                              (["-m", "benchmark_fen_batches." + name, "--help"], ROOT)):
                with self.subTest(name=name, args=args):
                    result = subprocess.run([sys.executable, *args], cwd=cwd, capture_output=True,
                                            text=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout)

    def test_existing_published_image_retains_its_original_entrypoint(self):
        job = render(LEGACY_IMAGE, auditor.INPUT, auditor.PREFIX)
        commands = job["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["commands"]
        self.assertEqual(commands[:2], ["-m", "stockfish_batch.benchmark"])

    def test_relocated_docker_runner_is_included_without_host_tools(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        rules = (ROOT / ".dockerignore").read_text().splitlines()
        self.assertIn("COPY benchmark_fen_batches ./benchmark_fen_batches", dockerfile)
        self.assertIn("!benchmark_fen_batches/benchmark.py", rules)
        self.assertIn("!benchmark_fen_batches/__init__.py", rules)
        for name in ("benchmark_dataset", "render_benchmark", "benchmark_audit"):
            self.assertNotIn(f"!benchmark_fen_batches/{name}.py", rules)

    def test_job_is_one_vm_four_sequential_variants_without_retries(self):
        image = "us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:" + "a" * 64
        root = "gs://chessism-batch-276704059200-us-central1/"
        job = render(image, root + "inputs/test/input.jsonl", root + "results/test")
        group, = job["taskGroups"]
        self.assertEqual((group["taskCount"], group["parallelism"], group["taskCountPerNode"]), ("1", "1", "1"))
        spec = group["taskSpec"]
        self.assertEqual(spec["maxRunDuration"], "3600s")
        self.assertEqual(spec["maxRetryCount"], 0)
        self.assertEqual(spec["computeResource"], {"cpuMilli": "4000", "memoryMib": "12288"})
        container = spec["runnables"][0]["container"]
        self.assertEqual(container["entrypoint"], "python")
        self.assertEqual(container["commands"][:2], ["-m", "benchmark_fen_batches.benchmark"])
        self.assertEqual(container["imageUri"], image)
        policy = job["allocationPolicy"]["instances"][0]["policy"]
        self.assertEqual(policy["machineType"], "n2d-standard-4")
        self.assertEqual(policy["provisioningModel"], "SPOT")
        self.assertEqual(len(MODES), 4)
        self.assertEqual([mode[2] for mode in MODES], [1, 1, 100, 500])
        configs = [Config("input", "out", max_positions=1000, upload_mode=m, batch_size=n).validate()
                   for _, m, n in MODES]
        self.assertTrue(all(c.analysis_settings() == configs[0].analysis_settings() for c in configs))
        self.assertTrue(all(c.run_timeout <= 900 and c.workers == 4 and c.threads == 1 for c in configs))

    def test_material_sample_is_unique_repeatable_and_excludes_terminal_positions(self):
        middle = "r1bqk2r/pppp1ppp/2n2n2/2b1p3/2B1P3/2NP1N2/PPP2PPP/R1BQ1RK1 w kq - 0 1"
        endgame = "8/5k2/8/4P3/4K3/8/8/8 w - - 0 1"
        candidates = [chess.STARTING_FEN, chess.STARTING_FEN, middle, endgame,
                      "8/8/8/8/8/8/4k3/6K1 w - - 0 1", "invalid"]
        # Both opening samples have >24 pieces; a small target tests validation/deduplication.
        targets = {"many_pieces": 2, "middle_material": 0, "few_pieces": 1}
        rows, _ = select(candidates, targets)
        self.assertEqual(len(rows), 3)
        self.assertEqual(select(candidates, targets)[0], rows)
        self.assertTrue(all(not chess.Board(r["fen"]).is_game_over() for r in rows))
        with self.assertRaises(ValueError):
            select(candidates)


if __name__ == "__main__":
    unittest.main()
