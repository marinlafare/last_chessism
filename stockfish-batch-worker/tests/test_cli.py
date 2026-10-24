"""Subprocess CLI checks: no engines, network, or credentials required."""
import json
import selectors
import signal
import subprocess
import sys
import unittest
from unittest.mock import AsyncMock, patch

from stockfish_batch import __main__ as cli
from stockfish_batch.config import Config
from stockfish_batch.watchdog import AnalysisStalled


class TimeoutExitTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchdog_exit_is_never_google_preemption(self):
        errors = [AnalysisStalled("no completed FEN"),
                  ExceptionGroup("worker", [AnalysisStalled("no completed FEN")]),
                  TimeoutError("upload stalled")]
        for error in errors:
            with self.subTest(error=error), patch.object(cli, "run", new=AsyncMock(side_effect=error)), \
                 patch.object(cli, "emit") as emit:
                self.assertEqual(await cli.execute(Config("input", "output")), 124)
                self.assertEqual(emit.call_args.kwargs["failure_kind"], "timeout")

    async def test_other_application_failure_remains_code_one(self):
        with patch.object(cli, "run", new=AsyncMock(side_effect=ValueError("bad result"))), \
             patch.object(cli, "emit"):
            self.assertEqual(await cli.execute(Config("input", "output")), 1)


class CliTests(unittest.TestCase):
    def test_help_and_invalid_options(self):
        result = subprocess.run([sys.executable, "-m", "stockfish_batch", "--help"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--max-positions", result.stdout)
        result = subprocess.run([sys.executable, "-m", "stockfish_batch", "--input", "x",
                                 "--output", "y", "--workers", "0"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)

    def test_sigterm_returns_nonzero_and_does_not_report_complete(self):
        # Exercise the real signal/exit-code path with a waiting job instead of burning CPU.
        code = """
import asyncio
from stockfish_batch import __main__ as cli
from stockfish_batch.config import Config
async def pending(config):
    cli.emit('ready')
    await asyncio.sleep(60)
cli.run = pending
raise SystemExit(asyncio.run(cli.execute(Config('unused-input', 'unused-output'))))
"""
        with subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as process:
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(10), "Child did not become ready")
                self.assertEqual(json.loads(process.stdout.readline())["event"], "ready")
                process.send_signal(signal.SIGTERM)
                output, error = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 143, error)
                events = [json.loads(line)["event"] for line in output.splitlines()]
                self.assertEqual(events, ["stopping", "interrupted"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
