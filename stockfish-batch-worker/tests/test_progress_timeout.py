import asyncio
from dataclasses import replace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

import chess

from stockfish_batch.config import Config
from stockfish_batch.engine import Engine
from stockfish_batch.watchdog import progress_timeout


class Search:
    multipv = [{"nodes": 200, "pv": []}]

    def __init__(self, increasing):
        self.increasing = increasing

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    async def __aiter__(self):
        for index in range(8):
            await asyncio.sleep(0.2)
            yield {"nodes": index + 1 if self.increasing else 1}

    async def wait(self):
        return None


class ProgressTimeoutTests(IsolatedAsyncioTestCase):
    def test_disabling_absolute_timeout_requires_stall_protection(self):
        with self.assertRaises(ValueError):
            Config("input", "output", run_timeout=0).validate()
        config = Config("input", "output", run_timeout=0, stall_timeout=3600).validate()
        for invalid in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                replace(config, stall_timeout=invalid).validate()

    async def test_no_progress_times_out(self):
        with self.assertRaisesRegex(TimeoutError, "No meaningful progress"):
            async with progress_timeout(0.05):
                await asyncio.sleep(0.2)

    async def test_engine_node_progress_can_outlive_the_stall_window(self):
        config = Config("input", "output", run_timeout=0, stall_timeout=1, position_timeout=1).validate()
        engine = Engine(config)
        engine.protocol = AsyncMock()
        engine.protocol.analysis.return_value = Search(increasing=True)
        progress = []
        engine.progress_callback = lambda: progress.append(True)
        result = await engine.analyse({"fen": chess.STARTING_FEN})
        self.assertEqual(len(progress), 8)
        self.assertEqual(result["analysis"][0]["nodes"], 200)
        engine.protocol.analyse.assert_not_called()

    async def test_repeated_uci_messages_do_not_hide_a_stalled_engine(self):
        engine = Engine(Config("input", "output", run_timeout=0, stall_timeout=1))
        engine.protocol = AsyncMock()
        engine.protocol.analysis.return_value = Search(increasing=False)
        with self.assertRaisesRegex(TimeoutError, "No meaningful progress"):
            await engine.analyse({"fen": chess.STARTING_FEN})
