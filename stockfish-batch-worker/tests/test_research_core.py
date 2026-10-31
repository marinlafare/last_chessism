import tempfile
from pathlib import Path
import unittest
from unittest.mock import AsyncMock
import chess
from stockfish_core import HASH_MIB, THREADS, NODES, MULTIPV
from stockfish_core.analysis import board_from_fen, configure, analyse, verify_binary


class ProfileTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_profile_resets_engine_and_uses_only_node_budget(self):
        engine = AsyncMock()
        engine.id = {"name": "Stockfish 16.1"}
        await configure(engine, THREADS, HASH_MIB)
        self.assertEqual(engine.configure.call_args.args[0], {
            "Threads": 1, "Hash": 256, "UCI_ShowWDL": True, "SyzygyPath": "", "SyzygyProbeLimit": 0})
        for _ in range(2): await analyse(engine, chess.Board(), NODES, MULTIPV)
        a, b = engine.analyse.call_args_list
        self.assertIsNot(a.kwargs["game"], b.kwargs["game"])
        self.assertEqual(a.args[1].nodes, 100000)
        self.assertIsNone(a.args[1].time)
        self.assertEqual(a.kwargs["multipv"], 4)

    def test_four_and_six_field_board_and_unapproved_binary(self):
        self.assertEqual(board_from_fen(" ".join(chess.STARTING_FEN.split()[:4])).fen(), chess.STARTING_FEN)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wrong-engine"
            path.touch()
            with self.assertRaisesRegex(ValueError, "research profile"): verify_binary(path)


if __name__ == "__main__": unittest.main()
