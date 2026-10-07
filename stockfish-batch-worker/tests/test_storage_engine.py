import asyncio
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import chess
import chess.engine
from google.api_core.exceptions import Forbidden, NotFound, PreconditionFailed

from stockfish_batch.checkpoints import validate_result
from stockfish_batch.config import Config
from stockfish_batch.engine import Engine, serializable
from stockfish_batch.storage import Storage


class StorageTests(unittest.TestCase):
    def test_local_create_is_atomic_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "object.json")
            store = Storage()
            self.assertIsNone(store.read(path))
            self.assertTrue(store.create(path, b"first"))
            self.assertFalse(store.create(path, b"second"))
            self.assertEqual(store.read(path), b"first")
            with self.assertRaises(ValueError):
                store.read(path, 2)
            self.assertEqual(len(list(Path(directory).iterdir())), 1)

    def test_gcs_generation_pins_and_create_precondition(self):
        store, blob = Storage(), MagicMock()
        blob.size, blob.generation = 2, 123
        blob.download_as_bytes.return_value = b"{}"
        with patch.object(store, "_blob", return_value=blob):
            self.assertEqual(store.read("gs://bucket/input"), b"{}")
            self.assertEqual(blob.download_as_bytes.call_args.kwargs["if_generation_match"], 123)
            self.assertTrue(store.create("gs://bucket/results/x", b"{}"))
            self.assertEqual(blob.upload_from_string.call_args.kwargs["if_generation_match"], 0)
            self.assertEqual(blob.upload_from_string.call_args.kwargs["checksum"], "crc32c")
            blob.upload_from_string.side_effect = PreconditionFailed("already exists")
            self.assertFalse(store.create("gs://bucket/results/x", b"{}"))
            blob.delete.assert_not_called()

    def test_gcs_errors_are_not_mistaken_for_missing(self):
        store, blob = Storage(), MagicMock()
        with patch.object(store, "_blob", return_value=blob):
            blob.reload.side_effect = NotFound("missing")
            self.assertIsNone(store.read("gs://bucket/x"))
            blob.reload.side_effect = Forbidden("denied")
            with self.assertRaises(Forbidden):
                store.read("gs://bucket/x")
            blob.reload.side_effect = None
            blob.size = 100
            with self.assertRaises(ValueError):
                store.read("gs://bucket/x", 10)
            blob.download_as_bytes.assert_not_called()


class EngineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Unit tests use a fake UCI process. Binary attestation is tested separately.
        patcher = patch("stockfish_batch.engine.verify_binary")
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_engine_lifecycle_and_new_game_per_position(self):
        protocol, transport = AsyncMock(), MagicMock()
        protocol.id = {"name": "Stockfish 16.1"}
        protocol.analyse.return_value = []
        config = Config("input", "output", hash_mb=16)
        with patch("chess.engine.popen_uci", return_value=(transport, protocol)):
            async with Engine(config) as engine:
                row = {"id": "start", "fen": chess.STARTING_FEN}
                await engine.analyse(row)
                await engine.analyse(row)
        first, second = protocol.analyse.call_args_list
        self.assertIsNot(first.kwargs["game"], second.kwargs["game"])
        self.assertEqual(first.args[1].nodes, 1_000_000)
        self.assertEqual(first.kwargs["multipv"], 4)
        self.assertEqual(protocol.configure.call_args.args[0]["Threads"], 1)
        self.assertTrue(protocol.configure.call_args.args[0]["UCI_ShowWDL"])
        protocol.quit.assert_awaited_once()
        transport.close.assert_called_once()

    async def test_wrong_engine_version_is_rejected_and_closed(self):
        protocol, transport = AsyncMock(), MagicMock()
        protocol.id = {"name": "Stockfish 99"}
        with patch("chess.engine.popen_uci", return_value=(transport, protocol)):
            with self.assertRaises(ValueError):
                async with Engine(Config("input", "output")):
                    self.fail("Must not start")
        transport.close.assert_called_once()

    async def test_terminal_positions_and_white_score_convention(self):
        engine = Engine(Config("input", "output"))
        for fen, score in (("7k/6Q1/5K2/8/8/8/8/8 b - - 0 1", 10000),
                           ("8/8/8/8/8/5k2/6q1/7K w - - 0 1", -10000),
                           ("7k/5Q2/6K1/8/8/8/8/8 b - - 0 1", 0)):
            row = {"id": "terminal", "fen": fen}
            result = await engine.analyse(row)
            self.assertEqual(result["analysis"]["score"], score)
            validate_result(result, row, 4)

    async def test_pov_serialization_matches_local_service(self):
        self.assertEqual(serializable(chess.engine.PovScore(chess.engine.Cp(42), chess.BLACK)), -42)
        self.assertEqual(serializable(chess.engine.PovWdl(chess.engine.Wdl(100, 700, 200), chess.BLACK)), [200, 700, 100])
        self.assertEqual(serializable(chess.Move.from_uci("e2e4")), "e2e4")

    async def test_result_validation_rejects_partial_error_illegal_and_nonfinite(self):
        row = {"id": "start", "fen": chess.STARTING_FEN}
        valid = {"fen": row["fen"], "is_valid": True, "analysis": [
            {"pv": ["e2e4"], "score": 20, "multipv": 1, "nodes": 100, "wdl": [10, 990, 0]}]}
        validate_result(valid, row, 1)
        for changes in ({"pv": ["e2e5"]}, {"pv": ["0000"]}, {"error": "failed"}, {"wdl": [0, 0, 0]},
                        {"nodes": 0}, {"time": float("nan")}, {"multipv": 2}, {"score": None}):
            result = copy.deepcopy(valid)
            result["analysis"][0].update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_result(result, row, 1)
        with self.assertRaises(ValueError):
            validate_result(valid, row, 4)


if __name__ == "__main__":
    unittest.main()
