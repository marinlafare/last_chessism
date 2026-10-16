from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import chess

from stockfish_batch.checkpoints import Checkpoints, encode, make_contract, parse_input
from stockfish_batch.config import Config, gcs_parts
from stockfish_batch.storage import Storage

ROW = {"id": "start", "fen": chess.STARTING_FEN}


class ContractTests(unittest.TestCase):
    def test_examples_are_twenty_valid_positions_with_distinct_ids(self):
        raw = (Path(__file__).resolve().parents[1] / "examples/input.jsonl").read_bytes()
        self.assertEqual(len(parse_input(raw, 20)), 20)

    def test_four_field_database_keys_and_six_field_fens_keep_their_identity(self):
        short = " ".join(chess.STARTING_FEN.split()[:4])
        for fen in (short, chess.STARTING_FEN, short + " 42 71"):
            with self.subTest(fen=fen):
                row = {**ROW, "fen": fen}
                self.assertEqual(parse_input(encode(row), 1), [row])
        self.assertEqual(chess.Board(short).fen(), chess.STARTING_FEN)

    def test_reject_empty_duplicate_unsafe_illegal_partial_and_excess_input(self):
        invalid = [b"", encode(ROW) * 2, encode({**ROW, "id": "../x"}),
                   encode({**ROW, "fen": "8/8/8/8/8/8/8/8 w - - 0 1"}),
                   encode({**ROW, "fen": "8/8/8/8/8/8/8/8 w - -"}),
                   encode({**ROW, "extra": True}), b"[]\n", b"invalid json"]
        invalid += [encode({**ROW, "fen": fen}) for fen in (
            None, 42, "x" * 257, chess.STARTING_FEN + " extra",
            *(" ".join(chess.STARTING_FEN.split()[:size]) for size in (0, 1, 2, 3, 5)),
        )]
        for raw in invalid:
            with self.subTest(raw=raw), self.assertRaises((ValueError, TypeError)):
                parse_input(raw, 20)
        with self.assertRaises(ValueError):
            parse_input(encode(ROW) + encode({**ROW, "id": "second"}), 1)

    def test_repeated_fens_with_different_ids_are_preserved(self):
        self.assertEqual(len(parse_input(encode(ROW) + encode({**ROW, "id": "again"}), 2)), 2)

    def test_config_limits(self):
        config = Config("input", "output")
        for changes in ({"workers": 0}, {"nodes": 0}, {"max_positions": 1001},
                        {"multipv": 11}, {"memory_mib": 1024}, {"threads": 1.5},
                        {"run_timeout": float("inf")}, {"position_timeout": float("nan")},
                        {"input": "output"}, {"output": "gs://bucket/inputs/x"},
                        {"output": "gs://bucket/results/"}, {"input": "https://example.com/x"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(config, **changes).validate()
        config.validate()
        replace(config, output="gs://bucket/results/test").validate()

    def test_gcs_uri_validation(self):
        self.assertEqual(gcs_parts("gs://bucket/inputs/test.jsonl"), ("bucket", "inputs/test.jsonl"))
        for uri in ("gs://bucket", "gs://bucket/a/../b", "gs://bucket/a?key=secret", "gs://bucket/a//b"):
            with self.assertRaises(ValueError):
                gcs_parts(uri)

    def test_resume_contract_binds_input_and_settings_but_not_parallelism(self):
        config = Config("input", "output")
        contract = make_contract(encode(ROW), [ROW], config, "binary-a")
        self.assertEqual(contract, make_contract(encode(ROW), [ROW], replace(config, workers=1), "binary-a"))
        self.assertNotEqual(contract, make_contract(encode(ROW), [ROW], replace(config, nodes=200), "binary-a"))
        self.assertNotEqual(contract, make_contract(encode(ROW), [ROW], config, "binary-b"))
        self.assertNotEqual(contract, make_contract(encode(ROW) + b"\n", [ROW], config, "binary-a"))

    def test_contract_conflict_never_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = Storage()
            contract = make_contract(encode(ROW), [ROW], Config("input", directory), "binary")
            Checkpoints(storage, directory, contract).open()
            before = (Path(directory) / "contract.json").read_bytes()
            with self.assertRaises(ValueError):
                Checkpoints(storage, directory, {**contract, "fingerprint": "other"}).open()
            self.assertEqual(before, (Path(directory) / "contract.json").read_bytes())


if __name__ == "__main__":
    unittest.main()
