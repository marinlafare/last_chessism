"""Offline result-contract checks: no database, engine, GPU, or cloud access."""
import unittest
from unittest.mock import AsyncMock

from chessism_api import evaluations as contract
from chessism_api.operations.fen_results import stage_fen_results


def result(**values):
    return {'fen': 'synthetic-position', 'score': 24, 'next_moves': None, **values}


class EvaluationContractTests(unittest.TestCase):
    def test_types_do_not_require_an_evaluation_timestamp(self):
        for score, source, expected in (
            (None, None, 'unscored'), (0, 'stockfish', 'cp'),
            (9998, 'stockfish', 'mate'), (-9998, 'stockfish_backup', 'mate'),
            (1000, 'tablebase', 'tablebase'), (0, None, 'unknown'),
            (9998, '', 'unknown'), (42, 'Stockfish', 'unknown'),
        ):
            self.assertEqual(contract.score_kind(score, source), expected)
        self.assertEqual(contract.score_kind(9998, None, verified=False), 'mate')
        contract.validate_result(result(), source='stockfish')

    def test_legacy_mates_cannot_be_relabelled_from_the_number_alone(self):
        import chess.engine
        from chessism_api.operations.mate_distance import mate_distance
        # The pre-95cd186 Leela and Stockfish serializers used 10000+mate().
        # A historical losing mate in three collides with a modern WINNING
        # mate in three. Zero-distance encoding also lost the winning side.
        legacy_loss = 10000 + chess.engine.Mate(-3).mate()
        modern_win = chess.engine.Mate(3).score(mate_score=10000)
        self.assertEqual(legacy_loss, modern_win)
        self.assertLess(chess.engine.Mate(-3).score(mate_score=10000), 0)
        for encoded in (legacy_loss, 10000, 10003):
            self.assertEqual(contract.score_kind(encoded, None), 'unknown')
            self.assertIsNone(mate_distance(encoded, None))
        # The absence of a timestamp is unrelated; known current encodings work.
        self.assertEqual(contract.score_kind(modern_win, 'stockfish'), 'mate')
        self.assertEqual(mate_distance(modern_win, 'stockfish'), 3)

    def test_fresh_result_validation_and_missing_vs_zero(self):
        for score in (0, -24, 9998, -9998, 24.0):
            contract.validate_result(result(score=score), source='stockfish')
        for score in (None, True, float('nan'), float('inf'), 1.5, '24'):
            with self.subTest(score=score), self.assertRaises(ValueError):
                contract.validate_result(result(score=score), source='stockfish')
        for values in ({'analysis_source': 'tablebase'}, {'tablebase_wdl': 2},
                       {'wdl_win': 500}, {'wdl_win': 500, 'wdl_draw': 500, 'wdl_loss': 10}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                contract.validate_result(result(**values), source='stockfish')

    def test_tablebase_has_its_own_typed_fields(self):
        row = result(score=1000, tablebase_wdl=2, tablebase_dtz=7,
                     wdl_win=1000, wdl_draw=0, wdl_loss=0)
        contract.validate_result(row, source='tablebase')
        for value in (None, 3, True, 'win'):
            with self.assertRaises(ValueError):
                contract.validate_result({**row, 'tablebase_wdl': value}, source='tablebase')

    def test_duplicate_batch_rejected_before_writes(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate FEN'):
            contract.validate_batch([result(), result()], source='stockfish')


class EvaluationWriterTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_results_and_continuations_never_reach_writer(self):
        session, interface = AsyncMock(), AsyncMock()
        invalid = [([result(score=None)], []), ([result(), result()], []),
            ([result()], [{'fen_fen': 'other', 'rank': 2, 'move': 'e2e4', 'score': 0}]),
            ([result()], [{'fen_fen': 'synthetic-position', 'rank': 2, 'move': 'e2e4', 'score': None}])]
        for rows, continuations in invalid:
            with self.assertRaises(ValueError):
                await stage_fen_results(session, rows, continuations, interface=interface)
        interface.update_fen_analysis_data.assert_not_awaited()
        session.execute.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
