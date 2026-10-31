"""Exact move-selection parity; no database or external requests."""
import itertools
import unittest

import chess

from chessism_api.operations.tablebase import _best_tablebase_move


def reference_move(tablebase, board):
    """Frozen pre-optimization selector; DTZ requested for every known child."""
    best_move = best_rank = None
    for move in sorted(board.legal_moves, key=lambda item: item.uci()):
        child = board.copy(stack=False)
        child.push(move)
        child_wdl = tablebase.get_wdl(child)
        if child_wdl is None:
            continue
        outcome = -int(child_wdl)
        distance = abs(int(tablebase.get_dtz(child) or 0))
        rank = (outcome, -distance if outcome > 0 else distance if outcome < 0 else 0)
        if best_rank is None or rank > best_rank:
            best_move, best_rank = move.uci(), rank
    return best_move


class FakeTablebase:
    def __init__(self, rows):
        self.rows = rows
        self.dtz_calls = []

    def get_wdl(self, board):
        return self.rows.get(board.peek().uci(), (None, None))[0]

    def get_dtz(self, board):
        move = board.peek().uci()
        self.dtz_calls.append(move)
        return self.rows[move][1]


class TablebaseSelectionTests(unittest.TestCase):
    def setUp(self):
        self.board = chess.Board('8/8/3k4/8/8/8/2R5/3K4 w - - 0 1')
        self.assertTrue(self.board.is_valid())
        self.moves = sorted(move.uci() for move in self.board.legal_moves)[:3]

    def test_all_outcome_groups_and_missing_distances_match_reference(self):
        for wdls in itertools.product((None, -2, -1, 0, 1, 2), repeat=3):
            for distances in ((None, 3, -7), (6, -6, 6), (0, None, -1)):
                tablebase = FakeTablebase(dict(zip(self.moves, zip(wdls, distances))))
                with self.subTest(wdls=wdls, distances=distances):
                    expected = reference_move(tablebase, self.board)
                    self.assertEqual(_best_tablebase_move(tablebase, self.board), expected)

    def test_dtz_is_not_read_for_worse_outcomes(self):
        tablebase = FakeTablebase(dict(zip(self.moves, ((-2, 4), (1, -8), (0, 0)))))
        self.assertEqual(_best_tablebase_move(tablebase, self.board), self.moves[0])
        self.assertEqual(tablebase.dtz_calls, [self.moves[0]])

    def test_draws_skip_all_dtz_and_ties_keep_uci_order(self):
        tablebase = FakeTablebase(dict.fromkeys(self.moves, (0, 0)))
        self.assertEqual(_best_tablebase_move(tablebase, self.board), self.moves[0])
        self.assertEqual(tablebase.dtz_calls, [])

    def test_no_moves_and_missing_tables_return_none(self):
        self.assertIsNone(_best_tablebase_move(FakeTablebase({}), self.board))
        mate = chess.Board('7k/6Q1/6K1/8/8/8/8/8 b - - 0 1')
        self.assertIsNone(_best_tablebase_move(FakeTablebase({}), mate))

    def test_parent_board_and_history_survive_success_and_probe_failure(self):
        self.board.push(next(iter(self.board.legal_moves)))
        before, history = self.board.fen(), list(self.board.move_stack)
        moves = [move.uci() for move in self.board.legal_moves]
        for failure in (None, 'get_wdl', 'get_dtz'):
            tablebase = FakeTablebase(dict.fromkeys(moves, (-2, 3)))
            if failure:
                def fail(_board):
                    raise RuntimeError('Synthetic probe failure')
                setattr(tablebase, failure, fail)
                with self.assertRaises(RuntimeError):
                    _best_tablebase_move(tablebase, self.board)
            else:
                _best_tablebase_move(tablebase, self.board)
            self.assertEqual(self.board.fen(), before)
            self.assertEqual(self.board.move_stack, history)


if __name__ == '__main__':
    unittest.main()
