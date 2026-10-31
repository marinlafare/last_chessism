import unittest
import chess
from chessism_api.operations.opening_book.catalog import build_catalog, load_catalog, position_key


class OpeningBookTests(unittest.TestCase):
    def test_pinned_catalogue_is_valid_and_deduplicated(self):
        catalog = load_catalog()
        self.assertGreater(catalog.lines, 1000)
        self.assertGreater(len(catalog.records), catalog.lines)
        self.assertEqual(len(catalog.records), len({row[:2] for row in catalog.records}))
        start = position_key(chess.Board())
        self.assertIn((start, 'e2e4'), {row[:2] for row in catalog.records})

    def test_transpositions_and_clock_independence(self):
        a, b = chess.Board(), chess.Board()
        for uci in ['g1f3', 'd7d5', 'g2g3', 'g8f6']:
            a.push_uci(uci)
        for uci in ['g2g3', 'd7d5', 'g1f3', 'g8f6']:
            b.push_uci(uci)
        self.assertEqual(position_key(a), position_key(b))
        b.halfmove_clock = 42
        b.fullmove_number = 55
        self.assertEqual(position_key(a), position_key(b))
        b.turn = not b.turn
        self.assertNotEqual(position_key(a), position_key(b))

    def test_castling_and_legal_en_passant_are_preserved(self):
        a = chess.Board('4k3/8/8/3pP3/8/8/8/4K3 w - d6 0 1')
        b = a.copy()
        b.ep_square = None
        self.assertNotEqual(position_key(a), position_key(b))
        a = chess.Board()
        b = a.copy()
        b.castling_rights = 0
        self.assertNotEqual(position_key(a), position_key(b))

    def test_shared_prefix_does_not_inherit_a_later_opening_name(self):
        catalog = build_catalog([{'eco': 'B20', 'name': 'Sicilian Defense', 'pgn': '1. e4 c5'}], 'a'*40)
        first = next(r for r in catalog.records if r[1] == 'e2e4')
        last = next(r for r in catalog.records if r[1] == 'c7c5')
        self.assertEqual(first[3], '[]')
        self.assertIn('Sicilian Defense', last[3])

    def test_malformed_inputs_are_rejected(self):
        with self.assertRaises(ValueError):
            build_catalog([], 'a'*40)
        with self.assertRaises(ValueError):
            build_catalog([], 'master')
        with self.assertRaises(ValueError):
            build_catalog([{'eco': 'ZZZ', 'name': 'Bad', 'pgn': '1. e4'}], 'a'*40)


if __name__ == '__main__':
    unittest.main()
