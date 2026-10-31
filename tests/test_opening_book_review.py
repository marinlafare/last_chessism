import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import chess
from chessism_api.operations.opening_book import review
from chessism_api.operations.opening_book.catalog import position_key


class ReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_annotation_is_independent_of_quality(self):
        fen = chess.STARTING_FEN
        rows = [{'fen_before': fen, 'uci': 'e2e4', 'classification': 'blunder'},
                {'fen_before': fen, 'uci': 'd2d4', 'classification': None}]
        matched = {(position_key(chess.Board()), 'e2e4'): {
            'opening_labels': [{'eco': 'B00', 'name': 'King’s Pawn'}], 'source_revision': 'a'*40}}
        session = SimpleNamespace(scalar=AsyncMock(return_value=True))
        with patch.object(review, 'lookup', AsyncMock(return_value=matched)) as lookup:
            await review.annotate_moves(session, rows)
        lookup.assert_awaited_once()
        self.assertEqual(rows[0]['book']['openings'][0]['eco'], 'B00')
        self.assertEqual(rows[0]['classification'], 'blunder')
        self.assertIsNone(rows[1]['book'])

    async def test_missing_catalogue_does_not_invent_book_flags(self):
        session = SimpleNamespace(scalar=AsyncMock(return_value=False))
        rows = [{'fen_before': chess.STARTING_FEN, 'uci': 'e2e4'}]
        with patch.object(review, 'lookup', AsyncMock()) as lookup:
            await review.annotate_moves(session, rows)
        lookup.assert_not_awaited()
        self.assertIsNone(rows[0]['book'])
