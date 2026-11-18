"""The overview never selects the large chart column or rebuilds the projection."""
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.exc import NoResultFound

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')

from chessism_api.operations import scored_rating_summary as summaries
from chessism_api.routers import fens


class ScoredRatingSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def read_summary(self, include_ratings):
        row = {'rating_basis': 'avg_elo', 'groups_payload': [{'key': 'medium', 'games': 7}],
               'ratings_payload': [{'rating': 1500, 'x_index': 1}]}
        result = MagicMock()
        result.mappings.return_value.one.return_value = row
        session = AsyncMock()
        session.in_transaction = MagicMock(return_value=True)
        session.execute.return_value = result
        payload = await summaries.get_scored_advantage_by_rating(
            include_ratings=include_ratings, session_factory=lambda: session)
        session.commit.assert_awaited_once()
        session.close.assert_awaited_once()
        return payload, str(session.execute.call_args.args[0])

    async def test_summary_only_does_not_select_chart_column(self):
        payload, query = await self.read_summary(False)
        self.assertNotIn('ratings_payload', query)
        self.assertNotIn('ratings', payload)
        self.assertEqual(payload['groups'], [{'key': 'medium', 'games': 7}])

    async def test_chart_read_remains_compatible(self):
        payload, query = await self.read_summary(True)
        self.assertIn('ratings_payload', query)
        self.assertEqual(payload['ratings'], [{'rating': 1500, 'x_index': 1}])

    async def test_cached_groups_do_not_rebuild_summary(self):
        with patch.object(summaries, 'refresh_scored_rating_summary', AsyncMock()) as refresh:
            await self.read_summary(False)
        refresh.assert_not_awaited()

    async def test_missing_initialized_summary_fails_without_request_time_rebuild(self):
        session = AsyncMock()
        session.in_transaction = MagicMock(return_value=True)
        result = MagicMock()
        result.mappings.return_value.one.side_effect = NoResultFound('Required summary is missing')
        session.execute.return_value = result
        with patch.object(summaries, 'refresh_scored_rating_summary', AsyncMock()) as refresh:
            with self.assertRaises(NoResultFound):
                await summaries.get_scored_advantage_by_rating(
                    include_ratings=False, session_factory=lambda: session)
        refresh.assert_not_awaited()
        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()

    async def test_routes_select_the_correct_payload(self):
        with patch.object(fens, 'get_scored_advantage_by_rating', AsyncMock(return_value={'groups': []})) as read:
            response = await fens.api_get_scored_rating_groups()
            read.assert_awaited_once_with(include_ratings=False)
            self.assertEqual(json.loads(response.body), {'groups': []})
            read.reset_mock()
            await fens.api_get_scored_advantage_by_rating()
            read.assert_awaited_once_with()


if __name__ == '__main__':
    unittest.main()
