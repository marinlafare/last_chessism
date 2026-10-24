"""The always-visible rating card must never read the large chart column."""
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from chessism_api.database import ask_db
from chessism_api.routers import fens


class ScoredRatingSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def read_summary(self, include_ratings):
        row = {"rating_basis": "avg_elo", "groups_payload": '[{"key":"medium","games":7}]',
               "ratings_payload": '[{"rating":1500,"x_index":1}]'}
        result = MagicMock()
        result.mappings.return_value.first.return_value = row
        session = AsyncMock()
        session.execute.return_value = result
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=session)
        context.__aexit__ = AsyncMock(return_value=False)
        with patch.object(ask_db, "AsyncDBSession", return_value=context):
            payload = await ask_db._read_scored_rating_summary(include_ratings=include_ratings)
        return payload, str(session.execute.call_args.args[0])

    async def test_summary_only_does_not_select_chart_column(self):
        payload, query = await self.read_summary(False)
        self.assertNotIn("ratings_payload", query)
        self.assertNotIn("ratings", payload)
        self.assertEqual(payload["groups"], [{"key": "medium", "games": 7}])

    async def test_chart_read_remains_compatible(self):
        payload, query = await self.read_summary(True)
        self.assertIn("ratings_payload", query)
        self.assertEqual(payload["ratings"], [{"rating": 1500, "x_index": 1}])

    async def test_cached_groups_do_not_rebuild_summary(self):
        expected = {"rating_basis": "avg_elo", "groups": []}
        with patch.object(ask_db, "_read_scored_rating_summary", AsyncMock(return_value=expected)) as read, \
                patch.object(ask_db, "refresh_scored_rating_summary", AsyncMock()) as refresh:
            self.assertEqual(await ask_db.get_scored_advantage_by_rating(include_ratings=False), expected)
        read.assert_awaited_once_with(include_ratings=False)
        refresh.assert_not_awaited()

    async def test_missing_summary_fallback_omits_chart_from_overview(self):
        rebuilt = {"rating_basis": "avg_elo", "groups": [], "ratings": [{"rating": 1500}]}
        with patch.object(ask_db, "_read_scored_rating_summary", AsyncMock(return_value=None)), \
                patch.object(ask_db, "refresh_scored_rating_summary", AsyncMock(return_value=rebuilt)):
            result = await ask_db.get_scored_advantage_by_rating(include_ratings=False)
        self.assertEqual(result, {"rating_basis": "avg_elo", "groups": []})

    async def test_routes_select_the_correct_payload(self):
        with patch.object(fens, "get_scored_advantage_by_rating", AsyncMock(return_value={"groups": []})) as read:
            response = await fens.api_get_scored_rating_groups()
            read.assert_awaited_once_with(include_ratings=False)
            self.assertEqual(json.loads(response.body), {"groups": []})
            read.reset_mock()
            await fens.api_get_scored_advantage_by_rating()
            read.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
