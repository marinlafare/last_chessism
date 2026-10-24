"""Time-bin result-filter contracts; no database or network access."""

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')

from chessism_api.operations import player_game_explorer as explorer
from chessism_api.operations.player_hero_analytics import AnalyticsScope
from chessism_api.operations.player_timezone import ResolvedPlayerTimezone


class TimeResultFilterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.session = AsyncMock()
        self.factory = MagicMock()
        self.factory.return_value.__aenter__.return_value = self.session
        scope = AnalyticsScope('fixture', 'all', None, None,
                               ResolvedPlayerTimezone(('UTC',), 'request_override'))
        self.patch_session = patch.object(explorer, 'AsyncDBSession', self.factory)
        self.patch_scope = patch.object(explorer, '_analytics_scope', AsyncMock(return_value=scope))
        self.patch_session.start()
        self.patch_scope.start()
        self.addCleanup(self.patch_session.stop)
        self.addCleanup(self.patch_scope.stop)

    def prepare(self, rows=()):
        self.session.reset_mock()
        summary = MagicMock()
        summary.mappings.return_value.one.return_value = {
            'games': 2, 'analyzed_games': 0, 'accuracy': None,
            'wins': 0, 'draws': 0, 'losses': 2, 'blunders': None,
        }
        games = MagicMock()
        games.mappings.return_value.all.return_value = list(rows)
        self.session.execute.side_effect = [None, None, summary, games]

    async def explore(self, **changes):
        return await explorer.explore_player_games('fixture', **{
            'scope_kind': 'weekday_hour', 'weekday': 1, 'hour': 9,
            'modes': ['blitz', 'rapid'], 'analyzed_only': False, **changes,
        })

    async def test_every_result_applies_to_both_summary_and_pagination(self):
        for scope in ['hour', 'weekday_hour']:
            for result, numeric in [('win', 1), ('draw', .5), ('loss', 0)]:
                with self.subTest(scope=scope, result=result):
                    self.prepare()
                    payload = await self.explore(scope_kind=scope, result=result)
                    queries = self.session.execute.call_args_list[2:]
                    self.assertEqual(len(queries), 2)
                    for call in queries:
                        sql, params = str(call.args[0]), call.args[1]
                        where = sql.split('WHERE\n')[-1]
                        self.assertIn('AND gp.result = :selected_result', where)
                        self.assertIn('EXTRACT(HOUR FROM', where)
                        self.assertIn('gp.mode = ANY(CAST(:modes AS text[]))', where)
                        self.assertEqual(params['selected_result'], numeric)
                        self.assertEqual(params['modes'], ['blitz', 'rapid'])
                        self.assertFalse(params['analyzed_only'])
                        self.assertEqual(params['hour'], 9)
                        if scope == 'weekday_hour':
                            self.assertIn('EXTRACT(ISODOW FROM', where)
                            self.assertEqual(params['weekday'], 1)
                    self.assertIn('ORDER BY gp.played_at DESC, gp.link DESC', str(queries[1].args[0]))
                    self.assertEqual(payload['selection']['result'], result)
                    self.assertEqual(payload['selection']['modes'], ['blitz', 'rapid'])

    async def test_clearing_result_returns_unfiltered_time_bin(self):
        self.prepare()
        payload = await self.explore(result=None)
        self.assertIsNone(payload['selection']['result'])
        for call in self.session.execute.call_args_list[2:]:
            self.assertNotIn(':selected_result', str(call.args[0]))
            self.assertNotIn('selected_result', call.args[1])

    async def test_pagination_keeps_result_and_descending_cursor(self):
        timestamp = datetime(2026, 10, 19, 9, tzinfo=timezone.utc)
        rows = [{'game_link': 20, 'played_at': timestamp}, {'game_link': 19, 'played_at': timestamp}]
        self.prepare(rows)
        with patch.object(explorer, '_compact_game', side_effect=lambda row, _: {'game_id': row['game_link']}):
            first = await self.explore(result='loss', limit=1)
        self.assertEqual(first['games'], [{'game_id': 20}])
        self.assertTrue(first['pagination']['has_more'])
        cursor = first['pagination']['next_cursor']
        self.assertEqual(explorer._decode_cursor(cursor), (timestamp, 20))
        self.prepare()
        await self.explore(result='loss', limit=1, cursor=cursor)
        sql, params = self.session.execute.call_args.args
        self.assertIn('(gp.played_at, gp.link) <', str(sql))
        self.assertEqual(params['selected_result'], 0)
        self.assertEqual(params['cursor_time'], timestamp)
        self.assertEqual(params['cursor_game_id'], 20)
        self.assertEqual(params['limit'], 2)

    async def test_invalid_results_and_other_scopes_still_rejected_before_db(self):
        for changes in [dict(result='other'), dict(result='win', scope_kind='weekday'),
                        dict(result='win', scope_kind='date', target_date='2026-10-19'),
                        dict(result='win', opponent='other'),
                        dict(result='win', scope_kind='blunders', analyzed_only=True)]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await self.explore(**changes)
        self.factory.assert_not_called()


if __name__ == '__main__':
    unittest.main()
