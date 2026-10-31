"""Offline checks for bounded coverage reads and explicit empty selections."""
import unittest
from unittest.mock import AsyncMock, Mock, patch

from chessism_api.database import ask_db
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations import player_game_scores
from chessism_api.operations.relational_coverage import coverage_source_sql, refresh_coverage


LAYOUT = RelationalStorage('core', 'archive', 'reads', 'relational_v3')


class CoverageOptimizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_selection_never_opens_a_database_session(self):
        with patch.object(ask_db, 'AsyncDBSession') as sessions:
            self.assertEqual(await ask_db.refresh_game_analysis_summary([]), {'games': 0, 'scope': 'links'})
            self.assertEqual(await ask_db.refresh_game_analysis_summary([None]), {'games': 0, 'scope': 'links'})
            sessions.assert_not_called()
        with patch.object(player_game_scores, 'AsyncDBSession') as sessions:
            self.assertEqual(await player_game_scores.refresh_game_player_engine_summaries(()),
                             {'games': 0, 'game_players': 0})
            sessions.assert_not_called()
        session = AsyncMock(info={SESSION_KEY: LAYOUT})
        self.assertEqual(await refresh_coverage(session, []), [])
        self.assertEqual(await refresh_coverage(session, fens=[]), [])
        session.execute.assert_not_called()

    async def test_batch_statistics_precede_locks_and_bounded_aggregate(self):
        session = AsyncMock(info={SESSION_KEY: LAYOUT})
        session.execute.return_value = Mock(mappings=Mock(return_value=[]))
        session.scalar.return_value = 2
        self.assertEqual(await refresh_coverage(session, [12, 13, 12]), [])
        sql = [str(call.args[0]) for call in session.execute.call_args_list]
        analyze = next(i for i, statement in enumerate(sql) if statement.startswith('ANALYZE'))
        lock = next(i for i, statement in enumerate(sql) if 'FOR UPDATE OF g' in statement)
        write = next(i for i, statement in enumerate(sql) if 'ON CONFLICT(link) DO UPDATE' in statement)
        self.assertLess(analyze, lock)
        self.assertLess(lock, write)
        self.assertIn('ORDER BY g.link FOR UPDATE OF g', sql[lock])
        self.assertIn('CROSS JOIN LATERAL', sql[write])
        self.assertIn('WHERE a.game_link=t.link HAVING count(*)>0', sql[write])

    async def test_fen_scope_stays_scoped_even_without_game_ids(self):
        session = AsyncMock(info={SESSION_KEY: LAYOUT})
        session.execute.return_value = Mock(mappings=Mock(return_value=[]))
        session.scalar.return_value = 1
        await refresh_coverage(session, fens=['position'])
        sql = [str(call.args[0]) for call in session.execute.call_args_list]
        self.assertTrue(any('SELECT DISTINCT a.game_link' in statement for statement in sql))
        self.assertIn('CROSS JOIN LATERAL', sql[-1])

    async def test_large_cloud_scopes_keep_a_grouped_plan_and_missing_fens_do_no_writes(self):
        session = AsyncMock(info={SESSION_KEY: LAYOUT})
        session.execute.return_value = Mock(mappings=Mock(return_value=[]))
        session.scalar.return_value = 10001
        await refresh_coverage(session, fens=['a-common-position'])
        self.assertNotIn('LATERAL', str(session.execute.call_args.args[0]))
        session.reset_mock()
        session.scalar.return_value = 0
        self.assertEqual(await refresh_coverage(session, fens=['missing-position']), [])
        self.assertFalse(any('FOR UPDATE' in str(call.args[0])
                             or 'ON CONFLICT(link)' in str(call.args[0])
                             for call in session.execute.call_args_list))

    def test_full_repair_retains_grouped_sql_and_both_paths_share_score_buckets(self):
        scoped = coverage_source_sql(LAYOUT, scoped=True)
        full = coverage_source_sql(LAYOUT, scoped=False)
        self.assertNotIn('LATERAL', full)
        self.assertIn('GROUP BY a.game_link', full)
        for expression in ('count(f.score)', 'abs(f.score)<50', 'abs(f.score)>=50',
                           'abs(f.score)>=150', 'abs(f.score)>=300', 'abs(f.score)>=9000'):
            self.assertIn(expression, scoped)
            self.assertIn(expression, full)
