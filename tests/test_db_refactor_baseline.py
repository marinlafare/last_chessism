import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from scripts.db_refactor_workload import (
    bind_analytics, make_cases, measure_case, row_digest, select_player_cohorts,
)
from scripts.run_db_refactor_baseline import assert_private_database
from scripts.watch_db_refactor import render


class WorkloadTests(unittest.TestCase):
    def test_fixed_distinct_cohorts(self):
        rows = [{'player_name':str(i), 'games':count} for i,count in
                enumerate((10000,1000,100,10,1))]
        cohorts = select_player_cohorts(rows)
        self.assertEqual([p['games'] for p in cohorts], [10000,100,10])
        self.assertEqual([p['cohort'] for p in cohorts], ['large','medium','small'])

    def test_too_few_players_fails_closed(self):
        with self.assertRaises(RuntimeError):
            select_player_cohorts([{'player_name':'one','games':1}])

    def test_bind_parameters_preserves_postgres_casts(self):
        sql,args = bind_analytics('SELECT :player, value::date, :mode, :date_from, :date_to', 'abc')
        self.assertEqual(sql,'SELECT $1, value::date, $2, $3, $4')
        self.assertEqual(args,['abc','all',None,None])

    def test_suite_uses_deployed_literals_without_importing_code(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root)/'chessism_api/operations/player_analytics.py'
            source.parent.mkdir(parents=True)
            source.write_text("raise RuntimeError('must not execute')\n"
                              "ENGINE_INSIGHTS_SQL = 'SELECT :player'\n"
                              "PLAYING_PATTERNS_SQL = 'SELECT :mode'\n")
            inputs = {'players':[{'player_name':label,'cohort':label,'mode':'blitz','year':2026,'link':1}
                                 for label in ('large','medium','small')],
                      'opening':{'mode':'blitz','n_moves':3,'opening':'e4 e5 Nf3 Nc6 Bb5 a6'}}
            cases = make_cases(inputs,root)
            self.assertEqual(len(cases),38)
            self.assertEqual(len({c['name'] for c in cases}),len(cases))
            self.assertTrue(all(c['sql'].startswith(('SELECT','WITH')) for c in cases))
            analytics = next(c for c in cases if c['name']=='large/engine_insights_sql')
            self.assertEqual(analytics['sql'],'SELECT $1')
            self.assertIn('deployed',analytics['source'])

    def test_fingerprints_normalize_json_object_order_but_not_row_order(self):
        self.assertEqual(row_digest([{'payload':'{"x":1,"y":2}'}]),
                         row_digest([{'payload':'{"y":2,"x":1}'}]))
        self.assertNotEqual(row_digest([{'n':1},{'n':2}]),row_digest([{'n':2},{'n':1}]))

    def test_watcher_does_not_claim_live_cutover(self):
        output = render({'state':'succeeded','phase':'baseline_complete','queries_completed':38,
                         'queries_total':38,'elapsed_seconds':65})
        self.assertIn('00:01:05',output)
        self.assertIn('38 / 38',output)
        self.assertIn('Candidate migration is still pending',output)
        self.assertIn('Live database unchanged',output)


class MeasurementTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_readonly_transactions_and_separates_first_observation(self):
        conn = MagicMock()
        conn.transaction.return_value.__aenter__ = AsyncMock()
        conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
        conn.fetch = AsyncMock(return_value=[{'score':12}])
        conn.fetchval = AsyncMock(return_value='[{"Execution Time":1}]')
        notify = AsyncMock()
        result = await measure_case(conn, {'name':'test','sql':'SELECT 12 AS score','args':[]}, 3, notify)
        self.assertEqual(result['status'],'succeeded')
        self.assertEqual(len(result['samples']),4)
        self.assertEqual(notify.await_count,4)
        self.assertEqual(conn.transaction.call_count,5)
        self.assertTrue(all(c.kwargs=={'readonly':True} for c in conn.transaction.call_args_list))
        self.assertEqual(result['result_sha256'],row_digest([{'score':12}]))
        self.assertIn('ANALYZE, BUFFERS',conn.fetchval.call_args.args[0])

    async def test_non_repeatable_results_fail(self):
        conn = MagicMock()
        conn.transaction.return_value.__aenter__ = AsyncMock()
        conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
        conn.fetch = AsyncMock(side_effect=[[{'n':1}],[{'n':2}]])
        with self.assertRaisesRegex(RuntimeError,'unstable'):
            await measure_case(conn,{'name':'x','sql':'SELECT 1','args':[]},1,AsyncMock())

    async def test_connection_guard_rejects_live_or_writable_database(self):
        conn = AsyncMock()
        expected = Path('/restore-test/db-refactor-baseline-example/pgdata')
        good = {'data_directory':str(expected),'port':'55432','address':None,'read_only':'on'}
        conn.fetchrow.return_value = good
        await assert_private_database(conn,expected)
        for field,value in (('data_directory','/var/lib/postgresql/data'),('port','5432'),
                            ('address','127.0.0.1'),('read_only','off')):
            conn.fetchrow.return_value = dict(good, **{field:value})
            with self.assertRaisesRegex(RuntimeError,'private read-only'):
                await assert_private_database(conn,expected)


if __name__ == '__main__':
    unittest.main()
