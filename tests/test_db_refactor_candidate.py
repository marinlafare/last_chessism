import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from scripts.db_refactor_candidate import guard,standard
from scripts.db_refactor_comparison import add_verified_medium,candidate_case,compare_results
from scripts.db_refactor_workload import digest
from scripts.run_db_refactor_candidate import verified_workspace
from scripts.watch_db_refactor import render


class CandidateTests(unittest.IsolatedAsyncioTestCase):
    async def test_medium_requires_real_profile_and_thousands_of_games(self):
        conn=AsyncMock()
        conn.fetchrow.side_effect=[{'player_name':'middle','games':8000,'joined':123},
                                  {'link':55,'mode':'blitz','year':2025}]
        original={'players':[{'player_name':'large','cohort':'large'}]}
        result=await add_verified_medium(conn,original)
        self.assertEqual(len(original['players']),1)
        self.assertEqual(result['players'][-1]['games'],8000)
        query=conn.fetchrow.call_args_list[0].args[0]
        self.assertIn('p.joined<>0',query)
        self.assertIn('count(*)>=1000',query)

    async def test_no_suitable_player_fails_instead_of_using_nineteen_games(self):
        conn=AsyncMock()
        conn.fetchrow.return_value=None
        with self.assertRaises(RuntimeError):
            await add_verified_medium(conn,{'players':[]})

    async def test_ddl_guard_rejects_production_even_on_a_local_socket(self):
        conn=AsyncMock()
        pgdata=Path('/restore-test/db-refactor-baseline-fixture/pgdata')
        conn.fetchrow.return_value={'directory':'/var/lib/postgresql/data','port':'55432','address':None}
        with self.assertRaises(RuntimeError):
            await guard(conn,pgdata)
        conn.fetchrow.return_value={'directory':str(pgdata),'port':'55432','address':None}
        await guard(conn,pgdata)
        with self.assertRaises(RuntimeError):
            await guard(conn,Path('/var/lib/postgresql/data'))

    def test_numeric_adapter_preserves_cases_and_args(self):
        old={'name':'large/fen_selection','sql':'SELECT a.fen_fen COLLATE "C" AS fen FROM game_fen_association a JOIN fen f ON f.fen = a.fen_fen',
             'args':['abc'],'source':'legacy'}
        new=candidate_case(old)
        self.assertIn('f.fen_id=a.fen_id',new['sql'])
        self.assertIn('f.fen COLLATE',new['sql'])
        self.assertIn('a.fen_fen',old['sql'])
        self.assertEqual(new['args'],old['args'])

    def test_unknown_numeric_mapping_fails_closed(self):
        with self.assertRaises(ValueError):
            candidate_case({'name':'unknown','sql':'SELECT z.fen_fen FROM unknown z','args':[]})

    def test_comparison_never_hides_result_mismatches_or_timeouts(self):
        old=[{'name':'a','status':'succeeded','warm_median_ms':10,'result_sha256':'abc'}]
        new=[{'name':'a','status':'succeeded','warm_median_ms':30,'result_sha256':'def'}]
        row=compare_results(old,new)[0]
        self.assertFalse(row['results_match'])
        self.assertTrue(row['investigate_slowdown'])
        row=compare_results(old,[{'name':'a','status':'timeout'}])[0]
        self.assertFalse(row['results_match'])
        with self.assertRaises(ValueError):
            compare_results(old,[])

    def test_baseline_manifest_and_paths_are_bound(self):
        root=Path('/restore-test')
        manifest={'backup':{'backup_id':'test'},'cases':[],'case_sha256':digest([])}
        status={'state':'succeeded','mode':'isolated_baseline','backup_id':'test',
                'workspace':'/restore-test/db-refactor-baseline-fixture'}
        self.assertEqual(verified_workspace(status,manifest,root),Path(status['workspace']))
        for field,value in [('state','failed'),('workspace','/var/lib/postgresql'),('backup_id','wrong')]:
            with self.assertRaises(ValueError):
                verified_workspace(dict(status,**{field:value}),manifest,root)

    def test_standard_classification_preserves_variant_and_custom_start_exceptions(self):
        predicate=standard()
        self.assertIn("g.rules='chess'",predicate)
        self.assertIn('initial_setup',predicate)
        with self.assertRaises(ValueError):
            standard('untrusted')

    def test_watcher_does_not_confuse_rehearsal_with_live_migration(self):
        output=render({'state':'needs_review','mode':'isolated_candidate','phase':'candidate_rehearsal_complete',
                       'result_mismatches':0,'slowdown_flags':3})
        self.assertIn('CANDIDATE REHEARSAL',output)
        self.assertIn('Slowdown flags: 3',output)
        self.assertIn('NOT ready for production',output)


if __name__=='__main__':
    unittest.main()
