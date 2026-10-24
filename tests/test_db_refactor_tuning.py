import unittest
from unittest.mock import AsyncMock

from scripts.db_refactor_tuning import game_page,tuned_candidate_case,verify_original_mismatches
from scripts.db_refactor_workload import row_digest


class TuningTests(unittest.IsolatedAsyncioTestCase):
    def case(self,name):
        return {'name':'large/'+name,'sql':'SELECT 1','args':['player'],'source':'original'}

    def test_recent_contract_casts_time_without_changing_digest_rules(self):
        original=self.case('recent_games')
        new=tuned_candidate_case(original)
        self.assertEqual(original['sql'],'SELECT 1')
        self.assertIn('time_elapsed::double precision',new['sql'])
        self.assertNotEqual(row_digest([{'time_elapsed':3}]),row_digest([{'time_elapsed':3.0}]))

    def test_frequency_keeps_complete_boundary_ties_before_fetching_fens(self):
        sql=tuned_candidate_case(self.case('position_frequencies'))['sql']
        self.assertIn('FETCH FIRST 100 ROWS WITH TIES',sql)
        self.assertIn('MATERIALIZED',sql)
        self.assertIn('f.fen COLLATE "C" LIMIT 100',sql)
        self.assertLess(sql.index('WITH TIES'),sql.index('JOIN public.fen'))

    def test_page_filters_both_sides_before_limiting_and_keeps_self_play(self):
        sql=game_page(100,mode=True,calendar=True,salience=True,extracted=True)
        self.assertIn('UNION ALL',sql)
        self.assertEqual(sql.count('g.year=$2'),2)
        self.assertEqual(sql.count('g.fens_done'),2)
        self.assertEqual(sql.count('s.game_player_id=gp.id'),2)
        self.assertEqual(sql.count('LIMIT 100'),3)

    def test_all_slow_read_shapes_use_native_tables(self):
        for name in ('recent_games','mode_games','calendar_filter','salience','position_frequencies','fen_selection'):
            sql=tuned_candidate_case(self.case(name))['sql']
            self.assertIn('refactor_candidate_v1.',sql)
            self.assertNotIn('refactor_read_v1.',sql)
        self.assertIn('s.game_player_id=g.game_player_id',tuned_candidate_case(self.case('salience'))['sql'])

    async def test_diagnosis_rejects_changed_values_and_checks_original_hashes(self):
        name='large/recent_games'
        case=self.case('recent_games')
        left=[{'link':1,'time_elapsed':3.0}]
        right=[{'link':1,'time_elapsed':3}]
        def prior():
            return {'manifest':{'reference_cases':[case],'candidate_cases':[case]},
                    'reference':{'results':[{'name':name,'result_sha256':row_digest(left)}]},
                    'candidate':{'results':[{'name':name,'result_sha256':row_digest(right)}]},
                    'comparison':{'comparisons':[{'name':name,'results_match':False}]}}
        class Transaction:
            async def __aenter__(self): return self
            async def __aexit__(self,*args): return False
        conn=AsyncMock()
        from unittest.mock import Mock
        conn.transaction=Mock(side_effect=lambda **kwargs:Transaction())
        conn.fetch.side_effect=[left,right]
        self.assertEqual((await verify_original_mismatches(conn,prior()))[0]['type_changes'],1)
        right=[{'link':1,'time_elapsed':4}]
        conn.fetch.side_effect=[left,right]
        with self.assertRaisesRegex(RuntimeError,'does not explain'):
            await verify_original_mismatches(conn,prior())
        unchanged_prior=prior()
        right=[{'link':1,'time_elapsed':5}]
        conn.fetch.side_effect=[left,right]
        with self.assertRaisesRegex(RuntimeError,'saved first rehearsal'):
            await verify_original_mismatches(conn,unchanged_prior)


if __name__=='__main__':
    unittest.main()
