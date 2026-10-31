import unittest

from scripts.compact_db_indexes import validate_index,uses_impact_index
from scripts.run_compact_index_retune import checked_source,unaffected_contract


class CompactIndexTests(unittest.TestCase):
    def row(self, expression='(n_games * abs(score))', value_type='bigint'):
        return {'table_name':'fen','table_schema':'public','unique':False,'primary':False,
            'valid':True,'ready':True,'keys':2,'descending':True,'expression':expression,
            'predicate':'(score IS NOT NULL)','second_key':'n_games','value_type':value_type}

    def test_reviewed_old_and_new_expressions(self):
        for expression in ('((n_games)::double precision * abs(score))',
                           '((n_games)::double precision * (abs(score))::double precision)'):
            validate_index(self.row(expression,'double precision'))
            with self.assertRaises(RuntimeError):
                validate_index(self.row(expression,'double precision'),integer=True)
        validate_index(self.row(),integer=True)

    def test_unexpected_target_or_definition_is_rejected(self):
        for key,value in [('table_name','player'),('expression','n_games + abs(score)'),
                          ('predicate',None),('second_key','fen'),('keys',3),('valid',False),
                          ('ready',False),('unique',True),('descending',False)]:
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                validate_index({**self.row(),key:value})
        with self.assertRaises(RuntimeError):
            validate_index(None)

    def test_plan_must_actually_use_the_reviewed_index(self):
        self.assertTrue(uses_impact_index({'Plans':[{'Index Name':'ix_fen_scored_impact_desc'}]}))
        self.assertFalse(uses_impact_index({'Node Type':'Sort','Plans':[{'Node Type':'Seq Scan'}]}))

    def test_only_expected_single_issue_evidence_can_be_reused(self):
        status={'state':'needs_review','phase':'compact_acceptance_complete','mode':'isolated_compact_acceptance',
            'live_database_changed':False,'private_postgres_stopped':True,'workspace':'/restore-test/compact-db-unit'}
        report={'performance_review':['scored/impact'],'small_database':'compact_acceptance_0123456789',
            'checks':[{'passed':True} for _ in range(17)],
            'queries':[{'name':str(i),'matches':True,'slowdown':False} for i in range(18)]+[{'name':'scored/impact','error':'timeout'}]}
        self.assertEqual(str(checked_source(status,report)),status['workspace'])
        for key,value in [('private_postgres_stopped',False),('live_database_changed',True),
                          ('workspace','/var/lib/postgresql/data'),('state','running')]:
            with self.subTest(key=key),self.assertRaises(ValueError):
                checked_source({**status,key:value},report)
        with self.assertRaises(ValueError):
            checked_source(status,{**report,'checks':[{'passed':False}]*17})

    def test_only_exact_index_is_removed_from_contract_comparison(self):
        target={'schemaname':'public','tablename':'fen','indexname':'ix_fen_scored_impact_desc'}
        other={'schemaname':'public','tablename':'fen','indexname':'ix_fen_backup_changed'}
        contract={'indexes':[target,other],'columns':[{'column_name':'score','data_type':'integer'}]}
        filtered=unaffected_contract(contract)
        self.assertEqual(filtered['indexes'],[other])
        self.assertEqual(filtered['columns'],contract['columns'])
        self.assertEqual(len(contract['indexes']),2)


if __name__=='__main__':
    unittest.main()
