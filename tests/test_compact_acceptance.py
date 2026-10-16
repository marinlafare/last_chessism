import unittest

from scripts.compact_db_queries import cases,canonical,decode_stream,digest,literal
from scripts.run_compact_db_acceptance import checked_workspace
from scripts.watch_compact_db import render


class CompactAcceptanceTests(unittest.TestCase):
    def test_literals_do_not_interpolate_names_as_sql(self):
        self.assertEqual(literal("a'; SELECT 1; --"),"'a''; SELECT 1; --'")
        with self.assertRaises(ValueError):
            literal(True)

    def test_only_integral_numeric_representation_is_normalized(self):
        self.assertEqual(digest({'score':19.0,'wdl':[200.,700.,100.]}),digest({'score':19,'wdl':[200,700,100]}))
        self.assertNotEqual(digest(1.5),digest(1))
        self.assertNotEqual(digest(None),digest(0))
        self.assertNotEqual(digest([1,2]),digest([2,1]))

    def test_psql_json_stream(self):
        self.assertEqual(decode_stream(' [\n{"a":1}\n]\n {"b":2}\n'),[[{'a':1}],{'b':2}])

    def test_cases_share_names_and_keep_queries_bounded(self):
        inputs=[{'player_name':'test','link':12}]
        old,new=cases(inputs,compact=False),cases(inputs,compact=True)
        self.assertEqual([c['name'] for c in old],[c['name'] for c in new])
        self.assertIn('public.moves',old[0]['sql'])
        self.assertIn('chessism_reads.moves',new[0]['sql'])
        self.assertIn('ORDER BY analyzed_at,fen_id LIMIT 1000',new[-1]['sql'])
        self.assertTrue(all(c['sql'].startswith('SELECT') for c in old+new))

    def test_candidate_evidence_and_paths_are_required(self):
        status={'state':'needs_review','phase':'private_candidate_complete','mode':'isolated_compact_candidate',
            'live_database_changed':False,'private_postgres_stopped':True,'workspace':'/restore-test/compact-db-unit'}
        self.assertEqual(str(checked_workspace(status)),status['workspace'])
        for key,value in [('state','running'),('live_database_changed',True),('private_postgres_stopped',False),
                          ('workspace','/var/lib/postgresql/data'),('workspace','/restore-test/other')]:
            with self.subTest(key=key),self.assertRaises(ValueError):
                checked_workspace({**status,key:value})

    def test_watcher_labels_acceptance_not_migration_complete(self):
        output=render({'mode':'isolated_compact_acceptance','state':'succeeded','checks_completed':9,
            'queries_completed':19,'queries_total':19})
        self.assertIn('Acceptance checks passed: 9',output)
        self.assertIn('Affected queries: 19 / 19',output)
        self.assertIn('NOT approval or completion of the live migration',output)


if __name__=='__main__':
    unittest.main()
