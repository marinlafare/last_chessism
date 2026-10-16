import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.launch_db_refactor_http_backup import runner_command
from scripts.run_db_refactor_migration_sequence import verified_source
from scripts.watch_db_refactor import render


class MigrationSequenceTests(unittest.TestCase):
    def test_launch_is_private_and_rebuilds_legacy_source(self):
        args=runner_command('migration-sequence-001')
        self.assertEqual(args[args.index('--network')+1],'none')
        self.assertIn('/candidate/scripts/run_db_refactor_migration_sequence.py',args)
        self.assertIn('/reports/baseline-001',args)
        self.assertNotIn('--release',args)
        self.assertNotIn('/var/run/docker.sock',' '.join(args))
        self.assertNotIn('db_data',' '.join(args))

    def test_gate_requires_same_backup_and_all_prior_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);baseline=root/'baseline';gate=root/'gate'
            baseline.mkdir();gate.mkdir()
            (baseline/'status.json').write_text(json.dumps({'backup_id':'source','live_database_changed':False}))
            (baseline/'manifest.json').write_text(json.dumps({'backup':{'completed_at':'snapshot'}}))
            status={'mode':'isolated_corpus_finalization','state':'succeeded','backup_id':'source',
                'rollback_verified':True,'private_postgres_stopped':True,'source_unchanged':True,'live_database_changed':False}
            acceptance={'state':'succeeded','atomic_summary_refresh':True,'restart_validated':True}
            def write():
                (gate/'status.json').write_text(json.dumps(status))
                (gate/'acceptance.json').write_text(json.dumps(acceptance))
            with patch('scripts.run_db_refactor_candidate.verified_workspace',return_value=Path('/private')):
                write();self.assertEqual(verified_source(baseline,gate),(Path('/private'),'source','snapshot'))
                for key,value in (('state','failed'),('backup_id','other'),('rollback_verified',False),
                                  ('source_unchanged',False),('private_postgres_stopped',False),('live_database_changed',True)):
                    old=status[key];status[key]=value;write()
                    with self.assertRaises(ValueError):verified_source(baseline,gate)
                    status[key]=old

    def test_progress_uses_real_index_counters(self):
        output=render({'mode':'isolated_migration_sequence','state':'running','row_checks':13,
            'database':{'activity':{'state':'active','operation':'Migration statement','query_seconds':25},
                        'counters':{'phase':'building index','blocks_done':20,'blocks_total':40}}})
        self.assertIn('WHOLE MIGRATION REHEARSAL',output)
        self.assertIn('Row checks passed: 13',output)
        self.assertIn('20 / 40 (50.0%)',output)
        self.assertIn('Live database unchanged',output)
