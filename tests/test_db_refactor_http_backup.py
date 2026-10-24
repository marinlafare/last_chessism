import argparse
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.db_refactor_e2e_support import private_environment,verified_source,configure_backup
from scripts.db_refactor_workload import digest
from scripts.launch_db_refactor_http_backup import runner_command,name_argument
from scripts.watch_db_refactor import render


class HttpBackupSafetyTests(unittest.TestCase):
    def test_environment_does_not_inherit_live_urls_or_credentials(self):
        workspace=Path('/restore-test/db-refactor-baseline-http-unit')
        with patch.dict(os.environ,{'DATABASE_URL':'live','REDIS_HOST':'redis',
            'GOOGLE_APPLICATION_CREDENTIALS':'secret','CLOUDSDK_CONFIG':'private',
            'PGBACKREST_CONFIG':'live','AUTH_COOKIE_NAME':'production','HTTP_PROXY':'external'}):
            env=private_environment(workspace,workspace/'pgbackrest.conf','throwaway')
        self.assertEqual(env['REDIS_HOST'],'127.0.0.1')
        self.assertIn('db-refactor-baseline-http-unit',env['DATABASE_URL'])
        self.assertEqual(env['CHESSISM_DATABASE_PROFILE'],'relational_v2')
        self.assertEqual(env['PYTHON_DOTENV_DISABLED'],'1')
        self.assertEqual(env['QUEUE_NAME'],'backup_queue')
        for key in ('GOOGLE_APPLICATION_CREDENTIALS','CLOUDSDK_CONFIG','HTTP_PROXY'):
            self.assertNotIn(key,env)
        for key in ('BACKUP_VOLUME_ROOT','DATABASE_RESTORE_TEST_ROOT','PGBACKREST_CONFIG'):
            self.assertTrue(env[key].startswith(str(workspace)+'/'))
        with self.assertRaises(ValueError): private_environment(Path('/var/lib/postgresql'),'unused','unused')

    def test_docker_plan_is_network_isolated_without_live_mounts(self):
        command=runner_command('http-backup-001')
        self.assertEqual(command[command.index('--network')+1],'none')
        self.assertNotIn('--privileged',command)
        self.assertNotIn('-p',command)
        mounts=[command[i+1] for i,value in enumerate(command) if value=='--mount']
        self.assertEqual(len(mounts),3)
        self.assertTrue(mounts[0].endswith('dst=/candidate,readonly'))
        self.assertFalse(any('db_data' in value or 'postgres_socket' in value or
            '/main-monitor-db-backups' in value or '/var/run/docker.sock' in value for value in mounts))
        for name in ('../http-backup-001','live','http-backup-1','http-backup-001;whoami'):
            with self.assertRaises(argparse.ArgumentTypeError): name_argument(name)

    def test_requires_accepted_canonical_restart_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            workspace=root/'db-refactor-baseline-release-unit'
            status={'mode':'isolated_canonical_release','state':'succeeded',
                'phase':'canonical_release_rehearsal_complete','private_postgres_stopped':True,
                'live_database_changed':False,'workspace':str(workspace),'backup_id':'accepted'}
            release={'committed_to_private_copy':True,'contract':{'version':'relational_v2'}}
            restart={'contract_hash':digest(release['contract'])}
            def write():
                for name,data in (('status',status),('canonical-release',release),('canonical-restart',restart)):
                    (root/(name+'.json')).write_text(json.dumps(data))
            write()
            with patch('scripts.db_refactor_e2e_support.ROOT',root):
                self.assertEqual(verified_source(root)[0],workspace)
                for key,value in (('state','running'),('private_postgres_stopped',False),
                                  ('live_database_changed',True),('workspace','/var/lib/postgresql')):
                    original=status[key];status[key]=value;write()
                    with self.assertRaises(ValueError): verified_source(root)
                    status[key]=original
                restart['contract_hash']='wrong';write()
                with self.assertRaises(ValueError): verified_source(root)

    def test_progress_reports_real_queue_and_backup_steps(self):
        output=render({'mode':'isolated_http_backup','state':'running','phase':'http_arq_full_backup',
            'http_checks':12,'completed_jobs':0,'worker_phase':'backing_up','worker_total':200,'worker_processed':100})
        self.assertIn('HTTP checks: 12',output)
        self.assertIn('50.0%',output)
        self.assertIn('Live database unchanged',output)
