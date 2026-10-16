import json
from pathlib import Path
import tempfile
import unittest

from scripts.run_db_refactor_application import verified_application_source
from scripts.db_refactor_online_backup import PrivateRedis,OnlineApplicationBackup
from scripts.watch_db_refactor import render


class ApplicationRehearsalTests(unittest.IsolatedAsyncioTestCase):
    def test_only_accepted_distinct_restore_can_be_written(self):
        with tempfile.TemporaryDirectory() as folder:
            report=Path(folder)
            root=report/'private'
            root.mkdir()
            original=root/'db-refactor-baseline-original'
            target=root/'db-refactor-baseline-verify-test'
            status={'state':'succeeded','mode':'isolated_write_validation','backup_id':'original',
                'live_database_changed':False,'source_comparison':'/reports/candidate-002',
                'workspace':str(original),'restored_workspace':str(target)}
            accepted={'offline_pgbackrest_roundtrip':'passed','write_contracts':'passed'}
            def write():
                for name,data in (('status',status),('acceptance',accepted),
                    ('backup-roundtrip',{'restored_pgdata':str(target/'pgdata')})):
                    (report/(name+'.json')).write_text(json.dumps(data))
            baseline={'backup':{'backup_id':'original'}}
            write()
            self.assertEqual(verified_application_source(report,baseline,Path('/reports/candidate-002'),root),target)
            for key,value in [('state','failed'),('live_database_changed',True),
                              ('restored_workspace',str(original)),('backup_id','different')]:
                old=status[key]; status[key]=value; write()
                with self.assertRaises(ValueError):
                    verified_application_source(report,baseline,Path('/reports/candidate-002'),root)
                status[key]=old
            write()
            target.symlink_to(original)
            with self.assertRaises(ValueError):
                verified_application_source(report,baseline,Path('/reports/candidate-002'),root)

    async def test_private_transport_preserves_ownership(self):
        from chessism_api.operations.backup_coordination import ensure_backup_reservation,release_backup
        redis=PrivateRedis()
        await ensure_backup_reservation(redis,'one')
        await release_backup(redis,'other')
        with self.assertRaises(RuntimeError):
            await ensure_backup_reservation(redis,'two')
        await release_backup(redis,'one')
        await ensure_backup_reservation(redis,'two')

    def test_online_target_cannot_be_live(self):
        with self.assertRaises(ValueError):
            OnlineApplicationBackup(Path('/var/lib/postgresql/data'),Path('/var/run/postgresql'),
                Path('/backup-volume'),Path('/restore-test/db-refactor-baseline-x/pgdata'),None,{})

    def test_monitor_has_real_bytes_not_a_guessed_whole_job_percentage(self):
        message=render({'mode':'isolated_application','state':'running',
            'phase':'online_application_full_backup','backup_phase':'backing_up',
            'backup_bytes_total':200,'backup_bytes_done':50})
        self.assertIn('25.0%',message)
        self.assertIn('Live database unchanged',message)
        self.assertIn('ONLINE BACKUP',message)


if __name__=='__main__':
    unittest.main()
