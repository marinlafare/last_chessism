import json
from pathlib import Path
import tempfile
import unittest

from scripts.db_refactor_cutover import foreign_key_action
from scripts.run_db_refactor_cutover_preflight import verified_source


class CutoverInventoryTests(unittest.TestCase):
    def test_foreign_keys_distinguish_active_from_retained_data(self):
        def row(source_schema,source_table,target_schema,target_table):
            return dict(source_schema=source_schema,source_table=source_table,
                        target_schema=target_schema,target_table=target_table)
        self.assertEqual(foreign_key_action(row('public','game','public','player')),'retain_with_legacy_snapshot')
        self.assertEqual(foreign_key_action(row('public','game_fen_association','public','fen')),'detach_snapshot_from_active_parent')
        self.assertEqual(foreign_key_action(row('public','player_stats','public','player')),'retarget_active_foreign_key')
        self.assertEqual(foreign_key_action(row('refactor_candidate_v1','game','refactor_candidate_v1','player')),
                         'retain_normalized_reference')

    def test_only_distinct_accepted_online_restore_is_selected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            report=root/'report';report.mkdir()
            target=root/'db-refactor-baseline-online-test'
            source=root/'db-refactor-baseline-original'
            status={'mode':'isolated_application','phase':'application_rehearsal_complete',
                'state':'needs_review','live_database_changed':False,'workspace':str(source),
                'restored_workspace':str(target),'backup_id':'verified'}
            accepted={'application_write_checks':'passed','online_backup_worker_and_wal_restore':'passed'}
            def write():
                (report/'status.json').write_text(json.dumps(status))
                (report/'acceptance.json').write_text(json.dumps(accepted))
            write()
            self.assertEqual(verified_source(report,root),(target,'verified'))
            for key,value in [('mode','live'),('state','running'),('live_database_changed',True),
                              ('restored_workspace',str(source))]:
                old=status[key];status[key]=value;write()
                with self.assertRaises(ValueError): verified_source(report,root)
                status[key]=old
            accepted['online_backup_worker_and_wal_restore']='failed';write()
            with self.assertRaises(ValueError): verified_source(report,root)
            accepted['online_backup_worker_and_wal_restore']='passed';write()
            target.symlink_to(source)
            with self.assertRaises(ValueError): verified_source(report,root)


if __name__=='__main__':
    unittest.main()
