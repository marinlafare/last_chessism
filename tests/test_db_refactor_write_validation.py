import unittest
from pathlib import Path
from unittest.mock import AsyncMock,Mock,patch

from scripts.db_refactor_write_store import standard_game,short_eco,CandidateStore
from scripts.db_refactor_candidate import START
from scripts.db_refactor_roundtrip import private_paths
from scripts.watch_db_refactor import render


class WriteValidationTests(unittest.IsolatedAsyncioTestCase):
    def test_archive_classification_and_eco_are_lossless(self):
        for setup in (None,'',START,START+' 0 1','  '+START+'   0 1  '):
            self.assertTrue(standard_game({'rules':'chess','initial_setup':setup}))
        self.assertFalse(standard_game({'rules':'chess960','initial_setup':None}))
        self.assertFalse(standard_game({'rules':'chess','initial_setup':'different'}))
        self.assertEqual(short_eco('no_eco'),'no_eco')
        self.assertEqual(short_eco('https://www.chess.com/openings/A-B'),'A-B')
        with self.assertRaises(ValueError):
            short_eco('unknown')

    async def test_no_autocommit_or_unguarded_writer(self):
        conn=AsyncMock()
        conn.is_in_transaction=Mock(return_value=False)
        with patch('scripts.db_refactor_write_store.guard',new=AsyncMock()):
            with self.assertRaisesRegex(RuntimeError,'active caller transaction'):
                await CandidateStore.open(conn,Path('/restore-test/db-refactor-baseline-x/pgdata'))
        conn.execute.assert_not_awaited()
        with self.assertRaises(RuntimeError):
            CandidateStore(conn,None)

    def test_backup_paths_cannot_target_production_or_existing_source(self):
        source=Path('/restore-test/db-refactor-baseline-source/pgdata')
        work=Path('/restore-test/db-refactor-acceptance-0123456789ab')
        restored=Path('/restore-test/db-refactor-baseline-verify/pgdata')
        self.assertEqual(private_paths(source,work,restored),(source,work,restored))
        for paths in ((Path('/var/lib/postgresql/data'),work,restored),
                      (source,work,source),(source,Path('/backup-volume'),restored),
                      (source,work,Path('/restore-test/pgdata'))):
            with self.assertRaises(ValueError):
                private_paths(*paths)

    def test_watcher_does_not_claim_a_live_migration_or_full_app_acceptance(self):
        report={'state':'running','mode':'isolated_write_validation',
                'phase':'backing_up_private_candidate','phase_progress_percent':37}
        output=render(report)
        self.assertIn('WRITE/BACKUP REHEARSAL',output)
        self.assertIn('37%',output)
        self.assertIn('Live database unchanged',output)
        report.update(state='succeeded',phase='write_backup_validation_complete')
        self.assertIn('integration / live deployment still pending',render(report))


if __name__=='__main__':
    unittest.main()
