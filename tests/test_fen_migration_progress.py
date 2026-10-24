import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from scripts.fen_migration_progress import ProgressReporter, database_progress

WATCH = Path(__file__).resolve().parents[1] / 'scripts/watch_fen_migration.py'
# The CLI imports its sibling module when invoked directly.
import sys
sys.path.insert(0, str(WATCH.parent))
from watch_fen_migration import render


class ProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_uses_catalog_only_and_handles_no_counters(self):
        conn = AsyncMock()
        conn.fetchrow.side_effect = [dict(state='active'), None, None]
        conn.fetchval.return_value = 1048576
        result = await database_progress(conn, 17, '0/100')
        self.assertIsNone(result['counters'])
        self.assertEqual(result['wal_bytes_since_start'], 1048576)
        for call in conn.fetchrow.await_args_list + conn.fetchval.await_args_list:
            self.assertTrue(call.args[0].strip().startswith('SELECT'))
            self.assertNotIn('FROM fen', call.args[0])

    async def test_heartbeat_does_not_add_duplicate_stages(self):
        with tempfile.TemporaryDirectory() as directory:
            reporter = ProgressReporter(Path(directory)/'status.json', 'test', {'phases': []})
            await reporter.publish('adding_fen_identifiers')
            with patch('scripts.fen_migration_progress.database_progress', AsyncMock(return_value={'activity': {}})), \
                 patch('scripts.fen_migration_progress.asyncio.sleep', AsyncMock(side_effect=asyncio.CancelledError)):
                with self.assertRaises(asyncio.CancelledError):
                    await reporter.monitor(AsyncMock(), 17, '0/100')
            self.assertEqual(reporter.status['phases'], [])
            self.assertIn('updated_at', reporter.status)
            await reporter.finish('succeeded', 'migration_complete_awaiting_application_deployment')
            self.assertEqual(len(reporter.status['phases']), 1)
            self.assertIn('App remains paused', render(reporter.status))

    async def test_stats_error_is_reported_without_failing_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            reporter = ProgressReporter(Path(directory)/'status.json', 'test', {})
            with patch('scripts.fen_migration_progress.database_progress', AsyncMock(side_effect=TimeoutError)), \
                 patch('scripts.fen_migration_progress.asyncio.sleep', AsyncMock(side_effect=asyncio.CancelledError)):
                with self.assertRaises(asyncio.CancelledError):
                    await reporter.monitor(AsyncMock(), 17, '0/100')
            self.assertEqual(reporter.status['state'], 'running')
            self.assertEqual(reporter.status['monitor_error'], 'TimeoutError')

    def test_percentage_is_only_for_current_substage(self):
        display = render({'state': 'running', 'database': {'counters': {
            'phase': 'building index: scanning table', 'blocks_done': 25, 'blocks_total': 100}}})
        self.assertIn('25.0% of this substage', display)
        self.assertNotIn('overall', display)
        self.assertIn('No exact row percentage', render({'state': 'running'}))

    def test_completed_deployment_does_not_claim_app_is_still_paused(self):
        display = render({'state': 'succeeded', 'started_at': 'this-migration'}, {
            'state': 'succeeded', 'migration_started_at': 'this-migration', 'post_migration_backup_tested': False})
        self.assertIn('app reopened', display)
        self.assertNotIn('App remains paused', display)
        self.assertIn('Test backup', display)

    def test_old_deployment_cannot_mark_a_new_migration_reopened(self):
        display = render({'state': 'succeeded', 'started_at': 'new-migration'}, {
            'state': 'succeeded', 'migration_started_at': 'old-migration'})
        self.assertIn('App remains paused', display)

    def test_reference_rehearsal_has_its_own_stages_and_never_claims_live_cutover(self):
        display = render({'state':'succeeded','mode':'isolated_rehearsal','migration_kind':'references'})
        self.assertIn('Copy game/position links',display)
        self.assertIn('Copy player position frequencies',display)
        self.assertIn('Live tables have not been changed',display)
        self.assertNotIn('App remains paused',display)

    def test_reference_rehearsal_explains_restore_before_migration_stages(self):
        display = render({'state': 'running', 'mode': 'isolated_rehearsal',
                          'migration_kind': 'references', 'phase': 'restoring_verified_backup'})
        self.assertIn('Mode: isolated rehearsal — live database unchanged', display)
        self.assertIn('Current operation: Restore verified backup into isolated workspace', display)

    def test_recovery_wait_and_failed_phase_are_visible(self):
        report = {'state': 'running', 'mode': 'isolated_rehearsal',
                  'phase': 'waiting_for_recovery'}
        self.assertIn('Wait for isolated PostgreSQL recovery to finish', render(report))
        report.update(state='failed', phase='rehearsal_failed', failed_phase='waiting_for_recovery')
        self.assertIn('Failed during: Wait for isolated PostgreSQL recovery to finish', render(report))


if __name__ == '__main__':
    unittest.main()
