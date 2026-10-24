"""Runs only through the isolated PostgreSQL harness or an explicit test DB."""
import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch
from sqlalchemy import select, func, text, inspect
from test_cloud_analysis_database import CloudDatabaseTests, TEST_URL
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim, CloudResultBatch, CloudRecoveryEvent
from chessism_api.operations.cloud_analysis import controller, importer, recovery_history
from chessism_api.operations.cloud_analysis.batch_validation import prepare_batch
from chessism_api.operations.cloud_analysis.read_recovery import RecoveringClient, RecoveringStorage
from chessism_api.operations.cloud_analysis.migrate_recovery import upgrade
from chessism_api.routers.cloud_analysis import resume_cloud_job


@unittest.skipUnless(TEST_URL, 'requires explicitly isolated PostgreSQL')
class ImportRecoveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = CloudDatabaseTests.asyncSetUp
    asyncTearDown = CloudDatabaseTests.asyncTearDown
    reserved_run = CloudDatabaseTests.reserved_run
    results = CloudDatabaseTests.results

    async def test_storage_thread_records_retries_and_recovery_durably(self):
        client = RecoveringClient()
        client.observe_job(self.job.id)
        storage = Mock()
        storage.read.side_effect = [TimeoutError('synthetic network failure'), b'verified bytes']
        wrapped = RecoveringStorage(storage, client.observer, client.stopping, delays=(0,))
        # The socket-only PostgreSQL image has no Google SDK. Its real exception
        # classifier is tested separately in the controller image; this checks
        # the thread-to-event-loop bridge and durable database commits.
        with patch('chessism_api.operations.cloud_analysis.read_recovery.transient_read_error',
                   side_effect=lambda error: isinstance(error, TimeoutError)):
            self.assertEqual(await asyncio.to_thread(wrapped.read, 'gs://test/batches/000000.json'), b'verified bytes')
        async with AsyncDBSession() as session:
            summary = await recovery_history.summary(session, self.job.id)
        self.assertEqual(summary['counts'], {'retry': 1, 'recovered': 1})
        self.assertEqual(summary['events'][0]['episode_id'], summary['events'][1]['episode_id'])

    async def test_history_migration_requires_idle_jobs_and_is_repeatable(self):
        async with self.engine.begin() as connection:
            await connection.run_sync(lambda c: CloudRecoveryEvent.__table__.drop(c))
        with self.assertRaisesRegex(ValueError, 'Finish/recover'):
            async with self.engine.begin() as connection:
                await connection.run_sync(upgrade)
        async with self.engine.begin() as connection:
            await connection.execute(text("UPDATE cloud_analysis_job SET status='complete'"))
            await connection.run_sync(upgrade)
            await connection.run_sync(upgrade)
            self.assertTrue(await connection.run_sync(lambda c: inspect(c).has_table('cloud_recovery_event')))

    async def test_prevalidated_writer_is_atomic_idempotent_and_rejects_changed_contract(self):
        run = await self.reserved_run(2)
        contract, raw = await self.results(run)
        prepared = prepare_batch(raw, run.positions, contract)
        await controller.save_run(run.id, contract={**contract, 'fingerprint': 'changed'})
        with self.assertRaisesRegex(ValueError, 'no longer matches'):
            await importer.import_batch(run.id, raw, 0, _prepared=prepared)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).imported, 0)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 2)
        await controller.save_run(run.id, contract=contract)
        with patch.object(importer, 'stage_fen_results', new_callable=AsyncMock, side_effect=RuntimeError('injected rollback')):
            with self.assertRaises(RuntimeError):
                await importer.import_batch(run.id, raw, 0, _prepared=prepared)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudResultBatch)), 0)
        self.assertEqual(await importer.import_batch(run.id, raw, 0, _prepared=prepared), 2)
        self.assertEqual(await importer.import_batch(run.id, raw, 0, _prepared=prepared), 0)
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).imported, 2)
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            receipt = await session.scalar(select(CloudResultBatch))
            self.assertGreater(receipt.validation_seconds, 0)
            self.assertGreater(receipt.transaction_seconds, 0)

    async def test_pause_resume_retains_error_counts_and_timing_after_status_is_cleared(self):
        await recovery_history.record(self.job.id, episode_id='a'*32, kind='retry',
            operation='storage read', error_type='TimeoutError', attempt=1, delay_seconds=5)
        await controller.save_job(self.job.id, status='paused', error='TimeoutError: test failure')
        await controller.save_job(self.job.id, status='paused', error='TimeoutError: test failure')
        result = await resume_cloud_job(self.job.id)
        self.assertEqual(result['status'], 'queued')
        async with AsyncDBSession() as session:
            job = await session.get(CloudAnalysisJob, self.job.id)
            self.assertIsNone(job.error)
            summary = await recovery_history.summary(session, self.job.id)
        self.assertEqual(summary['counts'], {'retry': 1, 'paused': 1, 'resumed': 1})
        self.assertGreaterEqual(summary['paused_seconds'], 0)
        self.assertEqual(summary['events'][1]['message'], 'TimeoutError: test failure')
        self.assertEqual(summary['events'][1]['episode_id'], summary['events'][2]['episode_id'])
        await controller.save_job(self.job.id, status='complete')
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudRecoveryEvent)), 3)


if __name__ == '__main__': unittest.main()
