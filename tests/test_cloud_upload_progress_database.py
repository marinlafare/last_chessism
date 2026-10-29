"""Real native-column queries against a disposable PostgreSQL schema only."""
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from tests import test_cloud_analysis_database as existing
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun
from chessism_api.operations.cloud_analysis import upload_progress


@unittest.skipUnless(existing.TEST_URL, 'requires explicitly isolated PostgreSQL database')
class UploadProgressDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown

    async def prepare(self, backend='cloud_run', status='uploading'):
        self.run_id = uuid4().hex
        launch = {'backend': backend, 'tasks': [
            {'index': 0, 'start': 0, 'count': 2}, {'index': 1, 'start': 2, 'count': 1}]}
        if backend == 'batch_spot':
            launch = {'backend': backend, 'units': [
                {'index': 0, 'count': 2, 'uploaded': True}, {'index': 1, 'count': 1}]}
        async with AsyncDBSession() as session, session.begin():
            job = await session.get(CloudAnalysisJob, self.job.id)
            job.selection = {**job.selection, 'backend': backend, 'execution_mode':
                'cloud_run_v1' if backend == 'cloud_run' else 'batch_sequence_v2'}
            job.target = 3
            session.add(CloudAnalysisRun(id=self.run_id, job_id=job.id, status=status,
                positions=[{'id': str(i), 'fen': fen} for i, fen in enumerate(self.fens)],
                launch=launch, receipts={}))

    async def test_cloud_run_reads_only_task_counters_and_preserves_job(self):
        await self.prepare()
        response = Mock(status_code=200, json=Mock(return_value={
            'state': 'ready', 'run_id': self.run_id, 'tasks': [1]}))
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.get.return_value = response
        # A full control-document read would load the FEN payload; forbid it.
        with patch.object(upload_progress.httpx, 'AsyncClient', return_value=client), \
             patch('chessism_api.database.cloud_codec.read_documents', side_effect=AssertionError('Full document read')):
            result = await upload_progress.read_progress(self.job.id)
        self.assertEqual(result, {'state': 'ready', 'total_fens': 3, 'uploaded_fens': 1})
        client.get.assert_awaited_once_with(f'http://cloud-billing:8080/upload-progress/{self.run_id}')
        async with AsyncDBSession() as session:
            run = await session.get(CloudAnalysisRun, self.run_id)
            self.assertEqual(run.status, 'uploading')
            self.assertEqual(run.imported_count, 0)
            self.assertEqual(len(run.positions), 3)

    async def test_batch_uses_durable_uploaded_flags_without_google_reads(self):
        await self.prepare('batch_spot')
        with patch.object(upload_progress.httpx, 'AsyncClient', side_effect=AssertionError('Unexpected cloud read')):
            result = await upload_progress.read_progress(self.job.id)
        self.assertEqual(result, {'state': 'ready', 'total_fens': 3, 'uploaded_fens': 2})

    async def test_post_upload_phase_uses_confirmed_total_without_google_reads(self):
        await self.prepare(status='run_discover')
        with patch.object(upload_progress.httpx, 'AsyncClient', side_effect=AssertionError('Unexpected cloud read')):
            result = await upload_progress.read_progress(self.job.id)
        self.assertEqual(result, {'state': 'ready', 'total_fens': 3, 'uploaded_fens': 3})
