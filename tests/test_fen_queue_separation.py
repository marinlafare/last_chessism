import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

for key,value in {'DATABASE_URL':'postgresql+asyncpg://test:test@localhost/test',
    'AUTH_COOKIE_NAME':'test','AUTH_GATE_COOKIE_NAME':'gate','AUTH_SESSION_TTL_DAYS':'1',
    'AUTH_GATE_TTL_MINUTES':'1','AUTH_COOKIE_SECURE':'false','SUPERADMIN_GATE_CODE':'test-only'}.items():
    os.environ.setdefault(key,value)

from chessism_api.operations.fen_backup.queue import FEN_BACKUP_QUEUE,FEN_BACKUP_TIMEOUT
from chessism_api.operations.fen_backup.worker import WorkerSettings
from chessism_api.operations.ingestion_pipeline.assist_worker import IngestionAssistWorker,belongs_to_run
from chessism_api.routers import analysis,jobs
from arq.worker import Worker


class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_backup_and_restore_use_dedicated_queue(self):
        for restore in (False,True):
            redis=AsyncMock()
            redis.enqueue_job.return_value=SimpleNamespace(job_id='new-job')
            with patch.object(analysis,'reserve_backup',AsyncMock(return_value=(True,None))), \
                 patch.object(analysis,'list_fen_analysis_backups',return_value=[{'filename':'saved.json'}]):
                if restore:
                    await analysis.api_restore_fen_analysis_backup('saved.json',redis)
                else:
                    await analysis.api_create_fen_analysis_backup(redis)
            self.assertEqual(redis.enqueue_job.call_args.kwargs['_queue_name'],FEN_BACKUP_QUEUE)
            self.assertEqual(redis.enqueue_job.call_args.kwargs['_job_timeout'],FEN_BACKUP_TIMEOUT)
        self.assertIn(FEN_BACKUP_QUEUE,jobs.KNOWN_QUEUES)
        self.assertNotEqual(FEN_BACKUP_QUEUE,'pipeline_queue')
        self.assertEqual(WorkerSettings.max_jobs,1)
        self.assertEqual({f.name for f in WorkerSettings.functions},
                         {'run_fen_analysis_backup_job','run_fen_analysis_restore_job'})

    async def test_queue_failure_releases_ownership(self):
        redis=AsyncMock();redis.enqueue_job.side_effect=RuntimeError('unavailable')
        with patch.object(analysis,'reserve_backup',AsyncMock(return_value=(True,None))), \
             patch.object(analysis,'release_backup',AsyncMock()) as release:
            with self.assertRaises(RuntimeError):await analysis.api_create_fen_analysis_backup(redis)
            release.assert_awaited_once()

    async def test_helper_filters_before_arq_claim_and_preserves_original_ids(self):
        run='7024c91d-a81f-46c5-ac33-0d35b9781533'
        worker=object.__new__(IngestionAssistWorker)
        worker.ingestion_run_id=run;worker._pool=AsyncMock();worker.queue_name='pipeline_queue'
        worker.job_counter=0;worker.max_jobs=1
        info={
            'backup':SimpleNamespace(function='run_fen_analysis_backup_job',kwargs={'ingestion_run_id':run}),
            'other':SimpleNamespace(function='run_fen_pipeline',kwargs={'ingestion_run_id':'other'}),
            'wanted':SimpleNamespace(function='run_fen_pipeline',kwargs={'ingestion_run_id':run}),
            'endgames':SimpleNamespace(function='run_tablebase_analysis_job',kwargs={'ingestion_run_id':run}),
        }
        with patch('chessism_api.operations.ingestion_pipeline.assist_worker.Job',
                   side_effect=lambda ident,*a,**k:SimpleNamespace(info=AsyncMock(return_value=info[ident]))), \
             patch.object(Worker,'start_jobs',AsyncMock()) as claim:
            await worker.start_jobs([key.encode() for key in info])
        self.assertEqual([c.args[0] for c in claim.await_args_list],[[b'wanted'],[b'endgames']])
        self.assertFalse(belongs_to_run(None,run))
