"""Real ARQ execution and persisted outcomes using explicitly isolated Redis."""
import asyncio
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import JobStatus
from arq.worker import Worker
from chessism_api.database.postgresql import ask_db
from chessism_api.operations import analysis
from analysis_failure_test_support import isolated_test_redis, isolated_test_redis_url


class AnalysisFailureArqTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis = isolated_test_redis()
        self.addAsyncCleanup(self.redis.aclose)
        await self.redis.ping()
        settings = RedisSettings.from_dsn(isolated_test_redis_url())
        settings.conn_retries = 0
        self.pool = await create_pool(settings)
        self.addAsyncCleanup(self.pool.aclose)
        self.ident = "analysis-failure-arq-" + uuid4().hex
        self.queue = "queue:" + self.ident

        async def cleanup():
            keys = [key async for key in self.redis.scan_iter(match=f"*{self.ident}*")]
            if keys:
                await self.redis.delete(*keys)

        self.addAsyncCleanup(cleanup)

    async def execute(self, failure=None):
        rows = Mock()
        rows.scalars.return_value.all.return_value = []
        session = SimpleNamespace(begin=AsyncMock(side_effect=failure), execute=AsyncMock(return_value=rows),
                                  rollback=AsyncMock(), close=AsyncMock())
        empty = SimpleNamespace(begin=AsyncMock(), execute=AsyncMock(return_value=rows),
                                rollback=AsyncMock(), close=AsyncMock())
        worker = Worker([analysis.run_analysis_job], redis_pool=self.pool, queue_name=self.queue,
                        burst=True, max_jobs=1, max_burst_jobs=1, poll_delay=0,
                        handle_signals=False, health_check_key="health:" + self.ident)
        self.addAsyncCleanup(worker.close)
        job = await self.pool.enqueue_job("run_analysis_job", 1, 1, 100000,
                                          _job_id=self.ident, _queue_name=self.queue)
        with patch.object(ask_db, "AsyncDBSession", side_effect=[session, empty]), \
             patch.object(analysis, "ANALYSIS_CONCURRENCY", 1), \
             patch.object(analysis, "_call_engine_service", new_callable=AsyncMock) as engine, \
             patch.object(analysis, "stage_fen_results", new_callable=AsyncMock) as write, \
             patch.object(analysis, "_refresh_scored_projections_after_analysis", new_callable=AsyncMock):
            await asyncio.wait_for(worker.async_run(), 3)
        engine.assert_not_awaited()
        write.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        # ARQ's complete status denotes an available result, including failed jobs.
        self.assertEqual(await job.status(), JobStatus.complete)
        result = await job.result_info()
        self.assertIsNotNone(result)
        progress = json.loads(await self.redis.get(f"chessism:job_progress:{self.ident}"))
        self.assertEqual(len(progress["attempt_id"]), 32)
        self.assertEqual(await self.redis.get(f"chessism:job_progress_attempt:{self.ident}"),
                         progress["attempt_id"])
        return result, progress

    async def test_database_failure_is_stored_as_unsuccessful_with_failed_progress(self):
        result, progress = await self.execute(RuntimeError("database selection unavailable"))
        self.assertFalse(result.success)
        self.assertIsInstance(result.result, RuntimeError)
        self.assertEqual(str(result.result), "database selection unavailable")
        self.assertEqual(progress["phase"], "failed")
        self.assertEqual((progress["processed"], progress["failed"]), (0, 0))

    async def test_genuinely_empty_selection_remains_successful(self):
        result, progress = await self.execute()
        self.assertTrue(result.success)
        self.assertEqual(progress["phase"], "complete")
        self.assertEqual((progress["processed"], progress["failed"]), (0, 0))
