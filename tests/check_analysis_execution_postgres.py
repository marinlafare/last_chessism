"""Canonical locks/commits + real ARQ/Redis + deterministic HTTP failures."""
import asyncio
from collections import Counter
from contextlib import ExitStack
import json
from uuid import uuid4
from unittest.mock import AsyncMock, patch

import httpx
from arq import create_pool
from arq.connections import RedisSettings
from arq.worker import Worker, func

from analysis_failure_test_support import isolated_test_redis, isolated_test_redis_url
from chessism_api.database.postgresql import analysis_selection as selection
from chessism_api.operations import analysis, engine_client
from tests.check_canonical_evaluations_postgres import EvaluationCase
from tests.canonical_schema_fixture import FENS


class AnalysisExecutionPostgresTests(EvaluationCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(selection, 'AsyncDBSession', self.factory))
        self.stack.enter_context(patch.object(analysis, 'ANALYSIS_CONCURRENCY', 1))
        self.stack.enter_context(patch.object(engine_client, 'RETRY_DELAYS', (0, 0)))
        # These optional derived projections are outside the transaction under test.
        for name in ('_increment_summary_for_analysis_results', '_refresh_scored_projections_after_analysis'):
            self.stack.enter_context(patch.object(analysis, name, new_callable=AsyncMock))
        self.redis = isolated_test_redis()
        self.addAsyncCleanup(self.redis.aclose)
        self.ident = 'stage-b-' + uuid4().hex
        self.ctx = {'redis':self.redis, 'job_id':self.ident}
        async def clear():
            keys = [key async for key in self.redis.scan_iter(match='*'+self.ident+'*')]
            if keys:
                await self.redis.delete(*keys)
        self.addAsyncCleanup(clear)

    def http(self, handler):
        client_type = httpx.AsyncClient
        return patch.object(analysis.httpx, 'AsyncClient',
            side_effect=lambda: client_type(transport=httpx.MockTransport(handler)))

    async def arq(self, handler, *, total=2):
        settings = RedisSettings.from_dsn(isolated_test_redis_url())
        settings.conn_retries = 0
        pool = await create_pool(settings)
        self.addAsyncCleanup(pool.aclose)
        worker = Worker([func(analysis.run_analysis_job, max_tries=1, timeout=3)],
            redis_pool=pool, queue_name=self.ident, burst=True, max_jobs=1, max_burst_jobs=1,
            poll_delay=0, handle_signals=False, health_check_key='health:'+self.ident)
        self.addAsyncCleanup(worker.close)
        job = await pool.enqueue_job('run_analysis_job', total, 1, 100000,
                                     _job_id=self.ident, _queue_name=self.ident)
        with self.http(handler):
            await asyncio.wait_for(worker.async_run(), 5)
        result = await job.result_info()
        self.assertIsNotNone(result)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        return result

    async def test_committed_results_survive_exhaustion_and_arq_records_failure(self):
        calls = Counter()
        async def handler(request):
            fen = json.loads(request.content)['fens'][0]
            calls[fen] += 1
            # The same acquired row stays locked through every HTTP attempt.
            rows = await self.db.fetch('SELECT fen FROM fen FOR UPDATE SKIP LOCKED')
            self.assertNotIn(fen, [r['fen'] for r in rows])
            if fen == FENS[0]:
                return httpx.Response(200, json=[{'fen':fen,'is_valid':True,'analysis':{'score':1500,'pv':[]}}])
            return httpx.Response(503)
        result = await self.arq(handler)
        self.assertFalse(result.success)
        self.assertEqual(calls, {FENS[0]:1,FENS[1]:3})
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[0]),1500)
        self.assertIsNone(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[1]))
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary WHERE id=1'),1)
        self.assertEqual(len(await self.db.fetch('SELECT fen FROM fen FOR UPDATE NOWAIT')),5)
        progress = json.loads(await self.redis.get('chessism:job_progress:'+self.ident))
        self.assertEqual((progress['phase'],progress['processed']),('failed',1))

    async def test_transient_failure_then_success_commits_once_and_arq_succeeds(self):
        calls = 0
        def handler(request):
            nonlocal calls
            calls += 1
            fen = json.loads(request.content)['fens'][0]
            return httpx.Response(503) if calls < 3 else httpx.Response(200,
                json=[{'fen':fen,'is_valid':True,'analysis':{'score':0,'pv':[]}}])
        result = await self.arq(handler,total=1)
        self.assertTrue(result.success)
        self.assertEqual(calls,3)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen WHERE score IS NOT NULL'),1)
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary WHERE id=1'),1)

    async def test_concurrent_workers_cannot_repeat_the_failed_position(self):
        calls = Counter()
        async def handler(request):
            fen = json.loads(request.content)['fens'][0]
            calls[fen] += 1
            await asyncio.sleep(0)
            return httpx.Response(503) if fen == FENS[0] else httpx.Response(200,
                json=[{'fen':fen,'is_valid':True,'analysis':{'score':20,'pv':[]}}])
        with patch.object(analysis,'ANALYSIS_CONCURRENCY',2):
            result = await self.arq(handler,total=4)
        self.assertFalse(result.success)
        self.assertEqual(calls[FENS[0]],3)
        self.assertTrue(all(count==1 for fen,count in calls.items() if fen!=FENS[0]))
        self.assertIsNone(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[0]))
        self.assertEqual(len(await self.db.fetch('SELECT fen FROM fen FOR UPDATE NOWAIT')),5)

    async def test_request_and_backoff_cancellation_release_real_locks(self):
        for backoff in (False,True):
            entered = asyncio.Event()
            calls = 0
            async def handler(request):
                nonlocal calls
                calls += 1
                entered.set()
                if backoff:
                    return httpx.Response(503)
                await asyncio.Event().wait()
            with self.subTest(backoff=backoff), self.http(handler), \
                 patch.object(engine_client,'RETRY_DELAYS',(60,60)):
                task = self.start(analysis.run_analysis_job(self.ctx,1,1,100000))
                await entered.wait()
                await asyncio.sleep(0)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(calls,1)
            self.assertEqual(self.engine.pool.checkedout(),0)
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen WHERE score IS NOT NULL'),0)
            self.assertEqual(len(await self.db.fetch('SELECT fen FROM fen FOR UPDATE NOWAIT')),5)

    async def test_database_selection_failure_never_calls_http_and_arq_fails(self):
        await self.db.execute('ALTER TABLE fen RENAME TO missing_fen')
        handler = AsyncMock(side_effect=AssertionError('HTTP must not run'))
        result = await self.arq(handler,total=1)
        self.assertFalse(result.success)
        handler.assert_not_awaited()
        progress = json.loads(await self.redis.get('chessism:job_progress:'+self.ident))
        self.assertEqual(progress['phase'],'failed')

    async def test_response_completion_cancellation_cannot_commit_the_acquired_batch(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            fen = json.loads(request.content)['fens'][0]
            entered.set()
            await release.wait()
            return httpx.Response(200, json=[{'fen':fen, 'is_valid':True,
                                             'analysis':{'score':25, 'pv':[]}}])

        with self.http(handler):
            task = self.start(analysis.run_analysis_job(self.ctx,1,1,100000))
            await entered.wait()
            release.set()
            task.cancel('original')  # Response readiness and cancellation share one turn.
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(calls,1)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen WHERE score IS NOT NULL'),0)
        self.assertEqual(await self.db.fetchval('SELECT analyzed_fens FROM database_summary WHERE id=1'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(len(await self.db.fetch('SELECT fen FROM fen FOR UPDATE NOWAIT')),5)
