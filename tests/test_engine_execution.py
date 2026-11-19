"""Finite transport attempts and fatal-job behavior; no real engine or database."""
import asyncio
from collections import Counter
from contextlib import ExitStack
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from chessism_api.operations import analysis, engine_client as transport
from chessism_api.routers import analysis as routes
from stockfish_core import http_policy as policy
from tests.test_analysis_failure_jobs import session_for, engine_result


class EngineTransportTests(unittest.IsolatedAsyncioTestCase):
    async def request(self, handler, fens=None):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(transport, 'RETRY_DELAYS', (0, 0)):
                return await transport.call_engine_service(client, transport.ENGINE_URL, fens or ['fen'], 100000)

    async def test_transient_failures_then_success_have_one_three_attempt_budget(self):
        for status in transport.RETRYABLE_STATUS:
            calls = []
            def handler(request):
                calls.append(request)
                return httpx.Response(status if len(calls) < 3 else 200, json=engine_result())
            with self.subTest(status=status):
                self.assertEqual(await self.request(handler), engine_result())
                self.assertEqual(len(calls), 3)
                for timeout in calls[0].extensions['timeout'].values():
                    self.assertGreater(timeout, 0)

    async def test_exhausted_statuses_and_permanent_rejections_fail_explicitly(self):
        for status in (400, 401, 403, 404, 422, 500, 408, 429, 502, 503, 504):
            count = 0
            def handler(request):
                nonlocal count
                count += 1
                return httpx.Response(status, text='rejected')
            with self.subTest(status=status), self.assertRaises(transport.EngineServiceRequestError) as caught:
                await self.request(handler)
            self.assertEqual(caught.exception.status_code, status)
            self.assertEqual(count, 3 if status in transport.RETRYABLE_STATUS else 1)

    async def test_persistent_transport_failures_and_timeouts_stop_at_three(self):
        for failure in (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                        httpx.RemoteProtocolError, httpx.ReadError):
            count = 0
            def handler(request):
                nonlocal count
                count += 1
                raise failure('synthetic disconnect', request=request)
            with self.subTest(failure=failure), self.assertRaises(transport.EngineServiceRequestError):
                await self.request(handler)
            self.assertEqual(count, 3)

    async def test_malformed_and_incomplete_responses_are_not_retried(self):
        replies = [None, [], {}, engine_result('wrong-fen'), [{'fen':'fen','is_valid':True,'analysis':{}}],
                   [{'fen':'fen','is_valid':True,'analysis':[]}],
                   [{'fen':'fen','is_valid':True,'analysis':{'score':float('inf')}}],
                   [{'fen':'fen','is_valid':True,'analysis':{'score':10**400}}],
                   [{'fen':'fen','is_valid':True,'analysis':{'score':True}}]]
        for reply in replies:
            count = 0
            def handler(request):
                nonlocal count
                count += 1
                if reply is None:
                    return httpx.Response(200, content=b'{bad json')
                # json.dumps permits the deliberately malformed infinite score.
                import json
                return httpx.Response(200, content=json.dumps(reply).encode())
            with self.subTest(reply=reply), self.assertRaises(transport.EngineServiceRequestError):
                await self.request(handler)
            self.assertEqual(count, 1)

    async def test_invalid_position_keeps_interactive_envelope_but_cannot_be_persisted(self):
        reply = [{'fen':'fen','is_valid':False,'analysis':{}}]
        result = await self.request(lambda request: httpx.Response(200, json=reply))
        self.assertEqual(result, reply)
        with self.assertRaises(transport.EngineServiceRequestError):
            analysis._format_engine_results(result)

    async def test_overall_deadline_cancels_the_active_request(self):
        stopped = asyncio.Event()
        async def handler(request):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        with patch.object(transport, 'request_seconds', return_value=.02):
            with self.assertRaises(transport.EngineServiceRequestError) as caught:
                await self.request(handler)
        self.assertEqual(caught.exception.status_code, 504)
        self.assertTrue(stopped.is_set())

    async def test_cancellation_during_request_or_backoff_never_retries(self):
        for backoff in (False, True):
            entered = asyncio.Event()
            calls = 0
            async def handler(request):
                nonlocal calls
                calls += 1
                entered.set()
                if backoff:
                    return httpx.Response(503)
                await asyncio.Event().wait()
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                task = asyncio.create_task(transport.call_engine_service(client, transport.ENGINE_URL, ['fen'], 100000))
                await entered.wait()
                await asyncio.sleep(0)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(calls, 1)

    def test_deadlines_scale_with_interactive_work_and_fit_inside_arq(self):
        self.assertGreater(policy.request_seconds(20, 100000000), policy.request_seconds(20, 100000))
        self.assertGreater(policy.execution_seconds(1), 300)
        self.assertLess(policy.request_seconds(1000, 100000000), policy.ANALYSIS_JOB_SECONDS)
        self.assertEqual(policy.MAX_ATTEMPTS, 3)

    def test_worker_registers_one_arq_attempt_with_a_finite_execution_budget(self):
        from worker import WorkerSettings
        registered = {item.name:item for item in WorkerSettings.functions if hasattr(item, 'name')}
        for name in ('run_analysis_job', 'run_player_analysis_job',
                     'run_analysis_loop_job', 'run_player_games_analysis_job'):
            with self.subTest(job=name):
                self.assertEqual(registered[name].max_tries, 1)
                self.assertEqual(registered[name].timeout_s, policy.ANALYSIS_JOB_SECONDS)
                self.assertLess(policy.request_seconds(1000, 100000000), registered[name].timeout_s)

    async def test_immediate_route_uses_same_policy_and_preserves_raw_scores(self):
        client_type = httpx.AsyncClient
        for score in (-2147483648, 2147483647, 10**400):
            calls = []
            def handler(request):
                import json
                calls.append(json.loads(request.content))
                return httpx.Response(503) if len(calls) == 1 else httpx.Response(200,
                    json=[{'fen':'fen','is_valid':True,'analysis':{'score':score}}])
            with self.subTest(score=score), patch.object(transport,'RETRY_DELAYS',(0,0)), \
                 patch.object(routes,'record_analysis_times',new_callable=AsyncMock), \
                 patch.object(routes.httpx,'AsyncClient',side_effect=lambda: client_type(transport=httpx.MockTransport(handler))):
                request = routes.FenAnalysisRequest(fens=['fen'],nodes_limit=250000,multipv=2)
                if score == 10**400:
                    with self.assertRaises(HTTPException) as caught:
                        await routes.api_analyze_fen(request)
                    self.assertEqual(caught.exception.status_code,502)
                else:
                    result = await routes.api_analyze_fen(request)
                    self.assertEqual(result[0]['analysis']['score'],score)
                self.assertEqual(len(calls),2)
                self.assertEqual(calls[0],{'fens':['fen'],'nodes_limit':250000,'multipv':2})


class EngineJobExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name in ('record_analysis_times', '_increment_summary_for_analysis_results',
                     '_refresh_scored_projections_after_analysis'):
            self.stack.enter_context(patch.object(analysis, name, new_callable=AsyncMock))
        self.progress = self.stack.enter_context(patch.object(analysis, '_write_job_progress', new_callable=AsyncMock))
        self.stage = self.stack.enter_context(patch.object(analysis, 'stage_fen_results', new_callable=AsyncMock))
        self.stack.enter_context(patch.object(transport, 'RETRY_DELAYS', (0, 0)))

    async def run_job(self, fetch, handler, *, workers=1, total=2):
        client_type = httpx.AsyncClient
        with patch.object(analysis, 'ANALYSIS_CONCURRENCY', workers), \
             patch.object(analysis.httpx, 'AsyncClient', side_effect=lambda: client_type(transport=httpx.MockTransport(handler))):
            return await analysis._run_analysis_job({'job_id':'bounded-engine'}, total, 1, 100000,
                fetch_batch=fetch, timing_source='test', job_id='test', fallback_arq_job_id='test', no_more_message='empty')

    async def test_exhaustion_preserves_prior_commit_and_never_reselects_failed_batch(self):
        first, second = session_for(), session_for()
        fetch = AsyncMock(side_effect=[(first,['ok']), (second,['bad'])])
        calls = Counter()
        def handler(request):
            import json
            fen = json.loads(request.content)['fens'][0]
            calls[fen] += 1
            return httpx.Response(200, json=engine_result(fen)) if fen == 'ok' else httpx.Response(503)
        with self.assertRaises(transport.EngineServiceRequestError):
            await self.run_job(fetch, handler)
        self.assertEqual(calls, {'ok':1,'bad':3})
        self.assertEqual(fetch.await_count, 2)
        first.commit.assert_awaited_once(); first.rollback.assert_not_awaited(); first.close.assert_awaited_once()
        second.commit.assert_not_awaited(); second.rollback.assert_awaited_once(); second.close.assert_awaited_once()
        self.assertEqual(self.progress.await_args.kwargs['phase'], 'failed')
        self.assertEqual(self.progress.await_args.kwargs['processed'], 1)

    async def test_database_write_and_invalid_result_fail_without_generic_processing_retry(self):
        for invalid in (False, True):
            session = session_for()
            fetch = AsyncMock(return_value=(session,['fen']))
            self.stage.reset_mock(side_effect=True)
            if not invalid:
                self.stage.side_effect = RuntimeError('database write failed')
            reply = [{'fen':'fen','is_valid':False,'analysis':{}}] if invalid else engine_result()
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                await self.run_job(fetch, lambda request: httpx.Response(200, json=reply))
            fetch.assert_awaited_once()
            session.rollback.assert_awaited_once(); session.close.assert_awaited_once()
            session.commit.assert_not_awaited()
            if invalid:
                self.stage.assert_not_awaited()

    async def test_concurrent_worker_cannot_reset_failed_positions_retry_budget(self):
        rollback_started, release_rollback, peer_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        failed, peer = session_for(), session_for()
        async def rollback():
            rollback_started.set()
            await release_rollback.wait()
        failed.rollback.side_effect = rollback
        fetch = AsyncMock(side_effect=[(failed,['bad']), (peer,['peer'])])
        calls = Counter()
        async def handler(request):
            import json
            fen = json.loads(request.content)['fens'][0]
            calls[fen] += 1
            if fen == 'bad':
                await peer_started.wait()
                return httpx.Response(503)
            peer_started.set()
            await rollback_started.wait()
            return httpx.Response(200, json=engine_result(fen))
        task = asyncio.create_task(self.run_job(fetch, handler, workers=2, total=4))
        await rollback_started.wait()
        # The peer completes while failed rollback still owns its locks.
        for _ in range(20):
            if peer.close.await_count:
                break
            await asyncio.sleep(0)
        release_rollback.set()
        with self.assertRaises(transport.EngineServiceRequestError):
            await task
        self.assertEqual(calls['bad'], 3)
        self.assertEqual(fetch.await_count, 2)
        failed.close.assert_awaited_once(); peer.close.assert_awaited_once()
