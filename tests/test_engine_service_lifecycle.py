"""Bounded service leases, disconnects and repeated cancellation without a real engine."""
import asyncio
from contextlib import ExitStack
import importlib
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import chess
import httpx
from fastapi import FastAPI, HTTPException
from analysis_failure_test_support import stockfish_analysis_module
from chessism_api.operations import engine_client

FEN = chess.STARTING_FEN


class EngineServiceLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = stockfish_analysis_module()
        self.engine = importlib.import_module('operations.engine')
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.created = []
        async def start(number):
            slot = (Mock(), SimpleNamespace(quit=AsyncMock()))
            self.created.append(slot)
            return slot
        self.start = self.stack.enter_context(patch.object(self.engine, '_start_engine', side_effect=start))
        self.pool = self.engine.EnginePool(1)
        self.stack.enter_context(patch.object(self.service, 'engine_pool', self.pool))
        self.search = self.stack.enter_context(patch.object(self.service, 'analyse', new_callable=AsyncMock))
        self.search.return_value = {'score':chess.engine.PovScore(chess.engine.Cp(25), chess.WHITE), 'pv':[]}
        self.stack.enter_context(patch.object(self.service, '_write_progress', new_callable=AsyncMock))
        self.request = self.service.AnalysisRequest(fens=[FEN])
        self.addAsyncCleanup(self.pool.shutdown)

    async def test_success_keeps_process_and_white_relative_score(self):
        result = await self.service.analyze_fens_endpoint(self.request)
        self.assertEqual(result[0]['analysis']['score'], 25)
        self.search.assert_awaited_once()
        self.created[0][1].quit.assert_not_awaited()
        self.assertEqual(self.pool.status()['workers'], {'total':1,'busy':0,'idle':1})

    async def test_http_client_and_service_have_three_total_searches_not_nested_retries(self):
        self.search.side_effect = chess.engine.EngineTerminatedError('synthetic disconnect')
        app = FastAPI()
        app.include_router(self.service.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
            with patch.object(engine_client,'RETRY_DELAYS',(0,0)):
                with self.assertRaises(engine_client.EngineServiceRequestError) as caught:
                    await engine_client.call_engine_service(client,'http://isolated/analyze',[FEN],100000)
        self.assertEqual(caught.exception.status_code,503)
        self.assertEqual(self.search.await_count,3)
        self.assertEqual(self.start.await_count,3)
        for transport, engine in self.created:
            engine.quit.assert_awaited_once()
            transport.close.assert_called_once()

    async def test_http_transient_failure_then_success_replaces_only_failed_engine(self):
        self.search.side_effect = [chess.engine.EngineTerminatedError('lost'), self.search.return_value]
        app = FastAPI()
        app.include_router(self.service.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
            with patch.object(engine_client,'RETRY_DELAYS',(0,0)):
                result = await engine_client.call_engine_service(client,'http://isolated/analyze',[FEN],100000)
        self.assertEqual(result[0]['analysis']['score'],25)
        self.assertEqual(self.search.await_count,2)
        self.created[0][0].close.assert_called_once()
        self.created[1][0].close.assert_not_called()

    async def test_position_timeout_has_no_nested_retry_and_discards_process(self):
        async def stuck(*args):
            await asyncio.Event().wait()
        self.search.side_effect = stuck
        with patch.object(self.service, 'ANALYSE_TIMEOUT_SEC', .01):
            with self.assertRaises(HTTPException) as caught:
                await self.service.analyze_fens_endpoint(self.request)
        self.assertEqual(caught.exception.status_code, 504)
        self.search.assert_awaited_once()
        self.created[0][1].quit.assert_awaited_once()
        self.created[0][0].close.assert_called_once()
        self.assertEqual(self.pool.status()['workers']['busy'], 0)
        self.search.side_effect = None
        await self.service.analyze_fens_endpoint(self.request)
        self.assertEqual(self.start.await_count, 2)

    async def test_engine_disconnect_is_transient_but_unexpected_error_is_not_retried(self):
        for error in (chess.engine.EngineTerminatedError('lost'), ValueError('bad engine result')):
            self.search.reset_mock()
            self.search.side_effect = error
            with self.subTest(error=type(error)), self.assertRaises((HTTPException, ValueError)) as caught:
                await self.service.analyze_fens_endpoint(self.request)
            if isinstance(error, chess.engine.EngineTerminatedError):
                self.assertEqual(caught.exception.status_code, 503)
            else:
                self.assertIs(caught.exception, error)
            self.search.assert_awaited_once()
            self.created[-1][0].close.assert_called_once()

    async def test_pool_wait_is_bounded_without_disrupting_existing_lease(self):
        async with self.pool.acquire():
            with patch.object(self.engine, 'QUEUE_SECONDS', .01):
                with self.assertRaises(HTTPException) as caught:
                    await self.service.analyze_fens_endpoint(self.request)
            self.assertEqual(caught.exception.status_code, 503)
            self.search.assert_not_awaited()
            self.assertEqual(self.pool.status()['workers']['busy'], 1)
            self.created[0][1].quit.assert_not_awaited()

    async def test_overall_batch_deadline_stops_search_and_releases_lease(self):
        async def stuck(*args):
            await asyncio.Event().wait()
        self.search.side_effect = stuck
        with patch.object(self.service, 'QUEUE_SECONDS', 0), \
             patch.object(self.service, 'execution_seconds', return_value=.01):
            with self.assertRaises(HTTPException) as caught:
                await self.service.analyze_fens_endpoint(self.request)
        self.assertEqual(caught.exception.status_code, 504)
        self.created[0][0].close.assert_called_once()
        self.assertEqual(self.pool.status()['workers']['busy'], 0)

    async def test_repeated_cancellation_drains_process_before_releasing_lease(self):
        entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def quit():
            closing.set()
            await release.wait()
        async def stuck(*args):
            self.created[0][1].quit.side_effect = quit
            entered.set()
            await asyncio.Event().wait()
        self.search.side_effect = stuck
        task = asyncio.create_task(self.service.analyze_fens_endpoint(self.request))
        await entered.wait()
        task.cancel()
        await closing.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertEqual(self.pool.status()['workers']['busy'], 1)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.created[0][0].close.assert_called_once()
        self.assertEqual(self.pool.status()['workers']['busy'], 0)

    async def test_client_disconnect_cancels_search_and_discards_process(self):
        entered = asyncio.Event()
        async def stuck(*args):
            entered.set()
            await asyncio.Event().wait()
        async def disconnected():
            await entered.wait()
            return True
        self.search.side_effect = stuck
        connection = SimpleNamespace(is_disconnected=disconnected)
        with self.assertRaises(HTTPException) as caught:
            await self.service.analyze_fens_endpoint(self.request, connection)
        self.assertEqual(caught.exception.status_code, 499)
        self.created[0][0].close.assert_called_once()
        self.assertEqual(self.pool.status()['workers']['busy'], 0)

    async def test_cancelled_initialization_closes_already_started_slots(self):
        second_started = asyncio.Event()
        first_transport, first_engine = Mock(), SimpleNamespace(quit=AsyncMock())
        async def start(number):
            if number == 1:
                return first_transport, first_engine
            second_started.set()
            await asyncio.Event().wait()
        pool = self.engine.EnginePool(2)
        with patch.object(self.engine, '_start_engine', side_effect=start):
            task = asyncio.create_task(pool.initialize())
            await second_started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        first_engine.quit.assert_awaited_once()
        first_transport.close.assert_called_once()
        self.assertFalse(pool.status()['ok'])
