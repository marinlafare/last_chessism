"""Same-turn completion/cancellation races on both supported Python runtimes."""
import asyncio
from contextlib import ExitStack
import importlib
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import chess
import httpx

from analysis_failure_test_support import stockfish_analysis_module
from chessism_api.operations import engine_client


def observed_task(operation):
    # Python 3.10 can drop cancel(msg) when awaiting a cancelled Task, even for
    # a plain Event.wait(). Inspect the exception at the consumer boundary.
    cancellations = []

    async def consume():
        try:
            return await operation
        except asyncio.CancelledError as error:
            cancellations.append(error)
            raise

    return asyncio.create_task(consume()), cancellations


class HttpCancellationRaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_external_cancel_during_deadline_cleanup_is_not_a_retryable_timeout(self):
        closing, release = asyncio.Event(), asyncio.Event()

        async def handler(request):
            try:
                await asyncio.Event().wait()
            finally:
                closing.set()
                await release.wait()

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(engine_client, 'request_seconds', return_value=.01):
                task, cancellations = observed_task(engine_client.call_engine_service(
                    client, 'http://isolated/analyze', ['fen'], 100000))
                await closing.wait()  # The deadline expired; child cleanup is still active.
                task.cancel('external')
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(cancellations[0].args, ('external',))

    async def test_ready_response_cancellation_never_reaches_next_consumer(self):
        entered, release = asyncio.Event(), asyncio.Event()
        persist = AsyncMock()
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return httpx.Response(200, json=[{'fen': 'fen', 'is_valid': True,
                                            'analysis': {'score': 25}}])

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            async def consume():
                rows = await engine_client.call_engine_service(client, 'http://isolated/analyze', ['fen'], 100000)
                await persist(rows)
                return rows

            task, cancellations = observed_task(consume())
            await entered.wait()
            release.set()
            self.assertTrue(task.cancel('original'))  # No yield after making the response ready.
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(cancellations[0].args, ('original',))
        persist.assert_not_awaited()
        self.assertEqual(calls, 1)

    async def test_repeated_cancellation_drains_http_and_preserves_original_over_cleanup_error(self):
        entered, closing, release, closed = (asyncio.Event() for _ in range(4))
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closing.set()
                await release.wait()
                closed.set()
                raise RuntimeError('secondary HTTP cleanup failure')

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            task, cancellations = observed_task(engine_client.call_engine_service(
                client, 'http://isolated/analyze', ['fen'], 100000))
            await entered.wait()
            task.cancel('original')
            await closing.wait()
            task.cancel('repeat')
            for _ in range(10):
                await asyncio.sleep(0)
            returned_before_cleanup = task.done()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            # Let a broken baseline's abandoned child finish before reporting failure.
            await closed.wait()
            self.assertFalse(returned_before_cleanup)
            self.assertEqual(cancellations[0].args, ('original',))
            self.assertEqual(calls, 1)


class ServiceCancellationRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = stockfish_analysis_module()
        self.engines = importlib.import_module('operations.engine')
        self.real_start = self.engines._start_engine
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.created = []

        async def start(number):
            transport, engine = Mock(), SimpleNamespace(quit=AsyncMock())
            self.created.append((transport, engine))
            return transport, engine

        self.start = self.stack.enter_context(patch.object(self.engines, '_start_engine', side_effect=start))
        self.pool = self.engines.EnginePool(1)
        self.addAsyncCleanup(self.pool.shutdown)
        self.stack.enter_context(patch.object(self.service, 'engine_pool', self.pool))
        self.search = self.stack.enter_context(patch.object(self.service, 'analyse', new_callable=AsyncMock))
        self.result = {'score': chess.engine.PovScore(chess.engine.Cp(25), chess.WHITE), 'pv': []}
        self.search.return_value = self.result
        self.stack.enter_context(patch.object(self.service, '_write_progress', new_callable=AsyncMock))
        self.request = self.service.AnalysisRequest(fens=[chess.STARTING_FEN])

    async def wait_until(self, predicate):
        for _ in range(200):
            if predicate():
                return
            await asyncio.sleep(0)
        self.fail('Expected event-loop handoff did not occur')

    async def test_ready_slot_cancelled_request_never_searches_and_slot_can_be_reused(self):
        await self.pool.initialize()
        slot = await self.pool._available.get()  # Another request owns the only slot.
        entered, release = asyncio.Event(), asyncio.Event()

        async def search(*args):
            entered.set()
            await release.wait()
            return self.result

        self.search.side_effect = search
        task = asyncio.create_task(self.service.analyze_fens_endpoint(self.request))
        await self.wait_until(lambda: bool(self.pool._available._getters))
        self.pool._available.put_nowait(slot)
        task.cancel('original')  # No yield between availability and cancellation.
        for _ in range(30):
            await asyncio.sleep(0)
        searched_after_cancellation = entered.is_set()
        release.set()  # Drain even the failing baseline without hanging the test.
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(searched_after_cancellation)
        self.assertFalse(slot.busy)
        self.assertEqual(self.pool._available.qsize(), 1)
        self.search.side_effect = None
        await self.service.analyze_fens_endpoint(self.request)
        self.search.assert_awaited_once()
        self.assertEqual(self.start.await_count, 1)

    async def test_cancelled_handoff_returns_slot_to_concurrent_waiter_exactly_once(self):
        await self.pool.initialize()
        slot = await self.pool._available.get()
        entered, release = asyncio.Event(), asyncio.Event()

        async def search(*args):
            entered.set()
            await release.wait()
            return self.result

        self.search.side_effect = search
        cancelled = asyncio.create_task(self.service.analyze_fens_endpoint(self.request))
        peer = asyncio.create_task(self.service.analyze_fens_endpoint(self.request))
        await self.wait_until(lambda: len(self.pool._available._getters) == 2)
        self.pool._available.put_nowait(slot)
        cancelled.cancel('original')
        await entered.wait()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled
        self.assertEqual((await peer)[0]['analysis']['score'], 25)
        self.search.assert_awaited_once()  # Only the uncancelled peer may search.
        self.assertEqual(self.pool._available.qsize(), 1)
        self.assertFalse(slot.busy)

    async def test_startup_completion_cancellation_closes_created_process(self):
        entered, release = asyncio.Event(), asyncio.Event()
        transport, engine = Mock(), SimpleNamespace(quit=AsyncMock())

        async def configure(*args):
            entered.set()
            await release.wait()

        with patch.object(self.engines, 'verify_binary'), \
             patch.object(self.engines.chess.engine, 'popen_uci', AsyncMock(return_value=(transport, engine))), \
             patch.object(self.engines, 'configure', side_effect=configure):
            task, cancellations = observed_task(self.real_start(1))
            await entered.wait()
            release.set()
            task.cancel('original')
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(cancellations[0].args, ('original',))
        engine.quit.assert_awaited_once()
        transport.close.assert_called_once()

    async def test_external_cancellation_during_startup_timeout_cleanup_overrides_timeout(self):
        closing, release = asyncio.Event(), asyncio.Event()

        async def quit():
            closing.set()
            await release.wait()

        async def configure(*args):
            await asyncio.Event().wait()

        transport, engine = Mock(), SimpleNamespace(quit=quit)
        with patch.object(self.engines, 'verify_binary'), \
             patch.object(self.engines.chess.engine, 'popen_uci', AsyncMock(return_value=(transport, engine))), \
             patch.object(self.engines, 'configure', side_effect=configure), \
             patch.object(self.engines, 'ENGINE_START_SECONDS', .01):
            task, cancellations = observed_task(self.real_start(1))
            await closing.wait()
            task.cancel('external')
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(cancellations[0].args, ('external',))
        transport.close.assert_called_once()

    async def test_external_cancellation_during_search_timeout_cleanup_overrides_http_error(self):
        closing, release = asyncio.Event(), asyncio.Event()

        async def quit():
            closing.set()
            await release.wait()

        async def search(*args):
            self.created[0][1].quit.side_effect = quit
            await asyncio.Event().wait()

        self.search.side_effect = search
        with patch.object(self.service, 'ANALYSE_TIMEOUT_SEC', .01):
            task, cancellations = observed_task(self.service._analyze(self.request))
            await closing.wait()
            task.cancel('external')
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(cancellations[0].args, ('external',))
        self.assertEqual(self.pool._available.qsize(), 1)
        self.assertIsNone(self.pool._slots[0].engine)
        self.created[0][0].close.assert_called_once()

    async def test_repeated_initialization_cancellation_preserves_original_after_slot_cleanup(self):
        second_start, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def quit():
            closing.set()
            await release.wait()

        transport, engine = Mock(), SimpleNamespace(quit=quit)

        async def start(number):
            if number == 1:
                return transport, engine
            second_start.set()
            await asyncio.Event().wait()

        pool = self.engines.EnginePool(2)
        with patch.object(self.engines, '_start_engine', side_effect=start):
            task, cancellations = observed_task(pool.initialize())
            await second_start.wait()
            task.cancel('original')
            await closing.wait()
            task.cancel('repeat')
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(cancellations[0].args, ('original',))
        transport.close.assert_called_once()
        self.assertFalse(pool.status()['ok'])

    async def test_search_completion_cancellation_discards_process_without_next_search(self):
        entered, release, finish_later = asyncio.Event(), asyncio.Event(), asyncio.Event()
        searches = 0

        async def search(*args):
            nonlocal searches
            searches += 1
            if searches == 1:
                entered.set()
                await release.wait()
            else:
                await finish_later.wait()
            return self.result

        self.search.side_effect = search
        request = self.service.AnalysisRequest(fens=[chess.STARTING_FEN, chess.STARTING_FEN])
        task = asyncio.create_task(self.service.analyze_fens_endpoint(request))
        await entered.wait()
        release.set()
        task.cancel('original')
        for _ in range(30):
            await asyncio.sleep(0)
        finish_later.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(searches, 1)
        self.created[0][1].quit.assert_awaited_once()
        self.created[0][0].close.assert_called_once()
        self.assertIsNone(self.pool._slots[0].engine)
        self.assertEqual(self.pool._available.qsize(), 1)
        self.search.side_effect = None
        await self.service.analyze_fens_endpoint(self.request)
        self.assertEqual(self.start.await_count, 2)

    async def test_repeat_cancel_during_failed_engine_cleanup_keeps_original_cancellation(self):
        entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def quit():
            closing.set()
            await release.wait()
            raise RuntimeError('secondary quit failure')

        async def search(*args):
            self.created[0][1].quit.side_effect = quit
            entered.set()
            await asyncio.Event().wait()

        self.search.side_effect = search
        task, cancellations = observed_task(self.service.analyze_fens_endpoint(self.request))
        await entered.wait()
        task.cancel('original')
        await closing.wait()
        task.cancel('repeat')
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(cancellations[0].args, ('original',))
        self.created[0][0].close.assert_called_once()
        self.assertIsNone(self.pool._slots[0].engine)
        self.assertEqual(self.pool._available.qsize(), 1)

    async def test_quit_completion_cancellation_closes_transport_and_propagates(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def quit():
            entered.set()
            await release.wait()

        transport, engine = Mock(), SimpleNamespace(quit=quit)
        slot = self.engines.EngineSlot(1, transport, engine)
        task = asyncio.create_task(self.pool._shutdown_slot(slot))
        await entered.wait()
        release.set()
        task.cancel('original')
        with self.assertRaises(asyncio.CancelledError):
            await task
        transport.close.assert_called_once()
        self.assertIsNone(slot.engine)


if __name__ == '__main__':
    unittest.main(verbosity=2)
