"""Task/connection lifetime regressions without a GPU or database."""
import asyncio
import threading
import unittest
from unittest.mock import AsyncMock

from chessism_api.operations.matrix_constructor.artifact_files import file_operation
from chessism_api.operations.cuda_summaries.snapshot_flow import _finish_session


class CudaResourceCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_transport_returns_the_actual_result(self):
        value=object()
        self.assertIs(await file_operation(lambda:value),value)

    async def test_task_failure_propagates(self):
        def fail():raise ValueError('synthetic decode failure')
        with self.assertRaisesRegex(ValueError,'synthetic decode'):await file_operation(fail)

    async def test_repeated_cancellation_waits_for_thread_even_when_thread_fails(self):
        for fails in (False,True):
            entered=asyncio.Event();release=threading.Event();closed=threading.Event();loop=asyncio.get_running_loop()
            def work():
                loop.call_soon_threadsafe(entered.set)
                try:
                    if not release.wait(2):raise RuntimeError('test release timeout')
                    if fails:raise ValueError('synthetic thread failure')
                    return 'done'
                finally:closed.set()
            task=asyncio.create_task(file_operation(work))
            try:
                await asyncio.wait_for(entered.wait(),1)
                for _ in range(3):
                    task.cancel();await asyncio.sleep(0);await asyncio.sleep(0)
                    self.assertFalse(task.done());self.assertFalse(closed.is_set())
            finally:
                release.set();await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(closed.is_set());self.assertTrue(task.cancelled())

    async def test_repeated_cancellation_drains_connection_invalidation(self):
        started=asyncio.Event();release=asyncio.Event();closed=asyncio.Event()
        async def invalidate():
            started.set();await release.wait();closed.set()
        session=AsyncMock();session.invalidate.side_effect=invalidate
        task=asyncio.create_task(_finish_session(session,invalidate=True))
        try:
            await asyncio.wait_for(started.wait(),1)
            for _ in range(3):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
        finally:release.set();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertTrue(closed.is_set());session.close.assert_not_awaited()
