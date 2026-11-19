"""Worker completion is authoritative before claims can be released."""
import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from chessism_api.operations.ingestion_pipeline import fen_batches, fen_orchestrator


class FenCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for name in ('start_ingestion_stage', 'finish_ingestion_stage',
                     'reset_stage_progress', 'increment_stage_progress',
                     'release_fen_processing_claims'):
            change = patch.object(fen_batches, name, new_callable=AsyncMock)
            change.start()
            self.addCleanup(change.stop)
        self.redis = SimpleNamespace(enqueue_job=AsyncMock())
        self.ctx = {'redis': self.redis, 'job_id': 'synthetic-coordinator'}
        self.tasks = []

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), 2)

    def start(self, operation):
        task = asyncio.create_task(operation)
        self.tasks.append(task)
        return task

    async def run_pipeline(self, total=2, workers=2):
        return await fen_batches.run_bounded_pipeline(self.ctx, total, 300, workers)

    async def test_repeated_cancellation_waits_for_worker_and_claim_cleanup(self):
        entered, finish_worker, cleaning, finish_cleanup = [asyncio.Event() for _ in range(4)]
        async def worker(**_kwargs):
            entered.set()
            await finish_worker.wait()
            return {'successful_game_links': [1], 'failed_game_links': [], 'positions': 2}
        async def cleanup():
            self.assertTrue(finish_worker.is_set())
            cleaning.set()
            await finish_cleanup.wait()
        self.redis.enqueue_job.return_value = SimpleNamespace(result=worker)
        fen_batches.release_fen_processing_claims.side_effect = cleanup
        task = self.start(self.run_pipeline(total=1, workers=1))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        fen_batches.release_fen_processing_claims.assert_not_awaited()
        finish_worker.set()
        await asyncio.wait_for(cleaning.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        finish_cleanup.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        fen_batches.release_fen_processing_claims.assert_awaited_once()

    async def test_fatal_worker_error_still_joins_sibling_before_release(self):
        entered, finish, completed = [asyncio.Event() for _ in range(3)]
        failure = RuntimeError('write failed')
        async def sibling(**_kwargs):
            entered.set()
            await finish.wait()
            completed.set()
            return {'successful_game_links': [2], 'failed_game_links': [], 'positions': 2}
        self.redis.enqueue_job.side_effect = [SimpleNamespace(result=AsyncMock(side_effect=failure)),
                                               SimpleNamespace(result=sibling)]
        task = self.start(self.run_pipeline())
        await asyncio.wait_for(entered.wait(), 2)
        self.assertFalse(task.done())
        fen_batches.release_fen_processing_claims.assert_not_awaited()
        finish.set()
        with self.assertRaisesRegex(RuntimeError, 'write failed') as caught:
            await asyncio.wait_for(task, 2)
        self.assertIs(caught.exception.__cause__, failure)
        self.assertTrue(completed.is_set())
        fen_batches.release_fen_processing_claims.assert_awaited_once()

    async def test_enqueue_error_does_not_abandon_already_enqueued_worker(self):
        worker = AsyncMock(return_value={'successful_game_links': [], 'failed_game_links': [], 'positions': 0})
        self.redis.enqueue_job.side_effect = [SimpleNamespace(result=worker), RuntimeError('enqueue failed')]
        with self.assertRaisesRegex(RuntimeError, 'enqueue failed'):
            await self.run_pipeline()
        worker.assert_awaited_once_with(timeout=None)
        fen_batches.release_fen_processing_claims.assert_awaited_once()

    async def test_cancellation_during_enqueue_drains_known_workers(self):
        enqueuing, finish = asyncio.Event(), asyncio.Event()
        worker = AsyncMock(return_value={'successful_game_links': [], 'failed_game_links': [], 'positions': 0})
        async def enqueue(*_args, **_kwargs):
            if self.redis.enqueue_job.await_count == 1:
                return SimpleNamespace(result=worker)
            enqueuing.set()
            await finish.wait()
        self.redis.enqueue_job.side_effect = enqueue
        task = self.start(self.run_pipeline())
        await asyncio.wait_for(enqueuing.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        worker.assert_awaited_once_with(timeout=None)
        fen_batches.release_fen_processing_claims.assert_awaited_once()

    async def test_child_cancellation_is_a_failed_result_and_siblings_are_joined(self):
        sibling = AsyncMock(return_value={'successful_game_links': [], 'failed_game_links': [], 'positions': 0})
        self.redis.enqueue_job.side_effect = [SimpleNamespace(result=AsyncMock(side_effect=asyncio.CancelledError)),
                                               SimpleNamespace(result=sibling)]
        with self.assertRaisesRegex(RuntimeError, 'Bounded FEN worker failed'):
            await self.run_pipeline()
        sibling.assert_awaited_once()
        fen_batches.release_fen_processing_claims.assert_awaited_once()

    async def test_cleanup_failure_preserves_original_fatal_error(self):
        self.redis.enqueue_job.side_effect = RuntimeError('original enqueue failure')
        fen_batches.release_fen_processing_claims.side_effect = RuntimeError('cleanup unavailable')
        with self.assertLogs(fen_batches.logger, level='ERROR'):
            with self.assertRaisesRegex(RuntimeError, 'original enqueue failure'):
                await self.run_pipeline()

    async def test_zero_work_does_not_enqueue_or_release_other_claims(self):
        result = await self.run_pipeline(total=0)
        self.assertEqual(result, {'claimed_games': 0, 'successful_games': 0,
                                 'failed_games': 0, 'successful_game_links': []})
        self.redis.enqueue_job.assert_not_awaited()
        fen_batches.release_fen_processing_claims.assert_not_awaited()

    async def test_orchestrator_uses_bounded_boundary_without_profile_selection(self):
        expected = {'claimed_games': 2, 'successful_games': 2, 'failed_games': 0,
                    'successful_game_links': [101,105]}
        with patch.object(fen_orchestrator, 'count_pending_games', AsyncMock(return_value=2)), \
                patch.object(fen_orchestrator, 'run_bounded_pipeline', AsyncMock(return_value=expected)) as run:
            self.assertEqual(await fen_orchestrator._run_fen_pipeline(self.ctx, 100, 300, 3), expected)
        run.assert_awaited_once_with(self.ctx, 2, 300, 3, ingestion_run_id=None)
