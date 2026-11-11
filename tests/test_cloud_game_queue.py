"""Offline contracts for bounded game-FEN reservation queues."""
import asyncio
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations.cloud_analysis import game_fen_queue as queue


class GameFenQueueTests(unittest.IsolatedAsyncioTestCase):
    def test_identity_is_stable_order_sensitive_and_keeps_first_game_priorities(self):
        self.assertEqual(queue.selection_identity([1,2,1]),queue.selection_identity([1,'2',1,None]))
        self.assertNotEqual(queue.selection_identity([1,2])[1],queue.selection_identity([2,1])[1])
        self.assertEqual(len(queue.selection_identity([])[1]),64)

    def test_build_preserves_game_minima_and_text_tiebreak(self):
        sql=queue.candidate_sql()
        self.assertIn('MIN(selected.priority)',sql)
        self.assertIn('CASE WHEN gfa.is_white',sql)
        self.assertIn('ORDER BY c.game_priority, c.move_priority, f.fen',sql)
        self.assertIn('public.game_fen_association',sql)
        self.assertNotIn('move_color',sql)
        self.assertNotIn('cloud_fen_claim',sql)  # Claims are checked live, not frozen.

    def test_page_is_bounded_before_fen_lookup_and_retains_skip_locked_rows(self):
        sql=queue.page_sql()
        self.assertIn('page AS MATERIALIZED',sql)
        self.assertLess(sql.index('LIMIT :limit'),sql.index('JOIN LATERAL'))
        self.assertIn('ordinal > :after',sql)
        self.assertIn('LEFT JOIN LATERAL',sql)
        self.assertIn('FOR UPDATE OF f SKIP LOCKED',sql)
        self.assertNotIn('game_fen_association',sql)

    async def test_invalid_batch_sizes_never_open_a_database_session(self):
        with patch.object(queue,'AsyncDBSession') as factory:
            for size in [0,-1,50001,True,1.5,None,'10']:
                with self.assertRaises(ValueError):
                    await queue.get_game_queue_fens('unused',[1],size)
            factory.assert_not_called()


    async def test_acquisition_failure_survives_failed_rollback_and_closes_session(self):
        failure = RuntimeError('original queue read failure')
        session = SimpleNamespace(begin=AsyncMock(),get=AsyncMock(side_effect=failure),
            rollback=AsyncMock(side_effect=OSError('rollback unavailable')),close=AsyncMock(),commit=AsyncMock())
        with patch.object(queue,'ensure_queue',AsyncMock(return_value='identity')), \
                patch.object(queue,'AsyncDBSession',return_value=session):
            with self.assertRaises(RuntimeError) as caught:
                await queue.get_game_queue_fens('job',[1],10)
        self.assertIs(caught.exception,failure)
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        session.commit.assert_not_awaited()

    async def test_repeated_cancellation_waits_for_acquisition_cleanup(self):
        selecting, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def blocked(*_args,**_kwargs):
            selecting.set()
            await asyncio.Event().wait()
        async def rollback():
            cleaning.set()
            await release.wait()
        session = SimpleNamespace(begin=AsyncMock(),get=AsyncMock(side_effect=blocked),
            rollback=AsyncMock(side_effect=rollback),close=AsyncMock(),commit=AsyncMock())
        with patch.object(queue,'ensure_queue',AsyncMock(return_value='identity')), \
                patch.object(queue,'AsyncDBSession',return_value=session):
            task = asyncio.create_task(queue.get_game_queue_fens('job',[1],10))
            try:
                await asyncio.wait_for(selecting.wait(),2)
                task.cancel('first')
                await asyncio.wait_for(cleaning.wait(),2)
                task.cancel('second')
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                session.close.assert_not_awaited()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task,2)
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        session.commit.assert_not_awaited()


if __name__=='__main__': unittest.main()
