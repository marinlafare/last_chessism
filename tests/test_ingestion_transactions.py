"""Operation-owned cleanup must complete under repeated cooperative cancellation."""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from chessism_api.database.postgresql.transactions import driver_connection, write_transaction


class IngestionTransactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_commit_only_after_operation_body_and_close_after_commit(self):
        session=AsyncMock()
        async with write_transaction(lambda:session):
            session.begin.assert_awaited_once()
            session.commit.assert_not_awaited()
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()

    async def test_primary_failure_survives_failed_rollback_and_close(self):
        session=AsyncMock()
        session.rollback.side_effect=OSError('rollback failed')
        session.close.side_effect=OSError('close failed')
        with self.assertRaisesRegex(ValueError,'primary import failure'):
            async with write_transaction(lambda:session):
                raise ValueError('primary import failure')
        session.commit.assert_not_awaited()
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()

    async def test_commit_failure_rolls_back_and_propagates(self):
        session=AsyncMock()
        session.commit.side_effect=OSError('commit failed')
        with self.assertRaisesRegex(OSError,'commit failed'):
            async with write_transaction(lambda:session):
                pass
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()

    async def test_close_failure_after_commit_is_not_reported_as_success(self):
        session=AsyncMock()
        session.close.side_effect=OSError('close failed')
        with self.assertRaisesRegex(OSError,'close failed'):
            async with write_transaction(lambda:session):
                pass
        session.commit.assert_awaited_once()

    async def test_repeated_cancellation_finishes_rollback_and_close(self):
        for phase in ('rollback','close'):
            for rollback_failure in (False,True):
                with self.subTest(phase=phase,rollback_failure=rollback_failure):
                    session=AsyncMock()
                    entered,cleanup_entered,release=asyncio.Event(),asyncio.Event(),asyncio.Event()
                    async def rollback():
                        if phase=='rollback':
                            cleanup_entered.set()
                            await release.wait()
                        if rollback_failure:
                            raise OSError('rollback failed')
                    async def close():
                        if phase=='close':
                            cleanup_entered.set()
                            await release.wait()
                    session.rollback.side_effect=rollback
                    session.close.side_effect=close
                    async def work():
                        async with write_transaction(lambda:session):
                            entered.set()
                            await asyncio.Future()
                    task=asyncio.create_task(work())
                    await entered.wait()
                    task.cancel()
                    await asyncio.wait_for(cleanup_entered.wait(),1)
                    try:
                        for _ in range(2):
                            task.cancel()
                            await asyncio.sleep(0)
                            self.assertFalse(task.done())
                    finally:
                        release.set()
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(task,1)
                    session.rollback.assert_awaited_once()
                    session.close.assert_awaited_once()
                    session.commit.assert_not_awaited()

    async def test_copy_connection_does_not_implicitly_begin_a_transaction(self):
        session=MagicMock()
        session.in_transaction.return_value=False
        session.execute=AsyncMock()
        with self.assertRaisesRegex(ValueError,'caller-owned'):
            await driver_connection(session)
        session.execute.assert_not_awaited()


if __name__=='__main__':
    unittest.main()
