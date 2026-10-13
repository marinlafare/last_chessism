import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from scripts.fen_rehearsal_readiness import wait_for_recovery_completion


class RecoveryReadinessTests(unittest.IsolatedAsyncioTestCase):
    async def test_waits_past_read_only_startup_until_recovery_finishes(self):
        connection = AsyncMock()
        connection.fetchrow.side_effect = [
            {'in_recovery': True, 'read_only': True},
            {'in_recovery': True, 'read_only': True},
            {'in_recovery': False, 'read_only': False},
        ]
        with patch('scripts.fen_rehearsal_readiness.asyncio.sleep', new_callable=AsyncMock) as sleep:
            await wait_for_recovery_completion(connection)
        self.assertEqual(sleep.await_count, 2)
        self.assertEqual(connection.fetchrow.await_count, 3)
        for call in connection.fetchrow.await_args_list:
            self.assertIn('pg_is_in_recovery()', call.args[0])
            self.assertNotIn('pg_current_wal_lsn', call.args[0])
            self.assertNotIn('pg_promote', call.args[0])
        connection.execute.assert_not_awaited()

    async def test_ready_database_does_not_sleep(self):
        connection = AsyncMock()
        connection.fetchrow.return_value = {'in_recovery': False, 'read_only': False}
        with patch('scripts.fen_rehearsal_readiness.asyncio.sleep', new_callable=AsyncMock) as sleep:
            await wait_for_recovery_completion(connection)
        sleep.assert_not_awaited()

    async def test_persistent_read_only_mode_fails_closed(self):
        connection = AsyncMock()
        connection.fetchrow.return_value = {'in_recovery': False, 'read_only': True}
        with self.assertRaisesRegex(RuntimeError, 'remains read-only'):
            await wait_for_recovery_completion(connection)

    async def test_recovery_timeout_is_bounded(self):
        connection = AsyncMock()
        connection.fetchrow.return_value = {'in_recovery': True, 'read_only': True}
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            await wait_for_recovery_completion(connection, timeout_seconds=0.03, poll_seconds=0.01)

    async def test_stuck_query_is_also_bounded(self):
        connection = AsyncMock()
        async def never_returns(*args):
            await asyncio.Event().wait()
        connection.fetchrow.side_effect = never_returns
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            await wait_for_recovery_completion(connection, timeout_seconds=0.03)

    async def test_connection_failure_propagates(self):
        connection = AsyncMock()
        connection.fetchrow.side_effect = ConnectionError('test connection closed')
        with self.assertRaises(ConnectionError):
            await wait_for_recovery_completion(connection)

    async def test_cancellation_propagates(self):
        connection = AsyncMock()
        connection.fetchrow.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await wait_for_recovery_completion(connection)

    async def test_invalid_probe_fails_closed(self):
        connection = AsyncMock()
        connection.fetchrow.return_value = None
        with self.assertRaisesRegex(RuntimeError, 'invalid recovery state'):
            await wait_for_recovery_completion(connection)

    async def test_invalid_limits_are_rejected(self):
        for limits in ({'timeout_seconds': 0}, {'poll_seconds': 0}):
            with self.assertRaises(ValueError):
                await wait_for_recovery_completion(AsyncMock(), **limits)


if __name__ == '__main__':
    unittest.main()
