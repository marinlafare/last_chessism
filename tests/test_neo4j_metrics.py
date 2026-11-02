import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from chessism_api.database.neo4j.sync_metrics import TimedQueries
from chessism_api.database.neo4j.sync_worker import stop_heartbeat


class QueryTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_times_submission_and_consumption_once_without_retaining_payload(self):
        transaction = AsyncMock()
        transaction.run.return_value.data.return_value = [{'n': 3}]
        timed = TimedQueries(transaction)
        with patch('chessism_api.database.neo4j.sync_metrics.time.monotonic', side_effect=[10, 14]):
            result = await timed.run('game_paths', 'synthetic query', rows=['private payload'])
            self.assertEqual(await result.data(), [{'n': 3}])
            await result.consume()
        self.assertEqual(dict(timed.seconds), {'game_paths':4})
        self.assertNotIn('private payload', repr(timed.seconds))

    async def test_query_and_result_failures_keep_timings_and_propagate(self):
        for fail_at in ('run', 'single'):
            transaction = AsyncMock()
            if fail_at == 'run':
                transaction.run.side_effect = ValueError('synthetic query failure')
            else:
                transaction.run.return_value.single.side_effect = ValueError('synthetic result failure')
            timed = TimedQueries(transaction)
            with patch('chessism_api.database.neo4j.sync_metrics.time.monotonic', side_effect=[10, 12]):
                with self.assertRaises(ValueError):
                    result = await timed.run('positions', 'synthetic query')
                    await result.single()
            self.assertEqual(dict(timed.seconds), {'positions':2})

    async def test_heartbeat_cleanup_is_idempotent(self):
        pulse = asyncio.create_task(asyncio.sleep(60))
        self.assertIsNone(await stop_heartbeat(pulse))
        self.assertTrue(pulse.cancelled())
        self.assertIsNone(await stop_heartbeat(pulse))
        self.assertIsNone(await stop_heartbeat(None))
