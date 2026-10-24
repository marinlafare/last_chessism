"""Shared leases protect both preview routes, including cancellation/outages."""
import asyncio
import unittest
from unittest.mock import AsyncMock

from fastapi import HTTPException
from redis.exceptions import ConnectionError as RedisConnectionError

from chessism_api.operations.exports.preview import preview_slot, RELEASE_LEASE


class PreviewSlotTests(unittest.IsolatedAsyncioTestCase):
    async def test_modes_share_the_same_owner_checked_slot(self):
        redis = AsyncMock()
        for analytical in (False, True):
            async with preview_slot(redis, 'client', analytical=analytical):
                key, owner = redis.set.call_args.args
                self.assertEqual(key, 'export-preview:client')
                self.assertEqual(redis.set.call_args.kwargs, {'nx': True, 'ex': 30})
            redis.eval.assert_awaited_with(RELEASE_LEASE, 1, key, owner)

    async def test_busy_and_unavailable_slots_never_run_or_release(self):
        for analytical in (False, True):
            for unavailable in (False, True):
                redis = AsyncMock()
                redis.set.return_value = False
                if unavailable:
                    redis.set.side_effect = RedisConnectionError('private detail')
                with self.assertRaises(HTTPException) as error:
                    async with preview_slot(redis, 'client', analytical=analytical):
                        self.fail('Busy/unavailable preview admitted')
                self.assertEqual(error.exception.status_code, 503 if unavailable else 429)
                self.assertNotIn('private detail', str(error.exception.detail))
                redis.eval.assert_not_awaited()

    async def test_query_error_and_cancellation_release_without_masking_original(self):
        for error in (ValueError('query failed'), asyncio.CancelledError()):
            redis = AsyncMock()
            redis.eval.side_effect = RedisConnectionError('release failed')
            with self.assertRaises(type(error)):
                async with preview_slot(redis, 'client'):
                    raise error
            redis.eval.assert_awaited_once()

    async def test_release_failure_does_not_discard_success(self):
        redis = AsyncMock()
        redis.eval.side_effect = RedisConnectionError('release failed')
        async with preview_slot(redis, 'client'):
            result = {'rows': [[0]]}
        self.assertEqual(result, {'rows': [[0]]})
