import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import FastAPI

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')

from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations import player_rating_distribution as ppd
from chessism_api.routers import players


def snapshot(rows):
    return {'version': 1, 'generated_at': '2026-10-19T12:00:00+00:00', 'counts': rows}


ROWS = [
    {'mode': 'blitz', 'rating': 2200, 'games': 1},
    {'mode': 'blitz', 'rating': 2209, 'games': 1},
    {'mode': 'blitz', 'rating': 2210, 'games': 3},
    {'mode': 'blitz', 'rating': 2239, 'games': 5},
    {'mode': 'rapid', 'rating': 2300, 'games': 10},
    {'mode': 'bullet', 'rating': None, 'games': 2},
    {'mode': 'bullet', 'rating': 0, 'games': 1},
]


class DistributionTests(unittest.TestCase):
    def test_exact_ten_point_boundaries_and_zero_intervals(self):
        data = ppd.percentage_distribution('hikaru', 'blitz', snapshot(ROWS))
        self.assertEqual(data['x'], [2200, 2210, 2220, 2230])
        self.assertEqual(data['y'], [20, 30, 0, 50])
        self.assertEqual(data['games_count'], 10)
        self.assertEqual(sum(data['y']), 100)
        self.assertEqual(data['y_unit'], 'percent')

    def test_modes_nulls_and_nonpositive_ratings(self):
        data = ppd.percentage_distribution('hikaru', 'all', snapshot(ROWS))
        self.assertEqual(data['games_count'], 20)
        self.assertEqual(data['excluded_games_count'], 3)
        self.assertEqual(data['total_games_count'], 23)
        self.assertEqual(data['y'][0], 10)
        self.assertEqual(data['y'][-1], 50)
        self.assertAlmostEqual(sum(data['y']), 100)
        for mode in ('bullet', 'rapid'):
            selected = ppd.percentage_distribution('hikaru', mode, snapshot(ROWS))
            self.assertEqual(selected['x'], [] if mode == 'bullet' else [2300])
            self.assertEqual(selected['y'], [] if mode == 'bullet' else [100])

    def test_empty_single_negative_and_extreme_ranges(self):
        for rows in ([], [{'mode': 'rapid', 'rating': -1, 'games': 10}]):
            data = ppd.percentage_distribution('a', 'all', snapshot(rows))
            self.assertEqual(data['x'], [])
            self.assertEqual(data['y'], [])
        with self.assertRaises(ValueError):
            ppd.percentage_distribution('a', 'all', snapshot([
                {'mode': 'rapid', 'rating': 1, 'games': 1},
                {'mode': 'rapid', 'rating': 2_000_000_000, 'games': 1},
            ]))

    def test_selection_is_normalized_and_restricted(self):
        self.assertEqual(ppd.normalize_selection(' Hikaru ', 'BLITZ'), ('hikaru', 'blitz'))
        for name, mode in (('', 'all'), ('a' * 129, 'all'), ('hikaru', 'daily')):
            with self.assertRaises(ValueError):
                ppd.normalize_selection(name, mode)


class DistributionServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.redis = AsyncMock()
        self.redis.get.return_value = None
        self.session = AsyncMock()
        self.session.info = {SESSION_KEY: RelationalStorage('candidate', 'archive', 'reads', 'relational_v3')}
        self.session.scalar.return_value = 'hikaru'
        result = MagicMock()
        result.mappings.return_value.all.return_value = ROWS
        self.session.execute.return_value = result
        self.context = MagicMock()
        self.context.__aenter__ = AsyncMock(return_value=self.session)
        self.context.__aexit__ = AsyncMock(return_value=False)

    async def call(self, mode='blitz'):
        with patch.object(ppd, 'AsyncDBSession', return_value=self.context), \
                patch.object(ppd, 'READ_SLOTS', asyncio.Semaphore(2)):
            return await ppd.get_player_rating_distribution('Hikaru', mode, self.redis)

    async def test_cache_hit_does_not_group_games(self):
        self.redis.get.return_value = json.dumps(snapshot(ROWS))
        data = await self.call()
        self.assertTrue(data['cache_hit'])
        self.session.scalar.assert_awaited_once()
        self.assertFalse(any('COUNT(' in str(call.args[0]) for call in self.session.execute.call_args_list))

    async def test_miss_reads_own_rating_and_caches_all_modes(self):
        data = await self.call()
        self.assertEqual(data['y'], [20, 30, 0, 50])
        query = str(self.session.execute.call_args.args[0])
        self.assertIn('CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END', query)
        self.assertIn('gp.player_id=', query)
        self.assertIn('GROUP BY gp.mode, gp.rating', query)
        self.assertNotIn('fen', query.lower())
        self.assertNotIn('LIMIT', query)
        self.assertEqual(self.session.execute.call_args.args[1], {'player': 'hikaru'})
        self.assertEqual(self.redis.set.call_args.kwargs, {'ex': 60})
        saved = json.loads(self.redis.set.call_args.args[1])
        self.assertEqual(saved['counts'], ROWS)
        self.redis.get.return_value = json.dumps(saved)
        data = await self.call('rapid')
        self.assertEqual(data['y'], [100])
        self.assertTrue(data['cache_hit'])

    async def test_deleted_player_cannot_use_cache(self):
        self.session.scalar.return_value = None
        self.redis.get.return_value = json.dumps(snapshot(ROWS))
        with self.assertRaises(LookupError):
            await self.call()
        self.redis.get.assert_not_awaited()

    async def test_invalid_cache_is_replaced(self):
        self.redis.get.return_value = 'broken json'
        self.assertFalse((await self.call())['cache_hit'])
        self.redis.set.assert_awaited_once()


class DistributionRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_contract_mode_validation_and_errors(self):
        app = FastAPI()
        app.include_router(players.router, prefix='/players')
        async def mock_redis():
            return SimpleNamespace()
        app.dependency_overrides[players.get_redis_pool] = mock_redis
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            expected = ppd.percentage_distribution('hikaru', 'blitz', snapshot(ROWS))
            with patch.object(players, 'get_player_rating_distribution', AsyncMock(return_value=expected)) as operation:
                response = await client.get('/players/hikaru/ppd?mode=blitz')
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)
                self.assertEqual(response.headers['Cache-Control'], 'private, no-store')
                response = await client.get('/players/hikaru/ppd?mode=invalid')
                self.assertEqual(response.status_code, 422)
                operation.assert_awaited_once()
            for error, status in ((LookupError('missing'), 404), (ValueError('bad'), 422), (ppd.DistributionBusy('busy'), 503)):
                with patch.object(players, 'get_player_rating_distribution', AsyncMock(side_effect=error)):
                    response = await client.get('/players/hikaru/ppd')
                    self.assertEqual(response.status_code, status)
                    if status == 503:
                        self.assertEqual(response.headers['Retry-After'], '3')
