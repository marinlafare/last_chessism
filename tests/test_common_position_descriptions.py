import json
import unittest
from unittest.mock import AsyncMock

from test_common_positions import E4, NOW, result
from chessism_api.operations import common_positions as positions
from chessism_api.operations import common_position_descriptions as cache
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY

TOP = [{'fen': E4, 'fen_id': 123, 'appearances': 42}]
LAYOUT = RelationalStorage('public', 'chessism_archive', 'chessism_reads', 'relational_v3')
MODAL = {'n_move': 1, 'move_color': 'white', 'san': 'e4', 'label_occurrences': 40, 'example_game_link': 7}


class DescriptionCacheTests(unittest.IsolatedAsyncioTestCase):
    def test_keys_exclude_page_rank_but_include_source_count_and_schema(self):
        key = cache.description_keys(TOP, NOW.isoformat(), LAYOUT)
        self.assertEqual(key, cache.description_keys([{**TOP[0], 'rank': 71}], NOW.isoformat(), LAYOUT))
        self.assertNotEqual(key, cache.description_keys(TOP, 'new-snapshot', LAYOUT))
        self.assertNotEqual(key, cache.description_keys([{**TOP[0], 'appearances': 43}], NOW.isoformat(), LAYOUT))
        self.assertNotEqual(key, cache.description_keys(TOP, NOW.isoformat(), None))
        self.assertEqual(cache.description_keys(TOP, None, LAYOUT), [None])

    async def test_completed_descriptions_survive_page_failure_without_requerying(self):
        session, redis = AsyncMock(), AsyncMock()
        session.info = {SESSION_KEY: LAYOUT}
        stored = {}
        async def mget(keys): return [stored.get(key) for key in keys]
        async def save(key, value, **kwargs): stored[key] = value
        redis.mget.side_effect = mget
        redis.set.side_effect = save
        session.execute.side_effect = [result([MODAL]), result([{'n_move': 1, 'white_move': 'e4', 'black_move': 'e5'}])]
        first = await positions.describe_positions(session, TOP, redis=redis, snapshot=NOW.isoformat())
        self.assertEqual(first[0]['example_line'], '1. e4')
        self.assertEqual(redis.set.call_args.kwargs['ex'], 86400)
        session.execute.reset_mock()
        again = await positions.describe_positions(session, TOP, 11, redis=redis, snapshot=NOW.isoformat())
        session.execute.assert_not_awaited()
        self.assertEqual(again, [{**first[0], 'rank': 11}])
        self.assertNotIn('rank', json.loads(next(iter(stored.values()))))

    async def test_invalid_cache_and_redis_outage_fall_back_to_database(self):
        redis = AsyncMock()
        redis.mget.return_value = ['not json', '{}', '[]', None]
        self.assertEqual(await cache.load_descriptions(redis, ['a', 'b', 'c', 'd']), [None] * 4)
        redis.mget.side_effect = RuntimeError('offline')
        with self.assertLogs(cache.LOG, level='WARNING'):
            self.assertEqual(await cache.load_descriptions(redis, ['a']), [None])
        redis.set.side_effect = RuntimeError('offline')
        with self.assertLogs(cache.LOG, level='WARNING'):
            await cache.save_description(redis, 'a', {**MODAL, 'example_line': '1. e4'})

    async def test_unknown_snapshot_never_reuses_description_cache(self):
        redis = AsyncMock()
        self.assertEqual(await cache.load_descriptions(redis, [None]), [None])
        await cache.save_description(redis, None, {})
        redis.mget.assert_not_awaited()
        redis.set.assert_not_awaited()
