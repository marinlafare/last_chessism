import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')

from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations import player_score_distribution as scores
from chessism_api.operations import player_score_histogram as batches


class QueryTests(unittest.TestCase):
    def test_histogram_does_not_materialize_distinct_positions_before_scoring(self):
        session = SimpleNamespace(info={
            SESSION_KEY: RelationalStorage('candidate', 'archive', 'reads', 'relational_v3'),
        })
        query = scores.positions_sql(session)
        self.assertIn('"candidate"."game_fen_association"', query)
        self.assertNotIn('GROUP BY', query)
        self.assertNotIn('occurrences AS MATERIALIZED', query)
        self.assertIn('WHEN f.score IS NULL', query)
        self.assertIn("f.analysis_source='tablebase'", query)
        self.assertIn('WHEN o.is_white THEN f.score ELSE -f.score', query)

    def test_bins_and_page_cursor_contract(self):
        bins = scores.bins()
        self.assertEqual(len(bins), 13)
        self.assertEqual((bins[6]['lower'], bins[6]['upper']), (-50, 50))
        self.assertTrue(bins[6]['lower_inclusive'])
        self.assertTrue(bins[6]['upper_inclusive'])
        cursor = scores.encode_cursor('selection', 120, 54321, 0)
        self.assertEqual(scores.decode_cursor(cursor, 'selection'), (120, 54321, 0))
        with self.assertRaises(ValueError):
            scores.decode_cursor(cursor, 'other-selection')


class SharedCalculationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pending = patch.object(scores, 'IN_FLIGHT', {})
        self.pending.start()
        self.addCleanup(self.pending.stop)
        progress = patch.object(scores, 'PROGRESS', {})
        progress.start()
        self.addCleanup(progress.stop)

    async def test_polled_job_keeps_running_and_returns_same_finished_result(self):
        release = asyncio.Event()

        async def calculate():
            await release.wait()
            return {'counts': [1, 2, 3]}

        factory = AsyncMock(side_effect=calculate)
        self.assertIsNone(await scores.shared_calculation('polled', factory, wait_seconds=0.001))
        self.assertEqual(scores.PROGRESS['polled']['phase'], 'queued')
        release.set()
        result = await scores.shared_calculation('polled', factory, wait_seconds=0.1)
        self.assertEqual(result, {'counts': [1, 2, 3]})
        self.assertEqual(await scores.shared_calculation('polled', factory, wait_seconds=0.1), result)
        factory.assert_awaited_once()

    async def test_polled_failure_is_reported_without_restart_loop(self):
        factory = AsyncMock(side_effect=scores.ScoreDistributionBusy('failed'))
        for _ in range(2):
            with self.assertRaises(scores.ScoreDistributionBusy):
                await scores.shared_calculation('failure', factory, wait_seconds=0.1)
        factory.assert_awaited_once()

    async def test_same_selection_calculates_once_for_concurrent_callers(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def calculate():
            entered.set()
            await release.wait()
            return {'counts': [3, 2, 1]}

        factory = AsyncMock(side_effect=calculate)
        first = asyncio.create_task(scores.shared_calculation('same', factory))
        await entered.wait()
        second = asyncio.create_task(scores.shared_calculation('same', factory))
        await asyncio.sleep(0)
        release.set()
        a, b = await asyncio.gather(first, second)
        self.assertIs(a, b)
        factory.assert_awaited_once()
        self.assertEqual(scores.IN_FLIGHT, {})

    async def test_remount_can_join_after_original_client_disconnects(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def calculate():
            entered.set()
            await release.wait()
            return 'done'

        factory = AsyncMock(side_effect=calculate)
        first = asyncio.create_task(scores.shared_calculation('player', factory))
        await entered.wait()
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertFalse(scores.IN_FLIGHT['player'].cancelled())
        second = asyncio.create_task(scores.shared_calculation('player', factory))
        release.set()
        self.assertEqual(await second, 'done')
        factory.assert_awaited_once()

    async def test_failed_calculation_is_removed_and_retry_can_succeed(self):
        factory = AsyncMock(side_effect=[ValueError('failed'), 'retry result'])
        with self.assertRaises(ValueError):
            await scores.shared_calculation('player', factory)
        self.assertNotIn('player', scores.IN_FLIGHT)
        self.assertEqual(await scores.shared_calculation('player', factory), 'retry result')

    async def test_deadline_cancels_work_and_releases_query_slot(self):
        entered = asyncio.Event()

        async def calculate():
            async with scores.read_slot():
                entered.set()
                await asyncio.Event().wait()

        semaphore = asyncio.Semaphore(1)
        with patch.object(scores, 'READ_SLOTS', semaphore), \
                patch.object(scores, 'CALCULATION_SECONDS', 0.05):
            with self.assertRaises(scores.ScoreDistributionBusy):
                await scores.shared_calculation('slow', calculate)
        self.assertTrue(entered.is_set())
        self.assertFalse(semaphore.locked())
        self.assertEqual(scores.IN_FLIGHT, {})

    async def test_pending_limit_does_not_reject_duplicate_request(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def calculate():
            entered.set()
            await release.wait()
            return 'done'

        factory = AsyncMock(side_effect=calculate)
        with patch.object(scores, 'MAX_IN_FLIGHT', 1):
            first = asyncio.create_task(scores.shared_calculation('same', factory))
            await entered.wait()
            with self.assertRaises(scores.ScoreDistributionBusy):
                await scores.shared_calculation('different', factory)
            second = asyncio.create_task(scores.shared_calculation('same', factory))
            await asyncio.sleep(0)
            release.set()
            self.assertEqual(await asyncio.gather(first, second), ['done', 'done'])
        factory.assert_awaited_once()


class HistogramServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_counts_cache_and_snapshot_contract(self):
        session = AsyncMock()
        session.info = {SESSION_KEY: RelationalStorage('candidate', 'archive', 'reads', 'relational_v3')}
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=session)
        context.__aexit__ = AsyncMock(return_value=False)
        endings = MagicMock()
        counts = [
            {'mode': 'bullet', 'score_kind': 'cp', 'bin_id': 6, 'positions': 7},
            {'mode': 'bullet', 'score_kind': 'unscored', 'bin_id': None, 'positions': 2},
            {'mode': 'bullet', 'score_kind': 'mate', 'bin_id': None, 'positions': 1},
        ]
        endings.mappings.return_value.all.return_value = [{'mode': 'bullet', 'games': 3}]
        session.execute.side_effect = [MagicMock(), endings]
        redis = AsyncMock()
        redis.get.return_value = None
        with patch.object(scores, 'AsyncDBSession', return_value=context), \
                patch.object(scores, 'prepare', AsyncMock()), \
                patch.object(scores, 'source_state', AsyncMock(return_value='snapshot')), \
                patch.object(scores, 'read_histogram_batches', AsyncMock(return_value=counts)) as reader, \
                patch.object(scores, 'READ_SLOTS', asyncio.Semaphore(1)):
            payload = await scores.calculate_distribution('player', scores.MODES, redis)
        self.assertIn('unnest(', reader.call_args.args[2])
        self.assertEqual(reader.call_args.args[4], 3)
        self.assertEqual(payload['by_mode']['bullet']['counts'][6], 7)
        self.assertEqual(payload['by_mode']['bullet']['unscored_positions'], 2)
        self.assertEqual(payload['by_mode']['bullet']['mate_positions'], 1)
        self.assertEqual(payload['snapshot'], 'snapshot')
        self.assertEqual(redis.set.call_args.kwargs['ex'], 86400)

    async def test_pending_route_returns_progress_not_an_error(self):
        import json
        from chessism_api.routers import players
        pending = {'status': 'calculating', 'progress': {'completed_games': 1000, 'total_games': 139553}}
        with patch.object(players, 'get_player_score_distribution', AsyncMock(return_value=pending)):
            response = await players.api_get_player_score_distribution('danielnaroditsky', AsyncMock(), False)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.headers['Retry-After'], '1')
        self.assertEqual(json.loads(response.body), pending)


class BatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.games = [
            [{'game_link': 10, 'is_white': True, 'mode': 'bullet'},
             {'game_link': 10, 'is_white': False, 'mode': 'bullet'}],
            [{'game_link': 20, 'is_white': False, 'mode': 'blitz'}],
        ]
        stream = MagicMock()
        stream.mappings.return_value = stream
        async def partitions(size):
            self.assertEqual(size, batches.BATCH_GAMES)
            for page in self.games:
                yield page
        stream.partitions.side_effect = partitions
        stream.close = AsyncMock()
        self.stream = stream
        self.session = AsyncMock()
        self.session.stream.return_value = stream
        self.updates = []

    async def call(self, total=3):
        return await batches.read_histogram_batches(
            self.session, 'SELECT game_link,is_white,mode FROM games',
            'WITH classified AS (SELECT * FROM scores)', {'player': 'test'},
            total, self.updates.append)

    async def test_all_batches_contribute_exact_counts_and_progress(self):
        results = []
        for rows in (
            [{'mode': 'bullet', 'score_kind': 'cp', 'bin_id': 6, 'positions': 4},
             {'mode': 'bullet', 'score_kind': 'unscored', 'bin_id': None, 'positions': 2}],
            [{'mode': 'bullet', 'score_kind': 'cp', 'bin_id': 6, 'positions': 5},
             {'mode': 'blitz', 'score_kind': 'mate', 'bin_id': None, 'positions': 1}],
        ):
            result = MagicMock()
            result.mappings.return_value.all.return_value = rows
            results.append(result)
        self.session.execute.side_effect = results
        data = await self.call()
        self.assertEqual(next(row['positions'] for row in data if row['score_kind'] == 'cp'), 9)
        self.assertEqual([row['completed_games'] for row in self.updates], [0, 2, 3])
        self.assertEqual(self.session.execute.call_args_list[0].args[1], {
            'game_links': [10, 10], 'player_sides': [True, False], 'game_modes': ['bullet', 'bullet']})
        self.stream.close.assert_awaited_once()

    async def test_failed_batch_closes_cursor_and_returns_no_partial_histogram(self):
        self.session.execute.side_effect = RuntimeError('read failed')
        with self.assertRaisesRegex(RuntimeError, 'read failed'):
            await self.call()
        self.stream.close.assert_awaited_once()

    async def test_incomplete_game_selection_is_rejected(self):
        result = MagicMock()
        result.mappings.return_value.all.return_value = []
        self.session.execute.return_value = result
        with self.assertRaisesRegex(RuntimeError, 'refusing partial'):
            await self.call(total=4)
        self.stream.close.assert_awaited_once()


class SnapshotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.session = AsyncMock()
        self.session.info = {}
        self.context = MagicMock()
        self.context.__aenter__ = AsyncMock(return_value=self.session)
        self.context.__aexit__ = AsyncMock(return_value=False)
        self.payload = {
            'version': 1, 'snapshot': 'old', 'stale': False,
            'generated_at': datetime.now(timezone.utc).isoformat(),
        }

    async def test_revision_uses_data_not_routine_summary_refresh_time(self):
        stamp = datetime.now(timezone.utc)
        counts = MagicMock()
        counts.first.return_value = (123, 456, 100, 356, 90)
        self.session.execute.return_value = counts
        self.session.scalar.side_effect = ['player', stamp, 'player', stamp]
        first = await scores.source_state(self.session, 'player')
        second = await scores.source_state(self.session, 'player')
        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)
        self.assertNotIn('refreshed_at', str(self.session.execute.call_args.args[0]))
        self.assertIn('ORDER BY analyzed_at DESC, fen_id DESC', str(self.session.scalar.call_args.args[0]))

    async def test_stale_histogram_is_immediate_until_refresh_requested(self):
        operation = AsyncMock(return_value={'snapshot': 'new', 'stale': False})
        with patch.object(scores, 'AsyncDBSession', return_value=self.context), \
                patch.object(scores, 'prepare', AsyncMock()), \
                patch.object(scores, 'source_state', AsyncMock(return_value='new')), \
                patch.object(scores, 'read_cache', AsyncMock(return_value=self.payload)), \
                patch.object(scores, 'shared_calculation', operation):
            data = await scores.get_distribution('player', AsyncMock())
            self.assertTrue(data['stale'])
            self.assertEqual(data['snapshot'], 'old')
            operation.assert_not_awaited()
            data = await scores.get_distribution('player', AsyncMock(), refresh=True)
            self.assertFalse(data['stale'])
            operation.assert_awaited_once()

    def test_retained_histogram_becomes_stale_after_freshness_window(self):
        self.assertTrue(scores.histogram_is_fresh(self.payload, 'old'))
        self.payload['generated_at'] = (datetime.now(timezone.utc) - timedelta(minutes=16)).isoformat()
        self.assertFalse(scores.histogram_is_fresh(self.payload, 'old'))
        self.assertFalse(scores.histogram_is_fresh(self.payload, 'different'))

    async def test_first_page_rebases_an_old_chart_without_rejection(self):
        chunk = {'version': 1, 'snapshot': 'new', 'has_more': True, 'rows': [
            {'fen_id': str(n + 1), 'appearances': 20 - n} for n in range(10)
        ]}
        with patch.object(scores, 'AsyncDBSession', return_value=self.context), \
                patch.object(scores, 'prepare', AsyncMock()), \
                patch.object(scores, 'source_state', AsyncMock(return_value='new')), \
                patch.object(scores, 'read_cache', AsyncMock(return_value=chunk)) as cache:
            data = await scores.get_positions('player', AsyncMock(), bin_id=6,
                modes=['bullet'], snapshot='old')
        self.assertEqual(data['snapshot'], 'new')
        self.assertTrue(data['source_changed'])
        self.assertEqual(len(data['rows']), 10)
        scope = scores.key_for('selection', 'player', ('bullet',), 6, 'new')
        self.assertEqual(scores.decode_cursor(data['next_cursor'], scope), (11, 10, 0))
        self.assertEqual(cache.call_args.args[1], scores.key_for('page', scope, None, None))

    async def test_cached_next_page_survives_source_changes(self):
        chunk = {'version': 1, 'snapshot': 'old', 'has_more': False, 'rows': [
            {'fen_id': str(n + 1), 'appearances': 20 - n} for n in range(10, 20)
        ]}
        scope = scores.key_for('selection', 'player', ('bullet',), 6, 'old')
        cursor = scores.encode_cursor(scope, 11, 10, 0)
        with patch.object(scores, 'AsyncDBSession', return_value=self.context), \
                patch.object(scores, 'prepare', AsyncMock()), \
                patch.object(scores, 'source_state', AsyncMock(return_value='new')), \
                patch.object(scores, 'read_cache', AsyncMock(return_value=chunk)):
            data = await scores.get_positions('player', AsyncMock(), bin_id=6,
                modes=['bullet'], snapshot='old', cursor=cursor)
        self.assertEqual(data['rows'], chunk['rows'])
        self.assertEqual(data['snapshot'], 'old')

    async def test_uncached_continuation_does_not_mix_changed_rankings(self):
        scope = scores.key_for('selection', 'player', ('bullet',), 6, 'old')
        cursor = scores.encode_cursor(scope, 2, 100, 0)
        with patch.object(scores, 'AsyncDBSession', return_value=self.context), \
                patch.object(scores, 'prepare', AsyncMock()), \
                patch.object(scores, 'source_state', AsyncMock(return_value='new')), \
                patch.object(scores, 'read_cache', AsyncMock(return_value=None)):
            with self.assertRaises(scores.ScoreDistributionChanged):
                await scores.get_positions('player', AsyncMock(), bin_id=6,
                    modes=['bullet'], snapshot='old', cursor=cursor)

    async def test_first_page_uses_snapshot_that_was_calculated_after_queuing(self):
        chunk = {'version': 1, 'snapshot': 'newer', 'has_more': True, 'rows': [
            {'fen_id': str(n + 1), 'appearances': 20 - n} for n in range(10)
        ]}
        with patch.object(scores, 'AsyncDBSession', return_value=self.context), \
                patch.object(scores, 'prepare', AsyncMock()), \
                patch.object(scores, 'source_state', AsyncMock(return_value='new')), \
                patch.object(scores, 'read_cache', AsyncMock(return_value=None)), \
                patch.object(scores, 'shared_calculation', AsyncMock(return_value=chunk)):
            data = await scores.get_positions('player', AsyncMock(), bin_id=6,
                modes=['bullet'], snapshot='old')
        self.assertEqual(data['snapshot'], 'newer')
        scope = scores.key_for('selection', 'player', ('bullet',), 6, 'newer')
        self.assertEqual(scores.decode_cursor(data['next_cursor'], scope), (11, 10, 0))


if __name__ == '__main__':
    unittest.main()
