"""Pure scheduling and mocked worker checks; never touches a live engine or DB."""

import asyncio
from contextlib import ExitStack
import unittest
from unittest.mock import AsyncMock, patch

from chessism_api.operations import analysis
from chessism_api.operations.analysis_scheduling import balanced_analysis_batch_size


class BatchSizingTests(unittest.TestCase):
    def size(self, total, requested=1000, workers=4, limit=1000):
        return balanced_analysis_batch_size(total, requested, concurrency=workers, service_limit=limit)

    def test_five_thousand_uses_two_balanced_waves(self):
        self.assertEqual(self.size(5000), 625)
        self.assertEqual(5000 // self.size(5000), 8)

    def test_already_balanced_work_and_one_worker_keep_the_cap(self):
        self.assertEqual(self.size(4000), 1000)
        self.assertEqual(self.size(24000, 500), 500)
        self.assertEqual(self.size(501, workers=1, limit=500), 500)
        self.assertEqual(self.size(5000, workers=5), 1000)
        self.assertEqual(self.size(1000, requested=500, workers=5, limit=500), 200)

    def test_small_and_partial_targets(self):
        self.assertEqual(self.size(100), 25)
        self.assertEqual(self.size(3), 1)
        self.assertEqual(self.size(501, limit=500), 126)
        self.assertEqual(self.size(0), 1000)

    def test_limits_and_coverage_for_different_pool_sizes(self):
        for workers in (1, 2, 3, 4, 5, 8):
            for requested in (1, 3, 100, 500, 1000):
                for total in (1, 5, 499, 501, 3999, 5000, 10001, 24000):
                    size = self.size(total, requested, workers, limit=500)
                    self.assertGreaterEqual(size, 1)
                    self.assertLessEqual(size, min(requested, 500))
                    full, tail = divmod(total, size)
                    self.assertEqual(full * size + tail, total)

    def test_invalid_controls_are_rejected(self):
        for requested, workers, limit in ((0, 4, 1000), (1000, 0, 1000), (1000, 4, 0)):
            with self.assertRaises(ValueError):
                self.size(5000, requested, workers, limit)


class BalancedWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_queue_runs_two_full_waves_without_duplicates(self):
        await self.assert_balanced_run(5000, workers=4, size=625, rounds=2, batch_limit=1000)

    async def test_thousand_fen_test_keeps_all_five_workers_busy(self):
        await self.assert_balanced_run(1000, workers=5, size=200, rounds=1, batch_limit=500)

    async def assert_balanced_run(self, total, *, workers, size, rounds, batch_limit):
        fetched = []
        engine_batches = []
        waves = []
        wave_events = [asyncio.Event() for _ in range(rounds)]
        active = 0
        maximum_active = 0
        next_fen = 0

        async def fetch_batch(limit):
            nonlocal next_fen
            fens = [f'fen-{i}' for i in range(next_fen, next_fen + limit)]
            next_fen += limit
            fetched.append(limit)
            return AsyncMock(), fens

        async def call_engine(client, url, fens, nodes, **kwargs):
            nonlocal active, maximum_active
            self.assertEqual(nodes, 1_000_000)
            engine_batches.append(fens)
            wave = (len(engine_batches) - 1) // workers
            active += 1
            maximum_active = max(maximum_active, active)
            if len(engine_batches) % workers == 0:
                waves.append(len(engine_batches))
                wave_events[wave].set()
            await asyncio.wait_for(wave_events[wave].wait(), timeout=2)
            active -= 1
            return [{'fen': fen, 'is_valid': True, 'analysis': {'score': 0, 'pv': [], 'time': 0.01}} for fen in fens]

        with ExitStack() as stack:
            stack.enter_context(patch.object(analysis, 'ANALYSIS_CONCURRENCY', workers))
            stack.enter_context(patch.object(analysis, '_call_engine_service', side_effect=call_engine))
            for name in ('record_analysis_times', '_increment_summary_for_analysis_results', '_refresh_scored_projections_after_analysis'):
                stack.enter_context(patch.object(analysis, name, new_callable=AsyncMock))
            stack.enter_context(patch.object(analysis.fen_interface, 'update_fen_analysis_data', new_callable=AsyncMock))
            result = await asyncio.wait_for(analysis._run_analysis_job(
                {}, total_fens_to_process=total, batch_size=batch_limit, nodes_limit=1_000_000,
                fetch_batch=fetch_batch, timing_source='test', job_id='BALANCED TEST',
                fallback_arq_job_id='test', no_more_message='done', max_batch_size=batch_limit,
            ), timeout=10)

        self.assertEqual(fetched, [size] * workers * rounds)
        self.assertEqual(waves, [workers * (i + 1) for i in range(rounds)])
        self.assertEqual(maximum_active, workers)
        self.assertEqual(len({fen for batch in engine_batches for fen in batch}), total)
        self.assertEqual(result['processed'], total)
        self.assertEqual(result['engine_processed'], total)
        self.assertEqual(result['failed_batches'], 0)


if __name__ == '__main__':
    unittest.main()
