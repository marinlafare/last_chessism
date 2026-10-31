"""Offline preparation regressions: bounded work, fail-closed gates, timings."""
import asyncio
import os
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.fen_claims import unclaimed_locked_fens, locked_eligible_fens
from chessism_api.operations.cloud_analysis.preflight import parallel_checks, verify_clean_workspace
from chessism_api.operations.cloud_analysis.preparation import PreparationTimings
from chessism_api.operations.cloud_analysis import work_units_controller as fleet
from cleaning_job.cloud import CleanupError


class ParallelChecksTests(unittest.TestCase):
    def test_checks_overlap_with_at_most_four_threads_and_keep_result_order(self):
        barrier, lock = threading.Barrier(4, timeout=5), threading.Lock()
        active, peak = 0, 0
        def check(index):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            barrier.wait()
            with lock:
                active -= 1
            # Prevent the next group from entering before this group leaves.
            barrier.wait()
            return index
        self.assertEqual(parallel_checks(*(lambda i=i: check(i) for i in range(8))), list(range(8)))
        self.assertEqual((active, peak), (0, 4))

    def test_failure_joins_all_readers_and_never_returns_partial_success(self):
        barrier, done = threading.Barrier(3, timeout=5), threading.Event()
        def fail():
            barrier.wait()
            raise CleanupError('quota failed')
        def reader():
            barrier.wait()
            done.set()
        with self.assertRaisesRegex(CleanupError, 'quota failed'):
            parallel_checks(fail, reader, lambda: barrier.wait())
        self.assertTrue(done.is_set())

    def test_inventory_failure_is_not_treated_as_empty(self):
        cloud = Mock()
        for name in ('jobs', 'resources', 'run_resources', 'images', 'objects'):
            getattr(cloud, name).return_value = []
        cloud.images.side_effect = CleanupError('permission denied')
        with self.assertRaisesRegex(CleanupError, 'permission denied'):
            verify_clean_workspace(cloud)
        cloud.publish.assert_not_called()


class PreparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_safety_recheck_uses_5000_element_arrays_and_preserves_input_order(self):
        fens = [f'fen-{i}' for i in range(10001)]
        session = Mock()
        async def read(statement, params):
            self.assertIn('unnest', str(statement))
            self.assertIn('f.score IS NULL', str(statement))
            self.assertIn('NOT EXISTS', str(statement))
            return Mock(all=lambda: list(reversed([f for f in params['fens'] if f != 'fen-2'])))
        session.scalars = AsyncMock(side_effect=read)
        self.assertEqual(await unclaimed_locked_fens(session, fens), [f for f in fens if f != 'fen-2'])
        self.assertEqual([len(c.args[1]['fens']) for c in session.scalars.await_args_list], [5000, 5000, 1])
        session.scalars.reset_mock()
        self.assertEqual(await unclaimed_locked_fens(session, []), [])
        session.scalars.assert_not_awaited()

    async def test_selection_is_followed_by_a_separate_snapshot_and_separate_timings(self):
        session = Mock()
        session.execute = AsyncMock(return_value=Mock(scalars=lambda: Mock(all=lambda: ['a', 'b'])))
        session.scalars = AsyncMock(return_value=Mock(all=lambda: ['b']))
        timings = PreparationTimings()
        self.assertEqual(await locked_eligible_fens(session, 'locked selection', timings=timings), ['b'])
        self.assertEqual([c[0] for c in session.mock_calls], ['execute', 'scalars'])
        self.assertEqual([r['phase'] for r in timings.rows], ['prepare_select', 'prepare_recheck'])

    async def test_failed_preflight_never_inspects_publishes_or_reserves(self):
        for failed in ('inventory', 'quota', 'recovery', 'bucket'):
            cloud = Mock()
            controller = SimpleNamespace(cloud=cloud, local_image='unused')
            job = SimpleNamespace(id='a'*32, selection={'n_vms': 1})
            run = SimpleNamespace(id='b'*32, status='preparing', launch={})
            if failed == 'recovery':
                cloud.require_recovery.side_effect = CleanupError(failed)
            with self.subTest(failed=failed), \
                 patch.object(fleet, 'verify_clean_workspace', side_effect=CleanupError(failed) if failed == 'inventory' else None), \
                 patch.object(fleet.batch_spot, 'check_quota', side_effect=CleanupError(failed) if failed == 'quota' else None), \
                 patch.object(fleet, 'check_bucket', side_effect=CleanupError(failed) if failed == 'bucket' else None), \
                 patch.object(fleet, 'inspect_worker') as inspect, \
                 patch.object(fleet, 'reserve_unit', new_callable=AsyncMock) as reserve, \
                 patch.object(PreparationTimings, 'save', new_callable=AsyncMock):
                with self.assertRaisesRegex(CleanupError, failed):
                    await fleet.advance(controller, job, run)
                inspect.assert_not_called()
                reserve.assert_not_awaited()
                cloud.publish.assert_not_called()
                cloud.start_recovery.assert_not_called()

    async def test_failed_measurement_preserves_exception_and_records_failure(self):
        timings = PreparationTimings()
        with self.assertRaisesRegex(ValueError, 'bad FEN'):
            with timings.measure('prepare_validate'):
                raise ValueError('bad FEN')
        row, = timings.rows
        self.assertEqual(row['outcome'], 'failed')
        self.assertGreaterEqual(row['duration_seconds'], 0)
        self.assertLessEqual(row['started_at'], row['finished_at'])
        self.assertIn('bad FEN', row['error'])


if __name__ == '__main__':
    unittest.main()
