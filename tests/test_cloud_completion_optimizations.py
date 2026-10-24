"""Offline completion checks: no cloud credentials, network or live database."""
import json
import os
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations.cloud_analysis import importer
from chessism_api.operations.cloud_analysis.cloud_run_results import verify_tasks
from chessism_api.operations.cloud_analysis.preparation import StageTimings
from cloud_job import cloud_run as api
from stockfish_batch.storage import MAX_MANIFEST_BYTES
from chessism_api.operations.cloud_analysis.artifacts import PERFORMANCE_MAX_BYTES


def completion_fixture(count=12):
    config = api.config_for('a' * 32, 500)
    positions, tasks, receipts, artifacts = [], [], {}, {}
    for index in range(count):
        row = {'id': str(index), 'fen': 'unused'}
        contract = {'fingerprint': f'fingerprint-{index}', 'position_count': 1}
        positions.append(row)
        tasks.append({'index': index, 'start': index, 'count': 1, 'contract': contract})
        record = {'id': row['id'], 'object': 'batches/000000.json', 'sha256': 'b' * 64}
        prefix = f'tasks/{index:06d}/'
        receipts[prefix + record['object']] = {'sha256': 'c' * 64,
            'records': [{**record, 'object': prefix + record['object']}]}
        output = api.task_config(config, index, count).output
        artifacts[output + '/manifest.json'] = {'status': 'complete', **contract, 'records': [record]}
        artifacts[output + '/performance.json'] = {'schema_version': 1, **contract,
            'resumed': 0, 'analyzed_this_attempt': 1,
            'workers': [{'worker': 0, 'positions': 1, 'analysis_seconds': 1, 'upload_wait_seconds': 0}],
            'metrics': {'worker_seconds': 1, 'fen_per_second': 1}}
    return config, tasks, positions, receipts, artifacts


class VerificationTests(unittest.TestCase):
    def test_parallel_reads_are_bounded_thread_local_ordered_and_complete(self):
        config, tasks, positions, receipts, artifacts = completion_fixture()
        lock, barrier = threading.Lock(), threading.Barrier(4, timeout=5)
        active, peak, instances, reads = 0, 0, [], []
        parent = self

        class Storage:
            def __init__(self):
                self.owner = threading.get_ident()
                with lock:
                    instances.append(self)

            def read(self, uri, limit):
                nonlocal active, peak
                parent.assertEqual(threading.get_ident(), self.owner)
                parent.assertEqual(limit, MAX_MANIFEST_BYTES if uri.endswith('manifest.json') else PERFORMANCE_MAX_BYTES)
                with lock:
                    active += 1
                    peak = max(peak, active)
                    reads.append(uri)
                try:
                    if uri.endswith('manifest.json'):
                        barrier.wait()
                    time.sleep(.005)
                    return json.dumps(artifacts[uri]).encode()
                finally:
                    with lock:
                        active -= 1

        reports = verify_tasks(Storage, config, tasks, positions, receipts)
        self.assertEqual([r['fingerprint'] for r in reports], [t['contract']['fingerprint'] for t in tasks])
        self.assertEqual((active, peak, len(instances)), (0, 4, 4))
        self.assertCountEqual(reads, artifacts)

    def test_missing_corrupt_or_uncommitted_results_fail_closed(self):
        for fault in ('manifest', 'performance', 'hash', 'receipt', 'permission'):
            with self.subTest(fault=fault):
                config, tasks, positions, receipts, artifacts = completion_fixture(1)
                output = api.task_config(config, 0, 1).output
                if fault in ('manifest', 'performance'):
                    del artifacts[output + '/' + fault + '.json']
                elif fault == 'hash':
                    artifacts[output + '/manifest.json']['records'][0]['sha256'] = 'wrong'
                elif fault == 'receipt':
                    receipts = {}
                storage = Mock()
                def read(uri, limit):
                    if fault == 'permission':
                        raise PermissionError('denied')
                    return json.dumps(artifacts[uri]).encode() if uri in artifacts else None
                storage.read.side_effect = read
                with self.assertRaises(PermissionError if fault == 'permission' else ValueError):
                    verify_tasks(lambda: storage, config, tasks, positions, receipts)

    def test_failure_drains_current_readers_without_scheduling_later_tasks(self):
        config, tasks, positions, receipts, artifacts = completion_fixture()
        lock, barrier = threading.Lock(), threading.Barrier(4, timeout=5)
        active, seen = 0, []

        class Storage:
            def read(self, uri, limit):
                nonlocal active
                index = int(uri.split('/tasks/')[1].split('/')[0])
                with lock:
                    active += 1
                    seen.append(index)
                try:
                    if uri.endswith('manifest.json'):
                        barrier.wait()
                        if index == 0:
                            raise TimeoutError('read timed out')
                    time.sleep(.01)
                    return json.dumps(artifacts[uri]).encode()
                finally:
                    with lock:
                        active -= 1

        with self.assertRaises(TimeoutError):
            verify_tasks(Storage, config, tasks, positions, receipts)
        self.assertEqual(active, 0)
        self.assertEqual(set(seen), {0, 1, 2, 3})
        # Retrying the same immutable data verifies the complete task set.
        storage = Mock()
        storage.read.side_effect = lambda uri, limit: json.dumps(artifacts[uri]).encode()
        self.assertEqual(len(verify_tasks(lambda: storage, config, tasks, positions, receipts)), 12)

    def test_invalid_concurrency_never_reads_storage(self):
        fixture = completion_fixture(1)
        factory = Mock()
        for concurrency in (0, 5, True, 1.5):
            with self.assertRaises(ValueError):
                verify_tasks(factory, *fixture[:4], concurrency=concurrency)
        factory.assert_not_called()


class ProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_run_uses_one_sql_union_and_strict_affected_refresh(self):
        session = AsyncMock()
        session.scalars.side_effect = [Mock(all=Mock(return_value=['run:positions'])),
                                      Mock(all=Mock(return_value=[101, 202]))]
        session.__aenter__.return_value = session
        timings = StageTimings()
        with patch.object(importer, 'AsyncDBSession', return_value=session), \
             patch('chessism_api.database.ask_db.refresh_game_analysis_summary', new_callable=AsyncMock) as refresh:
            await importer.refresh_projections(run_id='run', timings=timings)
        self.assertEqual(session.scalars.await_count, 2)
        query, params = session.scalars.call_args.args
        self.assertIn('SELECT DISTINCT gfa.game_link', str(query))
        self.assertIn('unnest(p.fen)', str(query))
        self.assertEqual(params, {'refs': ['run:positions']})
        refresh.assert_awaited_once_with([101, 202], strict=True)
        self.assertEqual([r['phase'] for r in timings.rows],
                         ['summary_affected_games', 'summary_game_coverage_and_players'])

    async def test_missing_reference_fails_and_empty_selection_never_refreshes_every_game(self):
        for refs, error in (([], True), ([None], True), (['run:positions'], False)):
            session = AsyncMock()
            session.scalars.side_effect = [Mock(all=Mock(return_value=refs)), Mock(all=Mock(return_value=[]))]
            session.__aenter__.return_value = session
            with patch.object(importer, 'AsyncDBSession', return_value=session), \
                 patch('chessism_api.database.ask_db.refresh_game_analysis_summary', new_callable=AsyncMock) as refresh:
                if error:
                    with self.assertRaisesRegex(ValueError, 'Missing work-unit'):
                        await importer.refresh_projections(run_id='run')
                else:
                    await importer.refresh_projections(run_id='run')
                refresh.assert_not_awaited()
