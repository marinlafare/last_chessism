"""Upload telemetry uses bounded reads, never job mutations or guessed counts."""
from concurrent.futures import Future
import unittest
from unittest.mock import Mock

from chessism_api.operations.cloud_analysis.upload_progress import observed_count
from cloud_billing.uploads import read_uploaded_tasks, UploadCache

RUN = 'a' * 32


class UploadCountsTests(unittest.TestCase):
    def test_counts_use_saved_task_sizes_including_short_last_task(self):
        tasks = [{'index': i, 'count': 5000 if i < 752 else 444} for i in range(753)]
        for indices, expected in (([], 0), ([0, 752], 5444), (list(range(753)), 3760444)):
            self.assertEqual(observed_count(tasks, {'state': 'ready', 'run_id': RUN,
                'tasks': indices}, RUN, 3760444), expected)

    def test_incomplete_or_mismatched_observations_do_not_become_zero(self):
        tasks = [{'index': 0, 'count': 5000}]
        self.assertIsNone(observed_count(tasks, {'state': 'pending'}, RUN, 5000))
        self.assertIsNone(observed_count(tasks, {'state': 'unavailable'}, RUN, 5000))
        for changes in ({'run_id': 'b' * 32}, {'tasks': [1]}, {'tasks': [0, 0]},
                        {'tasks': [False]}, {'tasks': '0'}, {'tasks': None}):
            with self.assertRaises(ValueError):
                observed_count(tasks, {'state': 'ready', 'run_id': RUN, 'tasks': [0], **changes}, RUN, 5000)
        with self.assertRaises(ValueError):
            observed_count(tasks, {'state': 'ready', 'run_id': RUN, 'tasks': [0]}, RUN, 5001)

    def test_inventory_scopes_only_exact_current_input_files(self):
        cloud = Mock()
        root = f'inputs/ui-{RUN}/tasks/'
        cloud.pages.return_value = [{'items': [
            {'name': root + '000002/input.jsonl', 'size': '120'},
            {'name': root + '000000/input.jsonl', 'size': '100'},
            {'name': root + '000001/input.jsonl', 'size': '0'},
            {'name': root + '000003/contract.json', 'size': '100'},
            {'name': f'inputs/ui-{"b" * 32}/tasks/000000/input.jsonl', 'size': '100'},
        ]}, {'items': [{'name': root + '000752/input.jsonl', 'size': '99'}]}]
        result = read_uploaded_tasks(RUN, cloud)
        self.assertEqual(result['tasks'], [0, 2, 752])
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(cloud.pages.call_args.kwargs['prefix'], root)
        self.assertNotIn('versions', cloud.pages.call_args.kwargs)
        cloud.request.assert_not_called()
        cloud.pages.reset_mock()
        for ident in ('../another', '', 'a' * 31):
            with self.assertRaises(ValueError): read_uploaded_tasks(ident, cloud)
        cloud.pages.assert_not_called()

    def test_out_of_range_inventory_fails_closed(self):
        cloud = Mock()
        cloud.pages.return_value = [{'items': [
            {'name': f'inputs/ui-{RUN}/tasks/001200/input.jsonl', 'size': '1'}]}]
        with self.assertRaises(ValueError): read_uploaded_tasks(RUN, cloud)

    def test_cache_never_waits_for_google_and_coalesces_readers(self):
        now = [100.0]
        future = Future()
        executor = Mock(submit=Mock(return_value=future))
        cache = UploadCache(cloud=Mock(), executor=executor, clock=lambda: now[0])
        self.assertEqual(cache.get(RUN)['state'], 'pending')
        self.assertEqual(cache.get(RUN)['state'], 'pending')
        executor.submit.assert_called_once()
        future.set_result({'state': 'ready', 'run_id': RUN, 'tasks': [0]})
        self.assertEqual(cache.get(RUN)['tasks'], [0])
        now[0] += 9
        self.assertEqual(cache.get(RUN)['tasks'], [0])
        executor.submit.assert_called_once()
        failing = Future()
        executor.submit.return_value = failing
        now[0] += 1
        self.assertEqual(cache.get(RUN)['tasks'], [0])
        failing.set_exception(RuntimeError('private transport diagnostic'))
        self.assertEqual(cache.get(RUN), {'state': 'unavailable', 'run_id': RUN})

    def test_cache_bounds_queued_observations(self):
        executor = Mock(submit=Mock(side_effect=lambda *args: Future()))
        cache = UploadCache(cloud=Mock(), executor=executor)
        for index in range(20): cache.get(f'{index:032x}')
        self.assertEqual(executor.submit.call_count, 8)
        self.assertEqual(len(cache.entries), 8)
