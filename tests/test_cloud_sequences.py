"""Frozen multi-loop requests: offline contract and planner coverage."""
import os
import unittest
os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.operations.cloud_analysis.sequence import cycle_sizes
from cloud_job.work_units import ranges, spec_for
from cloud_job.batch_spot import config_for
from dataclasses import asdict


class SequencePlanTests(unittest.TestCase):
    def test_two_million_is_four_normal_500k_cloud_lifecycles(self):
        request = CloudJobRequest(mode='player', player_name='hikaru', total_fens=500000, repeat_count=4)
        self.assertEqual(request.target, 500000)
        self.assertNotIn('repeat_count', request.model_dump())
        sizes = cycle_sizes(request.target * request.repeat_count, request.repeat_count)
        self.assertEqual(sizes, [500000]*4)
        for cycle, count in enumerate(sizes):
            units = []
            for index, (vm, size) in enumerate(ranges(count, 1)):
                ident = f'{cycle * 10 + index + 1:032x}'
                units.append({'count': size, 'run_id': ident, 'config': asdict(config_for(ident, size))})
            spec = spec_for(units, 'us-central1-docker.pkg.dev/chessism-production/chessism-workers/test@sha256:' + 'a'*64)
            self.assertEqual(len(spec['taskGroups'][0]['taskSpec']['runnables']), 10)
            self.assertEqual(sum(u['count'] for u in units), 500000)

    def test_sizes_balance_one_frozen_set_and_never_create_empty_loops(self):
        self.assertEqual(cycle_sizes(7, 3), [3, 2, 2])
        self.assertEqual(cycle_sizes(2, 4), [1, 1])
        for args in ((2000001, 4), (0, 4), (1, 21), (True, 4), (10, False)):
            with self.assertRaises(ValueError): cycle_sizes(*args)

    def test_repetition_is_explicit_bounded_and_batch_only(self):
        self.assertEqual(CloudJobRequest().repeat_count, 1)
        for value in (0, 21, 2.5, True, '4'):
            with self.assertRaises(ValueError): CloudJobRequest(repeat_count=value)
        with self.assertRaisesRegex(ValueError, 'only for Batch'):
            CloudJobRequest(backend='cloud_run', repeat_count=4)


if __name__ == '__main__': unittest.main()
