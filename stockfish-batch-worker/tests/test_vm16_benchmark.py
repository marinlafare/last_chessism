import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from benchmark_vm16 import run
from benchmark_vm16 import finish
from stockfish_batch.config import parse_args
from stockfish_batch.metrics import MemorySeries, Sampler


class VM16Tests(unittest.TestCase):
    ident = '20261006c016'
    image = run.REPOSITORY + 'vm16-20261006c016@sha256:' + 'a' * 64

    def test_one_spot_vm_sixteen_single_thread_engines_and_bounded_cost(self):
        config = run.configuration(self.ident)
        spec = run.specification(self.ident, self.image)
        self.assertEqual(len(spec['taskGroups']), 1)
        group = spec['taskGroups'][0]
        self.assertEqual((group['taskCount'], group['parallelism'], group['taskCountPerNode']), ('1', '1', '1'))
        task = group['taskSpec']
        self.assertEqual(task['computeResource'], {'cpuMilli': '16000', 'memoryMib': '12288'})
        self.assertEqual(task['maxRunDuration'], '1800s')
        self.assertEqual(task['maxRetryCount'], 0)
        self.assertEqual(len(task['runnables']), 1)
        container = task['runnables'][0]['container']
        self.assertEqual(parse_args(container['commands']), config)
        self.assertIn('--memory=12g --memory-swap=12g', container['options'])
        policy = spec['allocationPolicy']['instances'][0]['policy']
        self.assertEqual(policy['machineType'], 'n2d-highcpu-16')
        self.assertEqual(policy['provisioningModel'], 'SPOT')
        self.assertEqual((config.workers, config.threads, config.hash_mb), (16, 1, 256))
        self.assertEqual((config.nodes, config.multipv, config.batch_size), (100000, 4, 500))
        self.assertEqual((config.max_positions, config.stall_timeout, config.run_timeout), (25000, 300, 1740))

    def test_rejects_other_images_and_unsafe_identity(self):
        for ident in ('', '../foo', '20261006C016'):
            with self.assertRaises(ValueError):
                run.configuration(ident)
        for image in (run.REPOSITORY + 'other@sha256:' + 'a' * 64,
                      run.REPOSITORY + 'vm16-20261006c016:latest'):
            with self.assertRaises((ValueError, RuntimeError)):
                run.specification(self.ident, image)

    def test_quota_blocks_insufficient_cpu_and_memory_disk(self):
        quotas = [{'metric': key, 'limit': limit, 'usage': 0} for key, limit in
            [('N2D_CPUS', 16), ('PREEMPTIBLE_CPUS', 0), ('INSTANCES', 24),
             ('IN_USE_ADDRESSES', 8), ('SSD_TOTAL_GB', 500)]]
        client = Mock()
        client.request.side_effect = [{'quotas': quotas}, {'quotas': [{'metric': 'CPUS_ALL_REGIONS', 'limit': 32, 'usage': 0}]}]
        run.quota_check(client)
        for metric in ('N2D_CPUS', 'INSTANCES', 'IN_USE_ADDRESSES', 'SSD_TOTAL_GB'):
            changed = copy.deepcopy(quotas)
            next(q for q in changed if q['metric'] == metric)['usage'] = next(q for q in changed if q['metric'] == metric)['limit']
            client.request.side_effect = [{'quotas': changed}, {'quotas': [{'metric': 'CPUS_ALL_REGIONS', 'limit': 32, 'usage': 0}]}]
            with self.subTest(metric=metric), self.assertRaises(ValueError):
                run.quota_check(client)

    def test_no_retry_after_lost_submission_response(self):
        client = Mock()
        client.storage.return_value.read.return_value = b'input'
        client.submit.side_effect = TimeoutError('response lost')
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'input.jsonl').write_bytes(b'input')
            plan = {'id': self.ident, 'job': run.job_name(self.ident), 'specification': {}}
            with patch.object(run, 'load', return_value=plan), patch.object(run, 'assert_idle'), patch.object(run, 'quota_check', return_value={}):
                with self.assertRaises(TimeoutError):
                    run.launch(directory, client)
                with self.assertRaises(FileExistsError):
                    run.launch(directory, client)
            self.assertEqual(client.submit.call_count, 1)

    def test_launch_rejects_changed_cloud_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'input.jsonl').write_bytes(b'original')
            client = Mock()
            client.storage.return_value.read.return_value = b'changed'
            with patch.object(run, 'load', return_value={'id': self.ident}), patch.object(run, 'assert_idle'), patch.object(run, 'quota_check', return_value={}):
                with self.assertRaises(ValueError):
                    run.launch(directory, client)
            client.submit.assert_not_called()

    def test_completed_cleanup_status_and_no_zero_progress_claim(self):
        client = Mock()
        client.job.return_value = None
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            run.save(directory, 'cleanup-result.json', {'complete': True})
            with patch.object(run, 'load', return_value={'id': self.ident, 'job': run.job_name(self.ident)}):
                status = run.status(directory, client)
            self.assertEqual(status['state'], 'CLEANED')
            self.assertIsNone(status['uploaded_fens'])
            client.objects.assert_not_called()

    def test_collect_refuses_running_or_failed_jobs(self):
        for state in ('RUNNING', 'QUEUED', 'FAILED'):
            with patch.object(run, 'load', return_value={}), patch.object(run, 'status', return_value={'state': state}):
                with self.assertRaises(ValueError):
                    run.collect(Path('/unused'), Mock())


class MemoryTests(unittest.TestCase):
    def test_summary_has_range_mean_and_approximate_p95(self):
        series = MemorySeries()
        self.assertEqual(series.summary(), {'samples': 0})
        for value in range(1, 101):
            series.add(value * 1024 * 1024)
        result = series.summary()
        self.assertEqual(result['samples'], 100)
        self.assertEqual(result['min_bytes'], 1024 * 1024)
        self.assertEqual(result['max_bytes'], 100 * 1024 * 1024)
        self.assertEqual(result['mean_bytes'], 50.5 * 1024 * 1024)
        self.assertEqual(result['p95_upper_bound_bytes'], 96 * 1024 * 1024)
        series.add(-1)
        series.add(2 * MemorySeries.ceiling)
        self.assertEqual(series.summary(), result)

    def test_cgroup_and_host_measurements_are_distinct(self):
        files = {'/sys/fs/cgroup/memory.current': '1048576',
                 '/sys/fs/cgroup/memory.peak': '2097152',
                 '/sys/fs/cgroup/memory.events': 'oom_kill 0\n',
                 '/proc/stat': 'cpu 100 0 0 100 0 0 0 0\n',
                 '/proc/meminfo': 'MemTotal: 16777216 kB\nMemAvailable: 12582912 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n'}
        def read(path):
            if str(path) not in files:
                raise FileNotFoundError(str(path))
            return files[str(path)]
        with patch.object(Path, 'read_text', read):
            sampler = Sampler()
            sampler.sample_memory()
            memory = sampler.finish()['memory']
        self.assertEqual(memory['worker']['max_bytes'], 1024 * 1024)
        self.assertEqual(memory['cgroup_peak_bytes'], 2 * 1024 * 1024)
        self.assertEqual(memory['host_used_excluding_reclaimable']['max_bytes'], 4 * 1024 ** 3)
        self.assertEqual(memory['host_available']['min_bytes'], 12 * 1024 ** 3)
        self.assertEqual(memory['host_swap_used']['max_bytes'], 0)
        self.assertEqual(memory['cgroup_oom_kills_delta'], 0)

    def test_missing_metrics_are_unknown_not_zero(self):
        with patch.object(Path, 'read_text', side_effect=OSError('unavailable')):
            metrics = Sampler().finish()['memory']
        self.assertEqual(metrics['worker'], {'samples': 0})
        self.assertEqual(metrics['host_available'], {'samples': 0})
        self.assertIsNone(metrics['cgroup_peak_bytes'])
        self.assertIsNone(metrics['cgroup_oom_kills_delta'])

    def test_v1_memory_fallback(self):
        def read(path):
            if str(path) == '/sys/fs/cgroup/memory/memory.usage_in_bytes':
                return '1234'
            raise OSError('missing')
        with patch.object(Path, 'read_text', read):
            result = Sampler().finish()['memory']
        self.assertEqual(result['worker']['max_bytes'], 1234)


class FinishTests(unittest.TestCase):
    def test_log_ownership_is_exact_vm_and_job_not_time_window(self):
        owner = finish.owners('chessism-vm16-test-uid', ['us-central1-f/123456'])
        self.assertIn('resource.labels.job_id="chessism-vm16-test-uid"', owner)
        self.assertIn('resource.labels.instance_id="123456"', owner)
        self.assertIn('resource.labels.project_id="chessism-production"', owner)
        self.assertNotIn('timestamp', owner)
        for uid, instances in [('bad" OR true', ['us-central1-f/123']),
                               ('valid', []), ('valid', ['elsewhere/123'])]:
            with self.assertRaises(ValueError):
                finish.owners(uid, instances)

    def test_cleanup_requires_saved_inspection(self):
        with tempfile.TemporaryDirectory() as temporary:
            client = Mock()
            with patch.object(finish, 'load', return_value={}), patch.object(finish, 'cleaning_job') as clean:
                with self.assertRaises(ValueError):
                    finish.cleanup(Path(temporary), client)
                clean.assert_not_called()

    def test_malformed_saved_job_does_not_authorize_deletion(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            run.save(directory, 'comparison-summary.json', {})
            run.save(directory, 'pre-cleanup-job.json', {'name': 'other-job', 'status': {'state': 'SUCCEEDED'}})
            with patch.object(finish, 'load', return_value={'job': 'expected-job'}), patch.object(finish, 'cleaning_job') as clean:
                with self.assertRaises(ValueError):
                    finish.cleanup(directory, Mock())
                clean.assert_not_called()


if __name__ == '__main__':
    unittest.main()
