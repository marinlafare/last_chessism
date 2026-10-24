import csv
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from dev_tools.analysis_telemetry.recorder import atomic_json, progress_rate, record
from dev_tools.analysis_telemetry.report import Summary, flat, stats
from dev_tools.analysis_telemetry.system import SystemSampler


def sample(at, count, phase='analyzing', temperature=75):
    return {'timestamp_utc': f'time-{at}', 'elapsed_seconds': at, 'errors': [],
            'job': {'status': 'in_progress', 'progress': {'total': 1000, 'processed': count, 'phase': phase}},
            'engine': {'workers': {'busy': 5 if phase == 'analyzing' else 0, 'total': 5}},
            'system': {'package_c': temperature, 'cpu_max_c': temperature,
                       'hardware_throttle_counts': {'cpu0': {'package_throttle_count': 4}}}}


class ReportTests(unittest.TestCase):
    def test_rate_ignores_reset_and_marks_cooldown_boundary(self):
        self.assertEqual(progress_rate(sample(0, 10), sample(15, 40)), (2, False))
        self.assertEqual(progress_rate(sample(0, 40), sample(15, 10)), (None, False))
        self.assertEqual(progress_rate(sample(0, 40), sample(15, 40, 'cooling')), (0, True))
        self.assertEqual(progress_rate(None, sample(0, 40)), (None, False))

    def test_cooldown_and_actual_counter_changes(self):
        summary = Summary()
        for row in [sample(0, 10), sample(15, 20, 'cooling', 74), sample(30, 20, 'cooling', 61), sample(45, 22)]:
            summary.add(row)
        report = summary.payload()
        self.assertFalse(report['hardware_thermal_throttling_observed'])
        self.assertEqual(report['cooldowns'][0]['minimum_package_c'], 61)
        self.assertEqual(report['cooldowns'][0]['observed_seconds'], 30)
        last = sample(60, 30, temperature=86)
        last['system']['hardware_throttle_counts']['cpu0']['package_throttle_count'] = 6
        summary.add(last)
        self.assertTrue(summary.payload()['hardware_thermal_throttling_observed'])
        self.assertEqual(summary.payload()['samples_at_or_above_85c'], 1)

    def test_missing_sensors_are_not_reported_as_zero(self):
        summary = Summary()
        summary.add({'timestamp_utc': 'x', 'elapsed_seconds': 0, 'errors': ['sensor unavailable']})
        report = summary.payload()
        self.assertIsNone(report['sampled_package_c']['max'])
        self.assertIsNone(report['hardware_thermal_throttling_observed'])
        self.assertEqual(report['collection_error_samples'], 1)
        self.assertEqual(stats([1, 2, 3])['p95'], 3)

    def test_empty_system_and_atomic_report_are_json_safe(self):
        with TemporaryDirectory() as root:
            system = SystemSampler({}, cpu_root=root, group_root=root, proc_root=root, hwmon_root=root).sample()
            self.assertIsNone(system['package_c'])
            self.assertEqual(system['cpu_usage'], {})
            path = Path(root) / 'summary.json'
            atomic_json(path, system)
            self.assertEqual(json.loads(path.read_text()), system)
            self.assertFalse(path.with_suffix('.tmp').exists())


class RecorderTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_job_flushes_files_and_never_changes_redis(self):
        with TemporaryDirectory() as root:
            output = Path(root) / 'recording'
            args = SimpleNamespace(output_dir=str(output), job_id='test', container=[], interval=1,
                                   max_hours=1, finish_grace=0)
            redis = AsyncMock()
            redis.get.return_value = json.dumps({'processed': 1000, 'total': 1000, 'phase': 'complete'})
            job = AsyncMock()
            job.status.return_value = SimpleNamespace(value='complete')
            job.info.return_value = SimpleNamespace(function='run_analysis_job', enqueue_time='now',
                                                    kwargs={'nodes_limit': 1000000, 'game_links': [1, 2]})
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc)
            job.result_info.return_value = SimpleNamespace(success=True, start_time=now,
                                                           finish_time=now + timedelta(seconds=20), result=None)
            client = AsyncMock()
            client.__aenter__.return_value = client
            client.get.return_value = SimpleNamespace(raise_for_status=lambda: None,
                                                      json=lambda: {'workers': {'total': 5, 'busy': 0}})
            with patch('dev_tools.analysis_telemetry.recorder.Redis', return_value=redis), \
                    patch('dev_tools.analysis_telemetry.recorder.Job', return_value=job), \
                    patch('dev_tools.analysis_telemetry.recorder.httpx.AsyncClient', return_value=client), \
                    patch('dev_tools.analysis_telemetry.recorder.SystemSampler.sample', return_value={}):
                await record(args)
            report = json.loads((output / 'summary.json').read_text())
            self.assertEqual(report['recording_status'], 'finished')
            self.assertEqual(report['stop_reason'], 'job_finished')
            self.assertEqual(report['job_result']['duration_seconds'], 20)
            self.assertEqual(len((output / 'samples.jsonl').read_text().splitlines()), 1)
            with (output / 'metrics.csv').open() as stream:
                self.assertEqual(len(list(csv.DictReader(stream))), 1)
            self.assertNotIn('game_links', (output / 'metadata.json').read_text())
            self.assertEqual({call[0] for call in redis.mock_calls}, {'get', 'aclose'})


if __name__ == '__main__':
    unittest.main()
