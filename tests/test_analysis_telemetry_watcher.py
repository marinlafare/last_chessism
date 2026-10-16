"""Automatic telemetry follows running jobs without controlling their queue."""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from arq.jobs import JobStatus
from dev_tools.analysis_telemetry import watcher


class WatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_active_analysis_jobs_are_selected(self):
        redis = AsyncMock()
        redis.zrange.return_value = [b'queued', b'other', b'active']
        jobs = {}
        for key, status, function in (
            ('queued', JobStatus.queued, 'run_analysis_job'),
            ('other', JobStatus.in_progress, 'run_tablebase_analysis_job'),
            ('active', JobStatus.in_progress, 'run_player_games_analysis_job'),
        ):
            jobs[key] = AsyncMock()
            jobs[key].status.return_value = status
            jobs[key].info.return_value = SimpleNamespace(function=function)
        with patch.object(watcher, 'Job', side_effect=lambda key, *args, **kwargs: jobs[key]):
            self.assertEqual(await watcher.active_analysis_job(redis), 'active')
        jobs['queued'].info.assert_not_called()
        self.assertEqual({call[0] for call in redis.mock_calls}, {'zrange'})

    async def test_queue_discovery_is_paginated(self):
        redis = AsyncMock()
        redis.zrange.side_effect = [[str(i).encode() for i in range(100)], []]
        job = AsyncMock()
        job.status.return_value = JobStatus.queued
        with patch.object(watcher, 'Job', return_value=job):
            self.assertIsNone(await watcher.active_analysis_job(redis))
        self.assertEqual(redis.zrange.call_args.args, ('analysis_queue', 100, 199))

    def test_recordings_are_separate_and_paths_cannot_escape_output_root(self):
        with TemporaryDirectory() as root:
            first = watcher.recording_args(root, '../../job/1', 15)
            second = watcher.recording_args(root, '../../job/1', 15)
            self.assertEqual(Path(first.output_dir).parent, Path(root))
            self.assertNotEqual(first.output_dir, second.output_dir)
            self.assertEqual(first.job_id, '../../job/1')
            self.assertEqual(first.container, [])
            self.assertEqual(first.finish_grace, 0)

    async def test_jobs_and_stop_share_one_event_without_changing_redis(self):
        stop = asyncio.Event()
        redis = AsyncMock()
        recordings = []

        async def fake_record(args, *, stop):
            recordings.append(args)
            if len(recordings) == 2:
                stop.set()
            return 'job_finished'

        with TemporaryDirectory() as root, \
                patch.object(watcher, 'Redis', return_value=redis), \
                patch.object(watcher, 'active_analysis_job', new_callable=AsyncMock) as active, \
                patch.object(watcher, 'record', side_effect=fake_record) as record:
            active.side_effect = [None, 'first', 'second']
            await watcher.watch(root, interval=.001, stop=stop)
            self.assertEqual([args.job_id for args in recordings], ['first', 'second'])
            self.assertTrue(all(call.kwargs['stop'] is stop for call in record.await_args_list))
            self.assertEqual(json.loads((Path(root) / 'watcher.json').read_text())['status'], 'stopped')
        self.assertEqual({call[0] for call in redis.mock_calls}, {'aclose'})

    async def test_discovery_outage_is_retried_without_affecting_analysis(self):
        stop = asyncio.Event()
        redis = AsyncMock()

        async def finish(*args, **kwargs):
            stop.set()

        with TemporaryDirectory() as root, \
                patch.object(watcher, 'Redis', return_value=redis), \
                patch.object(watcher, 'active_analysis_job', new_callable=AsyncMock) as active, \
                patch.object(watcher, 'record', side_effect=finish):
            active.side_effect = [ConnectionError('test outage'), 'recovered']
            await watcher.watch(root, interval=.001, stop=stop)
            self.assertEqual(active.await_count, 2)
        redis.aclose.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
