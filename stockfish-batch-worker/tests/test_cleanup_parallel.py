"""No real cloud calls: bounded readers and deletion barriers under races."""
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch

from cleaning_job.cloud import Cloud, CleanupError, REGION
from cleaning_job import cleanup
from cleaning_job.parallel import read_parallel
from test_cleaning_job import FakeCloud


class ParallelCleanupTests(unittest.TestCase):
    def test_nested_readers_share_eight_http_slots_and_release_after_failure(self):
        cloud = Cloud('never-called')
        barrier, lock = threading.Barrier(8, timeout=5), threading.Lock()
        active, peak = 0, 0
        def response(service, path, **kwargs):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                barrier.wait()
                barrier.wait()
                if path == '0':
                    raise CleanupError('fake HTTP failure')
                return path
            finally:
                with lock: active -= 1
        with patch.object(cloud, '_request', side_effect=response), ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(cloud.request, 'compute', str(i)) for i in range(16)]
            with self.assertRaisesRegex(CleanupError, 'fake HTTP'): futures[0].result()
            self.assertEqual([f.result() for f in futures[1:]], [str(i) for i in range(1,16)])
        self.assertEqual((active, peak), (0, 8))
        with patch.object(cloud, '_request', return_value='ready'):
            self.assertEqual(cloud.request('compute', 'next'), 'ready')

    def test_compute_inventories_overlap_but_failure_never_returns_partial_success(self):
        cloud = Cloud('never-called')
        barrier, completed = threading.Barrier(4, timeout=5), []
        def response(service, path, **kwargs):
            barrier.wait()
            completed.append(path)
            if path.endswith('/disks'):
                return {'unreachables':['zone']}
            return {}
        with patch.object(cloud, 'request', side_effect=response):
            with self.assertRaisesRegex(CleanupError, 'unreachable'): cloud.resources()
        self.assertEqual(len(completed), 4)

    def test_all_parallel_preconditions_finish_before_any_deletion(self):
        cloud = FakeCloud()
        plan = cleanup.prepare(cloud, ['batch-one'])
        barrier, done = threading.Barrier(4, timeout=5), threading.Event()
        def fail(*args):
            barrier.wait()
            raise CleanupError('bucket changed')
        def read(*args):
            barrier.wait()
            self.assertFalse(cloud.writes)
            done.set()
        with patch.object(cleanup, 'check_other_jobs', side_effect=read), \
             patch.object(cleanup, 'check_bucket', side_effect=fail), \
             patch.object(cleanup, 'check_inventory', side_effect=read), \
             patch.object(cleanup.image_cleanup, 'remaining', side_effect=read):
            with self.assertRaisesRegex(CleanupError, 'bucket changed'):
                cleanup.apply(cloud, plan, wait_seconds=0)
        self.assertTrue(done.is_set())
        self.assertFalse(cloud.writes)

    def test_job_identity_rechecked_after_parallel_reads_before_delete(self):
        cloud = FakeCloud()
        plan = cleanup.prepare(cloud, ['batch-one'])
        read = cloud.objects
        def replace(prefix):
            cloud.job_data[(REGION, 'batch-one')]['uid'] = 'replacement-12345678'
            return read(prefix)
        with patch.object(cloud, 'objects', side_effect=replace):
            with self.assertRaises(CleanupError): cleanup.apply(cloud, plan, wait_seconds=0)
        self.assertFalse(cloud.writes)

    def test_empty_reader_group_is_empty_without_threads(self):
        self.assertEqual(read_parallel(), [])


if __name__ == '__main__': unittest.main()
