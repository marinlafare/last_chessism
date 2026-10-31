"""Disposable database only: cleanup progress is reporting, never authorization."""
import asyncio
import unittest
from sqlalchemy import select
import test_cloud_analysis_database as existing
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudCleanupProgress
from chessism_api.operations.cloud_analysis.cleanup_progress import observe, record
from chessism_api.routers.cloud_analysis import cloud_jobs


@unittest.skipUnless(existing.TEST_URL, 'requires explicitly isolated PostgreSQL')
class CleanupProgressTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown

    async def test_thread_acknowledgements_survive_sessions_and_are_exposed_without_payloads(self):
        callback = observe(self.job.id)
        await asyncio.to_thread(callback, 'a'*64, 'compute', 10533, None)
        await asyncio.to_thread(callback, 'a'*64, 'objects', 10533, 8400)
        data = await cloud_jobs()
        progress = next(j for j in data['jobs'] if j['id'] == self.job.id)['cleanup_progress']
        self.assertEqual((progress['phase'], progress['removed'], progress['total']), ('objects', 8400, 10533))
        await record(self.job.id, 'a'*64, 'image', 10533, None)
        async with AsyncDBSession() as session:
            row = await session.scalar(select(CloudCleanupProgress))
            self.assertEqual(row.removed, 8400)
        # A fresh authoritative inventory can correct the display after a lost
        # response; the cached count never permits deleting anything.
        await record(self.job.id, 'a'*64, 'objects', 10533, 8500)

    async def test_invalid_counters_and_changed_plan_total_fail_closed(self):
        await record(self.job.id, 'a'*64, 'objects', 100, 10)
        for args in [('a'*64, 'objects', 100, 101), ('a'*64, 'objects', 100, -1),
                     ('bad', 'objects', 100, 10), ('a'*64, 'done', 100, 10)]:
            with self.assertRaises(ValueError): await record(self.job.id, *args)
        with self.assertRaises(ValueError): await record(self.job.id, 'a'*64, 'objects', 101, 10)
        async with AsyncDBSession() as session:
            row = await session.scalar(select(CloudCleanupProgress))
            self.assertEqual((row.total, row.removed), (100, 10))

    async def test_cancellation_plans_are_separate_not_double_counted(self):
        await record(self.job.id, 'a'*64, 'objects', 100, 100)
        await record(self.job.id, 'b'*64, 'objects', 20, 5)
        data = await cloud_jobs()
        progress = next(j for j in data['jobs'] if j['id'] == self.job.id)['cleanup_progress']
        self.assertEqual((progress['removed'], progress['total']), (5, 20))


if __name__ == '__main__': unittest.main()
