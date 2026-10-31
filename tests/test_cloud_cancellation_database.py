"""Opt-in EMPTY isolated PostgreSQL, fake cloud; never the application database."""
from contextlib import ExitStack
import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch
from sqlalchemy import select, func
from fastapi import HTTPException
import test_cloud_analysis_database as existing
from test_cloud_work_units_database import WorkUnitDatabaseTests
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudJobCancellation, CloudFenClaim, Fen
from chessism_api.operations.cloud_analysis import controller, cancellation, stop_dispatcher
from chessism_api.routers.cloud_analysis import stop_and_cleanup, retry_cloud_job, resume_cloud_job
from cleaning_job.cloud import BUCKET
from cleaning_job.cloud import PROJECT, REGION, REPOSITORY
from cloud_job.launch import Client
from cloud_job.work_units import spec_for
from chessism_api.operations.cloud_analysis.work_units_controller import lanes


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL')
class CancellationDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown
    reserved_run = existing.CloudDatabaseTests.reserved_run
    root = WorkUnitDatabaseTests.root
    reload = WorkUnitDatabaseTests.reload
    reserve_units = WorkUnitDatabaseTests.reserve_units
    objects = WorkUnitDatabaseTests.objects

    async def test_priority_stop_dispatches_while_normal_processing_is_blocked(self):
        run = await self.reserved_run(2)
        launch = {'spec': {'taskGroups': []}, 'recovery_session': 1, 'jobs': []}
        await controller.save_run(run.id, launch=launch, status='running')
        cloud = self.fake_cloud()
        remote = {'name': f'projects/{PROJECT}/locations/{REGION}/jobs/chessism-ui-{run.id}-1',
                  'uid': 'priority-test-uid', 'status': {'state': 'RUNNING'}}
        cloud.jobs.return_value = [remote]
        started, release = asyncio.Event(), asyncio.Event()
        async def slow_import(*args):
            started.set()
            await release.wait()
        instance = controller.Controller(cloud, 'unused')
        with patch.object(instance, 'advance', side_effect=slow_import):
            work = asyncio.create_task(instance.tick())
            try:
                await asyncio.wait_for(started.wait(), 2)
                await stop_and_cleanup(self.job.id)
                self.assertIn(self.job.id, await stop_dispatcher.pending_stops())
                self.assertTrue(await asyncio.wait_for(stop_dispatcher.dispatch(instance, cloud, self.job.id), 2))
                self.assertFalse(work.done(), 'Stop must not depend on the import finishing')
                self.assertTrue(any(c.args[1].endswith(':cancel') for c in cloud.request.call_args_list))
                async with AsyncDBSession() as session:
                    current = await session.get(CloudAnalysisRun, run.id)
                    self.assertEqual(current.launch, launch, 'Priority signaling must not overwrite importer snapshots')
                    self.assertEqual((await session.get(CloudJobCancellation, self.job.id)).phase, 'stopping')
                cloud.delete_job.assert_not_called()
                cloud.submit.assert_not_called()
                await cancellation.save(self.job.id, phase='cleaning')
                calls = len(cloud.mock_calls)
                self.assertFalse(await stop_dispatcher.dispatch(instance, cloud, self.job.id))
                self.assertEqual(len(cloud.mock_calls), calls, 'No late marker writes during cleanup')
            finally:
                release.set()
                await work

    def fake_cloud(self, data=None):
        self.data = data or {}
        cloud = Mock()
        cloud.jobs.return_value = []
        cloud.run_resources.return_value = []
        cloud.resources.return_value = []
        cloud.recovery_executions.return_value = []
        cloud.images.return_value = []
        cloud.bucket.return_value = {'name': BUCKET}
        cloud.storage.return_value.read.side_effect = lambda uri, *args: self.data.get(uri)
        cloud.objects.side_effect = lambda prefix: [
            {'name': uri.removeprefix(f'gs://{BUCKET}/'), 'generation': '1', 'size': str(len(raw))}
            for uri, raw in self.data.items() if uri.startswith(f'gs://{BUCKET}/{prefix}')]
        cloud.delete_object.side_effect = lambda obj: self.data.pop(f"gs://{BUCKET}/{obj['name']}", None)
        return cloud

    async def finish(self, cloud):
        with patch.object(cancellation, 'refresh_unit_projections', new_callable=AsyncMock), \
             patch.object(cancellation, 'refresh_global_projections', new_callable=AsyncMock):
            for _ in range(40):
                await controller.Controller(cloud, 'not-used').tick()
                async with AsyncDBSession() as session:
                    job = await session.get(CloudAnalysisJob, self.job.id)
                self.assertNotEqual(job.status, 'paused', job.error)
                if job.status == 'cancelled':
                    return job
        self.fail('Cancellation did not finish')

    async def test_unstarted_stop_is_idempotent_releases_claims_and_never_launches(self):
        run = await self.reserved_run(2)
        first = await stop_and_cleanup(self.job.id)
        self.assertEqual(first, await stop_and_cleanup(self.job.id))
        cloud = self.fake_cloud()
        job = await self.finish(cloud)
        self.assertEqual(job.imported, 0)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            saved = await session.get(CloudAnalysisRun, run.id)
            self.assertTrue(saved.details_pruned)
            self.assertEqual(saved.position_count, 2)
            self.assertEqual((await session.get(CloudJobCancellation, self.job.id)).phase, 'complete')
        cloud.submit.assert_not_called()
        cloud.start_sequence.assert_not_called()
        with self.assertRaises(HTTPException):
            await retry_cloud_job(self.job.id)

    async def test_partial_results_survive_restart_and_cleanup(self):
        root = await self.reserve_units()
        data = await self.objects(root.launch['units'][0])
        cloud = self.fake_cloud(data)
        await stop_and_cleanup(self.job.id)
        job = await self.finish(cloud)
        self.assertEqual((job.target, job.imported, job.status), (3, 2, 'cancelled'))
        self.assertEqual(self.data, {})
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            self.assertEqual(await session.scalar(select(func.count()).select_from(Fen).where(Fen.score.is_not(None))), 2)
        cloud.start_recovery.assert_not_called()

    async def test_running_batch_waits_for_supervisor_then_cleans_exact_resources(self):
        root = await self.reserve_units()
        launch = dict(root.launch)
        launch['image'] = f"{REPOSITORY}ui-{root.launch['units'][0]['run_id']}@sha256:" + 'a' * 64
        launch['tasks'] = lanes(launch)
        for task in launch['tasks']:
            task['spec'] = spec_for([u for u in launch['units'] if u['vm_index'] == task['index']], launch['image'])
        await controller.save_run(root.id, launch=launch, status='running')
        task = launch['tasks'][0]
        name = f"chessism-ui-{task['run_id']}-1"
        remote = {**task['spec'], 'name': f'projects/{PROJECT}/locations/{REGION}/jobs/{name}',
                  'uid': 'cancel-test-uid-001', 'status': {'state': 'RUNNING'}}
        remotes = {name: remote}
        cloud = self.fake_cloud(await self.objects(launch['units'][0]))
        cloud.jobs.side_effect = lambda: list(remotes.values())
        cloud.job.side_effect = lambda name, region=None: remotes.get(name)
        cloud.resources.return_value = []
        cloud.logs.return_value = []
        cloud.log_entries.return_value = []
        cloud.check_job.side_effect = Client.check_job
        cloud.recovery_executions.return_value = [{'state': 'ACTIVE'}]
        cloud.request.side_effect = lambda *args, **kwargs: remote['status'].update(state='CANCELLED') or {}
        cloud.delete_job.side_effect = lambda name, region, uid: remotes.pop(name, None)
        await stop_and_cleanup(self.job.id)
        await controller.Controller(cloud, 'unused').tick()
        cloud.delete_job.assert_not_called()
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(CloudJobCancellation, self.job.id)).phase, 'stopping')
        cloud.recovery_executions.return_value = [{'state': 'SUCCEEDED'}]
        job = await self.finish(cloud)
        self.assertEqual((job.status, job.imported), ('cancelled', 2))
        cloud.delete_job.assert_called_once_with(name, REGION, 'cancel-test-uid-001')
        self.assertEqual(self.data, {})

    async def test_cleanup_failure_keeps_claims_and_resume_never_relaunches(self):
        await self.reserved_run(2)
        cloud = self.fake_cloud()
        await stop_and_cleanup(self.job.id)
        with patch.object(cancellation.cancelled_artifacts, 'apply', side_effect=ValueError('changed inventory')):
            for _ in range(12):
                await controller.Controller(cloud, 'unused').tick()
                async with AsyncDBSession() as session:
                    job = await session.get(CloudAnalysisJob, self.job.id)
                if job.status == 'paused':
                    break
        self.assertEqual(job.status, 'paused')
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(CloudFenClaim)), 2)
        await resume_cloud_job(self.job.id)
        await self.finish(cloud)
        cloud.submit.assert_not_called()
        cloud.start_sequence.assert_not_called()

    async def test_stop_intent_cannot_be_overwritten_by_a_finishing_tick(self):
        await stop_and_cleanup(self.job.id)
        for state in ('complete', 'failed', 'waiting', 'limit_reached'):
            await controller.save_job(self.job.id, status=state)
            async with AsyncDBSession() as session:
                self.assertEqual((await session.get(CloudAnalysisJob, self.job.id)).status, 'running')
