"""No cloud credentials or live DB: delayed uploads, receipts and restart gates."""
import os
from dataclasses import asdict
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations.cloud_analysis import runtime, cancellation, cancellation_drain as drain
from chessism_api.operations.cloud_analysis.preparation import StageTimings
from cleaning_job.cloud import BUCKET, CleanupError
from cleaning_job.cleanup import digest, InventoryChanged
from cloud_job import cloud_run_cleanup
from cloud_job import cloud_run
from chessism_api.operations.cloud_analysis import controller as controller_module
from test_cloud_run_cleanup import RunCloud, RUN
from test_cleaning_job import obj
from stockfish_batch.checkpoints import digest as content_digest


def setup():
    cloud = RunCloud()
    root = SimpleNamespace(id=RUN, cleanup_plan=None, launch=cloud.launch, receipts={})
    task = {'index': 0, 'count': 1000, 'output': f'gs://{BUCKET}/results/ui-{RUN}/tasks/000000',
            'contract': {'fingerprint': 'fingerprint', 'position_count': 1000}}
    return cloud, root, [task]


class DrainTests(unittest.TestCase):
    def test_late_uploads_reset_quiet_window_even_after_google_is_terminal(self):
        cloud, root, tasks = setup()
        with patch.object(drain, 'monotonic', return_value=0) as clock:
            state = drain.Drain([root], None, [RUN], {RUN: tasks})
            state.observe(cloud)
            clock.return_value = 100
            self.assertFalse(state.ready())
            cloud.object_data.append(obj(f'results/ui-{RUN}/tasks/000000/batches/000001.json', '2'))
            state.observe(cloud)
            clock.return_value = 120
            self.assertFalse(state.ready())
            clock.return_value = 131
            self.assertTrue(state.ready())
            self.assertEqual(cloud.writes, [])

    def test_replacements_unknown_paths_and_out_of_range_batches_block(self):
        for filename in ('tasks/000001/batches/000000.json', 'tasks/000000/batches/000002.json',
                         'surprise.json', 'tasks/000000/../../other.json', 'tasks/000000/batches/000000.json'):
            cloud, root, tasks = setup()
            state = drain.Drain([root], None, [RUN], {RUN: tasks})
            state.observe(cloud)
            cloud.object_data.append(obj(f'results/ui-{RUN}/{filename}', '2'))
            with self.subTest(filename=filename), self.assertRaises(CleanupError):
                state.observe(cloud)
            self.assertEqual(cloud.writes, [])

    def test_new_file_during_validation_invalidates_approval(self):
        cloud, root, tasks = setup()
        state = drain.Drain([root], None, [RUN], {RUN: tasks})
        before = state.observe(cloud)
        cloud.object_data.append(obj(f'results/ui-{RUN}/tasks/000000/manifest.json', '2'))
        after = state.observe(cloud)
        with self.assertRaises(drain.LateUploads):
            state.approve(before, after)
        with self.assertRaises(drain.LateUploads):
            state.require_approved(after)

    def test_partial_cleanup_can_resume_but_cannot_erase_prior_generations(self):
        cloud, root, tasks = setup()
        root.cleanup_plan = cloud_run_cleanup.prepare(cloud, root.launch, RUN, cancelled=True)
        old = deepcopy(root.cleanup_plan)
        cloud.object_data.pop()  # A previous exact-generation delete succeeded.
        cloud.remote = None
        state = drain.Drain([root], None, [RUN], {RUN: tasks})
        observed = state.observe(cloud)
        state.approve(observed, observed)
        plan = drain.replan_run(cloud, root, state)
        self.assertEqual(plan, old)
        restarted = drain.Drain([SimpleNamespace(**{**vars(root), 'cleanup_plan': plan})], None, [RUN], {RUN: tasks})
        self.assertIsNone(restarted.approved)
        cloud.object_data.append(obj(f'results/ui-{RUN}/tasks/000000/batches/000000.json', '99'))
        with self.assertRaisesRegex(CleanupError, 'replaced'):
            restarted.observe(cloud)

    def test_replanning_preserves_image_and_job_identity(self):
        for changed in ('image', 'job'):
            cloud, root, tasks = setup()
            root.cleanup_plan = cloud_run_cleanup.prepare(cloud, root.launch, RUN, cancelled=True)
            state = drain.Drain([root], None, [RUN], {RUN: tasks})
            before = state.observe(cloud)
            state.approve(before, before)
            if changed == 'image':
                cloud.image_data[0]['uploadTime'] = 'replacement'
            else:
                cloud.remote['uid'] = 'replacement'
            with self.assertRaises(CleanupError):
                drain.replan_run(cloud, root, state)
            self.assertEqual(cloud.writes, [])

    def test_artifact_plan_cannot_hide_replacement_seen_by_run_plan(self):
        cloud, root, tasks = setup()
        root.cleanup_plan = cloud_run_cleanup.prepare(cloud, root.launch, RUN, cancelled=True)
        artifacts = drain.cancelled_artifacts.prepare(cloud, [RUN])
        artifacts['objects'][0]['generation'] = '99'
        artifacts['sha256'] = digest(artifacts)
        with self.assertRaisesRegex(CleanupError, 'replaced'):
            drain.Drain([root], artifacts, [RUN], {RUN: tasks})

    def test_continuing_uploads_pause_with_checkpoints_intact(self):
        cloud, root, tasks = setup()
        with patch.object(drain, 'monotonic', return_value=0) as clock:
            state = drain.Drain([root], None, [RUN], {RUN: tasks})
            state.observe(cloud)
            clock.return_value = 590
            cloud.object_data.append(obj(f'results/ui-{RUN}/tasks/000000/manifest.json', '2'))
            state.observe(cloud)
            clock.return_value = 601
            with self.assertRaisesRegex(CleanupError, 'ten minutes'):
                state.ready()
            self.assertEqual(cloud.writes, [])

    def test_result_cannot_be_cleaned_before_matching_durable_receipt(self):
        cloud, root, tasks = setup()
        objects = [cloud.object_data[-1]]
        storage = Mock()
        cloud.storage = Mock(return_value=storage)
        storage.read.return_value = b'result bytes'
        with self.assertRaisesRegex(CleanupError, 'durable receipt'):
            drain.verify_artifacts(cloud, root, tasks, objects)
        root.receipts = {'tasks/000000/batches/000000.json': {'sha256': content_digest(b'result bytes'), 'records': []}}
        drain.verify_artifacts(cloud, root, tasks, objects)
        storage.read.return_value = b'replacement bytes'
        with self.assertRaisesRegex(CleanupError, 'durable receipt'):
            drain.verify_artifacts(cloud, root, tasks, objects)
        self.assertEqual(cloud.writes, [])

    def test_manifest_cannot_claim_unimported_results(self):
        cloud, root, tasks = setup()
        storage = Mock()
        cloud.storage = Mock(return_value=storage)
        storage.read.return_value = b'{"status":"complete","fingerprint":"fingerprint","position_count":1000,"records":[]}'
        with self.assertRaisesRegex(CleanupError, 'manifest differs'):
            drain.verify_artifacts(cloud, root, tasks, [obj(f'results/ui-{RUN}/tasks/000000/manifest.json')])


class DrainRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_paused_cleanup_drains_imports_replans_both_plans_then_deletes(self):
        cloud, root, tasks = setup()
        root.launch.update(backend='cloud_run', config=asdict(cloud_run.config_for(RUN, 1000)), tasks=tasks)
        root.status, root.details_pruned = 'running', False
        root.cleanup_plan = cloud_run_cleanup.prepare(cloud, root.launch, RUN, cancelled=True)
        stop = SimpleNamespace(phase='cleaning', cursor=0, batch_plan=None,
            artifact_plan=drain.cancelled_artifacts.prepare(cloud, [RUN]))
        late = obj(f'results/ui-{RUN}/tasks/000000/batches/000001.json', '2')
        cloud.object_data.append(late)
        cloud.storage = Mock()
        job, controller = SimpleNamespace(id='job'), SimpleNamespace(cloud=cloud)
        session = AsyncMock()
        session.scalars.side_effect = lambda *args: Mock(all=Mock(return_value=
            [root] if session.scalars.await_count % 3 == 1 else [RUN]))
        session.get.return_value = None  # no Batch sequence controller
        manager = AsyncMock()
        manager.__aenter__.return_value = session
        imported, verified = [], []
        async def save(ident, **changes):
            for key, value in changes.items():
                setattr(stop, key, value)
        async def save_run(ident, **changes):
            for key, value in changes.items():
                setattr(root, key, value)
        async def import_results(cloud_arg, ident, *, observed_objects):
            self.assertIn(late, observed_objects)
            self.assertFalse(cloud.writes)
            imported.append(ident)
        def verify(*args):
            self.assertEqual(imported, [RUN])
            self.assertFalse(cloud.writes)
            verified.append(RUN)
        with patch.object(cancellation, 'AsyncDBSession', return_value=manager), \
             patch.object(cancellation, 'save', side_effect=save), \
             patch.object(controller_module, 'save_run', side_effect=save_run), \
             patch.object(cancellation, 'resources', AsyncMock(return_value=(True, []))), \
             patch.object(cancellation, 'import_available', side_effect=import_results), \
             patch.object(drain, 'verify_artifacts', side_effect=verify), \
             patch.object(drain, 'monotonic', return_value=0) as clock:
            await cancellation.advance(controller, job, stop, StageTimings())
            self.assertEqual(stop.phase, 'settling')
            self.assertFalse(cloud.writes)
            clock.return_value = 121
            for _ in range(4):
                await cancellation.advance(controller, job, stop, StageTimings())
            self.assertEqual(stop.phase, 'cleaning')
            self.assertEqual(verified, [RUN])
            self.assertIn(late, root.cleanup_plan['objects'])
            self.assertIn(late, stop.artifact_plan['objects'])
            self.assertFalse(cloud.writes)
            for _ in range(2):
                await cancellation.advance(controller, job, stop, StageTimings())
            self.assertEqual(stop.phase, 'logs')
            self.assertFalse(cloud.object_data)
            self.assertEqual([w[0] for w in cloud.writes].count('run'), 1)
        cloud.storage.assert_not_called()  # No launch or upload path entered.

    async def test_late_uploads_reenter_waiting_without_launching_or_clearing_plans(self):
        controller = SimpleNamespace(continue_immediately=True)
        with patch.object(cancellation, 'advance', AsyncMock(side_effect=drain.LateUploads('late'))), \
             patch.object(cancellation, 'save', AsyncMock()) as save, \
             patch.object(StageTimings, 'save', AsyncMock()):
            await cancellation.tick(controller, SimpleNamespace(id='job'), SimpleNamespace(phase='planning'))
        save.assert_awaited_once_with('job', phase='settling', cursor=0)
        self.assertFalse(controller.continue_immediately)

    async def test_cleanup_race_recovers_only_when_observation_accepts_new_files(self):
        for replacement in (False, True):
            state = Mock(approved=None)
            state.observe.side_effect = CleanupError('replaced') if replacement else []
            controller = SimpleNamespace(cloud=Mock(), cancellation_drains={'job': state})
            with patch.object(cancellation, 'advance', AsyncMock(side_effect=InventoryChanged('inventory changed'))), \
                 patch.object(cancellation, 'save', AsyncMock()) as save, \
                 patch.object(StageTimings, 'save', AsyncMock()):
                if replacement:
                    with self.assertRaisesRegex(CleanupError, 'replaced'):
                        await cancellation.tick(controller, SimpleNamespace(id='job'), SimpleNamespace(phase='cleaning'))
                    save.assert_not_awaited()
                else:
                    state.observe.side_effect = None
                    await cancellation.tick(controller, SimpleNamespace(id='job'), SimpleNamespace(phase='cleaning'))
                    save.assert_awaited_once_with('job', phase='settling', cursor=0)


if __name__ == '__main__':
    unittest.main()
