"""Cancellation safety with fake Google only; no credentials or billable calls."""
import asyncio
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations.cloud_analysis import runtime, cancellation
from chessism_api.operations.cloud_analysis.preparation import StageTimings
from chessism_api.operations.cloud_analysis.cancellation_cloud import stop_batch, cancel_batch
from chessism_api.operations.cloud_analysis.ingestion import ingest_ready
from chessism_api.routers.cloud_analysis import cancel_cloud_job, stop_repeating
from cleaning_job.cancelled_artifacts import prepare, apply, validate
from cleaning_job.cleanup import digest
from cleaning_job.cloud import BUCKET, PROJECT, REGION, CleanupError
from fastapi import HTTPException


class CancellationTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_records_each_phase_attempt_including_failure_without_paused_time(self):
        for phase in ('stopping', 'settling', 'importing', 'planning', 'cleaning', 'logs', 'refreshing', 'finishing'):
            for fail in (False, True):
                with self.subTest(phase=phase, fail=fail):
                    saved = []
                    async def persist(timings, job_id, run_id):
                        self.assertEqual((job_id, run_id), ('job', None))
                        saved.extend(timings.rows)
                    operation = AsyncMock(side_effect=CleanupError('transient exhausted') if fail else None)
                    with patch.object(cancellation, 'advance', operation), patch.object(StageTimings, 'save', persist):
                        if fail:
                            with self.assertRaises(CleanupError):
                                await cancellation.tick(Mock(), SimpleNamespace(id='job'), SimpleNamespace(phase=phase))
                        else:
                            await cancellation.tick(Mock(), SimpleNamespace(id='job'), SimpleNamespace(phase=phase))
                    self.assertEqual(len(saved), 1)
                    self.assertEqual(saved[0]['phase'], 'cancel_' + phase)
                    self.assertEqual(saved[0]['outcome'], 'failed' if fail else 'complete')
                    self.assertIsNotNone(saved[0]['finished_at'])
                    self.assertGreaterEqual(saved[0]['duration_seconds'], 0)
                    self.assertIsInstance(operation.call_args.args[-1], StageTimings)


class CancellationSafetyTests(unittest.TestCase):
    def test_priority_signal_never_waits_for_supervisor_checks_or_authorizes_cleanup(self):
        cloud = Mock()
        cloud.jobs.return_value = []
        root = SimpleNamespace(id='a' * 32, status='running', launch={'recovery_session': 1})
        control = SimpleNamespace(control_run_id='a' * 32, session=1, execution='unused', phase='running')
        quiet, _, _ = stop_batch(cloud, [root], control, signal=True, check_supervisors=False)
        self.assertFalse(quiet)
        cloud.signal_sequence.assert_called_once_with(control, cancel=True)
        cloud.cancel_recovery.assert_called_once_with(root.id, 1)
        cloud.sequence_executions.assert_not_called()
        cloud.recovery_executions.assert_not_called()
        cloud.delete_job.assert_not_called()

    def test_simultaneous_workflow_cancel_is_idempotent_but_does_not_hide_errors(self):
        cloud = Mock()
        remote = {'name': f'projects/{PROJECT}/locations/{REGION}/jobs/chessism-ui-' + 'a' * 32 + '-1',
                  'uid': 'cancel-test-uid', 'status': {'state': 'RUNNING'}}
        cloud.request.side_effect = CleanupError('already cancelling')
        cloud.job.return_value = {**remote, 'status': {'state': 'CANCELLATION_IN_PROGRESS'}}
        cancel_batch(cloud, remote, {'saved': True})
        cloud.job.return_value = remote
        with self.assertRaises(CleanupError):
            cancel_batch(cloud, remote, {'saved': True})
        cloud.job.return_value = {**remote, 'uid': 'replacement-uid', 'status': {'state': 'CANCELLED'}}
        with self.assertRaises(CleanupError):
            cancel_batch(cloud, remote, {'saved': True})

    def test_already_cancelling_batch_waits_without_reissuing_cancel(self):
        cloud = Mock()
        cloud.recovery_executions.return_value = []
        root = SimpleNamespace(id='a' * 32, status='running', launch={'spec': {'saved': True}})
        cloud.jobs.return_value = [{'name': f'projects/{PROJECT}/locations/{REGION}/jobs/chessism-ui-{root.id}-1',
                                   'uid': 'cancel-test-uid', 'status': {'state': 'CANCELLATION_IN_PROGRESS'}}]
        quiet, _, _ = stop_batch(cloud, [root], None, signal=True)
        self.assertFalse(quiet)
        cloud.request.assert_not_called()

    def test_cancels_each_owned_running_vm_not_unrelated_jobs(self):
        cloud = Mock()
        cloud.recovery_executions.return_value = []
        tasks = [{'index': n, 'run_id': str(n + 1) * 32, 'count': 10,
                  'spec': {'task': n}, 'recovery_session': 1, 'jobs': []} for n in range(6)]
        root = SimpleNamespace(id='a' * 32, status='running', launch={'tasks': tasks, 'jobs': []})
        remotes = [{'name': f"projects/{PROJECT}/locations/{REGION}/jobs/chessism-ui-{task['run_id']}-1",
                    'uid': f'uid-test-{task["index"]}', 'status': {'state': 'RUNNING'}} for task in tasks]
        cloud.jobs.return_value = [*remotes, {'name': 'unrelated'}]
        quiet, saved, names = stop_batch(cloud, [root], None, signal=True)
        self.assertFalse(quiet)
        self.assertEqual(len(names), 6)
        self.assertEqual(cloud.cancel_recovery.call_count, 6)
        self.assertEqual(cloud.request.call_count, 6)
        self.assertTrue(all(call.args[1].endswith(':cancel') for call in cloud.request.call_args_list))
        cloud.start_recovery.assert_not_called()
        cloud.submit.assert_not_called()
        cloud.delete_job.assert_not_called()
        self.assertEqual(len(saved[root.id]['vm_statuses']), 6)

    def test_delayed_supervisor_prevents_cleanup_even_with_no_batch_jobs(self):
        cloud = Mock()
        cloud.jobs.return_value = []
        cloud.recovery_executions.return_value = [{'state': 'QUEUED'}]
        root = SimpleNamespace(id='a' * 32, status='running', launch={'recovery_session': 1})
        quiet, _, _ = stop_batch(cloud, [root], None, signal=False)
        self.assertFalse(quiet)
        cloud.cancel_recovery.assert_not_called()

    def test_replaced_batch_uid_is_never_cancelled(self):
        cloud = Mock()
        name = 'chessism-ui-' + 'b' * 32 + '-1'
        root = SimpleNamespace(id='a' * 32, status='running', launch={'tasks': [
            {'run_id': 'b' * 32, 'spec': {}, 'prior_uids': {name: 'original-uid'}, 'index': 0, 'count': 1}]})
        root.launch['tasks'][0]['spec'] = {'saved': True}
        cloud.jobs.return_value = [{'name': f'projects/{PROJECT}/locations/{REGION}/jobs/{name}',
                                  'uid': 'replacement-uid', 'status': {'state': 'RUNNING'}}]
        with self.assertRaisesRegex(ValueError, 'UID changed'):
            stop_batch(cloud, [root], None, signal=True)
        cloud.request.assert_not_called()

    def test_artifacts_reject_changed_scope_and_generation(self):
        ident = 'a' * 32
        cloud = Mock()
        cloud.bucket.return_value = {'name': BUCKET}
        cloud.images.return_value = []
        cloud.jobs.return_value = []
        cloud.run_resources.return_value = []
        obj = {'name': f'inputs/ui-{ident}/fens.jsonl', 'generation': '1', 'size': '12'}
        cloud.objects.side_effect = lambda prefix: [obj] if prefix.startswith('inputs/') else []
        plan = prepare(cloud, [ident])
        with self.assertRaisesRegex(Exception, 'scope changed'):
            validate(plan, ['b' * 32])
        obj['generation'] = '2'
        with self.assertRaisesRegex(Exception, 'New/replaced objects'):
            apply(cloud, plan, [ident])
        cloud.delete_object.assert_not_called()


class CancellationAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_stop_actions_are_gone(self):
        for handler in (cancel_cloud_job, stop_repeating):
            with self.assertRaises(HTTPException) as caught:
                await handler('a' * 32)
            self.assertEqual(caught.exception.status_code, 410)

    async def test_partial_import_scans_past_missing_batch(self):
        storage = Mock()
        storage.read.side_effect = lambda uri, limit: b'{}' if uri.endswith('contract.json') else b'later batch' if uri.endswith('000001.json') else None
        writer = AsyncMock(return_value=500)
        run = SimpleNamespace(id='a' * 32, receipts={})
        count = await ingest_ready(storage, run, [{'output': 'gs://test/results', 'count': 1000, 'contract': {}}],
                                   writer=writer, scan_missing=True)
        self.assertEqual(count, 500)
        self.assertEqual(writer.await_args.args[2], 1)


if __name__ == '__main__':
    unittest.main()
