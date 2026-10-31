"""Controller tests for the server-side supervisor, with all cloud calls mocked."""
from copy import deepcopy
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations.cloud_analysis import controller
from stockfish_batch.checkpoints import encode
from test_cloud_analysis import fixture


class SupervisedControllerTests(unittest.IsolatedAsyncioTestCase):
    def setup_run(self, supervisor_state="RETRYING", execution_state="ACTIVE", batch_state="FAILED"):
        rows, contract, raw = fixture()
        run = SimpleNamespace(id="a" * 32, job_id="b" * 32, positions=rows, contract=contract,
                              receipts={}, status="running", launch={"jobs": ["old"], "uid": "old-uid",
                              "recovery_session": 1, "recovery_execution": "execution", "spec": {}})
        state = {"status": supervisor_state, "jobs": ["old", "new"], "current_job": "new",
                 "uids": {"old": "old-uid", "new": "new-uid"}, "execution": "execution",
                 "application_failures": 2 if supervisor_state == "FAILED" else 0,
                 "preemptions": 12, "last_exit_code": 124}
        cloud = Mock()
        cloud.recovery_snapshot.return_value = state, {"state": execution_state}
        cloud.job.return_value = {"uid": "new-uid", "status": {"state": batch_state}}
        cloud.storage.return_value.read.side_effect = [encode(contract), raw]
        config = SimpleNamespace(output="gs://bucket/results/run")
        return cloud, run, config

    async def test_failed_attempt_is_not_failed_logical_job_while_google_recovery_continues(self):
        cloud, run, config = self.setup_run()
        with patch.object(controller, "save_run", new_callable=AsyncMock) as saved, \
             patch.object(controller, "save_job", new_callable=AsyncMock) as parent, \
             patch.object(controller, "import_batch", new_callable=AsyncMock) as imported:
            await controller.Controller(cloud, "image").poll(run, config)
        imported.assert_awaited_once()
        parent.assert_not_awaited()
        self.assertEqual(run.launch["jobs"], ["old", "new"])
        self.assertEqual(run.launch["uid"], "new-uid")
        self.assertTrue(any(call.kwargs.get("cloud_state") == "RECOVERING" for call in saved.await_args_list))
        cloud.submit.assert_not_called()
        cloud.delete_job.assert_not_called()

    async def test_application_budget_exhaustion_is_reported_separately_from_preemptions(self):
        cloud, run, config = self.setup_run("FAILED", "SUCCEEDED")
        with patch.object(controller, "save_run", new_callable=AsyncMock), \
             patch.object(controller, "save_job", new_callable=AsyncMock) as parent, \
             patch.object(controller, "import_batch", new_callable=AsyncMock):
            await controller.Controller(cloud, "image").poll(run, config)
        self.assertEqual(parent.await_args.kwargs["status"], "failed")
        self.assertIn("2 failed attempts", parent.await_args.kwargs["error"])
        self.assertIn("preemptions (12)", parent.await_args.kwargs["error"])

    async def test_batch_success_waits_for_supervisor_to_stop_before_finalization(self):
        cloud, run, config = self.setup_run("SUCCEEDED", "ACTIVE", "SUCCEEDED")
        with patch.object(controller, "save_run", new_callable=AsyncMock) as saved, \
             patch.object(controller, "import_batch", new_callable=AsyncMock):
            await controller.Controller(cloud, "image").poll(run, config)
        self.assertFalse(any(call.kwargs.get("status") == "finalizing" for call in saved.await_args_list))
        self.assertEqual(cloud.storage.return_value.read.call_count, 2)

    async def test_replaced_batch_uid_stops_before_import(self):
        cloud, run, config = self.setup_run()
        cloud.job.return_value["uid"] = "wrong"
        with patch.object(controller, "save_run", new_callable=AsyncMock), self.assertRaisesRegex(ValueError, "replaced"):
            await controller.Controller(cloud, "image").poll(run, config)
        cloud.storage.assert_not_called()

    async def test_supervised_submission_never_directly_creates_a_batch_job(self):
        cloud, run, _ = self.setup_run()
        run.status = "submitting"
        cloud.start_recovery.return_value = "execution"
        with patch.object(controller, "save_run", new_callable=AsyncMock) as saved:
            await controller.Controller(cloud, "image").advance(SimpleNamespace(selection={}), run)
        cloud.submit.assert_not_called()
        cloud.start_recovery.assert_called_once()
        self.assertEqual(saved.await_args.kwargs["launch"]["recovery_execution"], "execution")

    async def test_cancel_is_forwarded_before_polling_and_never_deletes_data(self):
        cloud, run, _ = self.setup_run()
        instance = controller.Controller(cloud, "image")
        with patch.object(instance, "poll", new_callable=AsyncMock):
            await instance.advance(SimpleNamespace(selection={"cancel_requested": True}), run)
        cloud.cancel_recovery.assert_called_once_with(run.id, 1)
        cloud.delete_job.assert_not_called()
        cloud.delete_object.assert_not_called()

    async def test_cleanup_planning_requires_stopped_supervisor(self):
        cloud, run, _ = self.setup_run()
        run.status = "cleanup_planning"
        run.launch["manifest"] = {}
        cloud.require_recovery_stopped.side_effect = ValueError("still active")
        with patch.object(controller, "validate_manifest"), patch.object(controller, "prepare") as prepare, \
             self.assertRaisesRegex(ValueError, "still active"):
            await controller.Controller(cloud, "image").advance(SimpleNamespace(selection={}), run)
        prepare.assert_not_called()
