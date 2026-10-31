"""Offline tests for the cloud supervisor bridge; never submits real jobs."""
import json
import unittest
from unittest.mock import Mock

from cloud_job.launch import Client, configuration, render
from cloud_job.recovery import SERVICE_ACCOUNT, WORKFLOW, control_prefix, execution_identity, validate_state
from stockfish_batch.checkpoints import digest, encode

RUN = "a" * 32
EXECUTION = WORKFLOW + "/executions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
IMAGE = "us-central1-docker.pkg.dev/chessism-production/chessism-workers/ui-test@sha256:" + "a" * 64


def fixture():
    config = configuration(RUN, 20, 100000, 300, stall_only=True)
    spec = render(config, IMAGE, 300, supervised=True)
    job = f"chessism-ui-{RUN}-1"
    state = {"version": 1, "run_id": RUN, "session": 1, "execution": EXECUTION,
             "spec_hash": digest(encode(spec)), "status": "SUCCEEDED", "jobs": [job],
             "uids": {job: "verified-uid"}, "current_job": job,
             "preemptions": 12, "application_failures": 0, "last_exit_code": 50001}
    return {"spec": spec, "recovery_session": 1, "recovery_execution": EXECUTION,
            "recovery_summary": state}, state


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.client.request = Mock()
        self.client.pages = Mock(return_value=[])
        self.client.storage = Mock()

    def test_supervised_tasks_have_no_shared_native_retry_counter(self):
        launch, _ = fixture()
        task = launch["spec"]["taskGroups"][0]["taskSpec"]
        self.assertEqual(task["maxRetryCount"], 0)
        self.assertNotIn("maxRunDuration", task)

    def test_deployment_accepts_google_service_account_resource_name(self):
        for name in (SERVICE_ACCOUNT, f"projects/chessism-production/serviceAccounts/{SERVICE_ACCOUNT}"):
            self.client.request.return_value = {
                "state": "ACTIVE", "labels": {"recovery_schema": "1"}, "serviceAccount": name}
            self.client.require_recovery()
        self.client.request.return_value["serviceAccount"] = "wrong@example.com"
        with self.assertRaises(ValueError): self.client.require_recovery()

    def test_state_requires_exact_scope_and_complete_inventory(self):
        launch, state = fixture()
        self.assertEqual(validate_state(state, RUN, 1, digest(encode(launch["spec"]))), state)
        for changes in ({"run_id": "b" * 32}, {"session": 2}, {"spec_hash": "wrong"},
                        {"execution": "projects/other/executions/test"}, {"jobs": []},
                        {"uids": {}}, {"preemptions": -1}, {"status": "UNKNOWN"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_state({**state, **changes}, RUN, 1, state["spec_hash"])

    def test_workflow_creation_has_recovery_arguments_and_no_native_submit(self):
        launch, _ = fixture()
        self.client.request.return_value = {"name": EXECUTION}
        self.assertEqual(self.client.start_recovery(RUN, launch), EXECUTION)
        args, kwargs = self.client.request.call_args
        self.assertEqual(args, ("executions", WORKFLOW + "/executions"))
        body = kwargs["body"]
        self.assertEqual(body["labels"]["run_id"], RUN)
        value = json.loads(body["argument"])
        self.assertEqual(value["prior_jobs"], [])
        self.assertEqual(value["spec"], launch["spec"])

    def test_lost_create_response_reuses_discovered_execution(self):
        launch, _ = fixture()
        argument = {"run_id": RUN, "session": 1, "spec": launch["spec"],
                    "spec_hash": digest(encode(launch["spec"])), "prior_jobs": [], "prior_uids": {}}
        self.client.pages.return_value = [{"executions": [{"name": EXECUTION, "argument": json.dumps(argument)}]}]
        self.assertEqual(self.client.start_recovery(RUN, launch), EXECUTION)
        self.client.request.assert_not_called()
        argument["spec_hash"] = "wrong"
        self.client.pages.return_value[0]["executions"][0]["argument"] = json.dumps(argument)
        with self.assertRaises(ValueError): self.client.start_recovery(RUN, launch)

    def test_authoritative_owner_overrides_duplicate_execution(self):
        launch, state = fixture()
        launch["recovery_execution"] = WORKFLOW + "/executions/" + "f" * 36
        self.client.recovery_document = Mock(side_effect=[
            {"execution": EXECUTION, "spec_hash": state["spec_hash"]}, state])
        self.client.request.return_value = {"state": "SUCCEEDED"}
        returned, _ = self.client.recovery_snapshot(RUN, launch)
        self.assertEqual(returned, state)
        self.client.request.assert_called_once_with("executions", EXECUTION)

    def test_supervisor_failure_never_silently_launches_more_work(self):
        launch, state = fixture()
        self.client.recovery_document = Mock(side_effect=[
            {"execution": EXECUTION, "spec_hash": state["spec_hash"]}, state])
        self.client.request.return_value = {"state": "FAILED"}
        with self.assertRaisesRegex(ValueError, "stopped unexpectedly"):
            self.client.recovery_snapshot(RUN, launch)

    def test_cleanup_requires_stopped_supervisor_even_after_files_disappear(self):
        launch, state = fixture()
        self.client.request.return_value = {"state": "ACTIVE"}
        with self.assertRaisesRegex(ValueError, "may still write"):
            self.client.require_recovery_stopped(RUN, launch)
        self.client.request.return_value = {"state": "SUCCEEDED"}
        self.client.require_recovery_stopped(RUN, launch)
        self.client.storage.assert_not_called()
        launch["recovery_summary"] = {**state, "status": "RETRYING"}
        with self.assertRaisesRegex(ValueError, "has not completed"):
            self.client.require_recovery_stopped(RUN, launch)

    def test_cancel_is_a_durable_create_only_marker_not_vm_deletion(self):
        self.client.cancel_recovery(RUN, 1)
        storage = self.client.storage.return_value
        uri, raw = storage.create.call_args.args
        self.assertEqual(uri, control_prefix(RUN, 1) + "cancel.json")
        self.assertTrue(json.loads(raw)["cancelled"])
        self.client.request.assert_not_called()

    def test_queued_duplicate_prevents_removing_ownership_fence(self):
        launch, _ = fixture()
        self.client.request.return_value = {"state": "SUCCEEDED"}
        self.client.pages.return_value = [{"executions": [{"state": "QUEUED"}]}]
        with self.assertRaisesRegex(ValueError, "duplicate may still run"):
            self.client.require_recovery_stopped(RUN, launch)

    def test_control_paths_cannot_escape_scope(self):
        for run, session in (("../elsewhere", 1), (RUN, 0), (RUN, True), (RUN, 4)):
            with self.assertRaises(ValueError): control_prefix(run, session)

    def test_numeric_project_execution_names_are_canonicalized(self):
        self.assertEqual(execution_identity(EXECUTION.replace("chessism-production", "276704059200")), EXECUTION)
        with self.assertRaises(ValueError): execution_identity(EXECUTION.replace("chessism-production", "other-project"))
