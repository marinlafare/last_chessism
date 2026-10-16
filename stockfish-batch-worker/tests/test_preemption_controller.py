"""Read-only/offline tests: none of these tests construct a Cloud client."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import recovery_support as recovery
import preemption_test as controller


class PreemptionControllerTests(unittest.TestCase):
    def test_fixture_is_bounded_unique_and_repeatable(self):
        raw, rows = recovery.fixture()
        self.assertEqual(len(rows), 200)
        self.assertEqual(len({row["id"] for row in rows}), 200)
        self.assertEqual(rows[0]["fen"], rows[20]["fen"])
        self.assertNotEqual(rows[0]["id"], rows[20]["id"])
        self.assertEqual(recovery.fixture()[0], raw)
        self.assertEqual(recovery.expected_contract(raw, rows)["position_count"], 200)
        self.assertEqual(recovery.expected_contract(raw, rows)["worker_version"], "1.0.0")

    def test_exact_job_cost_and_identity_bounds(self):
        job = recovery.job_config()
        self.assertEqual(len(job["taskGroups"]), 1)
        group = job["taskGroups"][0]
        self.assertEqual(group["taskCount"], "1")
        self.assertEqual(group["parallelism"], "1")
        task = group["taskSpec"]
        self.assertEqual(task["maxRunDuration"], "900s")
        self.assertEqual(task["maxRetryCount"], 0)
        container = task["runnables"][0]["container"]
        self.assertEqual(container["imageUri"], recovery.IMAGE)
        args = dict(zip(container["commands"][::2], container["commands"][1::2]))
        self.assertEqual(args["--input"], recovery.INPUT_URI)
        self.assertEqual(args["--output"], recovery.OUTPUT_URI)
        self.assertEqual(args["--max-positions"], "200")
        self.assertEqual(args["--nodes"], "1000000")
        self.assertEqual(args["--workers"], "4")
        self.assertEqual(args["--threads"], "1")
        self.assertEqual(args["--run-timeout"], "840")
        allocation = job["allocationPolicy"]
        self.assertEqual(allocation["serviceAccount"]["email"], recovery.ACCOUNT)
        self.assertEqual(len(allocation["instances"]), 1)
        policy = allocation["instances"][0]["policy"]
        self.assertEqual(policy["machineType"], "n2d-standard-4")
        self.assertEqual(policy["provisioningModel"], "SPOT")
        self.assertEqual(policy["bootDisk"]["sizeGb"], "30")

    def target(self):
        job = {"name": "projects/test/jobs/" + recovery.JOBS[0], "uid": "test-uid",
               "status": {"state": "RUNNING"}}
        vm = {"name": "test-worker", "id": "123", "zone": "zones/us-central1-c",
              "status": "RUNNING", "scheduling": {"provisioningModel": "SPOT"},
              "machineType": "machineTypes/n2d-standard-4",
              "serviceAccounts": [{"email": recovery.ACCOUNT}],
              "labels": {"batch-job-id": recovery.JOBS[0], "batch-job-uid": "test-uid",
                         "recovery-test": recovery.RUN}}
        return vm, job

    def test_only_exact_running_spot_vm_is_allowed(self):
        vm, job = self.target()
        self.assertEqual(recovery.validate_vm(vm, job), ("test-worker", "us-central1-c"))
        for key, bad in (("status", "TERMINATED"), ("id", None), ("zone", "zones/europe-west1-b"),
                         ("name", "--all"), ("machineType", "machineTypes/n2d-standard-8"),
                         ("serviceAccounts", [{"email": "different@example.com"}]),
                         ("scheduling", {"provisioningModel": "STANDARD"})):
            with self.subTest(key=key), self.assertRaises(ValueError):
                recovery.validate_vm({**vm, key: bad}, job)
        for key in ("batch-job-id", "batch-job-uid", "recovery-test"):
            changed = deepcopy(vm)
            changed["labels"][key] = "wrong"
            with self.subTest(label=key), self.assertRaises(ValueError):
                recovery.validate_vm(changed, job)
        for changed in ({**job, "name": "jobs/" + recovery.JOBS[1]},
                        {**job, "status": {"state": "SUCCEEDED"}}):
            with self.assertRaises(ValueError):
                recovery.validate_vm(vm, changed)

    def test_running_vm_is_allowed_when_batch_status_lags(self):
        vm, job = self.target()
        job["status"]["state"] = "SCHEDULED"
        self.assertEqual(recovery.validate_vm(vm, job), ("test-worker", "us-central1-c"))
        job["status"]["state"] = "QUEUED"
        with self.assertRaises(ValueError):
            recovery.validate_vm(vm, job)

    def test_controller_interrupts_from_verified_partial_work_despite_lag(self):
        vm, job = self.target()
        job["status"]["state"] = "SCHEDULED"
        failed = {**job, "status": {"state": "FAILED"}}
        task = {"status": {"state": "FAILED", "statusEvents": [
            {"taskExecution": {"exitCode": 50001}}]}}
        cloud = Mock()
        cloud.job.side_effect = [job, job, failed]
        cloud.inventory.return_value = {f"result-{i}": {} for i in range(8)}
        cloud.resources.return_value = [vm]
        cloud.command.side_effect = [vm, {"operationType": "simulateMaintenanceEvent"}, task]
        raw, rows = recovery.fixture()
        cloud.read_json.side_effect = [recovery.expected_contract(raw, rows), None]
        with patch.object(controller.time, "sleep"), patch.object(controller, "save"), \
             patch.object(controller, "emit"):
            self.assertEqual(controller.interrupt_first(cloud, job), failed)
        mutations = [call.args for call in cloud.command.call_args_list
                     if "simulate-maintenance-event" in call.args]
        self.assertEqual(mutations, [("compute", "instances", "simulate-maintenance-event",
                                    "test-worker", "--zone=us-central1-c", "--async")])
        cloud.submit.assert_not_called()

    def test_incompatible_partial_contract_cannot_trigger_interruption(self):
        vm, job = self.target()
        job["status"]["state"] = "SCHEDULED"
        cloud = Mock()
        cloud.job.return_value = job
        cloud.inventory.return_value = {f"result-{i}": {} for i in range(8)}
        cloud.read_json.return_value = {"fingerprint": "wrong"}
        with patch.object(controller, "emit"), self.assertRaises(ValueError):
            controller.interrupt_first(cloud, job)
        cloud.command.assert_not_called()
        cloud.resources.assert_not_called()

    def test_preemption_requires_reserved_exit_code_not_job_name(self):
        job = {"status": {"state": "FAILED", "statusEvents": [
            {"description": f"{recovery.JOBS[0]} changed state to FAILED"}]}}
        task = {"status": {"state": "FAILED", "statusEvents": []}}
        with self.assertRaises(ValueError):
            recovery.verify_preemption(job, task)
        for event in ({"taskExecution": {"exitCode": 50001}},
                      {"description": "Task failed due to Spot Preemption with exit code 50001."}):
            task["status"]["statusEvents"] = [event]
            recovery.verify_preemption(job, task)
        task["status"]["statusEvents"] = [{"taskExecution": {"exitCode": 1}}]
        with self.assertRaises(ValueError):
            recovery.verify_preemption(job, task)

    def test_resume_logs_prove_exact_skipped_ids(self):
        _, rows = recovery.fixture()
        old = {row["id"] for row in rows[:17]}
        events = [{"event": "started", "resumed": 17, "total": 200},
                  {"event": "complete", "resumed": 17, "analyzed_this_attempt": 183,
                   "saved": 200, "total": 200}]
        events += [{"event": "saved", "id": row["id"]} for row in rows[17:]]
        self.assertTrue(recovery.verify_resume_events(events, old, rows))
        self.assertFalse(recovery.verify_resume_events(events[:-1], old, rows))
        self.assertFalse(recovery.verify_resume_events([], old, rows))
        with self.assertRaises(ValueError):
            recovery.verify_resume_events(events + [{"event": "saved", "id": rows[0]["id"]}], old, rows)
        events[0]["resumed"] = 0
        with self.assertRaises(ValueError):
            recovery.verify_resume_events(events, old, rows)

    def recreation_evidence(self):
        vm, job = self.target()
        job["status"]["state"] = "FAILED"
        target = {"vm": vm, "job_uid": job["uid"]}
        task = {"status": {"state": "FAILED", "statusEvents": [{
            "taskExecution": {"exitCode": 50006},
            "description": "Task failed on zones/us-central1-c/instances/123 due to "
                           "VM is recreated during task execution with exit code 50006."}]}}
        operation = {"name": "test-operation", "status": "DONE", "targetId": "123",
                     "operationType": "simulateMaintenanceEvent",
                     "targetLink": f"https://compute.googleapis.com/compute/v1/projects/{recovery.PROJECT}"
                                   "/zones/us-central1-c/instances/test-worker"}
        return job, task, operation, target

    def test_vm_recreation_is_not_mislabeled_as_spot_preemption(self):
        job, task, operation, target = self.recreation_evidence()
        with self.assertRaises(ValueError):
            recovery.verify_preemption(job, task)
        evidence = recovery.verify_vm_recreation(job, task, operation, target)
        self.assertEqual(evidence["batch_exit_code"], 50006)
        self.assertEqual(evidence["failure_kind"], "vm_recreated")
        self.assertIs(evidence["strict_spot_preemption_code_observed"], False)

    def test_vm_recreation_requires_matching_successful_maintenance(self):
        job, task, operation, target = self.recreation_evidence()
        for field, bad in (("targetId", "456"), ("status", "RUNNING"), ("targetLink", "wrong-vm"),
                           ("operationType", "delete"), ("error", {"errors": [{"code": "FAILED"}]})):
            with self.subTest(field=field), self.assertRaises(ValueError):
                recovery.verify_vm_recreation(job, task, {**operation, field: bad}, target)
        for code in (1, 50001, 50002, 50005):
            changed = deepcopy(task)
            changed["status"]["statusEvents"][0]["taskExecution"]["exitCode"] = code
            with self.subTest(code=code), self.assertRaises(ValueError):
                recovery.verify_vm_recreation(job, changed, operation, target)
        with self.assertRaises(ValueError):
            recovery.verify_vm_recreation(job, task, operation, {**target, "job_uid": "wrong"})

    def test_vm_recreation_recovery_cannot_be_started_twice(self):
        raw, _ = recovery.fixture()
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "input.jsonl").write_bytes(raw)
            (out / "job.json").write_text(json.dumps(recovery.job_config()))
            (out / "recovery-started.json").write_text("{}")
            cloud = Mock()
            with patch.object(controller, "OUT", out), self.assertRaises(ValueError):
                controller.resume_after_vm_recreation(cloud)
            cloud.command.assert_not_called()
            cloud.submit.assert_not_called()

    def test_log_payload_formats(self):
        event = {"event": "started", "resumed": 0}
        logs = [{"textPayload": json.dumps(event)}, {"jsonPayload": event},
                {"jsonPayload": {"message": json.dumps(event)}}, {"textPayload": "not JSON"}]
        self.assertEqual(recovery.worker_events(logs), [event] * 3)

    def test_cleanup_requires_two_empty_observations(self):
        cloud = Mock()
        cloud.resources.side_effect = [[{"name": "vm"}], [], [], [], [], []]
        with patch.object(controller.time, "sleep") as sleep, patch.object(controller, "emit"):
            controller.cleanup_wait(cloud, "only-this-uid")
        self.assertEqual(cloud.resources.call_count, 6)
        self.assertEqual(sleep.call_count, 2)
        for call in cloud.resources.call_args_list:
            self.assertEqual(call.args[1], "only-this-uid")

    def test_cleanup_timeout_stops_before_next_job(self):
        cloud = Mock()
        cloud.resources.return_value = [{"name": "still-present"}]
        with patch.object(controller.time, "monotonic", side_effect=[0, 1, 4]), \
             patch.object(controller.time, "sleep"), patch.object(controller, "emit"), \
             self.assertRaises(TimeoutError):
            controller.cleanup_wait(cloud, "only-this-uid", seconds=3)
        cloud.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
