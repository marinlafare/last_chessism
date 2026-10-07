import copy
import os
import unittest

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations.cloud_analysis import runtime
from chessism_api.operations.cloud_analysis.log_cleanup import plan_logs, remove_logs
from cleaning_job.cloud import PROJECT, PROJECT_NUMBER, CleanupError

UID = "chessism-ui-test-12345678-abcd-12345"


def entry(instance="123"):
    return {"resource": {"type": "gce_instance", "labels": {
        "project_id": PROJECT, "zone": "us-central1-c", "instance_id": instance}}, "severity": "INFO"}


class Cloud:
    def __init__(self):
        self.streams = {"GCEGuestAgent": [entry()], "ping": [entry("foreign")],
                        "cloudaudit.googleapis.com%2Factivity": [entry()]}
        self.deleted = []
        self.running = []
        self.resources_left = []

    def jobs(self): return self.running
    def resources(self): return self.resources_left
    def logs(self): return [f"projects/{PROJECT}/logs/{name}" for name in self.streams]

    def log_entries(self, query, *, default_bucket=False):
        if not default_bucket:
            yield {**entry(), "protoPayload": {
                "serviceName": "compute.googleapis.com", "methodName": "v1.compute.instances.insert",
                "resourceName": f"projects/{PROJECT_NUMBER}/zones/us-central1-c/instances/{UID}-group0-0-abcd"}}
        else:
            name = query.split("/logs/")[1].split('"')[0]
            yield from self.streams.get(name, [])

    def delete_log(self, name):
        self.deleted.append(name)
        self.streams.pop(name, None)


class LogCleanupTests(unittest.TestCase):
    def test_only_wholly_owned_streams_are_deleted_and_plan_survives_restart(self):
        cloud = Cloud()
        plan = plan_logs(cloud, {UID})
        self.assertEqual(set(plan["streams"]), {"GCEGuestAgent"})
        self.assertEqual(set(plan["retained"]), {"ping"})
        self.assertEqual(cloud.deleted, [])
        report = remove_logs(cloud, copy.deepcopy(plan))
        self.assertTrue(report["complete"])
        self.assertEqual(cloud.deleted, ["GCEGuestAgent"])
        self.assertIn("cloudaudit.googleapis.com%2Factivity", cloud.streams)
        self.assertTrue(remove_logs(cloud, plan)["complete"])

    def test_unknown_old_entry_protects_entire_stream(self):
        cloud = Cloud()
        cloud.streams["GCEGuestAgent"].append(entry("456"))
        plan = plan_logs(cloud, {UID})
        self.assertEqual(plan["streams"], {})
        remove_logs(cloud, plan)
        self.assertEqual(cloud.deleted, [])

    def test_empty_or_wrong_owner_set_cannot_authorize_deletion(self):
        for owners in (set(), {"chessism-ui-another-123456789"}):
            cloud = Cloud()
            plan = plan_logs(cloud, owners)
            self.assertEqual(plan["streams"], {})

    def test_unrelated_entry_after_plan_stops_deletion(self):
        cloud = Cloud()
        plan = plan_logs(cloud, {UID})
        cloud.streams["GCEGuestAgent"].append(entry("456"))
        with self.assertRaises(CleanupError):
            remove_logs(cloud, plan)
        self.assertEqual(cloud.deleted, [])

    def test_active_compute_or_jobs_and_corrupted_plan_stop_cleanup(self):
        for key in ("running", "resources_left"):
            cloud = Cloud()
            plan = plan_logs(cloud, {UID})
            setattr(cloud, key, [{}])
            with self.assertRaises(CleanupError): remove_logs(cloud, plan)
            self.assertEqual(cloud.deleted, [])
        cloud = Cloud()
        plan = plan_logs(cloud, {UID})
        plan["streams"]["audit"] = {}
        with self.assertRaises(CleanupError): remove_logs(cloud, plan)

    def test_recent_entries_that_survive_deletion_keep_phase_pending(self):
        cloud = Cloud()
        plan = plan_logs(cloud, {UID})
        cloud.delete_log = lambda _: None
        report = remove_logs(cloud, plan)
        self.assertFalse(report["complete"])
        self.assertEqual(report["remaining"], ["GCEGuestAgent"])
