import copy
import os
import re
import unittest
from urllib.parse import unquote
from unittest.mock import patch

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
        self.log_reads = []

    def jobs(self): return self.running
    def resources(self): return self.resources_left
    def logs(self): return [f"projects/{PROJECT}/logs/{name}" for name in self.streams]

    def log_entries(self, query, *, default_bucket=False):
        self.log_reads.append((query, default_bucket))
        if not default_bucket:
            yield {**entry(), "protoPayload": {
                "serviceName": "compute.googleapis.com", "methodName": "v1.compute.instances.insert",
                "resourceName": f"projects/{PROJECT_NUMBER}/zones/us-central1-c/instances/{UID}-group0-0-abcd"}}
        else:
            for path in re.findall(r'logName="([^"]+)"', query):
                name = unquote(path.split('/logs/')[1])
                for row in self.streams.get(name, []):
                    yield {'logName': path, **row}

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

    def test_combined_scans_reduce_five_stream_cleanup_to_eight_queries(self):
        cloud = Cloud()
        logs = ['GCEGuestAgent', 'GCEGuestAgentManager', 'OSConfigAgent',
                'compute.googleapis.com/shielded_vm_integrity', 'google_metadata_script_runner']
        for log in logs:
            cloud.streams[log] = [entry()]
        cloud.streams['diagnostic-log'] = [entry('foreign')]
        plan = plan_logs(cloud, {UID})
        self.assertEqual(set(plan['streams']), set(logs))
        self.assertTrue(remove_logs(cloud, plan)['complete'])
        self.assertCountEqual(cloud.deleted, logs)
        reads = [query for query, is_default in cloud.log_reads if is_default]
        self.assertEqual(len(reads), 8)  # Was 7 planning + 5 validation + 5 fresh + 5 verification = 22.
        self.assertNotIn('timestamp', ''.join(reads))
        self.assertNotIn('instance_id', ''.join(reads))
        self.assertIn(' OR ', reads[0])
        self.assertIn('%2Fshielded_vm_integrity', reads[0])

    def test_unknown_stream_identity_or_partial_scan_failure_blocks_all_deletion(self):
        for bad in ({}, {'logName':f'projects/foreign/logs/GCEGuestAgent'}):
            cloud = Cloud()
            plan = plan_logs(cloud, {UID})
            with patch.object(cloud, 'log_entries', return_value=iter([bad])):
                with self.assertRaises(CleanupError): remove_logs(cloud, plan)
            self.assertFalse(cloud.deleted)
        cloud = Cloud()
        plan = plan_logs(cloud, {UID})
        def partial(*args, **kwargs):
            yield {'logName':f'projects/{PROJECT}/logs/GCEGuestAgent', **entry()}
            raise CleanupError('incomplete page')
        with patch.object(cloud, 'log_entries', side_effect=partial):
            with self.assertRaisesRegex(CleanupError, 'incomplete'): remove_logs(cloud, plan)
        self.assertFalse(cloud.deleted)

    def test_all_targets_checked_before_first_delete_and_late_entries_rechecked(self):
        cloud = Cloud()
        cloud.streams['OSConfigAgent'] = [entry()]
        plan = plan_logs(cloud, {UID})
        cloud.streams['OSConfigAgent'].append(entry('foreign'))
        with self.assertRaises(CleanupError): remove_logs(cloud, plan)
        self.assertFalse(cloud.deleted)
        cloud.streams['OSConfigAgent'] = [entry()]
        remove = cloud.delete_log
        def late_entry(log):
            remove(log)
            cloud.streams['OSConfigAgent'].append(entry('foreign'))
        cloud.delete_log = late_entry
        with self.assertRaises(CleanupError): remove_logs(cloud, plan)
        self.assertEqual(cloud.deleted, ['GCEGuestAgent'])
        self.assertIn('OSConfigAgent', cloud.streams)

    def test_project_is_rechecked_immediately_before_each_deletion(self):
        cloud = Cloud()
        plan = plan_logs(cloud, {UID})
        original = cloud.jobs
        calls = 0
        def jobs():
            nonlocal calls
            calls += 1
            return original() if calls == 1 else [{'name':'new-job'}]
        with patch.object(cloud, 'jobs', side_effect=jobs):
            with self.assertRaises(CleanupError): remove_logs(cloud, plan)
        self.assertFalse(cloud.deleted)
