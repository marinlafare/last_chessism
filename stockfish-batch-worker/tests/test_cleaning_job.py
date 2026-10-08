"""No credentials or real cloud access: all destructive operations are fake."""

from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from cleaning_job import cleaning_job
from cleaning_job import cleanup
from cleaning_job.__main__ import main
from cleaning_job.cloud import BUCKET, CleanupError, PROJECT, REGION, REPOSITORY
from cleaning_job.shared import cleanup_shared, TEST_LOGS


def job(name="batch-one", run="run-one", state="SUCCEEDED", region=REGION):
    return {
        "name": f"projects/{PROJECT}/locations/{region}/jobs/{name}",
        "uid": name + "-12345678-abcd-12345", "status": {"state": state},
        "taskGroups": [{"taskSpec": {"runnables": [{"container": {"commands": [
            "--input", f"gs://{BUCKET}/inputs/{run}/input.jsonl",
            "--output", f"gs://{BUCKET}/results/{run}",
        ]}}]}}],
    }


def obj(name="results/run-one/manifest.json", generation="10"):
    return {"name": name, "generation": generation, "size": "123"}


class FakeCloud:
    def __init__(self):
        self.job_data = {(REGION, "batch-one"): job()}
        self.object_data = [obj(), obj("inputs/run-one/input.jsonl")]
        self.resource_data = []
        self.bucket_data = {"name": BUCKET, "softDeletePolicy": {"retentionDurationSeconds": "0"}}
        self.image_data = []
        self.log_data = []
        self.writes = []
        self.leave_job = False

    def job(self, name, region=REGION):
        return deepcopy(self.job_data.get((region, name)))

    def jobs(self):
        return deepcopy(list(self.job_data.values()))

    def run_resources(self):
        return []

    def bucket(self):
        return deepcopy(self.bucket_data)

    def objects(self, prefix):
        return deepcopy([o for o in self.object_data if o["name"].startswith(prefix)])

    def resources(self):
        return deepcopy(self.resource_data)

    def delete_job(self, name, region, uid):
        self.writes.append(("job", name, region, uid))
        if not self.leave_job:
            self.job_data.pop((region, name), None)

    def delete_object(self, value):
        self.writes.append(("object", value["name"], value["generation"]))
        self.object_data = [o for o in self.object_data
                            if (o["name"], o["generation"]) != (value["name"], value["generation"])]

    def images(self):
        return deepcopy(self.image_data)

    def logs(self):
        return list(self.log_data)

    def delete_image(self, image):
        self.writes.append(("image", image))
        self.image_data = [i for i in self.image_data if i["uri"] != image]
        return {"name": "operations/test-image-delete"}

    def delete_log(self, log):
        self.writes.append(("log", log))


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeCloud()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def run_cleanup(self, **kwargs):
        return cleaning_job("batch-one", cloud=self.cloud, plan_directory=self.temp.name,
                            wait_seconds=0, **kwargs)

    def test_default_is_preview_and_records_exact_inventory(self):
        result = self.run_cleanup()
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["object_versions"], 2)
        plan = json.loads(Path(result["plan"]).read_text())
        self.assertEqual(plan["jobs"][0]["uid"], job()["uid"])
        self.assertEqual(plan["sha256"], cleanup.digest(plan))
        self.assertEqual(self.cloud.writes, [])

    def test_parallel_object_deletes_are_bounded_generation_pinned_and_resumable(self):
        self.cloud.object_data = [obj(f'results/run-one/batches/{i:06d}.json', str(i + 10)) for i in range(20)]
        plan = cleanup.prepare(self.cloud, ['batch-one'])
        original = self.cloud.delete_object
        active, peak, fail = 0, 0, True
        lock = threading.Lock()
        def remove(value):
            nonlocal active, peak, fail
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(.02)
                with lock:
                    if fail and value['generation'] == '13':
                        fail = False
                        raise CleanupError('transient delete failure')
                    original(value)
            finally:
                with lock: active -= 1
        self.cloud.delete_object = remove
        with self.assertRaisesRegex(CleanupError, 'transient'):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertEqual(active, 0)
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 8)
        self.assertTrue(cleanup.apply(self.cloud, plan, wait_seconds=0)['complete'])
        removed = [(w[1], w[2]) for w in self.cloud.writes if w[0] == 'object']
        self.assertEqual(len(removed), 20)
        self.assertEqual(set(removed), {(o['name'], o['generation']) for o in plan['objects']})

    def test_execute_is_unattended_scoped_and_resumable(self):
        other = obj("results/run-one-extra/keep.json")
        self.cloud.object_data += [other, obj("results/other/keep.json")]
        self.cloud.job_data[(REGION, "unrelated")] = job("unrelated", "other", "RUNNING")
        with patch("builtins.input", side_effect=AssertionError("No prompts allowed")):
            result = self.run_cleanup(execute=True)
        self.assertTrue(result["complete"])
        self.assertEqual([w[0] for w in self.cloud.writes], ["job", "object", "object"])
        self.assertEqual(len(self.cloud.object_data), 2)
        saved = json.loads(Path(result["plan"]).read_text())
        self.assertTrue(cleanup.verify(self.cloud, saved)["complete"])
        self.cloud.writes.clear()
        again = cleaning_job(plan=saved, execute=True, cloud=self.cloud, plan_directory=self.temp.name, wait_seconds=0)
        self.assertTrue(again["complete"])
        self.assertEqual(self.cloud.writes, [])

    def test_active_failed_unknown_states_refused_without_writes(self):
        for state in ("QUEUED", "SCHEDULED", "RUNNING", "FAILED", "CANCELLED", "DELETION_IN_PROGRESS", "UNKNOWN", None):
            with self.subTest(state=state):
                self.cloud.job_data[(REGION, "batch-one")]["status"]["state"] = state
                with self.assertRaises(CleanupError):
                    self.run_cleanup(execute=True)
                self.assertEqual(self.cloud.writes, [])

    def test_failed_retry_must_be_explicitly_abandoned_and_grouped(self):
        self.cloud.job_data[(REGION, "attempt-a")] = job("attempt-a", state="FAILED")
        with self.assertRaisesRegex(CleanupError, "also references"):
            self.run_cleanup(execute=True)
        result = cleaning_job(["attempt-a", "batch-one"], include_failed=True, execute=True,
                              cloud=self.cloud, plan_directory=self.temp.name, wait_seconds=0)
        self.assertTrue(result["complete"])
        self.assertEqual([w[0] for w in self.cloud.writes], ["job", "job", "object", "object"])

    def test_cancelled_previous_attempt_is_cleaned_only_with_successful_retry(self):
        self.cloud.job_data[(REGION, "attempt-a")] = job("attempt-a", state="CANCELLED")
        with self.assertRaises(CleanupError):
            cleaning_job(["attempt-a", "batch-one"], execute=True,
                         cloud=self.cloud, plan_directory=self.temp.name, wait_seconds=0)
        self.assertEqual(self.cloud.writes, [])
        result = cleaning_job(["attempt-a", "batch-one"], include_failed=True, execute=True,
                             cloud=self.cloud, plan_directory=self.temp.name, wait_seconds=0)
        self.assertTrue(result["complete"])

    def test_reference_in_another_region_or_environment_blocks_deletion(self):
        peer = job("peer", "independent", region="europe-west1")
        peer["taskGroups"][0]["taskSpec"]["environment"] = {"variables": {"RETRY": f"gs://{BUCKET}/results/run-one"}}
        self.cloud.job_data[("europe-west1", "peer")] = peer
        with self.assertRaisesRegex(CleanupError, "also references"):
            self.run_cleanup(execute=True)
        self.assertEqual(self.cloud.writes, [])

    def test_parent_bucket_and_glob_references_protect_job_data(self):
        for reference in (f"gs://{BUCKET}", f"gs://{BUCKET}/results/", f"gs://{BUCKET}/results/*", f"gs://{BUCKET}/results/run*"):
            with self.subTest(reference=reference):
                peer = job("peer", "independent")
                peer["taskGroups"][0]["taskSpec"]["environment"] = {"variables": {"SOURCE": reference}}
                self.cloud.job_data[(REGION, "peer")] = peer
                with self.assertRaisesRegex(CleanupError, "also references"):
                    self.run_cleanup(execute=True)
                self.assertEqual(self.cloud.writes, [])

    def test_worker_with_implicit_storage_paths_fails_closed(self):
        container = self.cloud.job_data[(REGION, "batch-one")]["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
        container["imageUri"] = REPOSITORY + "stockfish-analyzer@sha256:" + "a" * 64
        container["commands"] = [f"--input=gs://{BUCKET}/inputs/run-one/input.jsonl"]
        with self.assertRaisesRegex(CleanupError, "cannot infer"):
            self.run_cleanup(execute=True)
        self.assertEqual(self.cloud.writes, [])

    def test_wrong_bucket_roots_globs_and_traversal_rejected(self):
        original = job()
        for output in (f"gs://{BUCKET}/", f"gs://{BUCKET}/results/", f"gs://{BUCKET}/results/*",
                       f"gs://{BUCKET}/results/../foo", f"gs://{BUCKET}/results/run-one//foo",
                       "gs://unrelated/results/run-one", f"gs://{BUCKET}/inputs/run-one"):
            with self.subTest(output=output):
                changed = deepcopy(original)
                changed["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["commands"][3] = output
                self.cloud.job_data[(REGION, "batch-one")] = changed
                with self.assertRaises(CleanupError):
                    self.run_cleanup(execute=True)
                self.assertEqual(self.cloud.writes, [])

    def test_invalid_job_name_rejected_before_cloud_reads(self):
        for name in ("--all", "../run", "", "project/job"):
            with patch.object(self.cloud, "job", side_effect=AssertionError("should not read")):
                with self.assertRaises(CleanupError):
                    cleanup.prepare(self.cloud, [name])

    def test_missing_job_requires_prior_plan_not_guessed_prefix(self):
        self.cloud.job_data.clear()
        with self.assertRaisesRegex(CleanupError, "saved cleanup plan"):
            self.run_cleanup(execute=True)
        self.assertEqual(self.cloud.writes, [])

    def test_policy_and_object_hold_refuse_before_deleting_metadata(self):
        for field, value in (("softDeletePolicy", {"retentionDurationSeconds": "604800"}),
                             ("retentionPolicy", {"retentionPeriod": "10"})):
            original = deepcopy(self.cloud.bucket_data)
            self.cloud.bucket_data[field] = value
            with self.assertRaises(CleanupError):
                self.run_cleanup(execute=True)
            self.cloud.bucket_data = original
        self.cloud.object_data[0]["temporaryHold"] = True
        with self.assertRaises(CleanupError):
            self.run_cleanup(execute=True)
        self.assertEqual(self.cloud.writes, [])

    def test_all_noncurrent_generations_are_deleted_exactly(self):
        self.cloud.object_data.append(obj(generation="9"))
        self.assertTrue(self.run_cleanup(execute=True)["complete"])
        self.assertIn(("object", "results/run-one/manifest.json", "9"), self.cloud.writes)
        self.assertIn(("object", "results/run-one/manifest.json", "10"), self.cloud.writes)

    def test_snapshot_changes_fail_before_any_deletion(self):
        plan = cleanup.prepare(self.cloud, ["batch-one"])
        for mode in ("new", "generation", "uid"):
            with self.subTest(mode=mode):
                cloud = FakeCloud()
                if mode == "new":
                    cloud.object_data.append(obj("results/run-one/new.json"))
                elif mode == "generation":
                    cloud.object_data[0]["generation"] = "11"
                else:
                    cloud.job_data[(REGION, "batch-one")]["uid"] = "a-new-incarnation-1234"
                with self.assertRaises(CleanupError):
                    cleanup.apply(cloud, plan, wait_seconds=0)
                self.assertEqual(cloud.writes, [])

    def test_tampered_plan_cannot_expand_scope(self):
        plan = cleanup.prepare(self.cloud, ["batch-one"])
        plan["objects"].append(obj("private/database.backup"))
        with self.assertRaisesRegex(CleanupError, "checksum"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        plan["sha256"] = cleanup.digest(plan)
        with self.assertRaisesRegex(CleanupError, "Unsafe"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertEqual(self.cloud.writes, [])

    def test_leftover_compute_defers_files_without_forced_deletions(self):
        self.cloud.resource_data = [{"name": job()["uid"] + "-group0-0", "kind": "instanceTemplates"}]
        result = self.run_cleanup(execute=True)
        self.assertFalse(result["complete"])
        self.assertTrue(result["data_deletion_deferred"])
        self.assertEqual([w[0] for w in self.cloud.writes], ["job"])
        self.assertEqual(len(self.cloud.object_data), 2)

    def test_uid_resource_labels_and_prefixes_not_job_name_match(self):
        self.cloud.resource_data = [
            {"name": "owned-disk", "labels": {"batch-job-uid": job()["uid"]}},
            {"name": "owned-template", "properties": {"labels": {"batch-job-uid": job()["uid"]}}},
            {"name": "batch-one-some-unrelated-vm"},
        ]
        plan = cleanup.prepare(self.cloud, ["batch-one"])
        self.assertEqual(len(cleanup.owned_resources(self.cloud, plan)), 2)

    def test_async_job_deletion_is_bounded(self):
        self.cloud.leave_job = True
        with patch.object(cleanup.time, "sleep", side_effect=AssertionError("zero wait")):
            result = self.run_cleanup(execute=True)
        self.assertFalse(result["complete"])
        self.assertEqual(result["remaining_jobs"], ["batch-one"])
        self.assertEqual(len(self.cloud.object_data), 2)

    def test_retry_submitted_after_metadata_deletion_protects_results(self):
        original = self.cloud.delete_job

        def start_retry(*args):
            original(*args)
            self.cloud.job_data[(REGION, "retry")] = job("retry", state="QUEUED")

        self.cloud.delete_job = start_retry
        with self.assertRaisesRegex(CleanupError, "also references.*Saved cleanup plan"):
            self.run_cleanup(execute=True)
        self.assertEqual([w[0] for w in self.cloud.writes], ["job"])

    def test_crash_mid_objects_recovers_with_saved_plan(self):
        original = self.cloud.delete_object
        count = 0

        def fail_second(value):
            nonlocal count
            count += 1
            if count == 2:
                raise CleanupError("temporary network error")
            original(value)

        self.cloud.delete_object = fail_second
        with self.assertRaisesRegex(CleanupError, "Saved cleanup plan"):
            self.run_cleanup(execute=True)
        self.assertFalse(self.cloud.job_data)
        plans = list(Path(self.temp.name).glob("*.json"))
        self.assertEqual(len(plans), 1)
        self.cloud.delete_object = original
        result = cleaning_job(plan=json.loads(plans[0].read_text()), cloud=self.cloud,
                              execute=True, wait_seconds=0, plan_directory=self.temp.name)
        self.assertTrue(result["complete"])

    def test_script_only_smoke_has_no_objects_to_delete(self):
        self.cloud.job_data[(REGION, "batch-one")]["taskGroups"] = [{"taskSpec": {"runnables": [{"script": {"text": "nproc"}}]}}]
        self.assertTrue(self.run_cleanup(execute=True)["complete"])
        self.assertEqual(len(self.cloud.object_data), 2)
        self.assertEqual([w[0] for w in self.cloud.writes], ["job"])

    def test_plan_is_saved_before_first_mutation(self):
        original = self.cloud.delete_job

        def delete(*args):
            paths = list(Path(self.temp.name).glob("*.json"))
            self.assertEqual(len(paths), 1)
            cleanup.validate_plan(json.loads(paths[0].read_text()))
            return original(*args)

        self.cloud.delete_job = delete
        self.run_cleanup(execute=True)

    def test_cli_help_is_offline_and_execute_does_not_prompt(self):
        with patch("cleaning_job.__main__.Cloud", side_effect=AssertionError("help authenticates")), \
                patch("sys.stdout", new_callable=io.StringIO), self.assertRaises(SystemExit) as exit_info:
            main(["--help"])
        self.assertEqual(exit_info.exception.code, 0)
        with patch("cleaning_job.__main__.Cloud", return_value=self.cloud), \
                patch("sys.stdout", new_callable=io.StringIO) as output, \
                patch("builtins.input", side_effect=AssertionError("No prompts")):
            code = main(["job", "--job", "batch-one", "--execute", "--wait-seconds", "0",
                         "--plan-directory", self.temp.name])
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output.getvalue())["complete"])


class SharedCleanupTests(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeCloud()
        self.cloud.job_data.clear()
        self.image = REPOSITORY + "stockfish-analyzer@sha256:" + "a" * 64
        self.cloud.image_data = [{"uri": self.image}, {"uri": REPOSITORY + "keep@sha256:" + "b" * 64}]
        self.cloud.log_data = [f"projects/{PROJECT}/logs/batch_task_logs",
                               f"projects/{PROJECT}/logs/cloudaudit.googleapis.com%2Factivity"]

    def test_shared_preview_never_deletes(self):
        report = cleanup_shared(self.cloud, images=[self.image], logs=["batch_task_logs"])
        self.assertTrue(report["dry_run"])
        self.assertEqual(self.cloud.writes, [])

    def test_shared_execute_requires_dedicated_mode_and_idle_project(self):
        with self.assertRaises(CleanupError):
            cleanup_shared(self.cloud, images=[self.image], execute=True)
        self.cloud.job_data[(REGION, "batch-one")] = job()
        with self.assertRaises(CleanupError):
            cleanup_shared(self.cloud, images=[self.image], execute=True, dedicated_test_project=True)
        self.cloud.job_data.clear()
        self.cloud.resource_data = [{"name": "unrelated-disk"}]
        with self.assertRaises(CleanupError):
            cleanup_shared(self.cloud, images=[self.image], execute=True, dedicated_test_project=True)
        self.assertEqual(self.cloud.writes, [])

    def test_shared_images_must_be_pinned_and_logs_allowlisted(self):
        for image in (REPOSITORY + "stockfish:latest", "other/stockfish@sha256:" + "a" * 64):
            with self.assertRaises(CleanupError):
                cleanup_shared(self.cloud, images=[image])
        for log in ("cloudaudit.googleapis.com/activity", "ping", "diagnostic-log", "*"):
            with self.assertRaises(CleanupError):
                cleanup_shared(self.cloud, logs=[log])
        self.assertEqual(len(TEST_LOGS), 7)
        self.assertEqual(self.cloud.writes, [])

    def test_shared_only_deletes_selected_existing_digests_and_logs(self):
        with patch("builtins.input", side_effect=AssertionError("No prompts")):
            result = cleanup_shared(self.cloud, images=[self.image], logs=["batch_task_logs", "batch_agent_logs"],
                                    execute=True, dedicated_test_project=True)
        self.assertEqual(self.cloud.writes, [("image", self.image), ("log", "batch_task_logs")])
        self.assertEqual(len(self.cloud.image_data), 1)
        self.assertFalse(result["pending_images"])


if __name__ == "__main__":
    unittest.main()
