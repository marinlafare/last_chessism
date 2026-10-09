"""Image lifecycle safety tests using the same offline cloud fake as cleanup."""

from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cleaning_job import cleaning_job, cleanup
from cleaning_job.__main__ import main
from cleaning_job.cloud import CleanupError, REGION, REPOSITORY
from test_cleaning_job import FakeCloud, job

IMAGE = REPOSITORY + "stockfish-analyzer@sha256:" + "a" * 64
OTHER_IMAGE = REPOSITORY + "stockfish-analyzer@sha256:" + "b" * 64
UPLOADED = "2026-10-06T00:30:00Z"


def with_image(value, uri=IMAGE):
    value["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["imageUri"] = uri
    return value


class JobImageCleanupTests(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeCloud()
        self.cloud.job_data[(REGION, "batch-one")] = with_image(job())
        self.cloud.image_data = [{"uri": IMAGE, "uploadTime": UPLOADED},
                                 {"uri": OTHER_IMAGE, "uploadTime": UPLOADED}]
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def run_cleanup(self, **kwargs):
        return cleaning_job("batch-one", cloud=self.cloud, plan_directory=self.temp.name,
                            wait_seconds=0, **kwargs)

    def plan(self):
        return cleanup.prepare(self.cloud, ["batch-one"])

    def test_default_preview_records_exact_image_and_upload_without_writes(self):
        report = self.run_cleanup()
        self.assertTrue(report["image_cleanup_included"])
        self.assertEqual(report["images"], [{"uri": IMAGE, "upload_time": UPLOADED}])
        plan = json.loads(Path(report["plan"]).read_text())
        self.assertEqual(plan["version"], 2)
        self.assertEqual(plan["jobs"][0]["images"], [IMAGE])
        self.assertEqual(self.cloud.writes, [])

    def test_image_is_deleted_last_without_prompt_and_other_versions_survive(self):
        with patch("builtins.input", side_effect=AssertionError("No prompts")):
            result = self.run_cleanup(execute=True)
        self.assertTrue(result["complete"])
        self.assertEqual([w[0] for w in self.cloud.writes], ["job", "object", "object", "image"])
        self.assertEqual(self.cloud.writes[-1], ("image", IMAGE))
        self.assertEqual([i["uri"] for i in self.cloud.image_data], [OTHER_IMAGE])
        self.assertEqual(result["remaining_images"], [])
        self.assertEqual(result["image_delete_operations"], ["operations/test-image-delete"])

    def test_same_plan_is_idempotent_after_success(self):
        report = self.run_cleanup(execute=True)
        plan = json.loads(Path(report["plan"]).read_text())
        self.cloud.writes.clear()
        again = cleaning_job(plan=plan, cloud=self.cloud, plan_directory=self.temp.name, execute=True, wait_seconds=0)
        self.assertTrue(again["complete"])
        self.assertEqual(self.cloud.writes, [])

    def test_other_job_in_any_state_and_region_defers_only_image(self):
        for state in ("QUEUED", "SCHEDULED", "RUNNING", "FAILED", "SUCCEEDED", "DELETION_IN_PROGRESS"):
            with self.subTest(state=state):
                cloud = FakeCloud()
                cloud.job_data[(REGION, "batch-one")] = with_image(job())
                cloud.job_data[("europe-west1", "peer")] = with_image(job("peer", "other-run", state, "europe-west1"))
                cloud.image_data = deepcopy(self.cloud.image_data)
                report = cleaning_job("batch-one", cloud=cloud, plan_directory=self.temp.name, execute=True, wait_seconds=0)
                self.assertFalse(report["complete"])
                self.assertEqual(report["remaining_images"], [IMAGE])
                self.assertIn("/locations/europe-west1/jobs/peer", report["image_blockers"][IMAGE][0])
                self.assertEqual([w[0] for w in cloud.writes], ["job", "object", "object"])

    def test_shared_image_cleanup_resumes_when_peer_releases_it(self):
        self.cloud.job_data[(REGION, "peer")] = with_image(job("peer", "other-run", "RUNNING"))
        report = self.run_cleanup(execute=True)
        plan = json.loads(Path(report["plan"]).read_text())
        self.cloud.job_data.pop((REGION, "peer"))
        self.cloud.writes.clear()
        result = cleaning_job(plan=plan, cloud=self.cloud, plan_directory=self.temp.name, execute=True, wait_seconds=0)
        self.assertTrue(result["complete"])
        self.assertEqual(self.cloud.writes, [("image", IMAGE)])

    def test_grouped_attempts_delete_shared_image_only_once(self):
        self.cloud.job_data[(REGION, "failed-attempt")] = with_image(job("failed-attempt", state="FAILED"))
        result = cleaning_job(["failed-attempt", "batch-one"], cloud=self.cloud, plan_directory=self.temp.name,
                              execute=True, include_failed=True, wait_seconds=0)
        self.assertTrue(result["complete"])
        self.assertEqual([w for w in self.cloud.writes if w[0] == "image"], [("image", IMAGE)])

    def test_different_digest_does_not_block_same_package(self):
        self.cloud.job_data[(REGION, "peer")] = with_image(job("peer", "other-run", "RUNNING"), OTHER_IMAGE)
        self.assertTrue(self.run_cleanup(execute=True)["complete"])
        self.assertIn(("image", IMAGE), self.cloud.writes)

    def test_peer_mutable_tags_and_implicit_latest_conservatively_block(self):
        for reference in (IMAGE.split("@")[0] + ":latest", IMAGE.split("@")[0], "https://" + IMAGE):
            with self.subTest(reference=reference):
                self.cloud.job_data[(REGION, "peer")] = with_image(job("peer", "other-run", "RUNNING"), reference)
                report = self.run_cleanup()
                self.assertIn(IMAGE, report["image_blockers"])
        self.assertEqual(self.cloud.writes, [])

    def test_environment_script_references_are_also_protected(self):
        peer = job("peer", "other-run", "QUEUED")
        peer["taskGroups"][0]["taskSpec"]["runnables"].append({"script": {"text": f"docker run {IMAGE} --help"}})
        self.cloud.job_data[(REGION, "peer")] = peer
        self.assertIn(IMAGE, self.run_cleanup()["image_blockers"])

    def test_owned_tag_and_malformed_digest_are_refused_before_any_deletion(self):
        for uri in (IMAGE.split("@")[0] + ":latest", IMAGE[:-1], REPOSITORY + "../bad@sha256:" + "a" * 64):
            with self.subTest(uri=uri):
                self.cloud.job_data[(REGION, "batch-one")] = with_image(job(), uri)
                with self.assertRaises(CleanupError):
                    self.run_cleanup(execute=True)
                self.assertEqual(self.cloud.writes, [])

    def test_other_registry_images_are_never_delete_targets(self):
        self.cloud.job_data[(REGION, "batch-one")] = with_image(job(), "other.pkg.dev/project/repo/image@sha256:" + "a" * 64)
        report = self.run_cleanup(execute=True)
        self.assertTrue(report["complete"])
        self.assertFalse(any(w[0] == "image" for w in self.cloud.writes))

    def test_reupload_changes_identity_and_blocks_old_plan(self):
        plan = self.plan()
        self.cloud.image_data[0]["uploadTime"] = "2026-10-07T00:30:00Z"
        with self.assertRaisesRegex(CleanupError, "uploaded/replaced"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertEqual(self.cloud.writes, [])

    def test_reupload_after_files_are_removed_is_preserved(self):
        original = self.cloud.delete_object

        def delete(value):
            original(value)
            self.cloud.image_data[0]["uploadTime"] = "2026-10-07T00:30:00Z"

        self.cloud.delete_object = delete
        with self.assertRaisesRegex(CleanupError, "uploaded/replaced.*Saved cleanup plan"):
            self.run_cleanup(execute=True)
        self.assertFalse(self.cloud.object_data)
        self.assertFalse(any(w[0] == "image" for w in self.cloud.writes))

    def test_preview_lists_shared_image_blockers_without_modifying_it(self):
        self.cloud.job_data[(REGION, "peer")] = with_image(job("peer", "other-run", "QUEUED"))
        report = self.run_cleanup()
        self.assertEqual(report["image_blockers"][IMAGE], [self.cloud.job_data[(REGION, "peer")]["name"]])
        self.assertEqual(self.cloud.writes, [])

    def test_missing_image_is_ok_but_not_a_later_upload_of_that_digest(self):
        self.cloud.image_data = []
        plan = self.plan()
        self.assertIsNone(plan["images"][0]["upload_time"])
        self.assertTrue(cleanup.apply(self.cloud, plan, wait_seconds=0)["complete"])
        self.cloud.writes.clear()
        self.cloud.image_data = [{"uri": IMAGE, "uploadTime": UPLOADED}]
        with self.assertRaisesRegex(CleanupError, "uploaded/replaced"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertEqual(self.cloud.writes, [])

    def test_missing_timestamp_cannot_weaken_identity_check(self):
        del self.cloud.image_data[0]["uploadTime"]
        with self.assertRaisesRegex(CleanupError, "timestamp missing"):
            self.run_cleanup(execute=True)
        self.assertEqual(self.cloud.writes, [])

    def test_image_inventory_errors_do_not_become_already_missing(self):
        with patch.object(self.cloud, "images", side_effect=CleanupError("HTTP 403")):
            with self.assertRaisesRegex(CleanupError, "403"):
                self.run_cleanup(execute=True)
        self.assertEqual(self.cloud.writes, [])

    def test_running_compute_defers_data_and_image(self):
        self.cloud.resource_data = [{"name": self.cloud.job_data[(REGION, "batch-one")]["uid"] + "-group0-0"}]
        report = self.run_cleanup(execute=True)
        self.assertFalse(report["complete"])
        self.assertEqual([w[0] for w in self.cloud.writes], ["job"])

    def test_data_failure_preserves_image_and_can_resume(self):
        with patch.object(self.cloud, "delete_object", side_effect=CleanupError("HTTP 500")):
            with self.assertRaisesRegex(CleanupError, "Saved cleanup plan"):
                self.run_cleanup(execute=True)
        self.assertFalse(any(w[0] == "image" for w in self.cloud.writes))
        plan = json.loads(next(Path(self.temp.name).glob("*.json")).read_text())
        self.assertTrue(cleanup.apply(self.cloud, plan, wait_seconds=0)["complete"])

    def test_image_delete_failure_is_resumable_after_data_is_gone(self):
        with patch.object(self.cloud, "delete_image", side_effect=CleanupError("HTTP 500")):
            with self.assertRaisesRegex(CleanupError, "Saved cleanup plan"):
                self.run_cleanup(execute=True)
        self.assertFalse(self.cloud.job_data)
        self.assertFalse(self.cloud.object_data)
        plan = json.loads(next(Path(self.temp.name).glob("*.json")).read_text())
        self.assertTrue(cleanup.apply(self.cloud, plan, wait_seconds=0)["complete"])

    def test_image_delete_is_deferred_if_peer_appears_after_file_cleanup(self):
        original = self.cloud.delete_object

        def delete(value):
            original(value)
            self.cloud.job_data[(REGION, "peer")] = with_image(job("peer", "other-run", "QUEUED"))

        self.cloud.delete_object = delete
        report = self.run_cleanup(execute=True)
        self.assertFalse(report["complete"])
        self.assertIn(IMAGE, report["image_blockers"])
        self.assertFalse(any(w[0] == "image" for w in self.cloud.writes))

    def test_async_image_deletion_is_pending_not_falsely_complete(self):
        def pending(uri):
            self.cloud.writes.append(("image", uri))
            return {"name": "operations/deletion-pending"}

        self.cloud.delete_image = pending
        with patch("cleaning_job.__main__.Cloud", return_value=self.cloud), patch("sys.stdout", new_callable=io.StringIO) as output:
            code = main(["job", "--job", "batch-one", "--execute", "--wait-seconds", "0",
                         "--plan-directory", self.temp.name])
        self.assertEqual(code, 2)
        result = json.loads(output.getvalue())
        self.assertEqual(result["image_delete_operations"], ["operations/deletion-pending"])
        self.assertEqual(result["remaining_images"], [IMAGE])

    def test_legacy_plan_resumes_without_silently_deleting_images(self):
        plan = self.plan()
        plan["version"] = 1
        del plan["images"]
        for target in plan["jobs"]:
            del target["images"]
        plan["sha256"] = cleanup.digest(plan)
        report = cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertTrue(report["complete"])
        self.assertFalse(report["image_cleanup_included"])
        self.assertFalse(any(w[0] == "image" for w in self.cloud.writes))

    def test_modified_job_or_plan_cannot_expand_image_scope(self):
        plan = self.plan()
        plan["images"].append({"uri": OTHER_IMAGE, "upload_time": UPLOADED})
        plan["sha256"] = cleanup.digest(plan)
        with self.assertRaisesRegex(CleanupError, "scope mismatch"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        plan = self.plan()
        self.cloud.job_data[(REGION, "batch-one")] = with_image(job(), OTHER_IMAGE)
        with self.assertRaisesRegex(CleanupError, "image references changed"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertEqual(self.cloud.writes, [])

    def test_legacy_plan_cannot_smuggle_image_targets(self):
        plan = self.plan()
        plan["version"] = 1
        plan["sha256"] = cleanup.digest(plan)
        with self.assertRaisesRegex(CleanupError, "Legacy plans"):
            cleanup.apply(self.cloud, plan, wait_seconds=0)
        self.assertEqual(self.cloud.writes, [])


if __name__ == "__main__":
    unittest.main()
