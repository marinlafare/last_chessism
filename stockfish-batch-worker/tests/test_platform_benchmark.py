import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from benchmark_platforms import run
from benchmark_platforms import log_cleanup
from stockfish_batch.config import parse_args


class PlatformBenchmarkTests(unittest.TestCase):
    ident = "20261006ab01"
    image = run.REPOSITORY + "platform-20261006ab01@sha256:" + "a" * 64

    def test_identical_engine_settings_and_separate_outputs(self):
        first = run.configuration(self.ident, "batch")
        second = run.configuration(self.ident, "cloud-run")
        self.assertEqual(first.analysis_settings(), second.analysis_settings())
        self.assertEqual(first.input, second.input)
        self.assertNotEqual(first.output, second.output)
        self.assertEqual(first.nodes, 100000)
        self.assertEqual(first.workers, 4)
        self.assertEqual(first.max_positions, 25000)
        self.assertEqual(first.batch_size, 500)
        self.assertEqual(first.stall_timeout, 300)
        self.assertEqual(first.run_timeout, 3540)
        for config in (first, second):
            self.assertEqual(parse_args(run.arguments(config)), config)

    def test_both_specs_have_one_bounded_task_without_retries(self):
        spec = run.specifications(self.ident, self.image)
        group = spec["batch"]["taskGroups"][0]
        self.assertEqual(len(spec["batch"]["taskGroups"]), 1)
        self.assertEqual(group["parallelism"], "1")
        self.assertEqual(group["taskCount"], "1")
        self.assertEqual(group["taskSpec"]["maxRunDuration"], "3600s")
        self.assertEqual(group["taskSpec"]["maxRetryCount"], 0)
        policy = spec["batch"]["allocationPolicy"]["instances"][0]["policy"]
        self.assertEqual(policy["machineType"], "n2d-standard-4")
        self.assertEqual(policy["provisioningModel"], "SPOT")
        template = spec["cloud-run"]["template"]
        self.assertEqual(template["taskCount"], 1)
        self.assertEqual(template["parallelism"], 1)
        task = template["template"]
        self.assertEqual(task["timeout"], "3600s")
        self.assertEqual(task["maxRetries"], 0)
        self.assertEqual(task["containers"][0]["image"], self.image)
        self.assertEqual(task["containers"][0]["resources"]["limits"], {"cpu": "4", "memory": "12Gi"})

    def test_rejects_mutable_images_and_unsafe_identities(self):
        for ident in ("", "../foo", "not-a-benchmark", "20261006AB01"):
            with self.subTest(ident=ident), self.assertRaises(ValueError):
                run.names(ident)
        with self.assertRaises(RuntimeError):
            run.specifications(self.ident, run.REPOSITORY + "example:latest")

    def test_cloud_run_must_match_actual_deployment(self):
        spec = run.specifications(self.ident, self.image)["cloud-run"]
        run.assert_run_spec(copy.deepcopy(spec), spec)
        for field, value in (("maxRetries", 3), ("timeout", "7200s"), ("serviceAccount", "someone-else")):
            actual = copy.deepcopy(spec)
            actual["template"]["template"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                run.assert_run_spec(actual, spec)
        actual = copy.deepcopy(spec)
        actual["template"]["template"]["containers"][0]["command"] = ["sh"]
        with self.assertRaises(ValueError):
            run.assert_run_spec(actual, spec)

    def test_launch_fence_refuses_second_submission(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            run.fence(directory, "launch.started")
            with self.assertRaises(FileExistsError):
                run.fence(directory, "launch.started")

    def test_live_status_after_cleanup_does_not_claim_jobs_never_started(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            run.save(directory, "cleanup-result.json", {"complete": True})
            run.save(directory, "cloud-run-delete-complete.json", {"done": True})
            client = Mock()
            client.job.return_value = None
            with patch.object(run, "run_job", return_value=None):
                observed = run.inspect(directory, client, {"jobs": run.names(self.ident)})
            self.assertEqual(observed["states"], {"batch": "CLEANED", "cloud-run": "CLEANED"})

    def test_execution_states(self):
        self.assertEqual(run.execution_state(None), "NOT_STARTED")
        self.assertEqual(run.execution_state({"name": "job"}), "PENDING")
        self.assertEqual(run.execution_state({"startTime": "now"}), "RUNNING")
        for raw, expected in (("CONDITION_SUCCEEDED", "SUCCEEDED"), ("CONDITION_FAILED", "FAILED")):
            self.assertEqual(run.execution_state({"conditions": [{"type": "Completed", "state": raw}]}), expected)

    def test_execution_reference_accepts_short_or_full_name_only_for_this_job(self):
        job = run.names(self.ident)["cloud-run"]
        short = job + "-pd2qw"
        full = run.PARENT + "/jobs/" + job + "/executions/" + short
        self.assertEqual(run.execution_path(job, short), full)
        self.assertEqual(run.execution_path(job, full), full)
        for unsafe in ("another-job-pd2qw", "../" + short, full.replace(run.PROJECT, "another-project")):
            with self.subTest(name=unsafe), self.assertRaises(ValueError):
                run.execution_path(job, unsafe)

    def test_sample_is_deterministic_without_duplicates(self):
        rows = [{"id": str(i), "fen": f"8/8/8/8/8/8/4K3/7k w - - 0 {i+1}"} for i in range(12)]
        with patch.object(run, "COUNT", 5):
            first = run.sample(rows)
            self.assertEqual(first, run.sample(rows))
            self.assertEqual(len(first.splitlines()), 5)
            with self.assertRaises(ValueError):
                run.sample(rows + [rows[0]])
            with self.assertRaises(ValueError):
                run.sample(rows[:4])


class PlatformLogCleanupTests(unittest.TestCase):
    def test_entries_after_an_empty_page_still_protect_stream(self):
        cloud = Mock()
        cloud.request.side_effect = [{"nextPageToken": "second"}, {"entries": [{"textPayload": "other"}]}]
        self.assertTrue(log_cleanup.exists(cloud, "query"))
        self.assertEqual(cloud.request.call_count, 2)

    def test_repeated_tokens_never_authorize_deletion(self):
        cloud = Mock()
        cloud.request.return_value = {"nextPageToken": "same"}
        with self.assertRaises(ValueError):
            log_cleanup.exists(cloud, "query")

    def test_query_error_is_not_an_empty_stream(self):
        cloud = Mock()
        cloud.request.side_effect = RuntimeError("permission denied")
        with self.assertRaises(RuntimeError):
            log_cleanup.exists(cloud, "query")

    def test_unrelated_entries_protect_whole_stream(self):
        with patch.object(log_cleanup, "exists", return_value=True) as exists:
            self.assertEqual(log_cleanup.inspect(Mock(), "run.googleapis.com/stdout", "OWNER"), "retained_unrelated")
            self.assertEqual(exists.call_count, 1)
            query = exists.call_args.args[1]
            self.assertIn("AND NOT OWNER", query)
            self.assertNotIn("timestamp", query)

    def test_only_nonempty_owned_stream_is_eligible(self):
        with patch.object(log_cleanup, "exists", side_effect=[False, True]):
            self.assertEqual(log_cleanup.inspect(Mock(), "batch_task_logs", "OWNER"), "eligible")
        with patch.object(log_cleanup, "exists", side_effect=[False, False]):
            self.assertEqual(log_cleanup.inspect(Mock(), "batch_task_logs", "OWNER"), "empty")

    def test_audit_and_workflow_logs_never_eligible(self):
        for log in ("cloudaudit.googleapis.com/activity", "workflows.googleapis.com/executions_system"):
            with self.assertRaises(ValueError):
                log_cleanup.inspect(Mock(), log, "OWNER")


if __name__ == "__main__":
    unittest.main()
