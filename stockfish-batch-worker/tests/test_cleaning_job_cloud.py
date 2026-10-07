"""Verify REST paths, preconditions, pagination and errors without a network."""

import io
import json
import subprocess
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import traceback
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

from cleaning_job.cloud import ARTIFACT_PARENT, BUCKET, Cloud, CleanupError, PROJECT, REGION, REPOSITORY


class CloudAdapterTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 6, tzinfo=timezone.utc)
        clock = patch("cleaning_job.cloud._utcnow", side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)
        self.cloud = Cloud("/fake/google-cloud-sdk/bin/gcloud")
        self.auth = patch("cleaning_job.cloud.subprocess.run", return_value=self.credential())
        self.run = self.auth.start()
        self.addCleanup(self.auth.stop)
        self.network = patch("cleaning_job.cloud.urlopen")
        self.urlopen = self.network.start()
        self.addCleanup(self.network.stop)
        self.respond({})

    def credential(self, token="test-token", lifetime=3600):
        return Mock(stdout=json.dumps({"credential": {"access_token": token,
                    "token_expiry": (self.now + timedelta(seconds=lifetime)).isoformat()}}))

    def respond(self, value):
        self.urlopen.return_value.__enter__.return_value.read.return_value = json.dumps(value).encode()

    def test_auth_is_read_only_explicit_project_and_cached(self):
        self.cloud.bucket()
        self.cloud.bucket()
        self.run.assert_called_once_with(
            ["/fake/google-cloud-sdk/bin/gcloud", "config", "config-helper", "--format=json(credential)",
             "--min-expiry=300s", f"--project={PROJECT}", "--quiet"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        request = self.urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        self.assertEqual(self.urlopen.call_args.kwargs["timeout"], 30)

    def test_token_is_not_printed_in_failure(self):
        self.run.side_effect = subprocess.CalledProcessError(1, ["gcloud"], output="supersecret")
        with self.assertRaises(CleanupError) as error:
            self.cloud.bucket()
        self.assertNotIn("supersecret", str(error.exception))
        self.urlopen.assert_not_called()

    def test_real_expiry_not_fetch_time_drives_renewal(self):
        self.run.side_effect = [self.credential(lifetime=400), self.credential("renewed")]
        self.cloud.bucket()
        self.now += timedelta(seconds=101)
        self.cloud.bucket()
        self.assertEqual(self.run.call_count, 2)
        self.assertEqual(self.urlopen.call_args.args[0].get_header("Authorization"), "Bearer renewed")

    def test_monotonic_deadline_survives_wall_clock_moving_back(self):
        with patch("cleaning_job.cloud.time.monotonic", return_value=10) as mono:
            self.cloud.bucket()
            self.now -= timedelta(hours=2)
            mono.return_value = 3400
            self.run.return_value = self.credential("renewed")
            self.cloud.bucket()
        self.assertEqual(self.run.call_count, 2)

    def test_12_hour_process_repeatedly_renews_without_network_waits(self):
        self.run.side_effect = lambda *a, **kw: self.credential(f"token-{self.run.call_count}")
        for _ in range(145):  # Polls from hour zero through hour twelve.
            self.cloud.bucket()
            self.now += timedelta(minutes=5)
        self.assertEqual(self.urlopen.call_count, 145)
        self.assertEqual(self.run.call_count, 14)  # Not one token for the whole run.
        self.assertEqual(self.urlopen.call_args.args[0].get_header("Authorization"), "Bearer token-14")

    def test_401_forces_one_refresh_preserving_delete_preconditions(self):
        response = self.urlopen.return_value
        self.run.side_effect = [self.credential(), self.credential("new-token")]
        self.urlopen.side_effect = [HTTPError("https://example.invalid", 401, "expired", {}, io.BytesIO()), response]
        self.cloud.delete_object({"name": "results/run/a.json", "generation": "123"})
        first, second = [call.args[0] for call in self.urlopen.call_args_list]
        self.assertEqual(first.full_url, second.full_url)
        self.assertEqual(first.data, second.data)
        self.assertEqual(first.method, second.method)
        self.assertNotEqual(first.get_header("Authorization"), second.get_header("Authorization"))
        self.assertIn("--force-auth-refresh", self.run.call_args.args[0])

    def test_401_submission_retry_keeps_exact_job_identity_and_body(self):
        response = self.urlopen.return_value
        self.urlopen.side_effect = [HTTPError("https://example.invalid", 401, "expired", {}, io.BytesIO()), response]
        self.cloud.request("batch", "projects/test/locations/us-central1/jobs", method="POST",
                           params={"jobId": "same-job"}, body={"taskGroups": []})
        first, second = [call.args[0] for call in self.urlopen.call_args_list]
        self.assertEqual((first.full_url, first.method, first.data), (second.full_url, second.method, second.data))

    def test_repeated_401_stops_after_one_retry_and_403_does_not_refresh(self):
        for status, requests in ((401, 2), (403, 1)):
            self.run.reset_mock()
            self.urlopen.reset_mock()
            self.cloud = Cloud("fake-gcloud")
            self.urlopen.side_effect = HTTPError("https://example.invalid", status, "secret", {}, io.BytesIO(b"secret"))
            with self.assertRaises(CleanupError):
                self.cloud.bucket()
            self.assertEqual(self.urlopen.call_count, requests)
            self.assertEqual(self.run.call_count, requests)

    def test_bad_expiry_missing_token_or_failed_refresh_never_uses_stale_token(self):
        invalid = ["supersecret", "{}", '{"credential":null}',
                   self.credential(lifetime=299).stdout,
                   self.credential(lifetime=-1).stdout,
                   self.credential(token="").stdout,
                   self.credential(token="secret\nheader").stdout,
                   json.dumps({"credential": {"access_token": "secret", "token_expiry": "bad-secret"}})]
        for value in invalid:
            self.cloud = Cloud("fake-gcloud")
            self.run.side_effect = None
            self.run.return_value = self.credential()
            self.cloud.tokens.get()
            self.run.return_value = Mock(stdout=value)
            with self.assertRaises(CleanupError) as error:
                self.cloud.tokens.get(force=True)
            self.assertNotIn("secret", "".join(traceback.format_exception(error.exception)))
            self.assertIsNone(self.cloud.tokens._token)
        self.urlopen.assert_not_called()

    def test_concurrent_users_share_one_refresh(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            tokens = list(pool.map(lambda _: self.cloud.tokens.get()[0], range(32)))
        self.assertEqual(set(tokens), {"test-token"})
        self.run.assert_called_once()
        self.run.return_value = self.credential("replacement")
        with ThreadPoolExecutor(max_workers=8) as pool:
            tokens = list(pool.map(lambda _: self.cloud.tokens.get(
                force=True, rejected_token="test-token")[0], range(32)))
        self.assertEqual(set(tokens), {"replacement"})
        self.assertEqual(self.run.call_count, 2)

    def test_failed_401_refresh_does_not_replay_with_rejected_token(self):
        self.run.side_effect = [self.credential(), subprocess.TimeoutExpired("gcloud", 30, output="secret")]
        self.urlopen.side_effect = HTTPError("https://example.invalid", 401, "expired", {}, io.BytesIO())
        with self.assertRaises(CleanupError) as error:
            self.cloud.bucket()
        self.assertEqual(self.urlopen.call_count, 1)
        self.assertIsNone(self.cloud.tokens._token)
        self.assertNotIn("secret", "".join(traceback.format_exception(error.exception)))

    def test_only_structured_404_is_missing_not_permission_or_network_errors(self):
        for status in (400, 401, 403, 409, 412, 429, 500):
            self.urlopen.side_effect = HTTPError("https://example.invalid", status, "failure", {}, io.BytesIO(b"secret"))
            with self.subTest(status=status), self.assertRaises(CleanupError):
                self.cloud.job("batch-one")
        self.urlopen.side_effect = HTTPError("https://example.invalid", 404, "missing", {}, io.BytesIO())
        self.assertIsNone(self.cloud.job("batch-one"))
        with self.assertRaises(CleanupError):
            self.cloud.bucket()  # bucket absence is not a successful cleanup
        self.urlopen.side_effect = URLError("offline")
        with self.assertRaises(CleanupError):
            self.cloud.job("batch-one")

    def test_job_delete_request_id_is_uid_bound_and_repeatable(self):
        self.cloud.delete_job("batch-one", REGION, "job-uid-a")
        first = self.urlopen.call_args.args[0]
        self.cloud.delete_job("batch-one", REGION, "job-uid-a")
        second = self.urlopen.call_args.args[0]
        self.cloud.delete_job("batch-one", REGION, "job-uid-b")
        third = self.urlopen.call_args.args[0]
        self.assertEqual(first.get_method(), "DELETE")
        self.assertEqual(first.full_url, second.full_url)
        self.assertNotEqual(first.full_url, third.full_url)
        self.assertIn(f"projects/{PROJECT}/locations/{REGION}/jobs/batch-one?requestId=", first.full_url)

    def test_object_delete_encodes_name_and_pins_generation_twice(self):
        self.cloud.delete_object({"name": "results/run-one/a?# *.json", "generation": "123"})
        request = self.urlopen.call_args.args[0]
        url = urlsplit(request.full_url)
        self.assertEqual(request.get_method(), "DELETE")
        self.assertEqual(url.path, f"/storage/v1/b/{BUCKET}/o/results%2Frun-one%2Fa%3F%23%20%2A.json")
        self.assertEqual(parse_qs(url.query), {"generation": ["123"], "ifGenerationMatch": ["123"]})

    def test_objects_are_listed_by_literal_prefix_with_all_versions(self):
        self.respond({"items": [{"name": "results/run-one/x", "generation": "1"}]})
        self.assertEqual(len(self.cloud.objects("results/run-one/")), 1)
        url = urlsplit(self.urlopen.call_args.args[0].full_url)
        self.assertEqual(parse_qs(url.query), {"prefix": ["results/run-one/"], "versions": ["true"]})

    def test_jobs_all_regions_and_all_pages(self):
        with patch.object(self.cloud, "request", side_effect=[
            {"jobs": [{"name": "a"}], "nextPageToken": "next"}, {"jobs": [{"name": "b"}]},
        ]) as request:
            self.assertEqual(self.cloud.jobs(), [{"name": "a"}, {"name": "b"}])
        self.assertTrue(all(call.args[1] == f"projects/{PROJECT}/locations/-/jobs" for call in request.call_args_list))
        self.assertEqual(request.call_args.kwargs["params"]["pageToken"], "next")

    def test_log_inventory_is_paginated_and_restricted_to_deletable_bucket(self):
        with patch.object(self.cloud, "request", side_effect=[
            {"entries": [{"insertId": "a"}], "nextPageToken": "next"}, {"entries": [{"insertId": "b"}]},
        ]) as request:
            self.assertEqual(len(list(self.cloud.log_entries('logName="test"', default_bucket=True))), 2)
            body = request.call_args.kwargs["body"]
            self.assertEqual(body["resourceNames"], [f"projects/{PROJECT}/locations/global/buckets/_Default/views/_AllLogs"])
            self.assertEqual(body["pageToken"], "next")
        with patch.object(self.cloud, "request", return_value={"nextPageToken": "loop"}):
            with self.assertRaises(CleanupError): list(self.cloud.log_entries("test"))
    def test_incomplete_and_repeated_pagination_refused(self):
        for page in ({"unreachable": ["us-central1"]}, {"unreachables": ["us-central1-c"]},
                     {"warning": {"code": "SOMETHING_FAILED"}}):
            with patch.object(self.cloud, "request", return_value=page), self.assertRaises(CleanupError):
                self.cloud.jobs()
        with patch.object(self.cloud, "request", return_value={"nextPageToken": "repeat"}), self.assertRaises(CleanupError):
            self.cloud.jobs()

    def test_compute_all_four_types_and_warning_handling(self):
        collections = ("instances", "disks", "instanceGroupManagers", "instanceTemplates")
        replies = [{"items": {"zones/us-central1-c": {kind: [{"name": kind + "-one"}]}}} for kind in collections]
        with patch.object(self.cloud, "request", side_effect=replies) as request:
            resources = self.cloud.resources()
        self.assertEqual([r["kind"] for r in resources], list(collections))
        self.assertEqual([c.args[1] for c in request.call_args_list],
                         [f"projects/{PROJECT}/aggregated/{kind}" for kind in collections])
        with patch.object(self.cloud, "request", return_value={"items": {"regions/one": {"warning": {"code": "NO_RESULTS_ON_PAGE"}}}}):
            self.assertEqual(self.cloud.resources(), [])
        with patch.object(self.cloud, "request", return_value={"items": {"regions/one": {"warning": {"code": "UNREACHABLE"}}}}):
            with self.assertRaises(CleanupError):
                self.cloud.resources()

    def test_image_deletion_is_one_digest_not_repository(self):
        image = REPOSITORY + "stockfish-analyzer@sha256:" + "a" * 64
        self.cloud.delete_image(image)
        request = self.urlopen.call_args.args[0]
        url = urlsplit(request.full_url)
        self.assertEqual(request.get_method(), "DELETE")
        self.assertEqual(url.path, f"/v1/{ARTIFACT_PARENT}/packages/stockfish-analyzer/versions/sha256:" + "a" * 64)
        self.assertEqual(parse_qs(url.query), {"force": ["true"]})

    def test_log_delete_encodes_slash_without_deleting_bucket(self):
        self.cloud.delete_log("compute.googleapis.com/shielded_vm_integrity")
        request = self.urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "DELETE")
        self.assertEqual(request.full_url, f"https://logging.googleapis.com/v2/projects/{PROJECT}/logs/compute.googleapis.com%2Fshielded_vm_integrity")

    def test_error_in_successful_http_operation_is_not_success(self):
        self.respond({"done": True, "error": {"code": 7}})
        with self.assertRaises(CleanupError):
            self.cloud.delete_job("batch-one", REGION, "job-uid-a")


if __name__ == "__main__":
    unittest.main()
