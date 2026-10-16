"""Exercise the real Storage SDK transport with fake HTTP and virtual time."""
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock, patch

try:
    import requests
    from google.api_core.exceptions import Unauthorized, Forbidden
    from cloud_job.auth import GcloudCredentials
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False

from cleaning_job.cloud import BUCKET, CleanupError
from cloud_job.launch import Client


@unittest.skipUnless(SDK_AVAILABLE, "Google Storage SDK not installed")
class StorageCredentialsTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 6, tzinfo=timezone.utc)
        self.cloud = Client("fake-gcloud")
        clock = patch("cleaning_job.cloud._utcnow", side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)
        auth = patch("cleaning_job.cloud.subprocess.run", side_effect=self.credential)
        self.run = auth.start()
        self.addCleanup(auth.stop)
        transport = patch("requests.sessions.Session.request", side_effect=self.response)
        self.http = transport.start()
        self.addCleanup(transport.stop)
        self.adapter = self.cloud.storage()
        self.client = self.adapter._client
        self.addCleanup(self.client.close)

    def credential(self, *a, **kw):
        return Mock(stdout=json.dumps({"credential": {
            "access_token": f"token-{self.run.call_count}",
            "token_expiry": (self.now + timedelta(hours=1)).isoformat(),
        }}))

    def response(self, *a, status=200, **kw):
        response = requests.Response()
        response.status_code = status
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps({"name": "results/run/batch.json", "bucket": BUCKET,
                                       "generation": "123", "size": "10"}).encode()
        response.request = requests.Request("GET", "https://storage.googleapis.com").prepare()
        return response

    def reload(self):
        self.client.bucket(BUCKET).blob("results/run/batch.json").reload(retry=None, timeout=1)

    def test_one_storage_client_renews_through_twelve_hours(self):
        for _ in range(145):
            self.reload()
            self.now += timedelta(minutes=5)
        self.assertEqual(self.run.call_count, 14)
        self.assertEqual(self.http.call_count, 145)
        self.assertEqual(self.http.call_args.kwargs["headers"]["authorization"], "Bearer token-14")

    def test_sdk_401_forces_refresh_and_retries_same_request(self):
        self.http.side_effect = [self.response(status=401), self.response()]
        self.reload()
        self.assertEqual(self.http.call_count, 2)
        first, second = self.http.call_args_list
        self.assertEqual(first.args, second.args)
        self.assertEqual(first.kwargs["data"], second.kwargs["data"])
        self.assertEqual(first.kwargs["headers"]["authorization"], "Bearer token-1")
        self.assertEqual(second.kwargs["headers"]["authorization"], "Bearer token-2")
        self.assertIn("--force-auth-refresh", self.run.call_args.args[0])

    def test_repeated_401_is_bounded_and_403_never_refreshes(self):
        self.http.side_effect = lambda *a, **kw: self.response(status=401)
        with self.assertRaises(Unauthorized):
            self.reload()
        self.assertEqual(self.http.call_count, 2)
        self.assertEqual(self.run.call_count, 2)
        self.http.reset_mock()
        self.http.side_effect = lambda *a, **kw: self.response(status=403)
        with self.assertRaises(Forbidden):
            self.reload()
        self.assertEqual(self.http.call_count, 1)
        self.assertEqual(self.run.call_count, 2)

    def test_sdk_and_rest_share_renewal_and_do_not_reuse_rejected_tokens(self):
        with patch("cleaning_job.cloud.urlopen") as rest:
            rest.return_value.__enter__.return_value.read.return_value = b"{}"
            self.cloud.bucket()
            self.reload()
            self.assertEqual(self.run.call_count, 1)
            self.http.side_effect = [self.response(status=401), self.response()]
            self.reload()
            self.cloud.bucket()
            self.assertEqual(self.run.call_count, 2)
            self.assertEqual(rest.call_args.args[0].get_header("Authorization"), "Bearer token-2")

    def test_failed_renewal_stops_before_network_and_does_not_hide_missing_results(self):
        self.reload()
        self.now += timedelta(hours=2)
        self.run.side_effect = OSError("secret")
        with self.assertRaises(CleanupError):
            self.adapter.read(f"gs://{BUCKET}/results/run/batch.json")
        self.assertEqual(self.http.call_count, 1)


if __name__ == "__main__":
    unittest.main()
