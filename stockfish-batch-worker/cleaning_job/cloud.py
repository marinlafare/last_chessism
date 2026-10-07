"""Small Google REST adapter; Python standard library and an authenticated gcloud.

No ADC setup, service-account keys, shell commands, or additional dependencies.
Credentials stay in memory. All API URLs are constructed here, never from plans.
"""

import json
from datetime import datetime, timezone
import subprocess
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import uuid

PROJECT = "chessism-production"
PROJECT_NUMBER = "276704059200"
REGION = "us-central1"
BUCKET = "chessism-batch-276704059200-us-central1"
REPOSITORY = f"{REGION}-docker.pkg.dev/{PROJECT}/chessism-workers/"
ARTIFACT_PARENT = f"projects/{PROJECT}/locations/{REGION}/repositories/chessism-workers"
ENDPOINTS = {
    "batch": "https://batch.googleapis.com/v1/",
    "storage": "https://storage.googleapis.com/storage/v1/",
    "compute": "https://compute.googleapis.com/compute/v1/",
    "artifacts": "https://artifactregistry.googleapis.com/v1/",
    "logging": "https://logging.googleapis.com/v2/",
    "workflows": "https://workflows.googleapis.com/v1/",
    "executions": "https://workflowexecutions.googleapis.com/v1/",
    "run": "https://run.googleapis.com/v2/",
}


class CleanupError(RuntimeError):
    """A failed safety check or cloud request; never equivalent to an empty list."""


def _utcnow():
    return datetime.now(timezone.utc)


class AccessTokens:
    """Expiry-aware gcloud credentials shared by REST and the Storage SDK.

    A token read from gcloud may already be almost an hour old. Never infer its
    lifetime from when we fetched it. Refresh repeatedly for arbitrarily long
    jobs, with both wall-clock and monotonic checks and no secrets in errors.
    """

    margin = 300

    def __init__(self, gcloud):
        self.gcloud = gcloud
        self._token = None
        self._expiry = None
        self._deadline = 0
        self._lock = threading.Lock()

    def get(self, *, force=False, rejected_token=None):
        with self._lock:
            usable = (self._token and self._expiry and
                      (self._expiry - _utcnow()).total_seconds() > self.margin and
                      time.monotonic() < self._deadline)
            # Another client/thread may already have replaced the rejected token.
            if usable and (not force or (rejected_token and rejected_token != self._token)):
                return self._token, self._expiry
            self._token, self._expiry, self._deadline = None, None, 0
            try:
                result = subprocess.run(
                    [self.gcloud, "config", "config-helper", "--format=json(credential)",
                     "--force-auth-refresh" if force else f"--min-expiry={self.margin}s",
                     f"--project={PROJECT}", "--quiet"],
                    check=True, capture_output=True, text=True, timeout=30,
                )
                credential = json.loads(result.stdout)["credential"]
                token = credential["access_token"]
                expiry = datetime.fromisoformat(credential["token_expiry"].replace("Z", "+00:00"))
                remaining = (expiry - _utcnow()).total_seconds()
                if (not isinstance(token, str) or not token or any(c.isspace() for c in token)
                        or remaining <= self.margin):
                    raise ValueError("Unusable credential")
            except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError, AttributeError):
                # No subprocess stdout/stderr or parser exception chains: these
                # may contain credentials. Never fall back to the stale token.
                raise CleanupError("gcloud authentication refresh failed; check gcloud auth login and PATH") from None
            self._token, self._expiry = token, expiry
            self._deadline = time.monotonic() + remaining - self.margin
            return token, expiry


class Cloud:
    def __init__(self, gcloud="gcloud"):
        self.gcloud = gcloud
        self.tokens = AccessTokens(gcloud)

    def request(self, service, path, *, method="GET", params=None, missing_ok=False, body=None):
        url = ENDPOINTS[service] + path
        if params:
            url += "?" + urlencode(params)
        data = None if body is None else json.dumps(body).encode()
        token, _ = self.tokens.get()
        for attempt in range(2):
            request = Request(url, method=method, data=data, headers={
                "Authorization": "Bearer " + token,
                "User-Agent": "chessism-cleaning-job/1",
                "Content-Type": "application/json",
            })
            try:
                with urlopen(request, timeout=30) as response:
                    raw = response.read()
                    return json.loads(raw) if raw else {}
            except HTTPError as exc:
                exc.close()
                if exc.code == 401 and attempt == 0:
                    # Only explicit rejection authorizes an auth retry. Replay
                    # the identical request, including generation/requestId.
                    token, _ = self.tokens.get(force=True, rejected_token=token)
                    continue
                if missing_ok and exc.code == 404:
                    return None
                raise CleanupError(f"{service} {method} {path}: HTTP {exc.code}; cleanup stopped") from None
            except (URLError, TimeoutError, ValueError):
                raise CleanupError(f"{service} {method} {path}: request failed; cleanup stopped") from None

    def pages(self, service, path, **params):
        seen = set()
        while True:
            page = self.request(service, path, params=params)
            if page.get("unreachable") or page.get("unreachables"):
                raise CleanupError("Cloud inventory is incomplete: unreachable locations")
            if page.get("warning") and page["warning"].get("code") != "NO_RESULTS_ON_PAGE":
                raise CleanupError("Cloud inventory returned a warning; refusing incomplete inventory")
            yield page
            token = page.get("nextPageToken")
            if not token:
                break
            if token in seen:
                raise CleanupError("Cloud inventory repeated a page token")
            seen.add(token)
            params["pageToken"] = token

    def job(self, job_id, region=REGION):
        return self.request("batch", f"projects/{PROJECT}/locations/{region}/jobs/{job_id}", missing_ok=True)

    def jobs(self):
        return [job for page in self.pages("batch", f"projects/{PROJECT}/locations/-/jobs")
                for job in page.get("jobs", [])]

    def delete_job(self, job_id, region, uid):
        # Stable idempotency key for retries of this exact job incarnation.
        request_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PROJECT}/{region}/{uid}"))
        return self.operation(self.request("batch", f"projects/{PROJECT}/locations/{region}/jobs/{job_id}",
                                           method="DELETE", params={"requestId": request_id}, missing_ok=True))

    @staticmethod
    def operation(result):
        if result and result.get("error"):
            raise CleanupError("Cloud deletion operation returned an error; cleanup stopped")
        return result

    def bucket(self):
        return self.request("storage", f"b/{BUCKET}")

    def objects(self, prefix):
        # Include noncurrent generations too; never use delimiter/glob/bucket rm.
        return [obj for page in self.pages("storage", f"b/{BUCKET}/o", prefix=prefix, versions="true")
                for obj in page.get("items", [])]

    def delete_object(self, obj):
        return self.request("storage", f"b/{BUCKET}/o/{quote(obj['name'], safe='')}",
                            method="DELETE", missing_ok=True,
                            params={"generation": obj["generation"], "ifGenerationMatch": obj["generation"]})

    def resources(self):
        resources = []
        for collection in ("instances", "disks", "instanceGroupManagers", "instanceTemplates"):
            path = f"projects/{PROJECT}/aggregated/{collection}"
            for page in self.pages("compute", path):
                for scope, group in page.get("items", {}).items():
                    warning = group.get("warning", {})
                    if warning and warning.get("code") != "NO_RESULTS_ON_PAGE":
                        raise CleanupError(f"Incomplete Compute inventory: {collection}/{scope}")
                    for resource in group.get(collection, []):
                        resources.append({**resource, "kind": collection, "scope": scope})
        return resources

    def images(self):
        return [item for page in self.pages("artifacts", f"{ARTIFACT_PARENT}/dockerImages")
                for item in page.get("dockerImages", [])]

    def delete_image(self, image):
        package, digest = image.removeprefix(REPOSITORY).split("@", 1)
        return self.operation(self.request("artifacts", f"{ARTIFACT_PARENT}/packages/{quote(package, safe='')}/versions/{digest}",
                                           method="DELETE", params={"force": "true"}, missing_ok=True))

    def logs(self):
        prefix = f"projects/{PROJECT}/logs/"
        return [name for page in self.pages("logging", f"projects/{PROJECT}/logs")
                for name in page.get("logNames", []) if name.startswith(prefix)]

    def log_entries(self, filter_text, *, default_bucket=False):
        resource = f"projects/{PROJECT}"
        if default_bucket:
            resource += "/locations/global/buckets/_Default/views/_AllLogs"
        body = {"resourceNames": [resource], "filter": filter_text,
                "pageSize": 1000, "orderBy": "timestamp asc"}
        seen = set()
        for _ in range(100):
            page = self.request("logging", "entries:list", method="POST", body=body)
            for entry in page.get("entries", []):
                yield entry
            token = page.get("nextPageToken")
            if not token:
                return
            if token in seen:
                raise CleanupError("Log inventory repeated a page token")
            seen.add(token)
            body["pageToken"] = token
        raise CleanupError("Log inventory exceeds safety bound; no partial inventory may authorize deletion")

    def delete_log(self, log_id):
        return self.request("logging", f"projects/{PROJECT}/logs/{quote(log_id, safe='')}",
                            method="DELETE", missing_ok=True)
