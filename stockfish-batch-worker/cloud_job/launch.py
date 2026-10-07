import json
from pathlib import Path
import re
import subprocess

from cleaning_job.cloud import BUCKET, PROJECT, REGION, REPOSITORY, Cloud
from stockfish_batch import __version__
from stockfish_batch.checkpoints import digest, encode, make_contract
from stockfish_batch.config import Config
from stockfish_batch.storage import Storage
from .recovery import RecoveryClient

ROOT = Path(__file__).resolve().parents[1]


def configuration(run_id, count, nodes, seconds, *, stall_only=False):
    if not re.fullmatch(r"[a-f0-9]{32}", run_id) or not 300 <= seconds <= 3600:
        raise ValueError("Invalid run identity or time bound")
    if not 1 <= nodes <= 10000000:
        raise ValueError("Invalid node limit")
    prefix = f"gs://{BUCKET}"
    return Config(input=f"{prefix}/inputs/ui-{run_id}/input.jsonl",
                  output=f"{prefix}/results/ui-{run_id}", max_positions=count,
                  nodes=nodes, run_timeout=0 if stall_only else seconds - 60,
                  stall_timeout=seconds if stall_only else 0,
                  upload_mode="background", batch_size=500, compact_results=True).validate()


def render(config, image, seconds, *, supervised=False):
    from cleaning_job.images import validate_uri
    validate_uri(image)
    if not 300 <= seconds <= 3600:
        raise ValueError("Unsafe duration")
    if config.stall_timeout:
        if config.run_timeout != 0 or config.stall_timeout != seconds:
            raise ValueError("Invalid progress-based timeout")
    elif config.run_timeout != seconds - 60:
        raise ValueError("Unsafe duration")
    config.validate()
    job = json.loads((ROOT / "batch/smoke-test.json").read_text())
    task = job["taskGroups"][0]["taskSpec"]
    task.update(maxRunDuration=f"{seconds}s", maxRetryCount=0 if supervised else 1)
    if config.stall_timeout:
        task.pop("maxRunDuration")
    container = task["runnables"][0]["container"]
    container["imageUri"] = image
    fields = ("input", "output", "workers", "threads", "hash_mb", "memory_mib", "nodes",
              "multipv", "max_positions", "position_timeout", "run_timeout", "upload_mode",
              "batch_size", "upload_queue_size")
    container["commands"] = [part for key in fields for part in
                             ("--" + key.replace("_", "-"), str(getattr(config, key)))]
    if config.stall_timeout:
        container["commands"].extend(["--stall-timeout", str(config.stall_timeout)])
    if config.compact_results:
        container["commands"].append("--compact-results")
    # No task/agent log export for production jobs; audit logs are Google-retained.
    job.pop("logsPolicy", None)
    job["labels"] = {"app": "chessism", "purpose": "stockfish-ui"}
    job["allocationPolicy"]["labels"] = dict(job["labels"])
    return job


def docker(*arguments, timeout=60):
    result = subprocess.run(["docker", *arguments], check=True, capture_output=True,
                            text=True, timeout=timeout)
    return result.stdout


def inspect_worker(local_image):
    # Pin the local image ID before inspecting/pushing, never run a mutable remote tag.
    image_id = docker("image", "inspect", local_image, "--format", "{{.Id}}").strip()
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", image_id):
        raise ValueError("Cannot identify local worker image")
    script = ("import hashlib,json,chess,stockfish_batch;"
              "print(json.dumps({'engine_sha':hashlib.sha256(open('/usr/local/bin/stockfish','rb').read()).hexdigest(),"
              "'worker_version':stockfish_batch.__version__,'chess_version':chess.__version__}))")
    metadata = json.loads(docker("run", "--rm", "--network=none", "--read-only",
        "--cpus=1", "--memory=512m", "--pids-limit=64", "--cap-drop=ALL",
        "--security-opt=no-new-privileges", "--entrypoint=python", image_id, "-c", script))
    if metadata["worker_version"] != __version__:
        raise ValueError(f"Build the current worker ({__version__}) before enabling cloud analysis")
    return image_id, metadata


def expected_contract(raw, rows, config, metadata):
    contract = make_contract(raw, rows, config, metadata["engine_sha"])
    contract.update(worker_version=metadata["worker_version"], chess_version=metadata["chess_version"])
    contract.pop("fingerprint")
    return {**contract, "fingerprint": digest(encode(contract))}


class Client(RecoveryClient, Cloud):
    def storage(self):
        from google.cloud import storage
        from google.auth.transport.requests import AuthorizedSession
        from .auth import GcloudCredentials
        # Never wrap a cached access token in non-refreshable credentials. The
        # SDK and REST share actual expiry and bounded refresh-on-401 handling.
        credentials = GcloudCredentials(self.tokens)
        adapter = Storage()
        adapter._client = storage.Client(project=PROJECT, credentials=credentials,
                                        _http=AuthorizedSession(credentials, max_refresh_attempts=1))
        return adapter

    def publish(self, run_id, image_id):
        # A distinct package per chunk avoids deleting another job's image version.
        target = f"{REPOSITORY}ui-{run_id}:worker"
        docker("tag", image_id, target)
        docker("push", target, timeout=900)
        matches = [item["uri"] for item in self.images()
                   if item["uri"].startswith(f"{REPOSITORY}ui-{run_id}@")]
        if len(matches) != 1:
            raise ValueError("Expected exactly one immutable image digest for this run")
        return matches[0]

    def submit(self, job_id, spec):
        existing = self.job(job_id, REGION)
        if existing:
            self.check_job(existing, spec)
            return existing
        return self.request("batch", f"projects/{PROJECT}/locations/{REGION}/jobs",
                            method="POST", params={"jobId": job_id}, body=spec)

    @staticmethod
    def check_job(job, spec):
        # Never adopt a coincidentally named job with different input/image/settings.
        actual = job["taskGroups"][0]["taskSpec"]
        expected = spec["taskGroups"][0]["taskSpec"]
        actual_container = actual["runnables"][0]["container"]
        expected_container = expected["runnables"][0]["container"]
        if (any(actual_container.get(key) != value for key, value in expected_container.items())
                or actual_container.get("entrypoint") or len(actual["runnables"]) != 1):
            raise ValueError("Existing Batch job does not match durable launch parameters")
