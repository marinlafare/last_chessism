"""Fixed-scope helpers for the opt-in preemption-002 integration test."""
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockfish_batch.checkpoints import Checkpoints, digest, encode, make_contract, parse_input
from stockfish_batch.config import Config
from render_job import render

PROJECT = "chessism-production"
REGION = "us-central1"
BUCKET = "chessism-batch-276704059200-us-central1"
RUN = "preemption-002"
INPUT_KEY = f"inputs/{RUN}/input.jsonl"
RESULT_KEY = f"results/{RUN}"
INPUT_URI = f"gs://{BUCKET}/{INPUT_KEY}"
OUTPUT_URI = f"gs://{BUCKET}/{RESULT_KEY}"
IMAGE = "us-central1-docker.pkg.dev/chessism-production/chessism-workers/stockfish-analyzer@sha256:4327b80624bede156b8ebe8d50c910c580237266056ff96b349f20ed0a5c7d93"
ENGINE_SHA = "01a8cbe27fabf6aad1c6393d2510982805f682c8c61243cad2e84f51e974505a"
ACCOUNT = "chessism-batch-worker@chessism-production.iam.gserviceaccount.com"
JOBS = (f"chessism-{RUN}-a", f"chessism-{RUN}-b")
OUT = ROOT / "out" / RUN
TERMINAL = {"SUCCEEDED", "FAILED", "DELETION_IN_PROGRESS"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def fixture():
    source = parse_input((ROOT / "examples/input.jsonl").read_bytes(), 20)
    require(len(source) == 20, "Expected the original 20 example records")
    raw = b"".join(encode({"id": f"r{copy:02d}-{row['id']}", "fen": row["fen"]})
                   for copy in range(10) for row in source)
    return raw, parse_input(raw, 200)


def job_config():
    job = render(IMAGE, INPUT_URI, OUTPUT_URI)
    task = job["taskGroups"][0]["taskSpec"]
    args = task["runnables"][0]["container"]["commands"]
    args[args.index("--max-positions") + 1] = "200"
    labels = {"app": "chessism", "purpose": "stockfish-preemption", "recovery-test": RUN}
    job["labels"] = dict(labels)
    job["allocationPolicy"]["labels"] = dict(labels)
    require(task["maxRunDuration"] == "900s" and task["maxRetryCount"] == 0, "Unsafe task limits")
    require(job["taskGroups"][0]["taskCount"] == "1", "Only one task is allowed")
    return job


def expected_contract(raw, rows):
    config = Config(INPUT_URI, OUTPUT_URI, max_positions=200).validate()
    # This historical controller is pinned to the original v1 image, not the
    # currently checked-out worker. Preserve its original checkpoint identity.
    body = make_contract(raw, rows, config, ENGINE_SHA)
    body.pop("fingerprint")
    body["worker_version"] = "1.0.0"
    return {**body, "fingerprint": digest(encode(body))}


def validate_vm(vm, job):
    """Reject every instance except the exact running Spot worker for attempt A."""
    require(job["name"].rsplit("/", 1)[-1] == JOBS[0], "Only attempt A may be interrupted")
    # Batch's reported state can lag behind checkpoint writes by about a minute.
    # The caller additionally requires durable partial output and the expected contract.
    require(job["status"]["state"] in {"SCHEDULED", "RUNNING"}, "Attempt A is not allocated/active")
    labels = vm.get("labels", {})
    require(labels.get("batch-job-id") == JOBS[0], "VM job ID does not match")
    require(labels.get("batch-job-uid") == job["uid"], "VM job UID does not match")
    require(labels.get("recovery-test") == RUN, "VM recovery-test label does not match")
    require(vm.get("status") == "RUNNING", "VM is not running")
    require(vm.get("scheduling", {}).get("provisioningModel") == "SPOT", "VM is not Spot")
    require(vm.get("machineType", "").endswith("/n2d-standard-4"), "Unexpected VM size")
    require([a["email"] for a in vm.get("serviceAccounts", [])] == [ACCOUNT], "Wrong VM identity")
    zone = vm["zone"].rsplit("/", 1)[-1]
    require(re.fullmatch(r"us-central1-[a-z]", zone), "Wrong region")
    require(re.fullmatch(r"[a-z][a-z0-9-]{0,62}", vm["name"]), "Unsafe VM name")
    require(bool(vm.get("id")), "Missing immutable instance ID")
    return vm["name"], zone


def verify_preemption(job, task):
    """Require Batch's reserved preemption exit code, not a matching job name."""
    require(job["status"]["state"] == "FAILED", "Interrupted job did not fail")
    require(task["status"]["state"] == "FAILED", "Interrupted task did not fail")
    events = job["status"].get("statusEvents", []) + task["status"].get("statusEvents", [])
    require(any(event.get("taskExecution", {}).get("exitCode") in (50001, "50001")
                or re.search(r"\bexit code\s+50001\b", event.get("description", ""), re.I)
                for event in events), "Failure did not establish Spot preemption (exit code 50001)")


def verify_vm_recreation(job, task, operation, target):
    """Separate VM-loss evidence from strict Spot preemption; never relabel 50006."""
    require(job["name"].rsplit("/", 1)[-1] == JOBS[0], "Wrong interrupted job")
    require(job["status"]["state"] == task["status"]["state"] == "FAILED", "Task/job did not fail")
    require(target["job_uid"] == job["uid"], "Interruption evidence belongs to another job")
    vm = target["vm"]
    # Revalidate the recorded pre-interruption identity, not the VM's current state.
    _, zone = validate_vm(vm, {**job, "status": {"state": "RUNNING"}})
    require(operation.get("operationType") == "simulateMaintenanceEvent"
            and operation.get("status") == "DONE" and not operation.get("error"),
            "Maintenance request did not complete successfully")
    require(str(operation.get("targetId")) == str(vm["id"]), "Maintenance targeted a different VM ID")
    suffix = f"/projects/{PROJECT}/zones/{zone}/instances/{vm['name']}"
    require(operation.get("targetLink", "").endswith(suffix), "Maintenance targeted another resource")
    instance = f"zones/{zone}/instances/{vm['id']}"
    events = task["status"].get("statusEvents", [])
    require(any(event.get("taskExecution", {}).get("exitCode") in (50006, "50006")
                and instance in event.get("description", "")
                and "VM is recreated during task execution" in event.get("description", "")
                for event in events), "No VM-recreation failure for the interrupted instance")
    return {"batch_exit_code": 50006, "failure_kind": "vm_recreated",
            "strict_spot_preemption_code_observed": False,
            "maintenance_operation": operation["name"], "interrupted_instance_id": str(vm["id"])}


def verify_results(raw, rows, contract, manifest, values):
    require(contract == expected_contract(raw, rows), "Input/settings/engine contract mismatch")
    require(manifest["status"] == "complete" and manifest["position_count"] == 200, "Incomplete manifest")
    require(manifest["fingerprint"] == contract["fingerprint"], "Manifest fingerprint mismatch")
    require(len(values) == len(rows) == 200, "Expected 200 checkpoint files")
    require(set(values) == {row["id"] for row in rows}, "Checkpoint IDs differ from input")
    require([r["id"] for r in manifest["records"]] == [r["id"] for r in rows], "Manifest IDs/order mismatch")
    validator = Checkpoints(None, OUTPUT_URI, contract)
    for row, entry in zip(rows, manifest["records"]):
        value = validator.validate(values[row["id"]], row)
        require(entry["object"] == validator.name(row), "Unexpected result path")
        require(entry["sha256"] == value["result_sha256"], "Manifest checksum mismatch")


class Cloud:
    def __init__(self, executable):
        from google.cloud import storage
        from google.oauth2.credentials import Credentials
        self.executable = executable
        # Short-lived token stays in memory; never print it, persist it, or mount credentials.
        token = self.command("auth", "print-access-token", json_output=False).strip()
        self.storage = storage.Client(project=PROJECT, credentials=Credentials(token=token))
        self.bucket = self.storage.bucket(BUCKET)

    def command(self, *args, json_output=True):
        command = [self.executable, *args, f"--project={PROJECT}", "--quiet"]
        if json_output:
            command.append("--format=json")
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                text=True, timeout=55)
        if result.returncode:
            raise RuntimeError(f"gcloud {args[0]} {args[1]} failed: {result.stderr[-2000:]}")
        return json.loads(result.stdout or "null") if json_output else result.stdout

    def job(self, name):
        require(name in JOBS, "Job outside this test")
        return self.command("batch", "jobs", "describe", name, f"--location={REGION}")

    def resources(self, kind, uid):
        require(kind in {"instances", "disks"}, "Unsupported resource kind")
        require(re.fullmatch(r"[a-z0-9-]+", uid), "Invalid job UID")
        return self.command("compute", kind, "list", f"--filter=labels.batch-job-uid={uid}")

    def objects(self, prefix):
        require(prefix.startswith((RESULT_KEY + "/", INPUT_KEY)), "Storage prefix outside this test")
        from google.cloud.storage.retry import DEFAULT_RETRY
        return list(self.storage.list_blobs(BUCKET, prefix=prefix, timeout=10,
                                           retry=DEFAULT_RETRY.with_timeout(20), max_results=205))

    def read(self, key, generation=None):
        from google.api_core.exceptions import NotFound
        from google.cloud.storage.retry import DEFAULT_RETRY
        require(key == INPUT_KEY or key.startswith(RESULT_KEY + "/"), "Object outside this test")
        blob = self.bucket.blob(key, generation=generation)
        retry = DEFAULT_RETRY.with_timeout(20)
        try:
            blob.reload(timeout=10, retry=retry)
        except NotFound:
            return None
        require(blob.size <= 2 * 1024 * 1024, "Unexpectedly large test object")
        return blob.download_as_bytes(if_generation_match=blob.generation, timeout=10, retry=retry)

    def read_json(self, key, generation=None):
        raw = self.read(key, generation)
        return None if raw is None else json.loads(raw)

    def upload_input(self, raw):
        from google.cloud.storage.retry import DEFAULT_RETRY
        self.bucket.blob(INPUT_KEY).upload_from_string(raw, content_type="application/x-ndjson",
            if_generation_match=0, checksum="crc32c", timeout=10, retry=DEFAULT_RETRY.with_timeout(20))

    def submit(self, name):
        require(name in JOBS, "Job outside this test")
        return self.command("batch", "jobs", "submit", name, f"--location={REGION}",
                            f"--config={OUT / 'job.json'}")

    def inventory(self):
        return {blob.name: {"generation": str(blob.generation), "size": blob.size,
                            "crc32c": blob.crc32c, "md5": blob.md5_hash}
                for blob in self.objects(RESULT_KEY + "/positions/")}

    def logs(self, uid):
        require(re.fullmatch(r"[a-z0-9-]+", uid), "Invalid job UID")
        return self.command("logging", "read",
            f'logName="projects/{PROJECT}/logs/batch_task_logs" AND labels.job_uid="{uid}"',
            "--freshness=2h", "--order=asc", "--limit=1000")


def worker_events(logs):
    events = []
    for entry in logs:
        payload = entry.get("jsonPayload", entry.get("textPayload"))
        if isinstance(payload, dict) and "event" not in payload:
            payload = payload.get("message")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                continue
        if isinstance(payload, dict) and "event" in payload:
            events.append(payload)
    return events


def verify_resume_events(events, before_ids, rows):
    resumed = len(before_ids)
    starts = [e for e in events if e["event"] == "started"]
    ends = [e for e in events if e["event"] == "complete"]
    if not starts or not ends:
        return False  # Logs may lag the job result.
    require(all(e["resumed"] == resumed and e["total"] == 200 for e in starts), "Wrong resumed count")
    require(all(e["resumed"] == resumed and e["analyzed_this_attempt"] == 200 - resumed
                and e["saved"] == e["total"] == 200 for e in ends), "Wrong completion counts")
    saved = {e["id"] for e in events if e["event"] == "saved"}
    require(not saved.intersection(before_ids), "Previously saved positions were processed again")
    return saved == {r["id"] for r in rows} - before_ids
