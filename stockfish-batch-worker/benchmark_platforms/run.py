"""Prepare once, launch once, inspect without polling, and collect verified results.

This does not enqueue production jobs or write to the production database.
Launch has a durable at-most-once fence: a lost response needs inspection, NOT
another execute request. Cloud Run's jobs.run API is not inherently idempotent.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import subprocess

from cleaning_job.cloud import BUCKET, PROJECT, REGION, REPOSITORY
from cleaning_job.images import validate_uri
from cloud_job.launch import Client, ROOT, docker, expected_contract, inspect_worker
from stockfish_batch.checkpoints import (
    BatchCheckpoints, digest, encode, parse_input, semantic_digest,
)
from stockfish_batch.config import Config
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_INPUT_BYTES, MAX_MANIFEST_BYTES, Storage, child

COUNT = 25000
SEED = 106
SOURCE_RUN = "fcfc109ab7bf4f94b515e7b15a2b0fe6"
PARENT = f"projects/{PROJECT}/locations/{REGION}"
ARMS = ("batch", "cloud-run")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def now():
    return datetime.now(timezone.utc).isoformat()


def save(directory, name, value):
    """Durable local receipt, atomic replace; no credentials in these records."""
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / name
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(encode(value))
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(destination)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fence(directory, name):
    with (directory / name).open("x") as stream:
        stream.write(now() + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def names(ident):
    require(isinstance(ident, str) and re.fullmatch(r"[a-f0-9]{12}", ident), "Invalid benchmark identity")
    return {arm: f"chessism-platform-{ident}-{'spot' if arm == 'batch' else 'run'}" for arm in ARMS}


def configuration(ident, arm):
    names(ident)
    require(arm in ARMS, "Unknown platform")
    return Config(
        input=f"gs://{BUCKET}/inputs/platform-{ident}/input.jsonl",
        output=f"gs://{BUCKET}/results/platform-{ident}/{arm}",
        max_positions=COUNT, nodes=100000, workers=4, threads=1,
        memory_mib=12288, hash_mb=2048, multipv=4,
        run_timeout=3540, stall_timeout=300,
        upload_mode="background", batch_size=500, compact_results=True,
    ).validate()


def arguments(config):
    fields = ("input", "output", "workers", "threads", "hash_mb", "memory_mib", "nodes",
              "multipv", "max_positions", "position_timeout", "run_timeout", "stall_timeout",
              "upload_mode", "batch_size", "upload_queue_size")
    return [part for key in fields for part in
            ("--" + key.replace("_", "-"), str(getattr(config, key)))] + ["--compact-results"]


def specifications(ident, image):
    validate_uri(image)
    labels = {"app": "chessism", "purpose": "platform-benchmark", "benchmark_id": ident}
    batch = json.loads((ROOT / "batch/smoke-test.json").read_text())
    task = batch["taskGroups"][0]["taskSpec"]
    task.update(maxRunDuration="3600s", maxRetryCount=0)
    task["runnables"][0]["container"].update(
        imageUri=image, commands=arguments(configuration(ident, "batch")))
    batch["labels"] = labels
    batch["allocationPolicy"]["labels"] = labels
    # Both platforms export stdout for diagnosis of this isolated benchmark.
    cloud_run = {"labels": labels, "template": {
        "labels": labels, "taskCount": 1, "parallelism": 1,
        "template": {
            "serviceAccount": f"chessism-batch-worker@{PROJECT}.iam.gserviceaccount.com",
            "maxRetries": 0, "timeout": "3600s",
            "containers": [{"image": image,
                "args": arguments(configuration(ident, "cloud-run")),
                "resources": {"limits": {"cpu": "4", "memory": "12Gi"}}}],
        },
    }}
    return {"batch": batch, "cloud-run": cloud_run}


def sample(rows):
    require(len(rows) >= COUNT, "Source has fewer than 25,000 positions")
    require(len({row["fen"] for row in rows}) == len(rows), "Source has duplicate FENs")
    selected = random.Random(SEED).sample(rows, COUNT)
    raw = b"".join(encode(row) for row in selected)
    parse_input(raw, COUNT)
    return raw


def source():
    # Fixed, completed corpus, read-only transaction and a statement timeout.
    sql = ("SELECT json_build_object('positions', positions, 'status', status, 'active', "
           "(SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','failed'))) "
           f"FROM cloud_analysis_run WHERE id='{SOURCE_RUN}';")
    result = subprocess.run([
        "docker", "compose", "exec", "-T", "-e",
        "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=30000", "db",
        "sh", "-c", 'exec psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1',
    ], input=sql, text=True, capture_output=True, check=True, timeout=40, cwd=ROOT.parent)
    payload = json.loads(result.stdout)
    require(payload["active"] == 0 and payload["status"] == "complete", "Production cloud analysis is active")
    return payload["positions"]


def prepare(directory, client, ident, local_image):
    names(ident)
    directory.mkdir(parents=True, exist_ok=False)
    fence(directory, "prepare.started")
    require(not client.jobs(), "Existing Batch jobs: inspect before benchmarking")
    require(not any(p.get("jobs") for p in client.pages("run", PARENT + "/jobs")),
            "Existing Cloud Run jobs: inspect before benchmarking")
    raw = sample(source())
    require(Storage().create(str(directory / "input.jsonl"), raw), "Local input exists")
    image_id, metadata = inspect_worker(local_image)
    plan = {"id": ident, "created_at": now(), "source_run": SOURCE_RUN, "seed": SEED,
            "position_count": COUNT, "input_sha256": digest(raw), "jobs": names(ident),
            "local_image_id": image_id, "worker_metadata": metadata}
    save(directory, "plan.json", plan)
    print(json.dumps({"event": "publishing", "sample_count": COUNT}), flush=True)
    target = f"{REPOSITORY}platform-{ident}:worker"
    docker("tag", image_id, target)
    docker("push", target, timeout=900)
    matches = [item["uri"] for item in client.images()
               if item["uri"].startswith(f"{REPOSITORY}platform-{ident}@")]
    require(len(matches) == 1, "Expected exactly one immutable image digest")
    plan["image"] = matches[0]
    plan["specifications"] = specifications(ident, plan["image"])
    plan["contract"] = expected_contract(raw, parse_input(raw, COUNT), configuration(ident, "batch"), metadata)
    save(directory, "plan.json", plan)
    uri = configuration(ident, "batch").input
    store = client.storage()
    if not store.create(uri, raw):
        require(store.read(uri, MAX_INPUT_BYTES) == raw, "Cloud input differs; refusing overwrite")
    for arm, spec in plan["specifications"].items():
        save(directory, arm + "-spec.json", spec)
    operation = client.request("run", PARENT + "/jobs", method="POST",
                               params={"jobId": plan["jobs"]["cloud-run"]},
                               body=plan["specifications"]["cloud-run"])
    save(directory, "cloud-run-create.json", operation)
    print(json.dumps({"event": "prepared", "directory": str(directory), "jobs": plan["jobs"]}), flush=True)


def load(directory):
    plan = json.loads((directory / "plan.json").read_bytes())
    require(plan["jobs"] == names(plan["id"]), "Unexpected job names")
    require(plan["specifications"] == specifications(plan["id"], plan["image"]), "Unsafe or changed specifications")
    raw = (directory / "input.jsonl").read_bytes()
    require(digest(raw) == plan["input_sha256"], "Local input changed")
    rows = parse_input(raw, COUNT)
    require(len(rows) == COUNT, "Unexpected sample size")
    require(plan["contract"] == expected_contract(raw, rows,
            configuration(plan["id"], "batch"), plan["worker_metadata"]), "Contract changed")
    return plan


def run_job(client, plan):
    return client.request("run", PARENT + "/jobs/" + plan["jobs"]["cloud-run"], missing_ok=True)


def assert_run_spec(actual, expected):
    require(actual is not None, "Cloud Run job is missing")
    require(actual.get("labels") == expected["labels"], "Cloud Run job labels changed")
    template = actual["template"]
    task = template["template"]
    expected_task = expected["template"]["template"]
    require(template["taskCount"] == template["parallelism"] == 1, "Unexpected Cloud Run task count")
    require(task.get("maxRetries", 0) == 0 and task["timeout"] == "3600s", "Unsafe Cloud Run limits")
    require(task["serviceAccount"] == expected_task["serviceAccount"], "Unexpected service account")
    require(len(task["containers"]) == 1, "Unexpected container count")
    container = task["containers"][0]
    expected_container = expected_task["containers"][0]
    require(all(container.get(k) == v for k, v in expected_container.items()), "Container configuration changed")
    require(not container.get("command") and not container.get("env"), "Unexpected entrypoint or environment overrides")


def launch(directory, client):
    plan = load(directory)
    job = run_job(client, plan)
    assert_run_spec(job, plan["specifications"]["cloud-run"])
    require(job.get("terminalCondition", {}).get("state") == "CONDITION_SUCCEEDED" and not job.get("reconciling"),
            "Cloud Run job is not ready; inspect create operation before launching")
    require(not job.get("executionCount") and not client.job(plan["jobs"]["batch"]),
            "Execution already exists; inspect it, do not launch again")
    fence(directory, "launch.started")
    save(directory, "cloud-run-job.json", job)
    def start(arm):
        save(directory, arm + "-submit-intent.json", {"time": now()})
        if arm == "batch":
            result = client.submit(plan["jobs"][arm], plan["specifications"][arm])
        else:
            result = client.request("run", job["name"] + ":run", method="POST", body={"etag": job["etag"]})
        save(directory, arm + "-submission.json", {"received_at": now(), "response": result})
        return {"platform": arm, "accepted": True, "name": result.get("name")}
    # At most two submissions, issued together. Never retry jobs.run on timeout.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(start, arm) for arm in ARMS]
        failures = []
        for arm, future in zip(ARMS, futures):
            try:
                print(json.dumps(future.result()), flush=True)
            except Exception as exc:
                failures.append(arm)
                print(json.dumps({"platform": arm, "submission_error": str(exc),
                                  "action": "Inspect status; do not resubmit"}), flush=True)
        require(not failures, "One or more submission responses failed; inspect existing cloud resources")


def execution_state(execution):
    if not execution:
        return "NOT_STARTED"
    condition = next((c for c in execution.get("conditions", []) if c["type"] == "Completed"), {})
    state = condition.get("state")
    if state == "CONDITION_SUCCEEDED":
        return "SUCCEEDED"
    if state == "CONDITION_FAILED":
        return "FAILED"
    return "RUNNING" if execution.get("startTime") else "PENDING"


def execution_path(job_id, name):
    # ExecutionReference.name is a short ID, unlike Execution.name in the
    # operation response. Accept either representation, scoped to this job.
    prefix = PARENT + "/jobs/" + job_id + "/executions/"
    short = name[len(prefix):] if name.startswith(prefix) else name
    require(re.fullmatch(re.escape(job_id) + r"-[a-z0-9]+", short), "Unexpected execution identity")
    return prefix + short


def inspect(directory, client, plan):
    batch = client.job(plan["jobs"]["batch"])
    job = run_job(client, plan)
    execution = None
    if job:
        require(job.get("executionCount", 0) <= 1, "Multiple Cloud Run executions; benchmark is invalid")
        latest = job.get("latestCreatedExecution", {}).get("name")
        if latest:
            execution = client.request("run", execution_path(plan["jobs"]["cloud-run"], latest))
    states = {"batch": batch.get("status", {}).get("state") if batch else "NOT_STARTED",
              "cloud-run": execution_state(execution)}
    cleanup_file = directory / "cleanup-result.json"
    if cleanup_file.exists() and json.loads(cleanup_file.read_bytes()).get("complete"):
        if batch is None:
            states["batch"] = "CLEANED"
        if job is None and (directory / "cloud-run-delete-complete.json").exists():
            states["cloud-run"] = "CLEANED"
    result = {"checked_at": now(), "states": states, "batch": batch, "cloud-run": execution,
              "cloud-run-job": job}
    save(directory, "last-status.json", result)
    return result


def status(directory, client):
    plan = load(directory)
    observed = inspect(directory, client, plan)
    for arm in ARMS:
        if observed["states"][arm] == "CLEANED":
            print(f"{arm:10} CLEANED      cloud files removed; results were saved locally")
            continue
        objects = client.objects(f"results/platform-{plan['id']}/{arm}/batches/")
        files = {obj["name"] for obj in objects if re.fullmatch(r".*/batches/[0-9]{6}\.json", obj["name"])}
        print(f"{arm:10} {observed['states'][arm]:12} uploaded {min(len(files)*500, COUNT):,}/{COUNT:,} FENs")
    return observed


def collect(directory, client):
    plan = load(directory)
    observed = inspect(directory, client, plan)
    require(set(observed["states"].values()) == {"SUCCEEDED"}, "Both platforms must succeed before comparison")
    raw = (directory / "input.jsonl").read_bytes()
    rows = parse_input(raw, COUNT)
    storage = client.storage()
    require(storage.read(configuration(plan["id"], "batch").input, MAX_INPUT_BYTES) == raw, "Cloud input changed")
    report = {"benchmark_id": plan["id"], "count_per_platform": COUNT, "image": plan["image"],
              "input_sha256": digest(raw), "database_writes": 0, "platforms": {}}
    for arm in ARMS:
        prefix = configuration(plan["id"], arm).output
        destination = directory / "download" / arm
        def fetch(name, limit=MAX_MANIFEST_BYTES):
            blob = storage.read(child(prefix, name), limit)
            require(blob is not None, "Missing " + name)
            local = destination / name
            if not Storage().create(str(local), blob):
                require(local.read_bytes() == blob, "Local download differs from cloud")
            return json.loads(blob)
        contract = fetch("contract.json")
        require(contract == plan["contract"], "Unexpected worker/input/settings contract")
        validator = BatchCheckpoints(storage, prefix, contract, rows, 500)
        manifest = fetch("manifest.json")
        require(manifest["status"] == "complete" and manifest["position_count"] == COUNT
                and manifest["fingerprint"] == contract["fingerprint"], "Invalid completion manifest")
        values = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            batches = pool.map(lambda index: fetch(f"batches/{index:06d}.json", BatchCheckpoints.MAX_BYTES),
                               range(len(validator.groups)))
            for index, value in enumerate(batches):
                values.update(validator.validate_batch(value, index))
        require(len(manifest["records"]) == len(rows), "Manifest count differs")
        for row, entry in zip(rows, manifest["records"]):
            require(entry == validator.reference(row, values[row["id"]]), "Manifest checksum/order differs")
        performance = validate_performance(fetch("performance.json"), contract, max_workers=4)
        require(performance["resumed"] == 0 and performance["analyzed_this_attempt"] == COUNT,
                "Partial/retried sample is not comparable")
        report["platforms"][arm] = {"performance": performance,
            "semantic_sha256": semantic_digest(rows, values), "validated_positions": len(values)}
    report["chess_results_match"] = len({p["semantic_sha256"] for p in report["platforms"].values()}) == 1
    report["metric_note"] = "Compare worker_seconds and FEN/s. /proc/stat VM CPU % is not comparable on Cloud Run."
    save(directory, "completion-status.json", observed)
    save(directory, "report.json", report)
    print(json.dumps({"report": str(directory / "report.json"),
                      "chess_results_match": report["chess_results_match"]}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "launch", "status", "collect"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--id", help="12 lowercase hex characters; prepare only")
    parser.add_argument("--local-image", default="chessism-stockfish-batch:fen-compat-v1")
    parser.add_argument("--gcloud", default="gcloud")
    args = parser.parse_args()
    client = Client(args.gcloud)
    directory = args.directory.resolve()
    if args.action == "prepare":
        prepare(directory, client, args.id, args.local_image)
    else:
        {"launch": launch, "status": status, "collect": collect}[args.action](directory, client)


if __name__ == "__main__":
    main()
