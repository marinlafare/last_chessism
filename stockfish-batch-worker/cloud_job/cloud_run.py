"""Cloud Run Jobs specs and restart-safe, read-before-write control operations."""
from dataclasses import replace
import math

from cleaning_job.cloud import PROJECT, REGION, CleanupError
from cleaning_job.images import validate_uri
from stockfish_core import NODES, THREADS, HASH_MIB, MULTIPV, PROFILE_ID
from .launch import configuration

PARENT = f"projects/{PROJECT}/locations/{REGION}"
TASK_SECONDS = 604800  # Platform ceiling, NOT a per-FEN search budget.


def partitions(count, cpus):
    if type(cpus) is not int or not 1 <= cpus <= 64 or not 1 <= count <= 200000:
        raise ValueError("Cloud Run requires 1–64 CPUs and 1–200,000 FENs")
    size = min(5000, max(500, math.ceil(count / (cpus * 4 * 500)) * 500))
    return [(start, min(size, count - start)) for start in range(0, count, size)]


def config_for(run_id, size):
    return replace(configuration(run_id, size, NODES, 300, stall_only=True),
                   workers=1, threads=THREADS, hash_mb=HASH_MIB, multipv=MULTIPV,
                   memory_mib=1024).validate()


def task_config(config, index, count):
    from stockfish_batch.cloud_run_task import task_config as choose
    return choose(config, {"CLOUD_RUN_TASK_INDEX": str(index), "CLOUD_RUN_TASK_COUNT": str(count)})


def spec_for(run_id, config, image, task_count, cpus):
    validate_uri(image)
    fields = ("input", "output", "workers", "threads", "hash_mb", "memory_mib", "nodes",
              "multipv", "max_positions", "position_timeout", "run_timeout", "stall_timeout",
              "upload_mode", "batch_size", "upload_queue_size")
    args = [part for key in fields for part in ("--" + key.replace("_", "-"), str(getattr(config, key)))]
    return {"labels": {"app": "chessism", "run_id": run_id, "analysis_profile": PROFILE_ID},
            "template": {"taskCount": task_count, "parallelism": min(cpus, task_count),
                         "template": {"serviceAccount": f"chessism-batch-worker@{PROJECT}.iam.gserviceaccount.com",
                                      "timeout": f"{TASK_SECONDS}s", "maxRetries": 1,
                                      "containers": [{"image": image,
                                          "command": ["python", "-m", "stockfish_batch.cloud_run_task"],
                                          "args": args + ["--compact-results"],
                                          "resources": {"limits": {"cpu": "1", "memory": "1Gi"}}}]}}}


def matches(observed, expected):
    """Compare only submitted fields; server-populated fields are harmless."""
    if isinstance(expected, dict):
        return isinstance(observed, dict) and all(matches(observed.get(k), v) for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(observed, list) and len(observed) == len(expected) and all(
            matches(a, b) for a, b in zip(observed, expected))
    return observed == expected


def job_name(run_id):
    import re
    if not re.fullmatch(r"[a-f0-9]{32}", run_id):
        raise ValueError("Invalid cloud run identity")
    return PARENT + "/jobs/chessism-run-" + run_id


def executions(cloud, name):
    return [item for page in cloud.pages("run", name + "/executions") for item in page.get("executions", [])]


def terminal(execution):
    for condition in execution.get("conditions", []):
        if condition.get("type") == "Completed":
            state = condition.get("state")
            if state == "CONDITION_SUCCEEDED":
                return "SUCCEEDED"
            if state == "CONDITION_FAILED":
                return "CANCELLED" if execution.get("cancelledCount", 0) else "FAILED"
    return "RUNNING"


def ensure_job(cloud, name, spec):
    existing = cloud.request("run", name, missing_ok=True)
    if existing is None:
        cloud.operation(cloud.request("run", PARENT + "/jobs", method="POST",
                        params={"jobId": name.rsplit("/", 1)[1]}, body=spec))
        return None
    if not matches(existing, spec):
        raise CleanupError("Cloud Run job identity/configuration changed")
    if existing.get("reconciling"):
        return None
    if existing.get("terminalCondition", {}).get("state") != "CONDITION_SUCCEEDED":
        raise CleanupError("Cloud Run job is not ready; inspect permissions/quotas")
    return existing


def check_quota(cloud, cpus):
    # Service Usage reports regional effective limits, including overrides.
    # Do not mistake missing metrics/read errors for unlimited quota.
    from cleaning_job.cloud import PROJECT_NUMBER
    metrics = [m for page in cloud.pages("serviceusage",
        f"projects/{PROJECT_NUMBER}/services/run.googleapis.com/consumerQuotaMetrics", view="FULL")
        for m in page.get("metrics", [])]
    found = {}
    for metric in metrics:
        key = metric.get("metric", "").lower()
        kind = "cpu" if key == "run.googleapis.com/cpu_allocation" else "memory" if key == "run.googleapis.com/mem_allocation" else None
        if kind is None:
            continue
        for limit in metric.get("consumerQuotaLimits", []):
            for bucket in limit.get("quotaBuckets", []):
                dimensions = bucket.get("dimensions", {})
                if dimensions.get("region", dimensions.get("location")) == REGION:
                    amount = int(bucket["effectiveLimit"])
                    if amount >= 0:
                        found[kind] = min(found.get(kind, amount), amount)
    if set(found) != {"cpu", "memory"}:
        raise CleanupError("Cannot verify regional Cloud Run CPU/memory quotas; no upload or launch allowed")
    if found["cpu"] < cpus * 1000 or found["memory"] < cpus * 1024**3:
        raise CleanupError(f"Cloud Run quota insufficient for {cpus} concurrent 1-vCPU/1-GiB tasks")
    return found
