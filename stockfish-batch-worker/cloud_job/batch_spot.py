"""Fixed-size Spot fleet planning; one independently recoverable task per VM."""
from dataclasses import replace
import hashlib
import math
import re

from cleaning_job.cloud import CleanupError, PROJECT, REGION
from .launch import Client, configuration, render

MAX_VMS = 10
MACHINE = "n2d-highcpu-16"
CPUS = 16
VM_MEMORY_MIB = 16384
WORKER_MEMORY_MIB = 12288  # Leave 4 GiB for the OS, Batch agent and Docker.


def partitions(count, n_vms):
    if type(count) is not int or not 1 <= count <= 200000:
        raise ValueError("Batch requires 1–200,000 FENs")
    if type(n_vms) is not int or not 1 <= n_vms <= MAX_VMS:
        raise ValueError(f"Batch requires 1–{MAX_VMS} VMs")
    count_vms = min(count, n_vms)
    size, extra = divmod(count, count_vms)
    return [(index * size + min(index, extra), size + (index < extra)) for index in range(count_vms)]


def shard_id(run_id, index):
    if not re.fullmatch(r"[a-f0-9]{32}", run_id) or type(index) is not int or not 0 <= index < MAX_VMS:
        raise ValueError("Invalid Batch shard identity")
    return hashlib.sha256(f"batch-fleet-v1:{run_id}:{index}".encode()).hexdigest()[:32]


def config_for(run_id, count):
    return replace(configuration(run_id, count, 100000, 300, stall_only=True),
                   workers=CPUS, memory_mib=WORKER_MEMORY_MIB).validate()


def spec_for(config, image):
    spec = render(config, image, 300, supervised=True)
    task = spec["taskGroups"][0]["taskSpec"]
    task["computeResource"] = {"cpuMilli": str(CPUS * 1000), "memoryMib": str(WORKER_MEMORY_MIB)}
    task["runnables"][0]["container"]["options"] += " --memory=12g --memory-swap=12g --pids-limit=128"
    spec["allocationPolicy"]["instances"][0]["policy"]["machineType"] = MACHINE
    return spec


def verify_job(job, spec):
    Client.check_job(job, spec)
    groups = job.get("taskGroups", [])
    if len(groups) != 1 or any(str(groups[0].get(k)) != "1" for k in ("taskCount", "parallelism", "taskCountPerNode")):
        raise ValueError("Batch job has unexpected VM/task concurrency")
    task = groups[0]["taskSpec"]
    expected = spec["taskGroups"][0]["taskSpec"]
    if (task.get("maxRetryCount", 0) != 0 or task.get("maxRunDuration")
            or any(str(task.get("computeResource", {}).get(k)) != v for k, v in expected["computeResource"].items())):
        raise ValueError("Batch task resource/recovery policy changed")
    allocation = job.get("allocationPolicy", {})
    policies = allocation.get("instances", [])
    if (len(policies) != 1 or policies[0].get("policy", {}).get("machineType") != MACHINE
            or policies[0].get("policy", {}).get("provisioningModel") != "SPOT"
            or allocation.get("serviceAccount", {}).get("email") != spec["allocationPolicy"]["serviceAccount"]["email"]):
        raise ValueError("Batch Spot machine or service account changed")


def check_quota(cloud, n_vms):
    partitions(200000, n_vms)  # Reject malformed/unbounded concurrency before API calls.
    regional = cloud.request("compute", f"projects/{PROJECT}/regions/{REGION}")
    project = cloud.request("compute", f"projects/{PROJECT}")
    regional_quotas = {q["metric"]: q for q in regional.get("quotas", [])}
    global_quotas = {q["metric"]: q for q in project.get("quotas", [])}
    # Dedicated preemptible quota, when granted, replaces the regional family
    # quota. A zero/ungranted pool is how this project's successful Spot tests
    # appeared; Google remains authoritative about allocation/capacity.
    cpu_metric = "PREEMPTIBLE_CPUS" if regional_quotas.get("PREEMPTIBLE_CPUS", {}).get("limit", 0) > 0 else "N2D_CPUS"
    checks = [(regional_quotas, cpu_metric, n_vms * CPUS),
              (global_quotas, "CPUS_ALL_REGIONS", n_vms * CPUS),
              (regional_quotas, "INSTANCES", n_vms),
              (regional_quotas, "IN_USE_ADDRESSES", n_vms),
              (regional_quotas, "SSD_TOTAL_GB", n_vms * 30)]
    checked = {}
    for source, metric, needed in checks:
        quota = source.get(metric, {})
        limit, usage = quota.get("limit"), quota.get("usage")
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
               or v < 0 for v in (limit, usage)):
            raise CleanupError(f"Cannot verify Batch quota {metric}; nothing will be launched")
        available = limit - usage
        if available < needed:
            raise CleanupError(f"Insufficient {metric} quota for {n_vms} Spot VM(s): "
                               f"need {needed}, available {available:g}. Choose fewer VMs or request quota; no new VM launched.")
        checked[metric] = {"limit": limit, "usage": usage, "needed": needed}
    return checked
