"""Incremental, shard-aware imports and the fleet's final receipt gate."""
import asyncio
import json

from .importer import import_batch, validate_manifest
from stockfish_batch.config import Config
from stockfish_batch.checkpoints import BatchCheckpoints
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_MANIFEST_BYTES
from cloud_job.batch_spot import CPUS, MACHINE, VM_MEMORY_MIB
from .ingestion import ingest_ready


async def import_ready(storage, run, tasks, *, storage_factory=None):
    tasks = tasks if isinstance(tasks, list) else [tasks]
    items = [{**task, 'output': Config(**task['config']).validate().output} for task in tasks]
    return await ingest_ready(storage, run, items, storage_factory=storage_factory)


async def collect(storage, run, launch):
    reports = []
    manifests = []
    for task in launch["tasks"]:
        cfg = Config(**task["config"]).validate()
        prefix = f"tasks/{task['index']:06d}/"
        receipts = {key[len(prefix):]: {**value, "records": [
            {**record, "object": record["object"][len(prefix):]} for record in value["records"]]}
            for key, value in run.receipts.items() if key.startswith(prefix)}
        raw = await asyncio.to_thread(storage.read, cfg.output + "/manifest.json", MAX_MANIFEST_BYTES)
        if raw is None:
            raise ValueError("Successful Batch VM has no completion manifest; cleanup withheld")
        manifest = json.loads(raw)
        subset = run.positions[task["start"]:task["start"] + task["count"]]
        validate_manifest(manifest, subset, task["contract"], receipts)
        raw_report = await asyncio.to_thread(storage.read, cfg.output + "/performance.json", 128 * 1024)
        if raw_report is None:
            raise ValueError("Successful Batch VM has no performance report; cleanup withheld")
        reports.append(validate_performance(json.loads(raw_report), task["contract"], CPUS))
        manifests.append(manifest)
    launch = {**launch, "task_performance": reports, "task_manifests": manifests,
              "manifest": {"status": "complete", "fingerprint": run.contract["fingerprint"],
                           "position_count": len(run.positions),
                           "records": [r for k in sorted(run.receipts) for r in run.receipts[k]["records"]]}}
    launch["performance"] = {
        "backend": "batch_spot", "position_count": len(run.positions), "vm_count": len(reports),
        "machine_type": MACHINE, "cpus_per_vm": CPUS, "memory_gib_per_vm": VM_MEMORY_MIB // 1024,
        "worker_seconds_sum": sum(p["metrics"]["worker_seconds"] for p in reports),
        "vms": [{"index": t["index"], "positions": t["count"], "workers": p["workers"],
                 "analyzed_this_attempt": p["analyzed_this_attempt"], "resumed": p["resumed"],
                 "metrics": p["metrics"]} for t, p in zip(launch["tasks"], reports)],
        "note": "Successful reporting attempts only, not total wall time or billed duration. Interrupted attempts can add charges.",
    }
    validate_saved(run, launch)
    return launch


def validate_saved(run, launch):
    """Rechecked on every cleanup resume, without relying on deleted GCS files."""
    validate_manifest(launch["manifest"], run.positions, run.contract, run.receipts)
    tasks, reports, manifests = launch["tasks"], launch["task_performance"], launch["task_manifests"]
    if not len(tasks) == len(reports) == len(manifests):
        raise ValueError("Incomplete fleet reports; cleanup withheld")
    cursor = 0
    for index, (task, report, manifest) in enumerate(zip(tasks, reports, manifests)):
        if task["index"] != index or task["start"] != cursor or task["count"] <= 0:
            raise ValueError("Batch shard ranges changed")
        prefix = f"tasks/{index:06d}/"
        receipts = {k[len(prefix):]: {**v, "records": [
            {**r, "object": r["object"][len(prefix):]} for r in v["records"]]}
            for k, v in run.receipts.items() if k.startswith(prefix)}
        validate_manifest(manifest, run.positions[cursor:cursor + task["count"]], task["contract"], receipts)
        validate_performance(report, task["contract"], CPUS)
        cursor += task["count"]
    if cursor != len(run.positions):
        raise ValueError("Batch shard coverage changed")
