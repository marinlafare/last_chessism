"""Cloud Run backend. PostgreSQL is authoritative; no host needed during execution."""
import asyncio
from dataclasses import asdict
import json

from . import runtime
from .importer import import_batch, validate_manifest, refresh_projections
from .preflight import verify_clean_workspace
from cloud_job import cloud_run as api
from cloud_job.launch import inspect_worker, expected_contract
from stockfish_batch.config import Config
from stockfish_batch.checkpoints import encode, BatchCheckpoints
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_MANIFEST_BYTES
from stockfish_core import PROFILE_ID
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun
from .ingestion import ingest_ready


async def advance(controller, job, run):
    from .controller import save_run, save_job
    cloud = controller.cloud
    launch = dict(run.launch)
    name = api.job_name(run.id)
    if run.status == "preparing":
        await asyncio.to_thread(verify_clean_workspace, cloud)
        # Listing fails closed on missing Run permissions/API, before any upload.
        await asyncio.to_thread(require_no_run_jobs, cloud)
        ranges = api.partitions(len(run.positions), job.selection["n_cpus"])
        config = api.config_for(run.id, max(count for _, count in ranges))
        await asyncio.to_thread(api.check_quota, cloud, min(len(ranges), job.selection["n_cpus"]))
        image_id, metadata = await asyncio.to_thread(inspect_worker, controller.local_image)
        raw = b"".join(encode(row) for row in run.positions)
        contract = expected_contract(raw, run.positions, config, metadata)
        tasks = []
        for index, (start, count) in enumerate(ranges):
            subset = run.positions[start:start + count]
            raw_task = b"".join(encode(row) for row in subset)
            cfg = api.task_config(config, index, len(ranges))
            tasks.append({"index": index, "start": start, "count": count,
                          "contract": expected_contract(raw_task, subset, cfg, metadata)})
        launch.update(backend="cloud_run", profile=PROFILE_ID, config=asdict(config),
                      image_id=image_id, tasks=tasks, jobs=[], run_job=name, run_executions=[],
                      n_cpus=job.selection["n_cpus"])
        await save_run(run.id, contract=contract, launch=launch, status="publishing", error=None)
        return
    config = Config(**launch["config"]).validate()
    if run.status == "publishing":
        from cleaning_job.cleanup import check_bucket
        await asyncio.to_thread(check_bucket, cloud)
        launch["image"] = await asyncio.to_thread(cloud.publish, run.id, launch["image_id"])
        launch["spec"] = api.spec_for(run.id, config, launch["image"], len(launch["tasks"]), launch["n_cpus"])
        await save_run(run.id, launch=launch, status="uploading", error=None)
    elif run.status == "uploading":
        storage = await asyncio.to_thread(cloud.storage)
        for task in launch["tasks"]:
            cfg = api.task_config(config, task["index"], len(launch["tasks"]))
            raw = b"".join(encode(row) for row in run.positions[task["start"]:task["start"] + task["count"]])
            created = await asyncio.to_thread(storage.create, cfg.input, raw)
            if not created and await asyncio.to_thread(storage.read, cfg.input) != raw:
                raise ValueError("Cloud Run input changed; refusing reuse")
        await save_run(run.id, status="run_creating", error=None)
    elif run.status == "run_creating":
        remote = await asyncio.to_thread(api.ensure_job, cloud, name, launch["spec"])
        if remote:
            launch["run_uid"] = remote["uid"]
            await save_run(run.id, launch=launch, status="run_start", error=None)
    elif run.status == "run_start":
        remote = await asyncio.to_thread(require_job, cloud, launch)
        current = await asyncio.to_thread(api.executions, cloud, name)
        prior = {e["name"]: e["uid"] for e in launch["run_executions"]}
        if any(e["name"] not in prior or prior[e["name"]] != e["uid"] or api.terminal(e) == "RUNNING" for e in current):
            raise ValueError("Unexpected/running execution; refusing duplicate launch")
        # Persist BEFORE POST. jobs:run has no idempotency key. On any uncertain
        # response/restart, discover the execution; NEVER blindly replay POST.
        await save_run(run.id, status="run_discover", error=None)
        operation = await asyncio.to_thread(cloud.request, "run", name + ":run", method="POST", body={"etag": remote["etag"]})
        cloud.operation(operation)
        launch["run_operation"] = operation["name"]
        await save_run(run.id, launch=launch, error=None)
    elif run.status == "run_discover":
        await asyncio.to_thread(require_job, cloud, launch)
        current = await asyncio.to_thread(api.executions, cloud, name)
        prior = {e["name"]: e["uid"] for e in launch["run_executions"]}
        fresh = [e for e in current if e["name"] not in prior]
        if any(e["name"] in prior and e["uid"] != prior[e["name"]] for e in current):
            raise ValueError("Earlier Cloud Run execution identity changed")
        if len(fresh) != 1:
            raise ValueError("Cloud Run submission is unresolved; resume to discover it, never resubmit automatically")
        execution = fresh[0]
        verify_execution(execution, launch)
        launch["run_execution"] = {"name": execution["name"], "uid": execution["uid"]}
        launch["run_executions"] = [*launch["run_executions"], launch["run_execution"]]
        await save_run(run.id, launch=launch, status="running", error=None)
    elif run.status == "running":
        await asyncio.to_thread(require_job, cloud, launch)
        ident = launch["run_execution"]
        execution = await asyncio.to_thread(cloud.request, "run", ident["name"])
        verify_execution(execution, launch)
        if execution["uid"] != ident["uid"]:
            raise ValueError("Cloud Run execution identity changed")
        state = api.terminal(execution)
        if job.selection.get("cancel_requested") and state == "RUNNING":
            await asyncio.to_thread(cloud.request, "run", ident["name"] + ":cancel", method="POST", body={"etag": execution["etag"]})
        await save_run(run.id, cloud_state=state)
        storage = await asyncio.to_thread(cloud.storage)
        await ingest_ready(storage, run, [{**task, 'output': api.task_config(
            config, task['index'], len(launch['tasks'])).output} for task in launch['tasks']],
            storage_factory=cloud.storage)
        if state in {"FAILED", "CANCELLED"}:
            message = "Cloud Run stopped; committed results and checkpoints retained. Retry is a new billable execution."
            await save_run(run.id, status="failed", error=message)
            await save_job(run.job_id, status="failed", error=message)
        elif state == "SUCCEEDED":
            async with AsyncDBSession() as session:
                current = await session.get(CloudAnalysisRun, run.id)
            performance = []
            for task in launch["tasks"]:
                cfg = api.task_config(config, task["index"], len(launch["tasks"]))
                prefix = f"tasks/{task['index']:06d}/"
                receipts = {k[len(prefix):]: {**v, "records": [{**r, "object": r["object"][len(prefix):]} for r in v["records"]]}
                            for k, v in current.receipts.items() if k.startswith(prefix)}
                raw = await asyncio.to_thread(storage.read, cfg.output + "/manifest.json", MAX_MANIFEST_BYTES)
                if raw is None:
                    raise ValueError("Successful Cloud Run task has no manifest")
                validate_manifest(json.loads(raw), run.positions[task["start"]:task["start"] + task["count"]], task["contract"], receipts)
                report = await asyncio.to_thread(storage.read, cfg.output + "/performance.json", 128 * 1024)
                if report is None:
                    raise ValueError("Cloud Run task performance report missing")
                performance.append(validate_performance(json.loads(report), task["contract"], 1))
            launch["task_performance"] = performance
            launch["execution_summary"] = execution
            launch["manifest"] = {"status": "complete", "fingerprint": run.contract["fingerprint"],
                                  "position_count": len(run.positions), "records": [r for k in sorted(current.receipts) for r in current.receipts[k]["records"]]}
            validate_manifest(launch["manifest"], run.positions, run.contract, current.receipts)
            launch["performance"] = {"backend": "cloud_run", "position_count": len(run.positions),
                                     "task_count": len(performance), "parallelism": launch["spec"]["template"]["parallelism"],
                                     "worker_seconds_sum": sum(p["metrics"]["worker_seconds"] for p in performance),
                                     "note": "Sum of task processing times, not wall time or billed duration; task reports saved locally."}
            await save_run(run.id, launch=launch, status="finalizing", error=None)
    elif run.status == "finalizing":
        validate_manifest(launch["manifest"], run.positions, run.contract, run.receipts)
        await save_run(run.id, status="cleanup_planning", error=None)
    elif run.status in {"cleanup_planning", "cleaning"}:
        from cloud_job.cloud_run_cleanup import prepare, apply
        validate_manifest(launch["manifest"], run.positions, run.contract, run.receipts)
        for task, report in zip(launch["tasks"], launch.get("task_performance", []), strict=True):
            validate_performance(report, task["contract"], 1)
        if run.status == "cleanup_planning":
            plan = await asyncio.to_thread(prepare, cloud, launch, run.id)
            await save_run(run.id, cleanup_plan=plan, status="cleaning", error=None)
        else:
            result = await asyncio.to_thread(apply, cloud, run.cleanup_plan)
            launch["cleanup_report"] = result
            await save_run(run.id, launch=launch, status="refreshing" if result["complete"] else "cleaning", error=None)
    elif run.status == "failed":
        await save_job(run.job_id, status="failed", error=run.error)
    else:
        raise ValueError("Unknown Cloud Run phase: " + run.status)


def require_no_run_jobs(cloud):
    if any(page.get("jobs") for page in cloud.pages("run", api.PARENT + "/jobs")):
        raise ValueError("Earlier Cloud Run jobs remain; preserve them until imported/cleaned")


def require_job(cloud, launch):
    remote = cloud.request("run", launch["run_job"])
    if remote.get("uid") != launch["run_uid"] or not api.matches(remote, launch["spec"]):
        raise ValueError("Cloud Run job changed; refusing execution/cleanup")
    return remote


def verify_execution(execution, launch):
    if (not execution.get("uid") or not execution.get("name", "").startswith(launch["run_job"] + "/executions/")
            or execution.get("taskCount") != launch["spec"]["template"]["taskCount"]
            or execution.get("parallelism") != launch["spec"]["template"]["parallelism"]
            or not api.matches(execution.get("template"), launch["spec"]["template"]["template"])):
        raise ValueError("Unexpected Cloud Run execution configuration")
