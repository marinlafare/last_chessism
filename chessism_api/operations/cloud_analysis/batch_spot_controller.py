"""One logical request, concurrent independent Spot VMs, one final cleanup gate."""
import asyncio
from copy import deepcopy
from dataclasses import asdict

from . import runtime
from .preflight import verify_clean_workspace
from .importer import refresh_projections
from . import batch_spot_results as results
from .log_cleanup import plan_logs, remove_logs
from cloud_job import batch_spot as api
from cloud_job.launch import inspect_worker, expected_contract
from cleaning_job import cleaning_job
from cleaning_job.cleanup import prepare, check_bucket
from stockfish_batch.checkpoints import encode
from stockfish_batch.config import Config
from stockfish_core import PROFILE_ID
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun
from sqlalchemy import select
from . import history


async def require_stopped(cloud, launch):
    for task in launch["tasks"]:
        await asyncio.to_thread(cloud.require_recovery_stopped, task["run_id"], task)


async def advance(controller, job, run):
    from .controller import save_run, save_job
    cloud = controller.cloud
    launch = deepcopy(run.launch)
    if run.status == "preparing":
        await asyncio.to_thread(verify_clean_workspace, cloud)
        ranges = api.partitions(len(run.positions), job.selection["n_vms"])
        quota = await asyncio.to_thread(api.check_quota, cloud, len(ranges))
        await asyncio.to_thread(cloud.require_recovery)
        await asyncio.to_thread(check_bucket, cloud)
        image_id, metadata = await asyncio.to_thread(inspect_worker, controller.local_image)
        cfg = api.config_for(run.id, len(run.positions))
        contract = expected_contract(b"".join(map(encode, run.positions)), run.positions, cfg, metadata)
        tasks = []
        for index, (start, count) in enumerate(ranges):
            ident = api.shard_id(run.id, index)
            config = api.config_for(ident, count)
            subset = run.positions[start:start + count]
            tasks.append({"index": index, "start": start, "count": count, "run_id": ident,
                          "config": asdict(config), "status": "PENDING", "jobs": [], "recovery_session": 1,
                          "contract": expected_contract(b"".join(map(encode, subset)), subset, config, metadata)})
        launch.update(backend="batch_spot", multi_vm=True, profile=PROFILE_ID, tasks=tasks,
                      image_id=image_id, quota=quota, jobs=[], n_vms=job.selection["n_vms"], recovery_session=1)
        await save_run(run.id, contract=contract, launch=launch, status="publishing", error=None)
    elif run.status == "publishing":
        await asyncio.to_thread(check_bucket, cloud)
        launch["image"] = await asyncio.to_thread(cloud.publish, run.id, launch["image_id"])
        for task in launch["tasks"]:
            task["spec"] = api.spec_for(Config(**task["config"]), launch["image"])
        await save_run(run.id, launch=launch, status="uploading", error=None)
    elif run.status == "uploading":
        storage = await asyncio.to_thread(cloud.storage)
        for task in launch["tasks"]:
            raw = b"".join(map(encode, run.positions[task["start"]:task["start"] + task["count"]]))
            created = await asyncio.to_thread(storage.create, task["config"]["input"], raw)
            if not created and await asyncio.to_thread(storage.read, task["config"]["input"]) != raw:
                raise ValueError("Batch shard input already exists with different contents")
        await save_run(run.id, status="submitting", error=None)
    elif run.status == "submitting":
        # Check the entire fleet once before starting its first supervisor. A
        # resumed partial submission must not count its own running VMs twice.
        pending = [t for t in launch["tasks"] if t.get("status") != "SUCCEEDED"]
        if pending and not any(t.get("recovery_execution") for t in pending) and not job.selection.get("cancel_requested"):
            await asyncio.to_thread(api.check_quota, cloud, len(pending))
        for task in launch["tasks"]:
            if task.get("status") == "SUCCEEDED":
                continue
            if job.selection.get("cancel_requested"):
                await asyncio.to_thread(cloud.cancel_recovery, task["run_id"], task["recovery_session"])
            if not task.get("recovery_execution"):
                task["recovery_execution"] = await asyncio.to_thread(cloud.start_recovery, task["run_id"], task)
                # Persist each owner before continuing; lost replies are fenced
                # by the existing workflow's create-only ownership document.
                await save_run(run.id, launch=deepcopy(launch), error=None)
        await save_run(run.id, launch=launch, status="running", error=None)
    elif run.status == "running":
        await poll(controller, job, run, launch)
    elif run.status == "finalizing":
        results.validate_saved(run, launch)
        await save_run(run.id, status="cleanup_planning", error=None)
    elif run.status == "cleanup_planning":
        results.validate_saved(run, launch)
        await require_stopped(cloud, launch)
        plan = await asyncio.to_thread(prepare, cloud, launch["jobs"], include_failed=True)
        await save_run(run.id, cleanup_plan=plan, status="cleaning", error=None)
    elif run.status == "cleaning":
        results.validate_saved(run, launch)
        await require_stopped(cloud, launch)
        report = await asyncio.to_thread(cleaning_job, plan=run.cleanup_plan, execute=True,
            cloud=cloud, wait_seconds=0, plan_directory=runtime.CLEANUP_ROOT)
        launch["cleanup_report"] = report
        await save_run(run.id, launch=launch, status="log_planning" if report["complete"] else "cleaning", error=None)
    elif run.status == "log_planning":
        results.validate_saved(run, launch)
        plans = await history.cleanup_plans(run.id)
        uids = {j["uid"] for plan in plans if plan for j in plan.get("jobs", [])}
        launch["log_cleanup_plan"] = await asyncio.to_thread(plan_logs, cloud, uids)
        await save_run(run.id, launch=launch, status="log_cleaning", error=None)
    elif run.status == "log_cleaning":
        results.validate_saved(run, launch)
        report = await asyncio.to_thread(remove_logs, cloud, launch["log_cleanup_plan"])
        launch["log_cleanup_report"] = report
        await save_run(run.id, launch=launch, status="refreshing" if report["complete"] else "log_cleaning", error=None)
    elif run.status == "failed":
        await save_job(job.id, status="failed", error=run.error)
    else:
        raise ValueError(f"Unknown Batch fleet phase: {run.status}")


async def poll(controller, job, run, launch):
    from .controller import save_run, save_job
    cloud = controller.cloud
    storage = await asyncio.to_thread(cloud.storage)
    terminal = {"SUCCEEDED", "FAILED", "CANCELLED"}
    for task in launch["tasks"]:
        if job.selection.get("cancel_requested") and task["status"] not in terminal:
            await asyncio.to_thread(cloud.cancel_recovery, task["run_id"], task["recovery_session"])
        state, execution = await asyncio.to_thread(cloud.recovery_snapshot, task["run_id"], task)
        task["status"] = "RECOVERY_STARTING"
        if state:
            task.update(jobs=state["jobs"], recovery_summary=state, recovery_execution=state["execution"])
            if state["current_job"]:
                remote = await asyncio.to_thread(cloud.job, state["current_job"])
                if not remote or remote.get("uid") != state["uids"][state["current_job"]]:
                    raise ValueError("Batch VM job disappeared or changed identity; preserving results")
                api.verify_job(remote, task["spec"])
                task["batch_status"] = remote["status"]
                task["status"] = "RECOVERING" if state["status"] == "RETRYING" else remote["status"]["state"]
            if execution["state"] == "SUCCEEDED" and state["status"] in terminal:
                task["status"] = state["status"]
            elif task["status"] in terminal:
                task["status"] = "RECOVERING"  # Wait for the cloud supervisor's decision.
    # One bounded download pool serves every shard fairly; only its DB writer
    # is serialized. A missing checkpoint on one VM cannot block another VM.
    await results.import_ready(storage, run, launch['tasks'], storage_factory=cloud.storage)
    launch["jobs"] = [name for task in launch["tasks"] for name in task["jobs"]]
    launch["vm_statuses"] = [{"index": t["index"], "positions": t["count"], "state": t["status"],
        "preemptions": t.get("recovery_summary", {}).get("preemptions", 0),
        "application_failures": t.get("recovery_summary", {}).get("application_failures", 0)} for t in launch["tasks"]]
    states = [t["status"] for t in launch["tasks"]]
    finished = all(s in terminal for s in states)
    state = ("SUCCEEDED" if all(s == "SUCCEEDED" for s in states) else
             "CANCELLED" if finished and job.selection.get("cancel_requested") else
             "FAILED" if finished else "RUNNING")
    await save_run(run.id, launch=launch, cloud_state=state, error=None)
    if finished and state != "SUCCEEDED":
        message = "Batch fleet stopped. Successful VM results are safe; retry restarts only unfinished VMs from checkpoints."
        await save_run(run.id, status="failed", error=message)
        await save_job(run.job_id, status="failed", error=message)
    elif state == "SUCCEEDED":
        async with AsyncDBSession() as session:
            current = await session.get(CloudAnalysisRun, run.id)
        launch = await results.collect(storage, current, launch)
        await save_run(run.id, launch=launch, status="finalizing", error=None)
