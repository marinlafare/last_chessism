"""Authenticated UI requests only; no Docker, Google credentials or cloud calls."""
from datetime import datetime, timezone
import json
from uuid import uuid4

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import case, func, literal_column, select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudControllerHeartbeat
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest, MAX_CLOUD_FENS
from chessism_api.operations.cloud_analysis.admission import blocking_job, require_available
from chessism_api.operations.analysis_settings import DEFAULT_ANALYSIS_NODES, CLOUD_STALL_TIMEOUT_SECONDS
from chessism_api.redis_client import get_redis_pool

router = APIRouter()


async def controller_online(session):
    heartbeat = await session.get(CloudControllerHeartbeat, 1)
    return bool(heartbeat and (datetime.now(timezone.utc) - heartbeat.seen_at).total_seconds() < 90)


@router.get("")
async def cloud_jobs():
    async with AsyncDBSession() as session:
        blocker = await blocking_job(session)
        jobs = (await session.scalars(select(CloudAnalysisJob)
                .order_by(case(
                    (CloudAnalysisJob.status.in_(["running", "paused"]), 0),
                    (CloudAnalysisJob.status.in_(["queued", "failed", "waiting"]), 1),
                    else_=2,
                ), CloudAnalysisJob.created_at.desc()).limit(50))).all()
        runs = (await session.execute(select(
            CloudAnalysisRun.id, CloudAnalysisRun.job_id, CloudAnalysisRun.status,
            CloudAnalysisRun.cloud_state, CloudAnalysisRun.error,
            func.json_array_length(CloudAnalysisRun.positions).label("positions"),
            literal_column("COALESCE((SELECT SUM(json_array_length(value->'records')) "
                           "FROM json_each(cloud_analysis_run.receipts)), 0)").label("imported"),
            CloudAnalysisRun.launch["jobs"].label("batch_jobs"),
            CloudAnalysisRun.launch["performance"].label("performance"),
            CloudAnalysisRun.launch["recovery_session"].label("recovery_session"),
            CloudAnalysisRun.launch["recovery_summary"].label("recovery"),
            CloudAnalysisRun.launch["log_cleanup_report"].label("log_cleanup"),
        ).where(CloudAnalysisRun.job_id.in_([job.id for job in jobs])))).all() if jobs else []
        return {"controller_online": await controller_online(session),
                "cloud_busy": blocker is not None, "blocking_job_id": blocker.id if blocker else None,
                "jobs": [{
            "id": job.id, "status": job.status, "selection": {
                key: value for key, value in job.selection.items() if key != "game_links"
            }, "target": job.target, "imported": job.imported,
            "created_at": job.created_at, "error": job.error,
            "runs": [{"id": run.id, "status": run.status, "cloud_state": run.cloud_state,
                      "positions": run.positions, "imported": run.imported,
                      "error": run.error, "performance": run.performance,
                      "recovery_session": run.recovery_session, "recovery": run.recovery,
                      "log_cleanup": run.log_cleanup,
                      "batch_jobs": run.batch_jobs or []}
                     for run in runs if run.job_id == job.id],
        } for job in jobs]}


@router.post("", status_code=202)
async def create_cloud_job(data: CloudJobRequest, redis: ArqRedis = Depends(get_redis_pool)):
    selection = data.model_dump()
    selection["nodes"] = DEFAULT_ANALYSIS_NODES
    selection["stall_timeout_seconds"] = CLOUD_STALL_TIMEOUT_SECONDS
    selection["execution_mode"] = "single_vm_v1"
    selection["recovery_policy"] = "google-preemption-v1"
    if data.mode == "games":
        raw = await redis.get(f"chessism:player_game_analysis_plan:{data.plan_id}")
        if not raw:
            raise HTTPException(409, "Game preview expired; preview again")
        plan = json.loads(raw)
        selection["game_links"] = plan["game_links"]
        selection["player_name"] = plan.get("player_name", "")
        if not selection["game_links"]:
            raise HTTPException(409, "No incomplete games in this preview")
        # Use the trusted server-side preview, never a FEN count sent by the UI.
        target = plan.get("fens_to_analyze")
        if type(target) is not int or target < 0:
            raise HTTPException(409, "Game preview has no valid FEN count; preview again")
        if target == 0:
            raise HTTPException(409, "No missing FENs in this game selection")
        if target > MAX_CLOUD_FENS:
            raise HTTPException(409, f"This selection needs {target:,} FENs, exceeding the "
                                f"{MAX_CLOUD_FENS:,}-FEN cloud safety limit. Select fewer games "
                                "and preview again; no job was created.")
    else:
        target = data.target
    selection["total_fens"] = target
    async with AsyncDBSession() as session:
        if not await controller_online(session):
            raise HTTPException(409, "Cloud controller is offline; start it with Docker Compose")
        await require_available(session)
        job = CloudAnalysisJob(id=uuid4().hex, selection=selection, target=target,
                               imported=0, status="queued")
        session.add(job)
        await session.commit()
        return {"id": job.id, "status": job.status}


@router.post("/{job_id}/resume", status_code=202)
async def resume_cloud_job(job_id: str):
    """Retry failed transfers/cleanup; a terminal failed cloud task stays failed."""
    async with AsyncDBSession() as session:
        await require_available(session, own_job_id=job_id)
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        if not job or job.status not in {"paused", "waiting"}:
            raise HTTPException(409, "Only paused or waiting jobs can resume")
        job.status, job.error = "queued", None
        await session.commit()
    return {"id": job_id, "status": "queued"}


@router.post("/{job_id}/retry-cloud", status_code=202)
async def retry_cloud_job(job_id: str):
    """Explicit user action authorizes another bounded, billable submission."""
    async with AsyncDBSession() as session:
        await require_available(session, own_job_id=job_id)
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        if not job or job.status != "failed":
            raise HTTPException(409, "Only a failed cloud job can be retried")
        run = (await session.scalars(select(CloudAnalysisRun).where(
            CloudAnalysisRun.job_id == job.id, CloudAnalysisRun.status == "failed"
        ).with_for_update())).one()
        launch = dict(run.launch)
        if launch.get("recovery_session"):
            if run.cloud_state not in {"FAILED", "CANCELLED"} or launch["recovery_session"] >= 3:
                raise HTTPException(409, "Cloud retry unavailable (maximum three explicitly requested recovery sessions)")
            launch.update(recovery_session=launch["recovery_session"] + 1,
                          prior_jobs=launch["jobs"], prior_uids=launch["recovery_summary"]["uids"])
            launch.pop("recovery_execution", None)
            launch.pop("recovery_summary", None)
            job.selection = {key: value for key, value in job.selection.items() if key != "cancel_requested"}
        elif run.cloud_state != "FAILED" or len(run.launch["jobs"]) >= 3:
            raise HTTPException(409, "Cloud retry unavailable (maximum three submissions per chunk)")
        else:
            launch["jobs"] = [*launch["jobs"], f"chessism-ui-{run.id}-{len(launch['jobs']) + 1}"]
        run.launch, run.status, run.error = launch, "submitting", None
        job.status, job.error = "queued", None
        await session.commit()
    return {"id": job_id, "status": "queued"}


@router.post("/{job_id}/cancel", status_code=202)
async def cancel_cloud_job(job_id: str):
    """Durable intent; the host forwards it to the cloud supervisor, not a VM kill."""
    async with AsyncDBSession() as session:
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        if not job or job.status not in {"running", "paused"}:
            raise HTTPException(409, "Only active supervised cloud jobs can be cancelled")
        run = (await session.scalars(select(CloudAnalysisRun).where(
            CloudAnalysisRun.job_id == job.id, CloudAnalysisRun.status == "running"
        ))).one_or_none()
        if run is None or not run.launch.get("recovery_session"):
            raise HTTPException(409, "Wait until the recovery controller is running before cancelling")
        job.selection = {**job.selection, "cancel_requested": True}
        job.status = "running"
        await session.commit()
    return {"id": job_id, "status": "cancellation_requested"}
