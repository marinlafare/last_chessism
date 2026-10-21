"""Authenticated UI requests only; no Docker, Google credentials or cloud calls."""
from datetime import datetime, timezone
from collections import defaultdict
import json
from uuid import uuid4

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import case, func, literal_column, select, text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (CloudAnalysisJob, CloudAnalysisRun, CloudControllerHeartbeat,
    CloudPhaseTiming, CloudResultBatch, CloudUsage, CloudExecutionAttempt, CloudBatchUnit,
    CloudBatchSequence, CloudBatchCycle, CloudSequenceControl, CloudGameDraft, CloudGameDraftEntry, CloudJobCancellation)
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest, CloudVmCountRequest, MAX_CLOUD_FENS, MAX_CLOUD_RUN_FENS, MAX_BATCH_TOTAL_FENS, batch_cycle_count
from chessism_api.operations.cloud_analysis.admission import blocking_job, require_available
from chessism_api.operations.analysis_settings import DEFAULT_ANALYSIS_NODES, CLOUD_STALL_TIMEOUT_SECONDS
from chessism_api.redis_client import get_redis_pool
from chessism_api.database.cloud_codec import read_projections
from chessism_api.database.cloud_columns import SCHEMAS
from .cloud_game_drafts import router as game_drafts_router
from .cloud_billing import router as billing_router
from .cloud_history import router as history_router

router = APIRouter()
router.include_router(game_drafts_router, prefix='/drafts')
router.include_router(billing_router, prefix='/billing')
router.include_router(history_router, prefix='/history')


async def controller_online(session):
    heartbeat = await session.get(CloudControllerHeartbeat, 1)
    return bool(heartbeat and (datetime.now(timezone.utc) - heartbeat.seen_at).total_seconds() < 90)


@router.get("")
async def cloud_jobs():
    async with AsyncDBSession() as session:
        # References and their immutable documents must come from one snapshot
        # while the controller advances phases or compacts completed runs.
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        blocker = await blocking_job(session)
        jobs = (await session.execute(select(
                CloudAnalysisJob.id, CloudAnalysisJob.status, CloudAnalysisJob.target,
                CloudAnalysisJob.imported, CloudAnalysisJob.created_at, CloudAnalysisJob.error,
                CloudAnalysisJob._selection_ref.label('selection_ref'))
                .order_by(case(
                    (CloudAnalysisJob.status.in_(["running", "paused"]), 0),
                    (CloudAnalysisJob.status.in_(["queued", "failed", "waiting"]), 1),
                    else_=2,
                ), CloudAnalysisJob.created_at.desc()).limit(50))).all()
        runs = (await session.execute(select(
            CloudAnalysisRun.id, CloudAnalysisRun.job_id, CloudAnalysisRun.status,
            CloudAnalysisRun.cloud_state, CloudAnalysisRun.error,
            CloudAnalysisRun.position_count.label("positions"),
            CloudAnalysisRun.imported_count.label("imported"),
            CloudAnalysisRun._launch_ref.label('launch_ref'),
        ).outerjoin(CloudBatchCycle, CloudBatchCycle.run_id == CloudAnalysisRun.id)
          .where(CloudAnalysisRun.job_id.in_([job.id for job in jobs]),
                ~CloudAnalysisRun.id.in_(select(CloudBatchUnit.run_id)))
          .order_by(CloudBatchCycle.cycle_index, CloudAnalysisRun.created_at, CloudAnalysisRun.id))).all() if jobs else []
        sequences = {row.job_id: row for row in (await session.scalars(select(CloudBatchSequence)
                     .where(CloudBatchSequence.job_id.in_([job.id for job in jobs])))).all()} if jobs else {}
        controls = {row.job_id: row for row in (await session.scalars(select(CloudSequenceControl)
                    .where(CloudSequenceControl.job_id.in_([job.id for job in jobs])))).all()} if jobs else {}
        combined = {}
        if jobs:
            sources = (await session.execute(select(CloudGameDraft.submitted_job_id,
                CloudGameDraft.id, CloudGameDraftEntry.player_name, CloudGameDraftEntry.preview_fens)
                .join(CloudGameDraftEntry, CloudGameDraftEntry.draft_id == CloudGameDraft.id)
                .where(CloudGameDraft.submitted_job_id.in_([job.id for job in jobs]))
                .order_by(CloudGameDraftEntry.ordinal))).all()
            for job_id, draft_id, player, preview_fens in sources:
                entry = combined.setdefault(job_id, {'draft_id': draft_id, 'players': [], 'selections': 0,
                    'player_fen_previews': {}, 'fen_count_basis': 'saved_selection_previews'})
                entry['selections'] += 1
                # These saved preview counts can overlap across selections.
                # The job target remains the deduplicated, reserved total.
                entry['player_fen_previews'][player] = entry['player_fen_previews'].get(player, 0) + preview_fens
                if player not in entry['players']:
                    entry['players'].append(player)
        finalized = set((await session.scalars(select(CloudPhaseTiming.run_id).where(
            CloudPhaseTiming.job_id.in_(sequences), CloudPhaseTiming.phase == 'global_summaries',
            CloudPhaseTiming.outcome == 'complete', CloudPhaseTiming.finished_at.is_not(None)))).all()) if sequences else set()
        connection = await session.connection()
        cancellations = {row.job_id: row for row in (await session.execute(select(
            CloudJobCancellation.job_id, CloudJobCancellation.phase, CloudJobCancellation.requested_at,
            CloudJobCancellation.finished_at).where(CloudJobCancellation.job_id.in_([job.id for job in jobs])))).all()}
        selections = await connection.run_sync(read_projections, [job.selection_ref for job in jobs],
                                               set(SCHEMAS['selection']) - {'game_links'})
        projections = await connection.run_sync(read_projections, [run.launch_ref for run in runs], {
                'jobs', 'performance', 'recovery_session', 'recovery_summary', 'log_cleanup_report',
                'run_job', 'run_execution', 'run_executions', 'vm_statuses'})
        views = {run.id: projections[run.launch_ref] for run in runs}
        runs_by_job = defaultdict(list)
        for run in runs:
            runs_by_job[run.job_id].append(run)
        return {"controller_online": await controller_online(session),
                "cloud_busy": blocker is not None, "blocking_job_id": blocker.id if blocker else None,
                "jobs": [{
            "id": job.id, "status": job.status, "selection": selections[job.selection_ref],
            "target": job.target, "imported": job.imported,
            "combined_selection": combined.get(job.id),
            "created_at": job.created_at, "error": job.error,
            "stop_cleanup": ({'phase': cancellations[job.id].phase,
                'requested_at': cancellations[job.id].requested_at, 'finished_at': cancellations[job.id].finished_at}
                if job.id in cancellations else None),
            "sequence": ({'times': sequences[job.id].repeat_count,
                'requested': sequences[job.id].requested_count,
                'reserved': sequences[job.id].reserved_count,
                'frozen': sequences[job.id].frozen_at is not None,
                'stop_requested': sequences[job.id].stop_requested,
                'automatic': selections[job.selection_ref].get('execution_mode') == 'batch_sequence_v2',
                'phase': controls[job.id].phase if job.id in controls else 'selecting',
                'cloud_status': controls[job.id].remote_status if job.id in controls else None,
                'cloud_completed': controls[job.id].completed_cycles if job.id in controls else 0,
                'session': controls[job.id].session if job.id in controls else 1,
                'stop_acknowledged': controls[job.id].stop_acknowledged if job.id in controls else False,
                'safe_to_power_off': bool(job.id in controls and controls[job.id].accepted_at
                    and controls[job.id].remote_status in {'RUNNING', 'SUCCEEDED'})} if job.id in sequences else None),
            "runs": [{"id": run.id, "status": ('refreshing' if run.job_id in sequences
                      and selections[job.selection_ref].get('execution_mode') != 'batch_sequence_v2'
                      and run.status == 'complete' and run.positions and run.id not in finalized else run.status),
                      "cloud_state": run.cloud_state,
                      "positions": run.positions, "imported": run.imported,
                      "error": run.error, "performance": views[run.id].get('performance'),
                      "recovery_session": views[run.id].get('recovery_session'), "recovery": views[run.id].get('recovery_summary'),
                      "log_cleanup": views[run.id].get('log_cleanup_report'),
                      "run_job": views[run.id].get('run_job'), "run_execution": views[run.id].get('run_execution'),
                      "run_execution_count": len(views[run.id].get('run_executions', [])),
                      "vm_statuses": views[run.id].get('vm_statuses', []),
                      "batch_jobs": views[run.id].get('jobs', [])}
                     for run in runs_by_job[job.id]],
        } for job in jobs]}


@router.post("", status_code=202)
async def create_cloud_job(data: CloudJobRequest, redis: ArqRedis = Depends(get_redis_pool)):
    selection = data.model_dump()
    repeats = data.repeat_count
    automatic = data.backend == 'batch_spot' and repeats == 1
    selection["nodes"] = DEFAULT_ANALYSIS_NODES
    selection["stall_timeout_seconds"] = CLOUD_STALL_TIMEOUT_SECONDS
    selection["execution_mode"] = "cloud_run_v1" if data.backend == "cloud_run" else "batch_work_units_v1"
    selection["recovery_policy"] = "cloud-run-task-retry-v1" if data.backend == "cloud_run" else "google-preemption-v1"
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
        maximum = MAX_CLOUD_RUN_FENS if data.backend == 'cloud_run' else MAX_BATCH_TOTAL_FENS if automatic else MAX_CLOUD_FENS * repeats
        if target > maximum:
            raise HTTPException(409, f"This selection needs {target:,} FENs, exceeding the "
                                f"{maximum:,}-FEN cloud safety limit. Select fewer games "
                                "and preview again; no job was created.")
    else:
        target = data.target * repeats
    selection["total_fens"] = target
    if repeats > 1:
        selection['execution_mode'] = 'batch_sequence_v1'
    elif automatic:
        selection['execution_mode'] = 'batch_sequence_v2'
        repeats = batch_cycle_count(target, data.n_vms)
    async with AsyncDBSession() as session:
        if not await controller_online(session):
            raise HTTPException(409, "Cloud controller is offline; start it with Docker Compose")
        await require_available(session)
        job = CloudAnalysisJob(id=uuid4().hex, selection=selection, target=target,
                               imported=0, status="queued")
        session.add(job)
        if repeats > 1 or automatic:
            await session.flush()
            session.add(CloudBatchSequence(job_id=job.id, repeat_count=repeats, requested_count=target))
        await session.commit()
        return {"id": job.id, "status": job.status}


@router.get('/{job_id}/usage')
async def cloud_usage(job_id: str):
    """Local history only; never queries Google or triggers billable work."""
    async with AsyncDBSession() as session:
        if await session.scalar(select(CloudAnalysisJob.id).where(CloudAnalysisJob.id == job_id)) is None:
            raise HTTPException(404, 'Cloud job not found')
        run_ids = select(CloudAnalysisRun.id).where(CloudAnalysisRun.job_id == job_id)
        phases = (await session.scalars(select(CloudPhaseTiming).where(
            CloudPhaseTiming.job_id == job_id).order_by(CloudPhaseTiming.id))).all()
        usage = (await session.scalars(select(CloudUsage).where(CloudUsage.run_id.in_(run_ids)))).all()
        attempts = (await session.scalars(select(CloudExecutionAttempt).where(
            CloudExecutionAttempt.run_id.in_(run_ids)))).all()
        totals = (await session.execute(select(
            func.count().label('batches'), func.coalesce(func.sum(CloudResultBatch.position_count), 0).label('fens'),
            func.sum(CloudResultBatch.size_bytes).label('download_bytes'),
            func.sum(CloudResultBatch.download_seconds).label('download_seconds_sum'),
            func.sum(CloudResultBatch.transaction_seconds).label('transaction_seconds_sum')
        ).where(CloudResultBatch.run_id.in_(run_ids)))).mappings().one()
        def row(value):
            return {c.name: getattr(value, c.name) for c in value.__table__.columns}
        return {'job_id': job_id, 'phases': list(map(row, phases)), 'usage': list(map(row, usage)),
                'attempts': list(map(row, attempts)), 'imports': dict(totals),
                'note': 'Worker time and summed download time are not billed duration. NULL costs mean not reconciled.'}


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


@router.post('/{job_id}/stop-repeating', status_code=202)
async def stop_repeating(job_id: str):
    raise HTTPException(410, 'Stop-after-cycle has been removed. Use stop-and-cleanup.')


@router.post("/{job_id}/vms", status_code=202)
async def revise_batch_vms(job_id: str, data: CloudVmCountRequest):
    """Fix a preflight quota error without changing the frozen FEN selection.

    A paused controller does not advance this job. Empty launch metadata in
    preparing is the only phase guaranteed to precede every cloud write.
    """
    async with AsyncDBSession() as session:
        await require_available(session, own_job_id=job_id)
        if await session.get(CloudJobCancellation, job_id) is not None:
            raise HTTPException(409, 'This job is stopping. Resume cleanup instead of changing its VM count.')
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        if not job or job.status != "paused" or job.selection.get("execution_mode") not in {'batch_multi_vm_v1', 'batch_work_units_v1'}:
            raise HTTPException(409, "VM count can change only on a paused Batch preflight")
        runs = (await session.scalars(select(CloudAnalysisRun).where(
            CloudAnalysisRun.job_id == job_id).with_for_update())).all()
        if len(runs) != 1 or runs[0].status != "preparing" or runs[0].launch or runs[0].receipts or job.imported:
            raise HTTPException(409, "VM count is frozen after preparation; recover the existing fleet")
        job.selection = {**job.selection, "n_vms": data.n_vms}
        job.status, job.error, runs[0].error = "queued", None, None
        await session.commit()
    return {"id": job_id, "status": "queued"}


@router.post("/{job_id}/retry-cloud", status_code=202)
async def retry_cloud_job(job_id: str):
    """Explicit user action authorizes another bounded, billable submission."""
    async with AsyncDBSession() as session:
        await require_available(session, own_job_id=job_id)
        if await session.get(CloudJobCancellation, job_id) is not None:
            raise HTTPException(409, 'Cancelled work cannot restart. Resume cleanup if it is paused.')
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        if not job or job.status != "failed":
            raise HTTPException(409, "Only a failed cloud job can be retried")
        if job.selection.get('execution_mode') == 'batch_sequence_v2':
            from chessism_api.operations.cloud_analysis.auto_sequence_retry import retry
            try:
                await retry(session, job)
            except ValueError as exc:
                raise HTTPException(409, str(exc)) from exc
            await session.commit()
            return {'id': job_id, 'status': 'queued'}
        run = (await session.scalars(select(CloudAnalysisRun).where(
            CloudAnalysisRun.job_id == job.id, CloudAnalysisRun.status == "failed"
        ).with_for_update())).one()
        launch = dict(run.launch)
        if launch.get("multi_vm"):
            from chessism_api.operations.cloud_analysis.batch_spot_retry import retry_launch
            if run.cloud_state not in {"FAILED", "CANCELLED"}:
                raise HTTPException(409, "All Batch VMs must have stopped before retrying")
            try:
                launch = retry_launch(launch)
            except ValueError as exc:
                raise HTTPException(409, str(exc)) from exc
            run.launch, run.status, run.error = launch, "submitting", None
            job.selection = {k: v for k, v in job.selection.items() if k != "cancel_requested"}
            job.status, job.error = "queued", None
            await session.commit()
            return {"id": job_id, "status": "queued"}
        if launch.get("backend") == "cloud_run":
            if run.cloud_state not in {"FAILED", "CANCELLED"} or len(launch.get("run_executions", [])) >= 3:
                raise HTTPException(409, "Cloud Run retry unavailable; maximum three confirmed executions")
            launch.pop("run_execution", None)
            launch.pop("run_operation", None)
            run.launch, run.status, run.error = launch, "run_start", None
            job.selection = {k: v for k, v in job.selection.items() if k != "cancel_requested"}
            job.status, job.error = "queued", None
            await session.commit()
            return {"id": job_id, "status": "queued"}
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
    raise HTTPException(410, 'Cancellation without cleanup has been removed. Use stop-and-cleanup.')


@router.post('/{job_id}/stop-and-cleanup', status_code=202)
async def stop_and_cleanup(job_id: str):
    """Durable, idempotent intent; Google calls belong to the singleton controller."""
    async with AsyncDBSession() as session:
        await require_available(session, own_job_id=job_id)
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        cancellation = await session.get(CloudJobCancellation, job_id)
        if cancellation is not None:
            return {'id': job_id, 'status': 'cancelled' if cancellation.finished_at else 'stop_cleanup_requested', 'phase': cancellation.phase}
        if not job or job.status not in {'queued', 'running', 'paused', 'waiting', 'failed'}:
            raise HTTPException(409, 'Only unfinished cloud jobs can be stopped and cleaned')
        session.add(CloudJobCancellation(job_id=job_id))
        job.selection = {**job.selection, 'cancel_requested': True}
        job.status, job.error = 'queued', None
        await session.commit()
    return {'id': job_id, 'status': 'stop_cleanup_requested', 'phase': 'stopping'}
