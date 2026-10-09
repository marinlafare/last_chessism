"""Restartable host controller: one bounded state transition per tick."""
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from uuid import uuid4

from sqlalchemy import insert, select

from . import runtime
from cloud_job.launch import configuration, expected_contract, inspect_worker, render
from cleaning_job import cleaning_job
from cleaning_job.cleanup import prepare
from stockfish_batch.checkpoints import BatchCheckpoints, encode, parse_input
from stockfish_batch.config import Config
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_MANIFEST_BYTES
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim, CloudBatchUnit
from chessism_api.operations.analysis_settings import DEFAULT_ANALYSIS_NODES, CLOUD_STALL_TIMEOUT_SECONDS
from .importer import import_batch, refresh_projections, refresh_global_projections, validate_manifest
from .preflight import verify_clean_workspace
from .log_cleanup import plan_logs, remove_logs
from . import history


async def save_run(run_id, **changes):
    async with AsyncDBSession() as session, session.begin():
        run = await session.get(CloudAnalysisRun, run_id, with_for_update=True)
        if 'status' in changes and changes['status'] != run.status:
            await history.transition(session, run, run.status, changes['status'])
            if changes['status'] == 'finalizing':
                from chessism_api.database.cloud_codec import checksum
                run.results_verified_at = datetime.now(timezone.utc)
                launch = changes.get('launch', run.launch)
                if launch.get('workflow_version') == 3:
                    from .work_units_results import proof
                    run.completion_manifest_sha256 = checksum(proof(launch))
                else:
                    run.completion_manifest_sha256 = checksum(launch['manifest'])
            elif changes['status'] == 'refreshing':
                run.cleanup_completed_at = datetime.now(timezone.utc)
        if 'launch' in changes:
            await history.save_usage(session, run, changes['launch'])
        for key, value in changes.items():
            setattr(run, key, value)


async def save_job(job_id, **changes):
    async with AsyncDBSession() as session, session.begin():
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        for key, value in changes.items():
            setattr(job, key, value)
        job.updated_at = datetime.now(timezone.utc)


async def reserve(job, limit):
    if job.selection.get('execution_mode') == 'batch_work_units_v1':
        async with AsyncDBSession() as session, session.begin():
            run = CloudAnalysisRun(id=uuid4().hex, job_id=job.id, status='preparing',
                                   positions=[], launch={}, receipts={})
            session.add(run)
            await session.flush()
            await history.transition(session, run, None, 'preparing')
        return
    from chessism_api.database.ask_db import (
        get_fens_for_analysis, get_player_fens_for_analysis, get_game_set_fens_for_analysis,
    )
    selection = job.selection
    if selection["mode"] == "games":
        session, fens = await get_game_set_fens_for_analysis(selection["game_links"], limit, raise_errors=True)
    elif selection.get("player_name"):
        session, fens = await get_player_fens_for_analysis(selection["player_name"], limit, raise_errors=True)
    else:
        session, fens = await get_fens_for_analysis(limit, raise_errors=True)
    if session is None:
        await save_job(job.id, status="waiting", error="No unclaimed, unscored FENs available. Local work may still hold them. Resume to check again.")
        return
    try:
        positions = [{"id": hashlib.sha256(fen.encode()).hexdigest(), "fen": fen} for fen in fens]
        # Validate before committing any reservation or creating billable resources.
        parse_input(b"".join(encode(row) for row in positions), limit)
        run = CloudAnalysisRun(id=uuid4().hex, job_id=job.id, status="preparing",
                               positions=positions, launch={}, receipts={})
        session.add(run)
        await session.flush()
        await history.transition(session, run, None, 'preparing')
        for offset in range(0, len(fens), 1000):
            await session.execute(insert(CloudFenClaim),
                [{"fen": fen, "run_id": run.id} for fen in fens[offset:offset + 1000]])
        await session.commit()  # Reservations are durable BEFORE any cloud side effect.
    finally:
        await session.close()


class Controller:
    def __init__(self, cloud, local_image):
        self.cloud, self.local_image = cloud, local_image

    async def tick(self):
        self.continue_immediately = False
        async with AsyncDBSession() as session:
            job = (await session.scalars(select(CloudAnalysisJob).where(
                CloudAnalysisJob.status.in_(["queued", "running", "paused"])
            ).order_by(CloudAnalysisJob.created_at).limit(1))).first()
            if job is None:
                return False
            if job.status == "paused":
                # A transfer/API error is NOT evidence that its VM stopped.
                # Do not start another billable VM until this job is resumed.
                return False
            sequence_mode = job.selection.get('execution_mode') == 'batch_sequence_v1'
            runs = [] if sequence_mode else (await session.scalars(select(CloudAnalysisRun).where(
                CloudAnalysisRun.job_id == job.id,
                ~CloudAnalysisRun.id.in_(select(CloudBatchUnit.run_id))).order_by(CloudAnalysisRun.created_at))).all()
        run = next((item for item in runs if item.status != "complete"), None)
        try:
            await save_job(job.id, status="running", error=None)
            if sequence_mode:
                from .sequence import tick
                await tick(self, job)
                return True
            if run:
                await self.advance(job, run)
                async with AsyncDBSession() as session:
                    state = await session.scalar(select(CloudAnalysisRun.status).where(CloudAnalysisRun.id == run.id))
                self.continue_immediately = self.continue_immediately or (state != run.status and state not in {'failed'})
            elif job.imported >= job.target:
                async with history.timed_phase(job.id, 'global_summaries'):
                    await refresh_global_projections()
                await save_job(job.id, status="complete")
            elif len(runs) >= (1 if job.selection.get("execution_mode") in {"single_vm_v1", "cloud_run_v1", "batch_multi_vm_v1", "batch_work_units_v1"} else
                              job.selection["runs"] if job.selection["mode"] == "loop" else (job.target + 999) // 1000):
                async with history.timed_phase(job.id, 'global_summaries'):
                    await refresh_global_projections()
                await save_job(job.id, status="limit_reached", error="Selected results imported and cleaned. Some requested positions were unavailable when reserved; no extra VM was launched.")
            else:
                limit = min(1000, job.target - job.imported)
                if job.selection.get("execution_mode") in {"single_vm_v1", "cloud_run_v1", "batch_multi_vm_v1"}:
                    limit = job.target - job.imported
                elif job.selection["mode"] == "loop":
                    limit = min(limit, job.selection["positions_per_run"])
                await reserve(job, limit)
        except Exception as exc:
            # Durable phase is not rolled back: Resume repeats an idempotent step.
            message = f"{type(exc).__name__}: {exc}"[:1000]
            await save_job(job.id, status="paused", error=message)
            if run:
                await save_run(run.id, error=message)
        return True

    async def advance(self, job, run):
        if job is not None and job.selection.get('execution_mode') == 'batch_work_units_v1':
            from .work_units_controller import advance
            return await advance(self, job, run)
        if run.status == 'refreshing':
            await refresh_projections(run.positions)
            await history.compact_run(run.id)
            return
        if run.launch.get("multi_vm") or (job and job.selection.get("execution_mode") == "batch_multi_vm_v1"):
            from .batch_spot_controller import advance
            return await advance(self, job, run)
        if run.launch.get("backend") == "cloud_run" or (job and job.selection.get("backend") == "cloud_run"):
            from .cloud_run_controller import advance
            return await advance(self, job, run)
        # Preserve an existing run's checkpoint contract on recovery; new runs
        # always use fixed system limits, never a client-provided setting.
        config = (Config(**run.launch["config"]).validate() if "config" in run.launch else
                  configuration(run.id, len(run.positions), DEFAULT_ANALYSIS_NODES,
                                CLOUD_STALL_TIMEOUT_SECONDS, stall_only=True))
        # Legacy saved runs keep their original runtime limit and recovery spec.
        seconds = config.stall_timeout or config.run_timeout + 60
        raw = b"".join(encode(row) for row in run.positions)
        launch = dict(run.launch)
        if run.status == "preparing":
            await asyncio.to_thread(verify_clean_workspace, self.cloud)
            image_id, metadata = await asyncio.to_thread(inspect_worker, self.local_image)
            contract = expected_contract(raw, run.positions, config, metadata)
            launch.update(image_id=image_id, config=asdict(config), jobs=[], workflow_version=2,
                          recovery_session=1)
            await save_run(run.id, contract=contract, launch=launch, status="publishing", error=None)
        elif run.status == "publishing":
            if launch.get("recovery_session"):
                await asyncio.to_thread(self.cloud.require_recovery)
            bucket = await asyncio.to_thread(self.cloud.bucket)
            if str(bucket.get("softDeletePolicy", {}).get("retentionDurationSeconds", "0")) != "0":
                raise ValueError("Disable bucket soft delete before launching automatic hard-delete jobs")
            launch["image"] = await asyncio.to_thread(self.cloud.publish, run.id, launch["image_id"])
            launch["spec"] = render(config, launch["image"], seconds,
                                    supervised=bool(launch.get("recovery_session")))
            await save_run(run.id, launch=launch, status="uploading", error=None)
        elif run.status == "uploading":
            storage = await asyncio.to_thread(self.cloud.storage)
            created = await asyncio.to_thread(storage.create, config.input, raw)
            if not created and await asyncio.to_thread(storage.read, config.input) != raw:
                raise ValueError("Input already exists with different contents")
            await save_run(run.id, status="submitting", error=None)
        elif run.status == "submitting":
            if launch.get("recovery_session"):
                launch["recovery_execution"] = await asyncio.to_thread(self.cloud.start_recovery, run.id, launch)
                await save_run(run.id, status="running", launch=launch, error=None)
                return
            remote = await asyncio.to_thread(self.cloud.submit, launch["jobs"][-1],
                                             launch["spec"])
            launch["uid"] = remote["uid"]
            await save_run(run.id, status="running", launch=launch, error=None)
        elif run.status == "running":
            if launch.get("recovery_session") and job.selection.get("cancel_requested"):
                await asyncio.to_thread(self.cloud.cancel_recovery, run.id, launch["recovery_session"])
            await self.poll(run, config)
        elif run.status == "finalizing":
            validate_manifest(launch["manifest"], run.positions, run.contract, run.receipts)
            await save_run(run.id, status="cleanup_planning", error=None)
        elif run.status == "cleanup_planning":
            validate_manifest(launch["manifest"], run.positions, run.contract, run.receipts)
            if launch.get("workflow_version") == 2:
                validate_performance(launch.get("performance"), run.contract, config.workers)
            if launch.get("recovery_session"):
                await asyncio.to_thread(self.cloud.require_recovery_stopped, run.id, launch)
            plan = await asyncio.to_thread(prepare, self.cloud, launch["jobs"], include_failed=True)
            await save_run(run.id, cleanup_plan=plan, status="cleaning", error=None)
        elif run.status == "cleaning":
            # Recheck the durable receipt gate even on a cleanup-only restart.
            validate_manifest(launch["manifest"], run.positions, run.contract, run.receipts)
            if launch.get("recovery_session"):
                await asyncio.to_thread(self.cloud.require_recovery_stopped, run.id, launch)
            result = await asyncio.to_thread(cleaning_job, plan=run.cleanup_plan, execute=True,
                cloud=self.cloud, wait_seconds=0,
                plan_directory=runtime.CLEANUP_ROOT)
            if result["complete"]:
                await save_run(run.id, status="log_planning" if launch.get("workflow_version") == 2 else "refreshing", error=None)
        elif run.status == "log_planning":
            plans = await history.cleanup_plans(run.id)
            uids = {job["uid"] for plan in plans if plan for job in plan.get("jobs", [])}
            launch["log_cleanup_plan"] = await asyncio.to_thread(plan_logs, self.cloud, uids)
            await save_run(run.id, launch=launch, status="log_cleaning", error=None)
        elif run.status == "log_cleaning":
            validate_performance(launch.get("performance"), run.contract, config.workers)
            report = await asyncio.to_thread(remove_logs, self.cloud, launch["log_cleanup_plan"])
            launch["log_cleanup_report"] = report
            await save_run(run.id, launch=launch, status="refreshing" if report["complete"] else "log_cleaning", error=None)
        elif run.status == "failed":
            await save_job(job.id, status="failed", error=run.error)
        else:
            raise ValueError(f"Unknown durable run phase: {run.status}")

    async def poll(self, run, config):
        supervision = None
        if run.launch.get("recovery_session"):
            supervision, execution = await asyncio.to_thread(self.cloud.recovery_snapshot, run.id, run.launch)
            if supervision is None:
                await save_run(run.id, cloud_state="RECOVERY_STARTING")
                return
            launch = {**run.launch, "jobs": supervision["jobs"], "recovery_summary": supervision,
                      "recovery_execution": supervision["execution"]}
            if supervision["current_job"] is None:
                await save_run(run.id, launch=launch, cloud_state=supervision["status"])
                if supervision["status"] == "CANCELLED":
                    await save_run(run.id, status="failed", error="Cancelled before VM creation; checkpoints retained")
                    await save_job(run.job_id, status="failed", error="Cancelled by user")
                return
            launch["uid"] = supervision["uids"][supervision["current_job"]]
            await save_run(run.id, launch=launch)
            run.launch = launch
        remote = await asyncio.to_thread(self.cloud.job, run.launch["jobs"][-1])
        if not remote or remote.get("uid") != run.launch["uid"]:
            raise ValueError("Batch job disappeared or was replaced; preserving all unimported data")
        if supervision:
            self.cloud.check_job(remote, run.launch["spec"])
        state = remote["status"]["state"]
        await save_run(run.id, cloud_state="RECOVERING" if supervision and supervision["status"] == "RETRYING" else state, error=None)
        storage = await asyncio.to_thread(self.cloud.storage)
        raw_contract = await asyncio.to_thread(storage.read, config.output + "/contract.json")
        if raw_contract is not None:
            if json.loads(raw_contract) != run.contract:
                raise ValueError("Worker contract differs from the submitted input/settings/image")
            for index in range((len(run.positions) + 499) // 500):
                name = f"batches/{index:06d}.json"
                if name not in run.receipts:
                    raw = await asyncio.to_thread(storage.read, config.output + "/" + name, BatchCheckpoints.MAX_BYTES)
                    if raw is not None:
                        await import_batch(run.id, raw, index)
                    else:
                        # Probe the next upload on the next tick, not hundreds of future files.
                        break
        if supervision and (execution["state"] != "SUCCEEDED" or supervision["status"] not in {"SUCCEEDED", "FAILED", "CANCELLED"}):
            # A failed individual attempt is not a failed logical request.
            # The cloud supervisor can launch replacements with the host off.
            return
        if state in {"FAILED", "CANCELLED"}:
            message = "Cloud task failed; committed results are safe. Retry cloud resumes saved checkpoints and can incur charges."
            if supervision:
                message = ("Cancelled by user; committed checkpoints retained." if supervision["status"] == "CANCELLED" else
                           f"Application retry allowance exhausted ({supervision['application_failures']} failed attempts, "
                           f"last exit {supervision['last_exit_code']}); checkpoints retained. "
                           f"Google preemptions ({supervision['preemptions']}) did not consume this allowance.")
            await save_run(run.id, status="failed", error=message)
            await save_job(run.job_id, status="failed", error=message)
        elif state == "SUCCEEDED":
            raw_manifest = await asyncio.to_thread(storage.read, config.output + "/manifest.json", MAX_MANIFEST_BYTES)
            if raw_contract is None or raw_manifest is None:
                raise ValueError("Cloud reported success without a contract/completion manifest")
            async with AsyncDBSession() as session:
                current = await session.get(CloudAnalysisRun, run.id)
            manifest = json.loads(raw_manifest)
            validate_manifest(manifest, current.positions, current.contract, current.receipts)
            launch = {**run.launch, "manifest": manifest, "batch_status": remote["status"]}
            if launch.get("workflow_version") == 2:
                raw_report = await asyncio.to_thread(storage.read, config.output + "/performance.json", 128 * 1024)
                if raw_report is None:
                    raise ValueError("Successful worker is missing its performance summary; cleanup withheld")
                launch["performance"] = validate_performance(json.loads(raw_report), run.contract, config.workers)
            await save_run(run.id, status="finalizing", launch=launch)
