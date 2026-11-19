"""Shared durable run/job transitions; no dependency on a backend coordinator."""
from datetime import datetime, timezone

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.models import CloudAnalysisJob, CloudAnalysisRun, CloudJobCancellation
from . import history


async def save_run(run_id, **changes):
    async with AsyncDBSession() as session, session.begin():
        # Scalar transitions must not hydrate millions of positions/receipts.
        run = await session.get(CloudAnalysisRun, run_id, with_for_update=True,
                                execution_options={'cloud_documents': ()})
        if 'status' in changes and changes['status'] != run.status:
            await history.transition(session, run, run.status, changes['status'])
            if changes['status'] == 'finalizing':
                from chessism_api.database.postgresql.cloud_codec import checksum
                run.results_verified_at = datetime.now(timezone.utc)
                launch = (changes['launch'] if 'launch' in changes else
                          await session.run_sync(lambda _: run.launch))
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
        if changes.get('status') in {'complete', 'limit_reached', 'failed', 'waiting'}:
            stop = await session.get(CloudJobCancellation, job_id)
            if stop is not None and stop.finished_at is None:
                # A stop request can arrive during a cloud API call. Never let
                # that older tick make the durable cleanup intent unpollable.
                changes = {**changes, 'status': 'running'}
        if changes.get('status') == 'paused' and job.status != 'paused':
            from .recovery_history import transition
            await transition(session, job, 'paused', message=changes.get('error'))
        for key, value in changes.items():
            setattr(job, key, value)
        job.updated_at = datetime.now(timezone.utc)
        if changes.get('status') in {'complete', 'limit_reached', 'cancelled'}:
            from .game_fen_queue import discard_queue
            await discard_queue(session, job_id)
