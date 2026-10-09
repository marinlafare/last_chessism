"""Freeze one selection locally; consume it in full, sequential cloud lifecycles.

Only bounded 50k-unit reservations read FEN payloads. Future loops remain local
and claimed until their turn; no browser timers or live reselection between loops.
"""
from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import select, delete, func
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (
    CloudAnalysisJob, CloudAnalysisRun, CloudBatchSequence, CloudBatchCycle,
    CloudBatchUnit, CloudFenClaim, CloudPhaseTiming,
)
from . import history
from .schemas import MAX_CLOUD_FENS, MAX_BATCH_REPEATS

MODE = 'batch_sequence_v1'
LOCAL_STATES = {'pending', 'selecting', 'reserved'}
UNSTARTED_STATES = LOCAL_STATES | {'preparing'}


def cycle_sizes(target, repeats):
    if type(repeats) is not int or not 1 <= repeats <= MAX_BATCH_REPEATS:
        raise ValueError('Invalid loop count')
    if type(target) is not int or not 1 <= target <= repeats * MAX_CLOUD_FENS:
        raise ValueError('Each loop is limited to 500,000 FENs')
    size, extra = divmod(target, repeats)
    return [size + (index < extra) for index in range(min(target, repeats))]


async def initialize(job, sequence):
    async with AsyncDBSession() as session, session.begin():
        if await session.scalar(select(CloudBatchCycle.run_id).where(CloudBatchCycle.job_id == job.id).limit(1)):
            return
        for index, count in enumerate(cycle_sizes(sequence.requested_count, sequence.repeat_count)):
            run = CloudAnalysisRun(id=uuid4().hex, job_id=job.id, status='pending',
                                   positions=[], receipts={}, launch={})
            session.add(run)
            await session.flush()
            session.add(CloudBatchCycle(job_id=job.id, cycle_index=index, run_id=run.id, target_count=count))


async def cycles(job_id):
    async with AsyncDBSession() as session:
        return (await session.execute(select(CloudBatchCycle, CloudAnalysisRun)
            .join(CloudAnalysisRun, CloudAnalysisRun.id == CloudBatchCycle.run_id)
            .where(CloudBatchCycle.job_id == job_id).order_by(CloudBatchCycle.cycle_index))).all()


async def freeze(job_id):
    """Seal actual membership before the first upload, even after a restart."""
    async with AsyncDBSession() as session, session.begin():
        sequence = await session.get(CloudBatchSequence, job_id, with_for_update=True)
        runs = (await session.scalars(select(CloudAnalysisRun).join(
            CloudBatchCycle, CloudBatchCycle.run_id == CloudAnalysisRun.id)
            .where(CloudBatchCycle.job_id == job_id))).all()
        if any(r.status not in LOCAL_STATES for r in runs):
            raise ValueError('Cannot freeze a sequence after cloud execution started')
        sequence.reserved_count = sum(r.position_count for r in runs)
        sequence.frozen_at = datetime.now(timezone.utc)
        for run in runs:
            following = 'reserved' if run.position_count else 'complete'
            await history.transition(session, run, run.status, 'complete')
            run.status = following
            if not run.position_count:
                run.details_pruned = True
        job = await session.get(CloudAnalysisJob, job_id)
        # Retain the requested count in the sequence; progress uses actual FENs.
        job.target = sequence.reserved_count


async def select_next(controller, job, rows):
    from .controller import save_run
    from .work_units_controller import reserve_unit
    from cloud_job.work_units import ranges
    current = next(((cycle, run) for cycle, run in rows if run.position_count < cycle.target_count), None)
    if current is None:
        await freeze(job.id)
    else:
        cycle, run = current
        launch = run.launch
        if not launch:
            launch = {'backend': 'batch_spot', 'multi_vm': True, 'workflow_version': 3,
                      'units': [], 'tasks': [], 'jobs': [], 'n_vms': job.selection['n_vms'], 'recovery_session': 1}
            await save_run(run.id, status='selecting', launch=launch)
        schedule = ranges(cycle.target_count, job.selection['n_vms'])
        vm, count = schedule[len(launch['units'])]
        reserved = await reserve_unit(job, run, launch, vm, count)
        async with AsyncDBSession() as session, session.begin():
            sequence = await session.get(CloudBatchSequence, job.id, with_for_update=True)
            sequence.reserved_count = await session.scalar(select(func.coalesce(func.sum(CloudAnalysisRun.position_count), 0))
                .join(CloudBatchCycle, CloudBatchCycle.run_id == CloudAnalysisRun.id)
                .where(CloudBatchCycle.job_id == job.id))
        if reserved < count:
            # No replacement FENs are selected after this seal, including on retry.
            await freeze(job.id)
    controller.continue_immediately = True


async def stop_unstarted(job_id):
    """Only untouched local loops: release claims without any cloud deletion."""
    async with AsyncDBSession() as session, session.begin():
        sequence = await session.get(CloudBatchSequence, job_id, with_for_update=True)
        if not sequence.stop_requested:
            raise ValueError('Stopping loops requires explicit user intent')
        roots = (await session.scalars(select(CloudAnalysisRun).join(
            CloudBatchCycle, CloudBatchCycle.run_id == CloudAnalysisRun.id)
            .where(CloudBatchCycle.job_id == job_id))).all()
        if any(root.status not in UNSTARTED_STATES | {'complete'} for root in roots):
            raise ValueError('Finish or recover the current loop before stopping future loops')
        for root in roots:
            if root.status == 'complete':
                continue
            # A local state must never hide a published image or recovery owner.
            if (any(key in root.launch for key in ('image', 'spec', 'recovery_execution'))
                    or root.launch.get('jobs') or any(task.get('recovery_execution') or task.get('jobs')
                                                    for task in root.launch.get('tasks', []))):
                raise ValueError('Unstarted loop unexpectedly owns cloud resources')
            children = (await session.scalars(select(CloudAnalysisRun).join(
                CloudBatchUnit, CloudBatchUnit.run_id == CloudAnalysisRun.id)
                .where(CloudBatchUnit.root_run_id == root.id))).all()
            ids = [child.id for child in children]
            if ids:
                await session.execute(delete(CloudFenClaim).where(CloudFenClaim.run_id.in_(ids)))
            for child in children:
                child.positions, child.receipts = [], {}
                child.status, child.details_pruned = 'cancelled', True
            await history.transition(session, root, root.status, 'complete')
            root.status, root.details_pruned = 'cancelled', True
        job = await session.get(CloudAnalysisJob, job_id)
        job.status, job.error = 'cancelled', 'Stopped remaining loops; completed results are retained.'


async def tick(controller, job):
    from .controller import save_job, save_run
    from .work_units_controller import advance
    from .importer import refresh_global_projections
    async with AsyncDBSession() as session:
        sequence = await session.get(CloudBatchSequence, job.id)
    if sequence is None:
        raise ValueError('Missing durable Batch sequence')
    await initialize(job, sequence)
    rows = await cycles(job.id)
    if not sequence.frozen_at:
        if sequence.stop_requested:
            await stop_unstarted(job.id)
        else:
            await select_next(controller, job, rows)
        return
    # A loop is not over until its normal cleanup AND local finalization finish.
    for cycle, run in rows:
        if run.status != 'complete':
            break
        if not run.position_count:
            continue
        async with AsyncDBSession() as session:
            summarized = await session.scalar(select(CloudPhaseTiming.id).where(
                CloudPhaseTiming.run_id == run.id, CloudPhaseTiming.phase == 'global_summaries',
                CloudPhaseTiming.outcome == 'complete', CloudPhaseTiming.finished_at.is_not(None)).limit(1))
        if not summarized:
            if not run.results_verified_at or not run.cleanup_completed_at or run.imported_count != run.position_count:
                raise ValueError('Previous loop is not verified, imported and cleaned')
            async with history.timed_phase(job.id, 'global_summaries', run.id):
                await refresh_global_projections()
            controller.continue_immediately = True
            return
    else:
        if job.imported != sequence.reserved_count:
            raise ValueError('Sequence import count does not match its frozen set')
        limited = sequence.reserved_count < sequence.requested_count
        await save_job(job.id, status='limit_reached' if limited else 'complete',
                       error='Only the available unscored FENs were reserved; no replacements were selected.' if limited else None)
        return
    if sequence.stop_requested and run.status in UNSTARTED_STATES:
        await stop_unstarted(job.id)
        return
    if run.status == 'reserved':
        await save_run(run.id, status='preparing')
        controller.continue_immediately = True
        return
    before = run.status
    try:
        await advance(controller, job, run)
    except Exception as exc:
        await save_run(run.id, error=f'{type(exc).__name__}: {exc}'[:1000])
        raise
    async with AsyncDBSession() as session:
        after = await session.scalar(select(CloudAnalysisRun.status).where(CloudAnalysisRun.id == run.id))
    controller.continue_immediately = controller.continue_immediately or (after != before and after != 'failed')
