"""One cleanup plan for the whole sequence and its shared image."""
import asyncio
from datetime import datetime, timezone
from sqlalchemy import select
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudPhaseTiming
from cleaning_job.cleanup import prepare, cleaning_job
from . import history, runtime, work_units_results as results
from .log_cleanup import plan_logs, remove_logs


async def finish(controller, job, control, roots, sequence):
    from .controller import save_run, save_job
    from .auto_sequence import save_control, refresh_recovery
    from .work_units_controller import advance
    from .importer import refresh_global_projections
    cloud, first = controller.cloud, roots[0]
    if control.phase == 'cleanup_planning' and first.cleanup_plan and first.status == 'cleaning':
        await save_control(job.id, phase='cleaning')
        controller.continue_immediately = True
        return
    # Independent of Google files so a restart in partial deletion stays safe.
    await asyncio.to_thread(cloud.require_sequence_stopped, control)
    for root in roots:
        if root.status != 'complete':
            await results.verify_saved(root, root.launch)
    launch = dict(first.launch)
    if control.phase == 'cleanup_planning':
        # Every child supervisor shares this sequence execution; all must be terminal.
        # Older controllers could persist finalizing before recovery confirmed it.
        # Refresh/validate and save proof BEFORE planning deletion; never rely on
        # cloud state files once a durable cleanup plan exists and deletion starts.
        jobs = []
        for root in roots:
            refreshed = await refresh_recovery(cloud, root.launch, control, require_succeeded=True)
            for task in refreshed['tasks']:
                await asyncio.to_thread(cloud.require_recovery_stopped, task['run_id'], task)
            await save_run(root.id, launch=refreshed)
            jobs.extend(refreshed['jobs'])
        plan = await asyncio.to_thread(prepare, cloud, jobs, include_failed=True)
        await save_run(first.id, cleanup_plan=plan, status='cleaning')
        await save_control(job.id, phase='cleaning')
    elif control.phase == 'cleaning':
        report = await asyncio.to_thread(cleaning_job, plan=first.cleanup_plan, execute=True,
            cloud=cloud, wait_seconds=0, plan_directory=runtime.CLEANUP_ROOT)
        launch['cleanup_report'] = report
        phase = 'log_planning' if report['complete'] else 'cleaning'
        await save_run(first.id, launch=launch, status=phase)
        await save_control(job.id, phase=phase)
        if not report['complete']:
            return
    elif control.phase == 'log_planning':
        plans = await history.cleanup_plans(first.id)
        launch['log_cleanup_plan'] = await asyncio.to_thread(plan_logs, cloud,
            {j['uid'] for plan in plans if plan for j in plan.get('jobs', [])})
        await save_run(first.id, launch=launch, status='log_cleaning')
        await save_control(job.id, phase='log_cleaning')
    elif control.phase == 'log_cleaning':
        report = await asyncio.to_thread(remove_logs, cloud, launch['log_cleanup_plan'])
        launch['log_cleanup_report'] = report
        await save_run(first.id, launch=launch)
        if not report['complete']:
            return
        for root in roots:
            await save_run(root.id, status='refreshing')
        await save_control(job.id, phase='refreshing')
    else:
        for root in roots:
            if root.status != 'complete':
                await advance(controller, job, root)
                controller.continue_immediately = True
                return
        async with AsyncDBSession() as session:
            refreshed = await session.scalar(select(CloudPhaseTiming.id).where(
                CloudPhaseTiming.job_id == job.id, CloudPhaseTiming.run_id.is_(None),
                CloudPhaseTiming.phase == 'global_summaries', CloudPhaseTiming.outcome == 'complete',
                CloudPhaseTiming.finished_at.is_not(None)).limit(1))
        if not refreshed:
            async with history.timed_phase(job.id, 'global_summaries'):
                await refresh_global_projections()
        if job.imported != sequence.reserved_count:
            raise ValueError('Frozen sequence count differs from committed local imports')
        await save_control(job.id, phase='complete')
        limited = sequence.reserved_count < sequence.requested_count
        await save_job(job.id, status='limit_reached' if limited else 'complete',
                       error='Only available unscored FENs were selected.' if limited else None)
    controller.continue_immediately = True
