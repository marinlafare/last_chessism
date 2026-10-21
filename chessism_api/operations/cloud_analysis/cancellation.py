"""Restartable explicit cancellation: stop, salvage, clean, refresh, release.

Never marks incomplete analysis successful and never re-enters a launch path.
All cloud deletion uses saved, identity-checked plans after quiescence.
"""
import asyncio
from datetime import datetime, timezone
from sqlalchemy import delete, func, select, update
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (CloudAnalysisJob, CloudAnalysisRun, CloudJobCancellation,
    CloudBatchUnit, CloudSequenceControl, CloudResultBatch, CloudFenClaim)
from chessism_api.database import cloud_columns
from . import runtime, history
from .cancellation_cloud import stop_batch, stop_run
from .ingestion import ingest_ready
from .importer import refresh_unit_projections, refresh_global_projections
from cleaning_job import cancelled_artifacts
from cleaning_job.cleanup import prepare, cleaning_job, owned_resources
from cleaning_job.cloud import BUCKET


async def save(job_id, **changes):
    async with AsyncDBSession() as session, session.begin():
        row = await session.get(CloudJobCancellation, job_id, with_for_update=True)
        for key, value in changes.items():
            setattr(row, key, value)


async def resources(controller, roots, control, *, signal):
    from .controller import save_run
    quiet, launches, jobs = await asyncio.to_thread(stop_batch, controller.cloud, roots, control, signal=signal)
    for root in roots:
        if root.launch.get('backend') == 'cloud_run' and root.status != 'complete':
            stopped, launch = await asyncio.to_thread(stop_run, controller.cloud, root, signal=signal)
            quiet = quiet and stopped
            launches[root.id] = launch
    if signal:
        for ident, launch in launches.items():
            await save_run(ident, launch=launch)
    return quiet, jobs


def import_tasks(run):
    launch = run.launch
    if not launch.get('config'):
        return []  # Reserved locally, never uploaded.
    if launch.get('backend') == 'cloud_run':
        from cloud_job.cloud_run import task_config
        from stockfish_batch.config import Config
        cfg = Config(**launch['config']).validate()
        return [{**task, 'output': task_config(cfg, task['index'], len(launch['tasks'])).output} for task in launch['tasks']]
    if launch.get('multi_vm'):
        return [{**task, 'output': task['config']['output']} for task in launch['tasks']]
    return [{'output': launch['config']['output'], 'count': run.position_count, 'contract': run.contract}]


async def import_available(cloud, ident):
    async with AsyncDBSession() as session:
        run = await session.get(CloudAnalysisRun, ident)
    if run.details_pruned or run.results_verified_at:
        return
    tasks = import_tasks(run)
    if tasks:
        storage = await asyncio.to_thread(cloud.storage)
        await ingest_ready(storage, run, tasks, storage_factory=cloud.storage, scan_missing=True)
        async with AsyncDBSession() as session:
            saved = await session.get(CloudAnalysisRun, ident)
        # No valid completed object may be silently discarded (including gaps).
        for task in tasks:
            base = f'gs://{BUCKET}/'
            if not task['output'].startswith(base):
                raise ValueError('Unexpected cancellation result bucket')
            prefix = task['output'][len(base):].rstrip('/') + '/batches/'
            objects = await asyncio.to_thread(cloud.objects, prefix)
            receipt_prefix = f"tasks/{task['index']:06d}/" if task.get('index') is not None else ''
            for obj in objects:
                receipt = receipt_prefix + 'batches/' + obj['name'][len(prefix):]
                if receipt not in saved.receipts:
                    raise ValueError('Completed cloud batch was not imported; retaining checkpoints')
    async with AsyncDBSession() as session:
        row = await session.get(CloudAnalysisRun, ident)
        count = await session.scalar(select(func.coalesce(func.sum(CloudResultBatch.position_count), 0))
            .where(CloudResultBatch.run_id == ident))
        claims = await session.scalar(select(func.count()).select_from(CloudFenClaim).where(CloudFenClaim.run_id == ident))
        if row.imported_count != count or claims + count != row.position_count:
            raise ValueError('Partial import ledger/reservation counts disagree; cleanup withheld')


async def finish(job_id, all_ids):
    """Release only this job's remaining claims after cleanup and summaries."""
    async with AsyncDBSession() as session, session.begin():
        cancellation = await session.get(CloudJobCancellation, job_id, with_for_update=True)
        if cancellation.phase != 'finishing':
            raise ValueError('Cancellation cleanup is not finished')
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        imported = await session.scalar(select(func.coalesce(func.sum(CloudResultBatch.position_count), 0))
            .where(CloudResultBatch.run_id.in_(all_ids)))
        if imported != job.imported:
            raise ValueError('Cancelled job import count differs from durable receipts')
        await session.execute(delete(CloudFenClaim).where(CloudFenClaim.run_id.in_(all_ids)))
        now = datetime.now(timezone.utc)
        for ident in all_ids:
            run = await session.get(CloudAnalysisRun, ident, with_for_update=True)
            if run.details_pruned:
                continue
            run.cleanup_completed_at = now
            run.details_pruned = True
            run.positions, run.receipts = [], {}
            launch = dict(run.launch)
            for manifest in [launch.get('manifest'), *launch.get('task_manifests', [])]:
                if manifest:
                    manifest['records'] = []
            run.launch = launch
            await history.transition(session, run, run.status, 'cancelled')
            run.status, run.error = 'cancelled', None
        refs = (await session.scalars(select(CloudResultBatch.detail_record_id).where(
            CloudResultBatch.run_id.in_(all_ids), CloudResultBatch.detail_record_id.is_not(None)))).all()
        await session.execute(update(CloudResultBatch).where(CloudResultBatch.run_id.in_(all_ids)).values(detail_record_id=None))
        if refs:
            await session.execute(delete(cloud_columns.ROOTS).where(cloud_columns.ROOTS.c.id.in_(refs)))
        cancellation.phase, cancellation.finished_at = 'complete', now
        job.status, job.error, job.updated_at = 'cancelled', None, now


async def tick(controller, job, cancellation):
    from .controller import save_run
    async with AsyncDBSession() as session:
        roots = (await session.scalars(select(CloudAnalysisRun).where(CloudAnalysisRun.job_id == job.id,
            ~CloudAnalysisRun.id.in_(select(CloudBatchUnit.run_id))).order_by(CloudAnalysisRun.created_at, CloudAnalysisRun.id))).all()
        all_ids = (await session.scalars(select(CloudAnalysisRun.id).where(CloudAnalysisRun.job_id == job.id)
            .order_by(CloudAnalysisRun.id))).all()
        data_ids = (await session.scalars(select(CloudAnalysisRun.id).where(CloudAnalysisRun.job_id == job.id,
            ~CloudAnalysisRun.id.in_(select(CloudBatchUnit.root_run_id))).order_by(CloudAnalysisRun.id))).all()
        control = await session.get(CloudSequenceControl, job.id)
    phase, cloud = cancellation.phase, controller.cloud
    if phase == 'stopping':
        quiet, _ = await resources(controller, roots, control, signal=True)
        if not quiet:
            return
        await save(job.id, phase='importing', cursor=0)
    elif phase == 'importing':
        if cancellation.cursor < len(data_ids):
            await import_available(cloud, data_ids[cancellation.cursor])
            await save(job.id, cursor=cancellation.cursor + 1)
        else:
            await save(job.id, phase='planning', cursor=0)
    elif phase == 'planning':
        quiet, jobs = await resources(controller, roots, control, signal=False)
        if not quiet:
            raise ValueError('Cloud work is still active; cleanup withheld')
        # Adopt an existing plan if normal cleanup had already started.
        batch_plan = next((root.cleanup_plan for root in roots if root.cleanup_plan
                           and root.cleanup_plan.get('backend') != 'cloud_run'), None)
        if not batch_plan and jobs:
            batch_plan = await asyncio.to_thread(prepare, cloud, jobs, include_failed=True)
        for root in roots:
            if root.launch.get('backend') == 'cloud_run' and root.launch.get('run_uid') and not root.cleanup_plan:
                from cloud_job.cloud_run_cleanup import prepare as prepare_run
                plan = await asyncio.to_thread(prepare_run, cloud, root.launch, root.id, cancelled=True)
                await save_run(root.id, cleanup_plan=plan)
        artifact_plan = await asyncio.to_thread(cancelled_artifacts.prepare, cloud, all_ids) if all_ids else None
        await save(job.id, batch_plan=batch_plan, artifact_plan=artifact_plan, phase='cleaning')
    elif phase == 'cleaning':
        quiet, _ = await resources(controller, roots, control, signal=False)
        if not quiet:
            raise ValueError('Supervisor/execution is still active; cleanup withheld')
        if cancellation.batch_plan:
            report = await asyncio.to_thread(cleaning_job, plan=cancellation.batch_plan, execute=True,
                cloud=cloud, wait_seconds=0, plan_directory=runtime.CLEANUP_ROOT)
            if not report['complete']:
                return
        for root in roots:
            if root.cleanup_plan and root.cleanup_plan.get('backend') == 'cloud_run':
                from cloud_job.cloud_run_cleanup import apply as clean_run
                report = await asyncio.to_thread(clean_run, cloud, root.cleanup_plan)
                if not report['complete']:
                    return
        uids = {uid for root in roots for task in root.launch.get('tasks', [root.launch])
                for uid in [*task.get('prior_uids', {}).values(), *task.get('recovery_summary', {}).get('uids', {}).values()]}
        if uids and await asyncio.to_thread(owned_resources, cloud, {'jobs': [{'uid': uid} for uid in uids]}):
            return  # Metadata deletion alone is not proof that VM/disk cleanup finished.
        if cancellation.artifact_plan and not await asyncio.to_thread(
                cancelled_artifacts.apply, cloud, cancellation.artifact_plan, all_ids):
            return
        await save(job.id, phase='logs')
    elif phase == 'logs':
        if cancellation.batch_plan and roots:
            from .log_cleanup import plan_logs, remove_logs
            launch = dict(roots[0].launch)
            if not launch.get('log_cleanup_plan'):
                launch['log_cleanup_plan'] = await asyncio.to_thread(plan_logs, cloud,
                    {item['uid'] for item in cancellation.batch_plan['jobs']})
                await save_run(roots[0].id, launch=launch)
                controller.continue_immediately = True
                return
            launch['log_cleanup_report'] = await asyncio.to_thread(remove_logs, cloud, launch['log_cleanup_plan'])
            await save_run(roots[0].id, launch=launch)
            if not launch['log_cleanup_report']['complete']:
                return
        await save(job.id, phase='refreshing', cursor=0)
    elif phase == 'refreshing':
        if cancellation.cursor < len(data_ids):
            ident = data_ids[cancellation.cursor]
            async with AsyncDBSession() as session:
                row = await session.get(CloudAnalysisRun, ident)
            if row.imported_count and not row.details_pruned:
                await refresh_unit_projections([ident])
            await save(job.id, cursor=cancellation.cursor + 1)
        else:
            if job.imported:
                async with history.timed_phase(job.id, 'cancelled_global_summaries'):
                    await refresh_global_projections()
            await save(job.id, phase='finishing')
    elif phase == 'finishing':
        await finish(job.id, all_ids)
    else:
        raise ValueError('Unexpected stop/cleanup phase: ' + phase)
    controller.continue_immediately = True
