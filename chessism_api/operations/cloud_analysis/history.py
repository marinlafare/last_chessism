"""Column-based timing/usage records and terminal-only detail compaction."""
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import time
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudPhaseTiming, CloudResultBatch, CloudUsage, CloudExecutionAttempt, CloudFenClaim
from chessism_api.database import cloud_columns as columns


@asynccontextmanager
async def timed_phase(job_id, phase, run_id=None):
    started = time.monotonic()
    async with AsyncDBSession() as session, session.begin():
        row = CloudPhaseTiming(job_id=job_id, run_id=run_id, phase=phase,
                               started_at=datetime.now(timezone.utc), outcome='running')
        session.add(row)
        await session.flush()
        ident = row.id
    outcome, error = 'complete', None
    try:
        yield
    except BaseException as exc:
        outcome, error = 'failed', f'{type(exc).__name__}: {exc}'[:1000]
        raise
    finally:
        async with AsyncDBSession() as session, session.begin():
            await session.execute(update(CloudPhaseTiming).where(CloudPhaseTiming.id == ident).values(
                finished_at=datetime.now(timezone.utc), duration_seconds=time.monotonic() - started,
                outcome=outcome, error=error))


async def transition(session, run, previous, following):
    now = datetime.now(timezone.utc)
    open_rows = (await session.scalars(select(CloudPhaseTiming).where(
        CloudPhaseTiming.run_id == run.id, CloudPhaseTiming.finished_at.is_(None)))).all()
    for row in open_rows:
        row.finished_at = now
        row.duration_seconds = max(0, (now - row.started_at).total_seconds())
        row.outcome = 'failed' if following == 'failed' else 'complete'
    if following not in {'complete', 'failed'}:
        session.add(CloudPhaseTiming(job_id=run.job_id, run_id=run.id, phase=following,
                                    started_at=now, outcome='running'))


async def save_usage(session, run, launch):
    reports = launch.get('task_performance') or ([launch['performance']] if 'metrics' in launch.get('performance', {}) else [])
    for index, report in enumerate(reports):
        metrics = report['metrics']
        backend = launch.get('backend', 'batch_spot')
        task = launch.get('tasks', [{}] * len(reports))[index]
        cfg = task.get('config', launch.get('config', {}))
        vm_gib = launch.get('performance', {}).get('memory_gib_per_vm')
        values = dict(run_id=run.id, task_index=index, backend=backend,
                      cpu_count=1 if backend == 'cloud_run' else cfg.get('workers'),
                      memory_mib=1024 if backend == 'cloud_run' else (vm_gib * 1024 if vm_gib else None),
                      worker_memory_mib=cfg.get('memory_mib'),
                      machine_type=launch.get('performance', {}).get('machine_type'),
                      worker_seconds=metrics.get('worker_seconds'), analyzed_fens=report.get('analyzed_this_attempt'),
                      peak_memory_bytes=metrics.get('memory', {}).get('cgroup_peak_bytes'),
                      cpu_busy_percent=metrics.get('vm_cpu_busy_percent'))
        statement = insert(CloudUsage).values(**values)
        await session.execute(statement.on_conflict_do_update(index_elements=['run_id', 'task_index'],
            set_={k: v for k, v in values.items() if k not in {'run_id', 'task_index'}}))
    backend = launch.get('backend', 'batch_spot')
    if backend == 'cloud_run':
        summary = launch.get('execution_summary', {})
        attempts = [(0, ident['name'], ident['uid'], summary if summary.get('uid') == ident['uid'] else {}, {})
                    for ident in launch.get('run_executions', [])]
    else:
        attempts = []
        for task in launch.get('tasks', [launch]):
            recovery = task.get('recovery_summary', {})
            uids = {**task.get('prior_uids', {}), **recovery.get('uids', {})}
            if task.get('uid') and task.get('jobs'):
                uids[task['jobs'][-1]] = task['uid']
            for name, uid in uids.items():
                current = recovery.get('current_job', (task.get('jobs') or [None])[-1])
                attempts.append((task.get('index', 0), name, uid,
                                 task.get('batch_status', {}) if name == current else {}, recovery))
    def timestamp(value):
        return datetime.fromisoformat(value.replace('Z', '+00:00')) if value else None
    for index, name, uid, status, recovery in attempts:
        values = dict(run_id=run.id, resource_uid=uid, task_index=index, backend=backend,
                      resource_name=name, recovery_session=recovery.get('session', launch.get('recovery_session')),
                      observed_at=datetime.now(timezone.utc))
        if status:
            if backend == 'cloud_run':
                values.update(started_at=timestamp(status.get('startTime')),
                              finished_at=timestamp(status.get('completionTime')))
                if status.get('completionTime'):
                    values['state'] = 'FAILED' if status.get('failedCount') else 'SUCCEEDED'
            else:
                values['state'] = status.get('state')
                if status.get('runDuration'):
                    values['reported_run_seconds'] = float(status['runDuration'].removesuffix('s'))
                for event in status.get('statusEvents', []):
                    description = event.get('description', '')
                    if ' to RUNNING ' in description:
                        values['started_at'] = timestamp(event.get('eventTime'))
                    if any(f' to {state} ' in description for state in ('SUCCEEDED', 'FAILED', 'CANCELLED')):
                        values['finished_at'] = timestamp(event.get('eventTime'))
        stmt = insert(CloudExecutionAttempt).values(**values)
        await session.execute(stmt.on_conflict_do_update(index_elements=['run_id', 'resource_uid'],
            set_={k: v for k, v in values.items() if k not in {'run_id', 'resource_uid'}}))


async def compact_run(run_id, *, historical=False):
    """After verified cloud cleanup AND local summaries, release transient IDs.

    Keep compact batch checksums/counts/timings, engine settings, attempts and
    performance summaries. Never compact failed or still-recoverable runs.
    """
    async with AsyncDBSession() as session, session.begin():
        run = await session.get(CloudAnalysisRun, run_id, with_for_update=True)
        if run.details_pruned:
            return False
        if historical:
            job = await session.get(CloudAnalysisJob, run.job_id)
            if run.status != 'complete' or job.status not in {'complete', 'limit_reached'}:
                return False
            from .importer import validate_manifest
            from chessism_api.database.cloud_codec import checksum
            manifest = run.launch.get('manifest')
            if not manifest:
                proof = run.launch.get('import_receipt', {})
                if not proof.get('database_receipts_verified') or not run.launch.get('cleanup_verification', {}).get('complete'):
                    return False
                from stockfish_batch.checkpoints import digest, encode
                manifest = {'status': 'complete', 'fingerprint': run.contract['fingerprint'],
                            'position_count': run.position_count,
                            'records': [r for key in sorted(run.receipts) for r in run.receipts[key]['records']]}
                if digest(encode(manifest)) != proof.get('manifest_sha256'):
                    raise ValueError('Archived benchmark completion hash mismatch; details retained')
                run.launch = {**run.launch, 'manifest': manifest}
            validate_manifest(manifest, run.positions, run.contract, run.receipts)
            if await session.scalar(select(CloudFenClaim.fen).where(CloudFenClaim.run_id == run.id).limit(1)):
                raise ValueError('Completed history still owns FEN claims; compaction withheld')
            run.completion_manifest_sha256 = checksum(run.launch['manifest'])
        elif run.status != 'refreshing' or not run.results_verified_at or not run.cleanup_completed_at:
            raise ValueError('Only a cleaned, locally refreshed run can be compacted')
        launch = dict(run.launch)
        for manifest in [launch.get('manifest'), *launch.get('task_manifests', [])]:
            if manifest:
                manifest['records'] = []
        # Remaining cleanup targets are no longer needed, but retain job/UID
        # ownership for future safe log cleanup and audits.
        plan = dict(run.cleanup_plan) if run.cleanup_plan else None
        if plan:
            plan['objects'] = []
            from cleaning_job.cleanup import digest
            plan['sha256'] = digest(plan)
        run.details_pruned = True
        run.positions, run.receipts = [], {}
        run.launch, run.cleanup_plan = launch, plan
        detail_ids = (await session.scalars(select(CloudResultBatch.detail_record_id).where(
            CloudResultBatch.run_id == run_id, CloudResultBatch.detail_record_id.is_not(None)))).all()
        await session.execute(update(CloudResultBatch).where(CloudResultBatch.run_id == run_id).values(detail_record_id=None))
        if detail_ids:
            await session.execute(delete(columns.ROOTS).where(columns.ROOTS.c.id.in_(detail_ids)))
        if not historical:
            await transition(session, run, 'refreshing', 'complete')
        run.status = 'complete'
    return True


async def cleanup_plans(current_id):
    from chessism_api.database.cloud_codec import read_documents
    async with AsyncDBSession() as session:
        refs = (await session.scalars(select(CloudAnalysisRun._cleanup_plan_ref).where(
            (CloudAnalysisRun.status == 'complete') | (CloudAnalysisRun.id == current_id)))).all()
        refs = [ref for ref in refs if ref]
        connection = await session.connection()
        return list((await connection.run_sync(read_documents, refs)).values()) if refs else []
