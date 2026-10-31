"""One explicit 10,000-FEN / two-task test using the production controller.

Only the initial local cycle plan differs from normal 500k splitting. Never
changes the UI policy, engine settings, Google workflow or recovery policy.
"""
import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from uuid import UUID, uuid5

from dotenv import dotenv_values
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from cloud_job.run_host import host_database_url, gcloud_executable
from cloud_job.sequencing import MODE
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudBatchCycle, CloudBatchSequence, CloudSequenceControl
from chessism_api.operations.analysis_settings import DEFAULT_ANALYSIS_NODES, CLOUD_STALL_TIMEOUT_SECONDS
from chessism_api.operations.cloud_analysis.admission import require_available
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
from chessism_api.routers.cloud_analysis import controller_online

TASK_FENS = 5000
TASKS = 2


async def enqueue(job_id):
    async with AsyncDBSession() as session, session.begin():
        await require_available(session, own_job_id=job_id)
        existing = await session.get(CloudAnalysisJob, job_id)
        if existing:
            cycles = (await session.scalars(select(CloudBatchCycle).where(CloudBatchCycle.job_id == job_id))).all()
            if (existing.selection.get('execution_mode') != MODE or existing.target != TASK_FENS * TASKS
                    or existing.selection.get('n_vms') != 1 or len(cycles) != TASKS
                    or any(c.target_count != TASK_FENS for c in cycles)):
                raise ValueError('Existing job differs from this test; refusing to alter it')
            return  # Lost response/rerun must not create another paid job.
        if not await controller_online(session):
            raise ValueError('The Compose cloud controller must be online before enqueueing')
        selection = {**CloudJobRequest(total_fens=TASK_FENS * TASKS, n_vms=1).model_dump(),
            'execution_mode': MODE, 'recovery_policy': 'google-preemption-v1',
            'nodes': DEFAULT_ANALYSIS_NODES, 'stall_timeout_seconds': CLOUD_STALL_TIMEOUT_SECONDS}
        session.add(CloudAnalysisJob(id=job_id, selection=selection, target=TASK_FENS * TASKS,
                                    imported=0, status='queued'))
        await session.flush()
        session.add(CloudBatchSequence(job_id=job_id, repeat_count=TASKS, requested_count=TASK_FENS * TASKS))
        await session.flush()
        for index in range(TASKS):
            ident = uuid5(UUID(job_id), f'offline-test-cycle-{index}').hex
            session.add(CloudAnalysisRun(id=ident, job_id=job_id, status='pending',
                                        positions=[], receipts={}, launch={}))
            await session.flush()
            session.add(CloudBatchCycle(job_id=job_id, cycle_index=index, run_id=ident, target_count=TASK_FENS))


async def status(job_id):
    async with AsyncDBSession() as session:
        job = await session.get(CloudAnalysisJob, job_id)
        if not job:
            raise ValueError('Test job not found')
        control = await session.get(CloudSequenceControl, job_id)
        roots = (await session.scalars(select(CloudAnalysisRun).join(CloudBatchCycle,
            CloudBatchCycle.run_id == CloudAnalysisRun.id).where(CloudBatchCycle.job_id == job_id)
            .order_by(CloudBatchCycle.cycle_index))).all()
        return {'job_id': job.id, 'status': job.status, 'target': job.target,
            'imported': job.imported, 'error': job.error,
            'phase': control.phase if control else 'selecting',
            'control_id': control.control_run_id if control else None,
            'execution': control.execution if control else None,
            'accepted_at': str(control.accepted_at) if control and control.accepted_at else None,
            'cloud_status': control.remote_status if control else None,
            'cloud_completed': control.completed_cycles if control else 0,
            'cycles': [{'id': r.id, 'positions': r.position_count, 'imported': r.imported_count,
                'status': r.status, 'tasks': [{'run_id': t['run_id'], 'state': t['status'],
                    'jobs': t.get('jobs', [])} for t in r.launch.get('tasks', [])]} for r in roots]}


async def retry_unstarted(job_id):
    """Explicit test recovery only when no cloud task or ownership fence exists."""
    from cloud_job.launch import Client
    cloud = Client(gcloud=gcloud_executable())
    await asyncio.to_thread(cloud.require_sequence)
    async with AsyncDBSession() as session, session.begin():
        await require_available(session, own_job_id=job_id)
        job = await session.get(CloudAnalysisJob, job_id, with_for_update=True)
        c = await session.get(CloudSequenceControl, job_id, with_for_update=True)
        if (not job or job.status != 'paused' or job.target != TASK_FENS * TASKS or job.imported
                or not c or c.phase != 'running' or c.accepted_at or c.session >= 3):
            raise ValueError('This is not an unacknowledged paused test startup')
        peers = await asyncio.to_thread(cloud.sequence_executions, c.control_run_id, c.session)
        if not peers or any(p.get('state') != 'FAILED' for p in peers):
            raise ValueError('A sequence execution might still run')
        if await asyncio.to_thread(cloud.jobs):
            raise ValueError('Batch jobs exist; refusing automatic startup recovery')
        for name in ('owner.json', 'state.json'):
            if await asyncio.to_thread(cloud.sequence_document, c.control_run_id, c.session, name):
                raise ValueError('Cloud sequence already acquired ownership; inspect instead')
        roots = (await session.scalars(select(CloudAnalysisRun).join(CloudBatchCycle,
            CloudBatchCycle.run_id == CloudAnalysisRun.id).where(CloudBatchCycle.job_id == job_id))).all()
        if len(roots) != TASKS or any(r.position_count != TASK_FENS for r in roots):
            raise ValueError('Test selection differs')
        for root in roots:
            for task in root.launch['tasks']:
                if task.get('jobs') or task.get('recovery_summary'):
                    raise ValueError('A task has started')
                for name in ('owner.json', 'state.json'):
                    if await asyncio.to_thread(cloud.recovery_document, task['run_id'], task['recovery_session'], name):
                        raise ValueError('Task owner exists; retain checkpoints')
        c.session += 1
        c.phase, c.execution, c.plan_sha256 = 'replanning', None, None
        c.remote_status, c.failure_import_cursor = None, None
        c.import_cursor, c.completed_cycles = 0, 0
        job.status, job.error = 'queued', None


async def run(args):
    values = dotenv_values(ROOT / '.env')
    engine = create_async_engine(host_database_url(values.get('DATABASE_URL')), connect_args={'ssl': False})
    AsyncDBSession.configure(bind=engine)
    try:
        if args.enqueue:
            await enqueue(args.job_id)
        if args.retry_unstarted:
            await retry_unstarted(args.job_id)
        print(json.dumps(await status(args.job_id), indent=2))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job-id', required=True)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--enqueue', action='store_true', help='Explicitly authorize the two paid cloud tasks')
    actions.add_argument('--retry-unstarted', action='store_true', help='Retry only a proven ownerless startup failure')
    args = parser.parse_args()
    if not re.fullmatch('[a-f0-9]{32}', args.job_id):
        parser.error('job-id must be 32 lowercase hex characters')
    asyncio.run(run(args))
