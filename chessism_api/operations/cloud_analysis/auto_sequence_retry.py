"""Explicit, database-only retry intent; the host submits a new fenced session."""
from copy import deepcopy
from sqlalchemy import select
from chessism_api.database.models import CloudBatchCycle, CloudAnalysisRun, CloudSequenceControl, CloudBatchSequence


async def retry(session, job):
    control = await session.get(CloudSequenceControl, job.id, with_for_update=True)
    if (not control or control.phase != 'running' or control.session >= 3
            or control.remote_status not in {'FAILED', 'CANCELLED', 'STOPPED'}):
        raise ValueError('Sequence retry requires a stopped supervisor; maximum three explicit sessions')
    roots = (await session.scalars(select(CloudAnalysisRun).join(CloudBatchCycle,
        CloudBatchCycle.run_id == CloudAnalysisRun.id).where(CloudBatchCycle.job_id == job.id))).all()
    for root in roots:
        if not root.position_count:
            continue
        launch = deepcopy(root.launch)
        for task in launch['tasks']:
            if task['status'] == 'SUCCEEDED':
                continue
            if task['status'] == 'PENDING' and not task.get('jobs') and not task.get('recovery_summary'):
                continue  # Never started; no retry allowance was consumed.
            state = task.get('recovery_summary', {})
            if task['status'] not in {'FAILED', 'CANCELLED'} or state.get('status') != task['status']:
                raise ValueError('All task supervisors must be terminal before retry')
            task.update(recovery_session=task['recovery_session'] + 1, status='PENDING',
                        prior_jobs=state['jobs'], prior_uids=state['uids'])
            task.pop('recovery_execution', None)
            task.pop('recovery_summary', None)
        launch['recovery_session'] = control.session + 1
        root.launch, root.error = launch, None
        if root.status != 'finalizing':
            root.status, root.cloud_state = 'cloud_queued', None
    control.session += 1
    control.phase, control.execution, control.plan_sha256 = 'replanning', None, None
    control.accepted_at, control.remote_status = None, None
    control.failure_import_cursor = None
    control.completed_cycles, control.import_cursor, control.stop_acknowledged = 0, 0, False
    sequence = await session.get(CloudBatchSequence, job.id, with_for_update=True)
    sequence.stop_requested = False
    job.selection = {k: v for k, v in job.selection.items() if k != 'cancel_requested'}
    job.status, job.error = 'queued', None
