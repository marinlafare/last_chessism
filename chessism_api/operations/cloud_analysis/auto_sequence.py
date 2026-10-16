"""Cloud-owned cycles, host-owned import; no launch depends on local finalization."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone

from sqlalchemy import select
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisRun, CloudBatchSequence, CloudSequenceControl
from cloud_job import batch_spot, work_units
from cloud_job.launch import inspect_worker
from cloud_job.recovery import validate_state
from cloud_job.sequencing import MODE, prefix, sealed, checksum64, cycle_document
from stockfish_batch.checkpoints import encode, digest
from stockfish_batch.storage import MAX_INPUT_BYTES
from stockfish_core import PROFILE_ID
from cleaning_job.cleanup import check_bucket
from . import sequence, work_units_results as results
from .work_units_controller import lanes
from .preflight import verify_clean_workspace, parallel_checks


async def save_control(job_id, **changes):
    async with AsyncDBSession() as session, session.begin():
        control = await session.get(CloudSequenceControl, job_id, with_for_update=True)
        for key, value in changes.items():
            setattr(control, key, value)


def create_exact(storage, uri, raw, limit=MAX_INPUT_BYTES):
    if not storage.create(uri, raw) and storage.read(uri, limit) != raw:
        raise ValueError('Existing sequence input differs; refusing overwrite or duplicate submission')


async def prepare(controller, job, control, roots):
    from .controller import save_run
    cloud = controller.cloud
    if control.phase == 'preparing':
        await asyncio.to_thread(verify_clean_workspace, cloud)
        quota, _, _ = await asyncio.to_thread(parallel_checks,
            lambda: batch_spot.check_quota(cloud, job.selection['n_vms']),
            cloud.require_sequence, lambda: check_bucket(cloud))
        image_id, _ = await asyncio.to_thread(inspect_worker, controller.local_image)
        for root in roots:
            await save_run(root.id, status='publishing', launch={**root.launch,
                'image_id': image_id, 'profile': PROFILE_ID, 'quota': quota})
        await save_control(job.id, image_id=image_id, phase='publishing')
    elif control.phase == 'publishing':
        image = await asyncio.to_thread(cloud.publish, control.control_run_id, control.image_id)
        for root in roots:
            launch = deepcopy(root.launch)
            launch.update(image=image, image_id=control.image_id, tasks=lanes(launch))
            for task in launch['tasks']:
                task['spec'] = work_units.spec_for([u for u in launch['units'] if u['vm_index'] == task['index']], image)
            await save_run(root.id, launch=launch, status='uploading')
        await save_control(job.id, image=image, phase='uploading')
    elif control.phase == 'uploading':
        storage = await asyncio.to_thread(cloud.storage)
        for root in roots:
            launch = deepcopy(root.launch)
            for index, unit in enumerate(launch['units']):
                if unit.get('uploaded'):
                    continue
                async with AsyncDBSession() as session:
                    child = await session.get(CloudAnalysisRun, unit['run_id'])
                raw = b''.join(map(encode, child.positions))
                del child
                await asyncio.to_thread(create_exact, storage, unit['config']['input'], raw)
                launch['units'][index]['uploaded'] = True
                await save_run(root.id, launch=launch)
                return  # Bounded memory and an interruption-safe upload checkpoint.
        await upload_plan(cloud, job, control, roots)
    elif control.phase == 'submitting':
        if not control.execution:
            # The image/all inputs/immutable plan must exist before this call.
            execution = await asyncio.to_thread(cloud.start_sequence, control)
            await save_control(job.id, execution=execution)
        await save_control(job.id, phase='running')
        for root in roots:
            if root.status != 'finalizing':
                await save_run(root.id, status='cloud_queued', error=None)
    elif control.phase == 'replanning':
        await upload_plan(cloud, job, control, roots)
    controller.continue_immediately = True


async def upload_plan(cloud, job, control, roots):
    storage = await asyncio.to_thread(cloud.storage)
    base = prefix(control.control_run_id, control.session)
    descriptors = []
    for index, root in enumerate(roots):
        cycle = cycle_document(root)
        # Keep fleet metadata out of the parent's Workflows variable space.
        # Each parallel branch loads only its own <=20 bounded work units.
        tasks = []
        for task_index, task in enumerate(cycle['tasks']):
            task_raw, task_sha = sealed(task)
            await asyncio.to_thread(create_exact, storage,
                base + f'task-{index}-{task_index}.json', task_raw)
            tasks.append({'run_id': task['run_id'], 'checksum': checksum64(task_sha)})
        cycle['tasks'] = tasks
        raw, sha = sealed(cycle)
        await asyncio.to_thread(create_exact, storage, base + f'cycle-{index}.json', raw)
        descriptors.append({'run_id': root.id, 'count': root.position_count, 'checksum': checksum64(sha)})
    raw, sha = sealed({'version': 2, 'control_id': control.control_run_id, 'session': control.session,
                       'n_vms': job.selection['n_vms'], 'cycles': descriptors})
    await asyncio.to_thread(create_exact, storage, base + 'plan.json', raw)
    await save_control(job.id, phase='submitting', plan_sha256=sha)


async def refresh_recovery(cloud, saved_launch, control, *, require_succeeded=False):
    """Batch state is progress, not proof that its recovery owner has finished."""
    launch = deepcopy(saved_launch)
    for task in launch['tasks']:
        state = await asyncio.to_thread(cloud.recovery_document, task['run_id'], task['recovery_session'], 'state.json')
        if state is None:
            if require_succeeded:
                raise ValueError('Missing recovery completion state; refusing cleanup')
            task['status'] = 'PENDING'
            continue
        validate_state(state, task['run_id'], task['recovery_session'], digest(encode(task['spec'])))
        allowed = {control.execution, task.get('recovery_execution')}
        if state['execution'] not in allowed:
            raise ValueError('Task is controlled by an unexpected sequence execution')
        if require_succeeded and state['status'] != 'SUCCEEDED':
            raise ValueError('Recovery has not completed; refusing cleanup')
        task.update(recovery_summary=state, recovery_execution=state['execution'], jobs=state['jobs'])
        task['status'] = state['status']
        if state['current_job']:
            remote = await asyncio.to_thread(cloud.job, state['current_job'])
            if not remote or remote.get('uid') != state['uids'][state['current_job']]:
                raise ValueError('Sequence Batch identity changed; retain results')
            work_units.verify_job(remote, task['spec'])
            task['batch_status'] = remote['status']
            # A terminal Batch job may still need recovery validation or retry.
            # Never freeze RUNNING recovery metadata just because Batch finished.
            if (state['status'] not in {'SUCCEEDED', 'FAILED', 'CANCELLED', 'RETRYING'}
                    and remote['status']['state'] in {'QUEUED', 'SCHEDULED', 'RUNNING'}):
                task['status'] = remote['status']['state']
    launch['jobs'] = [name for task in launch['tasks'] for name in task['jobs']]
    launch['vm_statuses'] = [{'index': t['index'], 'positions': t['count'], 'state': t['status'],
        'preemptions': t.get('recovery_summary', {}).get('preemptions', 0),
        'application_failures': t.get('recovery_summary', {}).get('application_failures', 0)} for t in launch['tasks']]
    return launch


async def poll_root(cloud, root, control, *, ingest):
    """Child recovery may finish while the encompassing workflow stays active."""
    from .controller import save_run
    if root.status in {'complete', 'finalizing'}:
        return
    launch = await refresh_recovery(cloud, root.launch, control)
    if ingest:
        for index, unit in enumerate(launch['units']):
            if not unit.get('verified'):
                launch['units'][index] = await results.import_unit(cloud, unit)
    states = [t['status'] for t in launch['tasks']]
    cloud_state = ('SUCCEEDED' if all(s == 'SUCCEEDED' for s in states)
                   else 'FAILED' if all(s in {'SUCCEEDED', 'FAILED', 'CANCELLED'} for s in states)
                   else 'PENDING' if all(s == 'PENDING' for s in states) else 'RUNNING')
    if cloud_state == 'SUCCEEDED' and all(u.get('verified') for u in launch['units']):
        async with AsyncDBSession() as session:
            current = await session.get(CloudAnalysisRun, root.id)
        await results.verify_saved(current, launch)
        await save_run(root.id, launch=results.performance(launch), cloud_state=cloud_state, status='finalizing', error=None)
    else:
        await save_run(root.id, launch=launch, cloud_state=cloud_state,
                       status='cloud_queued' if cloud_state == 'PENDING' else 'running', error=None)


async def tick(controller, job):
    from .controller import save_job
    async with AsyncDBSession() as session:
        saved = await session.get(CloudBatchSequence, job.id)
        control = await session.get(CloudSequenceControl, job.id)
    if saved is None:
        raise ValueError('Missing automatic sequence state')
    if (saved.stop_requested or job.selection.get('cancel_requested')) and (control is None or control.phase == 'preparing'):
        await sequence.stop_unstarted(job.id)
        return
    await sequence.initialize(job, saved)
    rows = await sequence.cycles(job.id)
    if not saved.frozen_at:
        if saved.stop_requested or job.selection.get('cancel_requested'):
            await sequence.stop_unstarted(job.id)
        else:
            await sequence.select_next(controller, job, rows)
        return
    roots = [run for _, run in rows if run.position_count]
    if not roots:
        await save_job(job.id, status='limit_reached', error='No available unscored FENs in the frozen selection.')
        return
    if control is None:
        if saved.stop_requested or job.selection.get('cancel_requested'):
            await sequence.stop_unstarted(job.id)
            return
        async with AsyncDBSession() as session, session.begin():
            session.add(CloudSequenceControl(job_id=job.id, control_run_id=roots[0].launch['units'][0]['run_id']))
        controller.continue_immediately = True
        return
    # Once publishing starts, never pretend an untouched local loop owns nothing.
    # Signals are included before submission and honored by Google before launch.
    if saved.stop_requested or job.selection.get('cancel_requested'):
        await asyncio.to_thread(controller.cloud.signal_sequence, control,
                                cancel=bool(job.selection.get('cancel_requested')))
    if control.phase in {'preparing', 'publishing', 'uploading', 'submitting', 'replanning'}:
        await prepare(controller, job, control, roots)
        controller.continue_immediately = True
        return
    if control.phase in {'cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing', 'complete'}:
        from .auto_sequence_cleanup import finish
        await finish(controller, job, control, roots, saved)
        return
    if control.phase != 'running':
        raise ValueError('Unexpected sequence phase: ' + control.phase)
    state, execution = await asyncio.to_thread(controller.cloud.sequence_snapshot, control)
    if state is None:
        return  # Google has not acknowledged its durable owner/plan yet.
    control.execution = state['execution']
    failed = execution['state'] == 'SUCCEEDED' and state['status'] in {'FAILED', 'CANCELLED', 'STOPPED'}
    if failed and control.failure_import_cursor is None:
        control.failure_import_cursor = control.import_cursor
    await save_control(job.id, execution=control.execution, remote_status=state['status'],
        accepted_at=control.accepted_at or datetime.now(timezone.utc),
        failure_import_cursor=control.failure_import_cursor,
        completed_cycles=state['completed_cycles'], stop_acknowledged=state.get('stop_acknowledged', False))
    candidates = [root for root in roots if root.status != 'finalizing']
    selected = candidates[control.import_cursor % len(candidates)].id if candidates else None
    for root in roots:
        await poll_root(controller.cloud, root, control, ingest=root.id == selected)
    await save_control(job.id, import_cursor=control.import_cursor + 1)
    updated = [r for _, r in await sequence.cycles(job.id) if r.position_count]
    if execution['state'] == 'SUCCEEDED' and state['status'] == 'SUCCEEDED':
        if all(root.status == 'finalizing' for root in updated):
            await save_control(job.id, phase='cleanup_planning')
            controller.continue_immediately = True
        elif any(a.imported_count != b.imported_count or a.status != b.status for a, b in zip(roots, updated)):
            controller.continue_immediately = True
    elif failed:
        # One catch-up pass across all cycles before exposing a recoverable failure.
        if control.import_cursor - control.failure_import_cursor + 1 >= len(roots):
            await save_job(job.id, status='failed', error=f"Cloud sequence {state['status'].lower()}; "
                'future tasks stopped. Results/checkpoints retained. Explicit retry is required to resume.')
