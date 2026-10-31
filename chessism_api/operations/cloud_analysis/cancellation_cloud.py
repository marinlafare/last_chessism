"""Stop known executions without ever starting/restarting a supervisor or VM."""
from copy import deepcopy
import re
from cloud_job import cloud_run as run_api
from cloud_job.recovery import execution_identity
from cleaning_job.cloud import PROJECT, REGION, CleanupError
from cleaning_job.cleanup import identity, TERMINAL

WORKFLOW_TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELLED'}
BATCH_CANCELLING = {'CANCELLATION_IN_PROGRESS', 'CANCELLING'}


def cancel_batch(cloud, remote, spec):
    """A workflow may cancel the same job between our GET and POST."""
    ident = identity(remote)
    name = ident['id']
    try:
        cloud.operation(cloud.request('batch', f'projects/{PROJECT}/locations/{REGION}/jobs/{name}:cancel',
                                      method='POST', body={}))
    except CleanupError:
        current = cloud.job(name)
        if not current or current.get('uid') != ident['uid']:
            raise
        cloud.check_job(current, spec)
        if current.get('status', {}).get('state') not in TERMINAL | BATCH_CANCELLING:
            raise


def stop_batch(cloud, roots, control, *, signal, check_supervisors=True):
    quiet, launches, tasks = True, {}, {}
    if control:
        if signal:
            cloud.signal_sequence(control, cancel=True)
    for root in roots:
        launch = deepcopy(root.launch)
        if launch.get('backend') == 'cloud_run' or root.status == 'complete':
            continue
        launches[root.id] = launch
        for task in launch.get('tasks', [launch]):
            ident = task.get('run_id', root.id)
            tasks[ident] = (root.id, task)
            sessions = task.get('recovery_session', 0)
            if sessions:
                if signal:
                    cloud.cancel_recovery(ident, sessions)
    current = []
    for remote in cloud.jobs():
        name = remote.get('name', '').rsplit('/', 1)[-1]
        match = re.fullmatch(r'chessism-ui-([a-f0-9]{32})-[1-9][0-9]*', name)
        if not match or match[1] not in tasks:
            continue
        root_id, task = tasks[match[1]]
        spec = task.get('spec')
        if not spec:
            raise ValueError('Unexpected Batch job before a saved specification; cleanup withheld')
        cloud.check_job(remote, spec)
        ident = identity(remote)
        if ident['region'] != REGION:
            raise ValueError('Unexpected region for a saved Batch job')
        known = {**task.get('prior_uids', {}), **task.get('recovery_summary', {}).get('uids', {})}
        if name in known and known[name] != ident['uid']:
            raise ValueError('Batch job UID changed; stop/cleanup withheld')
        known[name] = ident['uid']
        task['prior_uids'] = known
        task['jobs'] = sorted(set(task.get('jobs', [])) | {name})
        task['batch_status'] = remote['status']
        state = remote['status']['state']
        if task is not launches[root_id]:
            task['status'] = state
        if state not in TERMINAL | {'DELETION_IN_PROGRESS'}:
            quiet = False
            if signal and state not in BATCH_CANCELLING:
                cancel_batch(cloud, remote, spec)
        current.append(name)
    # Send cancellation BEFORE waiting on supervisor/duplicate inventories.
    # The priority dispatcher only sends signals; it cannot prove quiescence.
    if not check_supervisors:
        quiet = False
    else:
        if control:
            executions = []
            for session in range(1, control.session + 1):
                executions.extend(cloud.sequence_executions(control.control_run_id, session))
            if control.execution:
                executions.append(cloud.request('executions', execution_identity(control.execution)))
            quiet = all(e.get('state') in WORKFLOW_TERMINAL for e in executions) and quiet
            if control.phase == 'running' and not executions:
                raise ValueError('Sequence submission is unresolved; retain all data until its execution is found')
        for ident, (_, task) in tasks.items():
            for session in range(1, task.get('recovery_session', 0) + 1):
                peers = cloud.recovery_executions(ident, session)
                quiet = all(e.get('state') in WORKFLOW_TERMINAL for e in peers) and quiet
            if task.get('recovery_execution'):
                owner = cloud.request('executions', execution_identity(task['recovery_execution']))
                quiet = owner.get('state') in WORKFLOW_TERMINAL and quiet
    for root_id, launch in launches.items():
        lanes = launch.get('tasks', [launch])
        launch['jobs'] = sorted({name for task in lanes for name in task.get('jobs', [])})
        if launch.get('tasks'):
            launch['vm_statuses'] = [{'index': task['index'], 'positions': task['count'],
                'state': task.get('status', 'PENDING'),
                'preemptions': task.get('recovery_summary', {}).get('preemptions', 0),
                'application_failures': task.get('recovery_summary', {}).get('application_failures', 0)} for task in lanes]
    return quiet, launches, current


def stop_run(cloud, root, *, signal):
    from .cloud_run_controller import verify_execution
    launch = deepcopy(root.launch)
    name = launch.get('run_job')
    if not name:
        return True, launch
    remote = cloud.request('run', name, missing_ok=True)
    if remote is None:
        if launch.get('run_uid') and not root.cleanup_plan:
            raise ValueError('Cloud Run job disappeared without a cleanup plan')
        return True, launch
    if not run_api.matches(remote, launch['spec']) or (launch.get('run_uid') and launch['run_uid'] != remote.get('uid')):
        raise ValueError('Cloud Run job changed; cleanup withheld')
    launch['run_uid'] = remote['uid']
    known = {e['name']: e['uid'] for e in launch.get('run_executions', [])}
    quiet = True
    for execution in run_api.executions(cloud, name):
        verify_execution(execution, launch)
        if execution['name'] in known and known[execution['name']] != execution['uid']:
            raise ValueError('Cloud Run execution UID changed')
        known[execution['name']] = execution['uid']
        if run_api.terminal(execution) == 'RUNNING':
            quiet = False
            if signal:
                cloud.operation(cloud.request('run', execution['name'] + ':cancel', method='POST', body={'etag': execution['etag']}))
    if root.status == 'run_discover' and not known:
        raise ValueError('Cloud Run submission is unresolved; wait for its execution before cleanup')
    launch['run_executions'] = [{'name': name, 'uid': uid} for name, uid in known.items()]
    return quiet, launch
