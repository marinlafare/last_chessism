"""Bounded sequence contracts shared by the host and its offline tests."""
from copy import deepcopy
import base64
import json
import re
from pathlib import Path

from cleaning_job.cloud import BUCKET
from stockfish_batch.checkpoints import digest, encode
from .recovery import WORKFLOW, execution_identity

MODE = 'batch_sequence_v2'
# Mirrors work_units.MAX_FENS_PER_VM without creating a launch import cycle.
LIMIT = 1_000_000
MAX_TASKS = 20
MAX_TOTAL_FENS = 10_000_000


def sealed(value):
    payload = encode(value)
    return encode({'payload': payload.decode()}), digest(payload)


def checksum64(sha256):
    return base64.b64encode(bytes.fromhex(sha256)).decode()


def cycle_sizes(total, n_vms=1):
    if type(total) is not int or not 1 <= total <= MAX_TOTAL_FENS:
        raise ValueError('Select 1–10,000,000 Batch FENs')
    if type(n_vms) is not int or not 1 <= n_vms <= 10:
        raise ValueError('Invalid VM count')
    capacity = LIMIT * n_vms
    full, rest = divmod(total, capacity)
    return [capacity] * full + ([rest] if rest else [])


def prefix(control_id, session):
    if not re.fullmatch(r'[a-f0-9]{32}', control_id) or type(session) is not int or not 1 <= session <= 3:
        raise ValueError('Invalid cloud sequence identity')
    # Within the first input's existing cleanup/IAM scope, separate from per-task recovery.
    return f'gs://{BUCKET}/inputs/ui-{control_id}/sequence-{session}/'


def cycle_document(run):
    tasks = []
    for task in run.launch['tasks']:
        item = {key: deepcopy(task.get(key, default)) for key, default in
                (('run_id', None), ('spec', None), ('prior_jobs', []), ('prior_uids', {}))}
        item.update(session=task['recovery_session'], spec_hash=digest(encode(task['spec'])),
                    skip=task.get('status') == 'SUCCEEDED',
                    checks=[{'output': u['config']['output'], 'count': u['count'],
                             'fingerprint': u['contract']['fingerprint']}
                            for u in run.launch['units'] if u['vm_index'] == task['index']])
        tasks.append(item)
    return {'run_id': run.id, 'count': run.position_count, 'tasks': tasks}


class SequenceClient:
    def require_sequence(self):
        self.require_recovery()
        workflow = self.request('workflows', WORKFLOW)
        if workflow.get('labels', {}).get('sequence_schema') != '2':
            raise ValueError('Deploy cloud sequence schema 2 before submitting automatic Batch sequences')
        # A rejected deployment may update labels while retaining older source.
        # Never treat a version label alone as proof of deployed functionality.
        expected = Path(__file__).with_name('recovery.yaml').read_text().strip()
        if workflow.get('sourceContents', '').strip() != expected:
            raise ValueError('Deployed recovery source differs from the controller; deploy and verify before launch')

    def sequence_document(self, control_id, session, name):
        raw = self.storage().read(prefix(control_id, session) + name, 512 * 1024)
        return None if raw is None else json.loads(raw)

    def sequence_executions(self, control_id, session):
        return [execution for page in self.pages('executions', WORKFLOW + '/executions',
            filter=f'labels."sequence_id":"{control_id}" AND labels."session":"{session}"', view='FULL')
            for execution in page.get('executions', [])]

    def start_sequence(self, control):
        prefix(control.control_run_id, control.session)
        args = {'mode': 'sequence_v2', 'run_id': control.control_run_id,
                'session': control.session, 'plan_hash': control.plan_sha256,
                'plan_checksum': checksum64(control.plan_sha256)}
        peers = self.sequence_executions(control.control_run_id, control.session)
        if any(json.loads(p.get('argument', 'null')) != args for p in peers):
            raise ValueError('Existing cloud sequence has different arguments')
        if peers:
            return execution_identity(peers[0]['name'])
        result = self.request('executions', WORKFLOW + '/executions', method='POST', body={
            'argument': json.dumps(args), 'callLogLevel': 'LOG_NONE',
            'labels': {'app': 'chessism', 'sequence_id': control.control_run_id, 'session': str(control.session)},
        })
        return execution_identity(result['name'])

    def sequence_snapshot(self, control):
        owner = self.sequence_document(control.control_run_id, control.session, 'owner.json')
        name = control.execution
        if owner:
            if owner.get('plan_hash') != control.plan_sha256:
                raise ValueError('Sequence owner belongs to a different plan')
            name = owner.get('execution')
        name = execution_identity(name)
        execution = self.request('executions', name)
        state = self.sequence_document(control.control_run_id, control.session, 'state.json')
        if state:
            if (state.get('version') != 2 or state.get('run_id') != control.control_run_id
                    or state.get('session') != control.session or state.get('plan_hash') != control.plan_sha256
                    or state.get('execution') != name or state.get('status') not in
                    {'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'STOPPED'}
                    or type(state.get('completed_cycles')) is not int or not 0 <= state['completed_cycles'] <= MAX_TASKS):
                raise ValueError('Invalid sequence state; retain all recovery data')
        if execution['state'] in {'FAILED', 'CANCELLED', 'UNAVAILABLE'}:
            raise ValueError('Cloud sequence supervisor stopped unexpectedly; inspect existing Batch tasks before retrying')
        if execution['state'] == 'SUCCEEDED' and (not state or state['status'] not in {'SUCCEEDED', 'FAILED', 'CANCELLED', 'STOPPED'}):
            raise ValueError('Sequence finished without terminal proof')
        return state, execution

    def signal_sequence(self, control, *, cancel=False):
        name = 'cancel.json' if cancel else 'stop.json'
        self.storage().create(prefix(control.control_run_id, control.session) + name,
                              encode({'run_id': control.control_run_id, 'session': control.session}))

    def require_sequence_stopped(self, control):
        # Safe after a partial cleanup has already removed control files.
        if control.remote_status != 'SUCCEEDED' or not control.accepted_at:
            raise ValueError('Sequence has not succeeded; refusing data cleanup')
        if self.request('executions', execution_identity(control.execution))['state'] != 'SUCCEEDED':
            raise ValueError('Sequence execution is still active')
        for session in range(1, control.session + 1):
            for peer in self.sequence_executions(control.control_run_id, session):
                if peer.get('state') not in {'SUCCEEDED', 'FAILED', 'CANCELLED'}:
                    raise ValueError('Duplicate sequence could still launch work; refusing cleanup')
