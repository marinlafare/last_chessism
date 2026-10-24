"""200k-FEN sizing + process-loss recovery test with useful, reserved FENs.

prepare publishes only; launch submits once; status/collect never resubmit.
One Spot VM, one task, two sequential containers, no automatic task retries.
"""
import argparse
import copy
import json
from pathlib import Path
import re
from uuid import uuid4

from benchmark_platforms.run import arguments, fence, now, require, save
from benchmark_vm16.run import assert_idle, quota_check
from benchmark_vm16 import large_database as database
from cleaning_job.cleanup import check_bucket
from cleaning_job.cloud import BUCKET, PROJECT, REGION, REPOSITORY
from cleaning_job.images import validate_uri
from cloud_job.launch import Client, ROOT, docker, expected_contract, inspect_worker
from stockfish_batch.checkpoints import BatchCheckpoints, digest, encode, parse_input
from stockfish_batch.config import Config
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import Storage, MAX_INPUT_BYTES, MAX_MANIFEST_BYTES, child

COUNT, CUT = 200000, 25000
SCRIPT = Path(__file__).with_name('large_phase.py')
PARENT = f'projects/{PROJECT}/locations/{REGION}'


def job_name(ident):
    require(isinstance(ident, str) and re.fullmatch(r'[a-f0-9]{12}', ident), 'Invalid test identity')
    return f'chessism-vm200-{ident}'


def configuration(ident, phase='resume'):
    job_name(ident)
    require(phase in ('interrupt', 'resume'), 'Invalid phase')
    return Config(input=f'gs://{BUCKET}/inputs/vm200-{ident}/input.jsonl',
        output=f'gs://{BUCKET}/results/vm200-{ident}', max_positions=COUNT,
        workers=16, threads=1, hash_mb=256, memory_mib=12288, nodes=100000,
        multipv=4, run_timeout=1700 if phase == 'interrupt' else 7000, stall_timeout=300,
        upload_mode='background', batch_size=500, compact_results=True).validate()


def specification(ident, image, script):
    job_name(ident)
    validate_uri(image)
    require(image.startswith(f'{REPOSITORY}vm200-{ident}@'), 'Foreign image package')
    spec = json.loads((ROOT / 'batch/smoke-test.json').read_bytes())
    task = spec['taskGroups'][0]['taskSpec']
    template = task['runnables'][0]['container']
    task.update(computeResource={'cpuMilli': '16000', 'memoryMib': '12288'},
                maxRunDuration='7200s', maxRetryCount=0)
    task['runnables'] = []
    for phase in ('interrupt', 'resume'):
        container = copy.deepcopy(template)
        container.update(imageUri=image, entrypoint='python',
            commands=['-c', script, phase, str(CUT), *arguments(configuration(ident, phase))])
        container['options'] += ' --memory=12g --memory-swap=12g --pids-limit=128'
        task['runnables'].append({'container': container, 'timeout': '1800s' if phase == 'interrupt' else '7100s'})
    spec['allocationPolicy']['instances'][0]['policy']['machineType'] = 'n2d-highcpu-16'
    labels = {'app': 'chessism', 'purpose': 'vm200-benchmark', 'benchmark_id': ident}
    spec['labels'] = labels
    spec['allocationPolicy']['labels'] = labels
    return spec


def prepare(directory, client, ident, local_image):
    job_name(ident)
    require(not directory.exists(), 'Output directory already exists; inspect it')
    assert_idle(client)
    check_bucket(client)
    quota = quota_check(client)
    require(not client.objects(f'inputs/vm200-{ident}/') and not client.objects(f'results/vm200-{ident}/'),
            'Cloud prefixes already exist')
    require(not any(i['uri'].startswith(f'{REPOSITORY}vm200-{ident}@') for i in client.images()),
            'Image package already exists')
    image_id, metadata = inspect_worker(local_image)
    script = SCRIPT.read_text()
    directory.mkdir(parents=True, exist_ok=False)
    fence(directory, 'prepare.started')
    save(directory, 'quota-before.json', quota)
    plan = {'id': ident, 'job': job_name(ident), 'created_at': now(), 'count': COUNT,
        'db_job_id': uuid4().hex, 'db_run_id': uuid4().hex,
        'local_image_id': image_id, 'worker_metadata': metadata,
        'phase_script': script, 'phase_script_sha256': digest(script.encode()),
        'source_note': '200000 unclaimed, unscored FENs chosen by the normal ANALYZE ALL selector; durable claims prevent duplicate local analysis.'}
    # Save identities BEFORE reserving rows; an ambiguous DB commit remains discoverable.
    save(directory, 'plan.json', plan)
    raw, rows, contract = database.execute(database.reserve, plan, configuration(ident))
    require(Storage().create(str(directory / 'input.jsonl'), raw), 'Input already saved')
    plan.update(input_sha256=digest(raw), contract=contract)
    save(directory, 'plan.json', plan)
    target = f'{REPOSITORY}vm200-{ident}:worker'
    print(json.dumps({'event': 'publishing', 'target': target}), flush=True)
    docker('tag', image_id, target)
    docker('push', target, timeout=900)
    matches = [i['uri'] for i in client.images() if i['uri'].startswith(f'{REPOSITORY}vm200-{ident}@')]
    require(len(matches) == 1, 'Expected exactly one image digest')
    plan['image'] = matches[0]
    plan['specification'] = specification(ident, plan['image'], script)
    plan['contract'] = expected_contract(raw, rows, configuration(ident), metadata)
    save(directory, 'plan.json', plan)
    save(directory, 'batch-spec.json', plan['specification'])
    require(client.storage().create(configuration(ident).input, raw), 'Cloud input exists; do not overwrite')
    print(json.dumps({'event': 'prepared', 'job': plan['job'], 'count': COUNT}), flush=True)


def load(directory, *, validate_input=True):
    plan = json.loads((directory / 'plan.json').read_bytes())
    require(plan['job'] == job_name(plan['id']) and plan['count'] == COUNT, 'Wrong plan')
    require(digest(plan['phase_script'].encode()) == plan['phase_script_sha256'], 'Script changed')
    require(plan['specification'] == specification(plan['id'], plan['image'], plan['phase_script']), 'Unsafe specification')
    if validate_input:
        raw = (directory / 'input.jsonl').read_bytes()
        require(digest(raw) == plan['input_sha256'], 'Input changed')
        rows = parse_input(raw, COUNT)
        require(len(rows) == COUNT and len({r['fen'] for r in rows}) == COUNT, 'Wrong sample')
        require(expected_contract(raw, rows, configuration(plan['id']), plan['worker_metadata']) == plan['contract'],
                'Contract changed')
    return plan


def check_job(job, spec):
    """The production checker deliberately accepts only one runnable; this test has two."""
    groups = job.get('taskGroups', [])
    require(len(groups) == 1, 'Unexpected task groups')
    group = groups[0]
    require(int(group['taskCount']) == int(group['parallelism']) == 1, 'Unexpected task parallelism')
    actual, expected = group['taskSpec'], spec['taskGroups'][0]['taskSpec']
    require(actual.get('maxRunDuration') == '7200s' and int(actual.get('maxRetryCount', 0)) == 0,
            'Task cost bounds changed')
    require(all(int(actual['computeResource'][k]) == int(v) for k, v in expected['computeResource'].items()),
            'Task resources changed')
    require(len(actual['runnables']) == 2, 'Expected exactly two sequential phases')
    for observed, planned in zip(actual['runnables'], expected['runnables']):
        require(observed.get('timeout') == planned['timeout'] and
                not any(observed.get(k) for k in ('background', 'ignoreExitStatus', 'alwaysRun')),
                'Runnable execution bounds changed')
        require(all(observed['container'].get(k) == value for k, value in planned['container'].items()),
                'Runnable input, code or image changed')
    allocation = job['allocationPolicy']
    require(len(allocation['instances']) == 1 and allocation['instances'][0]['policy']['machineType'] == 'n2d-highcpu-16'
            and allocation['instances'][0]['policy']['provisioningModel'] == 'SPOT', 'VM type changed')
    require(allocation['serviceAccount'] == spec['allocationPolicy']['serviceAccount'], 'Service account changed')
    require(all(job.get('labels', {}).get(k) == v for k, v in spec['labels'].items()), 'Ownership labels changed')


def launch(directory, client):
    plan = load(directory)
    require(not client.jobs() and not any(r['kind'] == 'instances' for r in client.resources()),
            'Other cloud work exists; inspect before launching')
    database.execute(database.check, plan)
    save(directory, 'quota-launch.json', quota_check(client))
    require(client.storage().read(configuration(plan['id']).input, MAX_INPUT_BYTES) ==
            (directory / 'input.jsonl').read_bytes(), 'Cloud input differs')
    fence(directory, 'launch.started')
    save(directory, 'submit-intent.json', {'time': now(), 'job': plan['job']})
    job = client.submit(plan['job'], plan['specification'])
    save(directory, 'submission.json', {'received_at': now(), 'response': job})
    database.execute(database.record_submission, plan, job)
    print(json.dumps({'event': 'submitted', 'job': plan['job'], 'uid': job['uid'],
                      'state': job['status']['state']}), flush=True)


def status(directory, client):
    plan = load(directory, validate_input=False)
    job = client.job(plan['job'])
    require(job is not None, 'Job missing; inspect cleanup receipts, never resubmit blindly')
    check_job(job, plan['specification'])
    submitted = json.loads((directory / 'submission.json').read_bytes())['response']
    require(job['uid'] == submitted['uid'], 'Job UID changed')
    objects = client.objects(f"results/vm200-{plan['id']}/")
    count = sum(bool(re.fullmatch(r'.*/batches/[0-9]{6}\.json', obj['name'])) for obj in objects)
    resumed = any(obj['name'].endswith('/recovery/interruption.json') for obj in objects)
    state = job['status']['state']
    result = {'checked_at': now(), 'state': state, 'uploaded_fens': min(COUNT, count * 500),
              'phase': 'resume' if resumed else 'before planned interruption', 'job': job}
    save(directory, 'last-status.json', result)
    print(f"{plan['job']}  {state}  {result['uploaded_fens']}/{COUNT} saved FENs  [{result['phase']}]")
    return result


def collect(directory, client):
    plan = load(directory)
    observed = status(directory, client)
    require(observed['state'] == 'SUCCEEDED', 'Only collect successful test; failures retain recovery evidence')
    store = client.storage()
    prefix = configuration(plan['id']).output
    def fetch(name, limit=MAX_MANIFEST_BYTES):
        raw = store.read(child(prefix, name), limit)
        require(raw is not None, 'Missing ' + name)
        target = directory / 'download' / name
        require(Storage().create(str(target), raw) or target.read_bytes() == raw, 'Local/cloud result differs')
        return json.loads(raw)
    rows = parse_input((directory / 'input.jsonl').read_bytes(), COUNT)
    contract = fetch('contract.json')
    require(contract == plan['contract'], 'Contract mismatch')
    validator = BatchCheckpoints(store, prefix, contract, rows, 500)
    manifest = fetch('manifest.json')
    require(manifest['status'] == 'complete' and manifest['position_count'] == COUNT
            and manifest['fingerprint'] == contract['fingerprint'] and len(manifest['records']) == COUNT,
            'Invalid final manifest')
    # One batch at a time: never materialize 200000 full analysis records locally.
    for index, group in enumerate(validator.groups):
        values = validator.validate_batch(fetch(f'batches/{index:06d}.json', validator.MAX_BYTES), index)
        for row, entry in zip(group, manifest['records'][index*500:(index+1)*500]):
            require(entry == validator.reference(row, values[row['id']]), 'Manifest checksum/order mismatch')
        if index % 50 == 0:
            print(json.dumps({'event': 'verified_batches', 'count': index + 1}), flush=True)
    interrupted, resumed = fetch('recovery/interruption.json'), fetch('recovery/resume.json')
    require(interrupted['child_exit_code'] == -9 and interrupted['signal'] == 9,
            'No verified controlled interruption')
    require(resumed['checkpoint_recovery_verified'] is True and
            interrupted['fingerprint'] == resumed['fingerprint'] == contract['fingerprint'], 'Recovery mismatch')
    current = {o['name'].rsplit('/', 1)[-1]: o for o in client.objects(f"results/vm200-{plan['id']}/batches/")}
    for name, old in interrupted['checkpoints'].items():
        obj = current[name]
        require(all(str(obj[k]) == str(old[k]) for k in ('generation', 'size', 'crc32c')), 'Old checkpoint changed')
    performance = validate_performance(fetch('performance.json'), contract, 16)
    require(performance['resumed'] == resumed['worker_started']['resumed'] >= CUT
            and performance['analyzed_this_attempt'] == COUNT - performance['resumed'], 'Wrong resume count')
    require(len(performance['workers']) == 16 and performance['metrics']['memory']['worker']['samples'] > 0,
            'Missing 16-engine telemetry')
    report = {'id': plan['id'], 'positions': COUNT, 'db_job_id': plan['db_job_id'], 'db_run_id': plan['db_run_id'],
              'recovery_scope': 'Forced analyzer process-group loss, followed by a fresh container; not VM/50001 recovery',
              'interrupted': interrupted, 'resumed': resumed, 'performance': performance,
              'all_result_checksums_and_legal_pvs_verified': True}
    save(directory, 'completion-status.json', observed)
    save(directory, 'report.json', report)
    print(json.dumps({'event': 'collected', 'report': str(directory / 'report.json'),
                      'resumed_fens': performance['resumed'], 'memory': performance['metrics']['memory']}, indent=2))
    database.execute(database.import_results, directory, plan)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'launch', 'status', 'collect'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--id')
    parser.add_argument('--local-image', default='chessism-stockfish-batch:pipeline-v2')
    parser.add_argument('--gcloud', default='gcloud')
    args = parser.parse_args()
    client = Client(args.gcloud)
    if args.action == 'prepare':
        prepare(args.directory.resolve(), client, args.id, args.local_image)
    else:
        {'launch': launch, 'status': status, 'collect': collect}[args.action](args.directory.resolve(), client)


if __name__ == '__main__':
    main()
