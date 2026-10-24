"""Prepare, launch once, inspect once, or collect a bounded single-VM benchmark."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import subprocess

from benchmark_platforms.run import arguments, fence, now, require, save
from cleaning_job.cloud import BUCKET, PROJECT, REGION, REPOSITORY
from cleaning_job.images import validate_uri
from cloud_job.launch import Client, ROOT, docker, expected_contract, inspect_worker
from stockfish_batch.checkpoints import BatchCheckpoints, digest, parse_input, semantic_digest
from stockfish_batch.config import Config
from stockfish_batch.performance import validate_performance
from stockfish_batch.storage import MAX_INPUT_BYTES, MAX_MANIFEST_BYTES, Storage, child

COUNT = 25000
BASELINE_SHA = '60831804e94af3f247484671285f72c1aa3bdd9661910e50acf212be596e5b5c'
BASELINE = ROOT / 'out/platform-20261006ab01'
MACHINE = 'n2d-highcpu-16'
PARENT = f'projects/{PROJECT}/locations/{REGION}'


def job_name(ident):
    require(isinstance(ident, str) and re.fullmatch(r'[a-f0-9]{12}', ident), 'Invalid benchmark identity')
    return f'chessism-vm16-{ident}'


def configuration(ident):
    job_name(ident)
    return Config(input=f'gs://{BUCKET}/inputs/vm16-{ident}/input.jsonl',
        output=f'gs://{BUCKET}/results/vm16-{ident}', max_positions=COUNT,
        workers=16, threads=1, hash_mb=256, memory_mib=12288, nodes=100000,
        multipv=4, run_timeout=1740, stall_timeout=300,
        upload_mode='background', batch_size=500, compact_results=True).validate()


def specification(ident, image):
    validate_uri(image)
    require(image.startswith(f'{REPOSITORY}vm16-{ident}@'), 'Image belongs to another benchmark')
    config = configuration(ident)
    spec = json.loads((ROOT / 'batch/smoke-test.json').read_text())
    task = spec['taskGroups'][0]['taskSpec']
    task.update(computeResource={'cpuMilli': '16000', 'memoryMib': '12288'},
                maxRunDuration='1800s', maxRetryCount=0)
    container = task['runnables'][0]['container']
    container.update(imageUri=image, commands=arguments(config))
    container['options'] += ' --memory=12g --memory-swap=12g'
    spec['allocationPolicy']['instances'][0]['policy']['machineType'] = MACHINE
    labels = {'app': 'chessism', 'purpose': 'vm16-benchmark', 'benchmark_id': ident}
    spec['labels'] = labels
    spec['allocationPolicy']['labels'] = labels
    return spec


def assert_idle(client):
    require(not client.jobs(), 'Existing Batch jobs: inspect before launching')
    require(not any(r['kind'] == 'instances' for r in client.resources()), 'Other VMs exist')
    # Read-only, bounded database check. Never enqueue, reserve or import FENs.
    result = subprocess.run(['docker', 'compose', 'exec', '-T', '-e',
        'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=30000', 'db',
        'sh', '-c', 'exec psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1'],
        input="SELECT count(*) FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','failed');",
        cwd=ROOT.parent, text=True, capture_output=True, check=True, timeout=40)
    require(result.stdout.strip() == '0', 'Production cloud analysis is active')


def quota_check(client):
    regional = client.request('compute', f'projects/{PROJECT}/regions/{REGION}')
    global_info = client.request('compute', f'projects/{PROJECT}')
    quotas = {q['metric']: q for q in regional['quotas']}
    quotas.update({q['metric']: q for q in global_info['quotas']})
    # This project has no dedicated Spot quota. If it obtains one later, use it.
    cpu_metric = 'PREEMPTIBLE_CPUS' if quotas.get('PREEMPTIBLE_CPUS', {}).get('limit', 0) else 'N2D_CPUS'
    for metric, count in ((cpu_metric, 16), ('CPUS_ALL_REGIONS', 16), ('INSTANCES', 1),
                          ('IN_USE_ADDRESSES', 1), ('SSD_TOTAL_GB', 30)):
        q = quotas.get(metric)
        require(q is not None and q['limit'] - q['usage'] >= count, 'Insufficient quota: ' + metric)
    return {'checked_at': now(), 'quotas': quotas}


def prepare(directory, client, ident, local_image):
    job_name(ident)
    raw = (BASELINE / 'input.jsonl').read_bytes()
    require(digest(raw) == BASELINE_SHA, 'Saved baseline input changed')
    rows = parse_input(raw, COUNT)
    require(len(rows) == COUNT and len({r['fen'] for r in rows}) == COUNT, 'Invalid sample')
    baseline_plan = json.loads((BASELINE / 'plan.json').read_bytes())
    baseline_report = json.loads((BASELINE / 'report.json').read_bytes())
    image_id, metadata = inspect_worker(local_image)
    require(metadata == baseline_plan['worker_metadata'], 'Engine/library identity differs from baseline')
    # The new image must contain the extended telemetry, not just the same engine.
    telemetry = json.loads(docker('run', '--rm', '--network=none', '--read-only', '--cpus=1',
        '--memory=512m', '--cap-drop=ALL', '--security-opt=no-new-privileges',
        '--entrypoint=python', image_id, '-c',
        'import json; from stockfish_batch.metrics import Sampler; print(json.dumps(Sampler().finish()))'))
    require('memory' in telemetry, 'Build the memory-instrumented benchmark image first')
    assert_idle(client)
    quota = quota_check(client)
    require(not client.objects(f'inputs/vm16-{ident}/')
            and not client.objects(f'results/vm16-{ident}/'), 'Benchmark storage prefix already exists')
    require(not any(i['uri'].startswith(f'{REPOSITORY}vm16-{ident}@') for i in client.images()),
            'Benchmark image package already exists')
    directory.mkdir(parents=True, exist_ok=False)
    fence(directory, 'prepare.started')
    Storage().create(str(directory / 'input.jsonl'), raw)
    save(directory, 'baseline-report.json', baseline_report)
    save(directory, 'quota-before.json', quota)
    plan = {'id': ident, 'job': job_name(ident), 'created_at': now(), 'count': COUNT,
        'input_sha256': BASELINE_SHA, 'local_image_id': image_id, 'worker_metadata': metadata,
        'baseline_directory': str(BASELINE), 'baseline_report_sha256': digest((directory / 'baseline-report.json').read_bytes())}
    save(directory, 'plan.json', plan)
    target = f'{REPOSITORY}vm16-{ident}:worker'
    print(json.dumps({'event': 'publishing', 'target': target}), flush=True)
    docker('tag', image_id, target)
    docker('push', target, timeout=900)
    matches = [i['uri'] for i in client.images() if i['uri'].startswith(f'{REPOSITORY}vm16-{ident}@')]
    require(len(matches) == 1, 'Expected one immutable benchmark image')
    plan['image'] = matches[0]
    plan['specification'] = specification(ident, plan['image'])
    plan['contract'] = expected_contract(raw, rows, configuration(ident), metadata)
    save(directory, 'plan.json', plan)
    save(directory, 'batch-spec.json', plan['specification'])
    store = client.storage()
    require(store.create(configuration(ident).input, raw), 'Cloud input already exists; inspect instead of overwriting')
    print(json.dumps({'event': 'prepared', 'job': plan['job'], 'directory': str(directory)}), flush=True)


def load(directory):
    plan = json.loads((directory / 'plan.json').read_bytes())
    require(plan['job'] == job_name(plan['id']), 'Unexpected job name')
    require(plan['specification'] == specification(plan['id'], plan['image']), 'Unsafe or changed specification')
    raw = (directory / 'input.jsonl').read_bytes()
    require(digest(raw) == plan['input_sha256'] == BASELINE_SHA, 'Input changed')
    rows = parse_input(raw, COUNT)
    require(len(rows) == COUNT, 'Wrong position count')
    require(plan['contract'] == expected_contract(raw, rows, configuration(plan['id']),
            plan['worker_metadata']), 'Contract changed')
    require(digest((directory / 'baseline-report.json').read_bytes()) == plan['baseline_report_sha256'],
            'Baseline report changed')
    return plan


def launch(directory, client):
    plan = load(directory)
    assert_idle(client)
    save(directory, 'quota-launch.json', quota_check(client))
    require(client.storage().read(configuration(plan['id']).input, MAX_INPUT_BYTES)
            == (directory / 'input.jsonl').read_bytes(), 'Cloud input changed')
    fence(directory, 'launch.started')
    save(directory, 'submit-intent.json', {'time': now(), 'job': plan['job']})
    result = client.submit(plan['job'], plan['specification'])
    save(directory, 'submission.json', {'received_at': now(), 'response': result})
    print(json.dumps({'event': 'submitted', 'job': plan['job'], 'uid': result.get('uid'),
                      'state': result.get('status', {}).get('state')}), flush=True)


def status(directory, client):
    plan = load(directory)
    job = client.job(plan['job'])
    if job:
        client.check_job(job, plan['specification'])
    cleanup = directory / 'cleanup-result.json'
    state = job.get('status', {}).get('state') if job else ('CLEANED' if cleanup.exists()
        and json.loads(cleanup.read_bytes()).get('complete') else 'NOT_FOUND')
    uploaded = None
    if job:
        prefix = f"results/vm16-{plan['id']}/batches/"
        objects = {v['name'] for v in client.objects(prefix)
                   if re.fullmatch(re.escape(prefix) + r'[0-9]{6}\.json', v['name'])}
        uploaded = min(len(objects) * 500, COUNT)
    observed = {'checked_at': now(), 'state': state, 'uploaded_fens': uploaded, 'total': COUNT, 'job': job}
    save(directory, 'last-status.json', observed)
    print(f"{plan['job']}  {state}  uploaded {uploaded if uploaded is not None else '—'}/{COUNT} FENs")
    return observed


def collect(directory, client):
    plan = load(directory)
    observed = status(directory, client)
    require(observed['state'] == 'SUCCEEDED', 'Wait for success; failed/interrupted runs are not comparable')
    prefix = configuration(plan['id']).output
    store = client.storage()
    rows = parse_input((directory / 'input.jsonl').read_bytes(), COUNT)
    def fetch(name, limit=MAX_MANIFEST_BYTES):
        blob = store.read(child(prefix, name), limit)
        require(blob is not None, 'Missing ' + name)
        local = directory / 'download' / name
        if not Storage().create(str(local), blob):
            require(local.read_bytes() == blob, 'Local result differs from cloud')
        return json.loads(blob)
    contract = fetch('contract.json')
    require(contract == plan['contract'], 'Unexpected contract')
    validator = BatchCheckpoints(store, prefix, contract, rows, 500)
    manifest = fetch('manifest.json')
    require(manifest['status'] == 'complete' and manifest['position_count'] == COUNT
            and manifest['fingerprint'] == contract['fingerprint'], 'Invalid manifest')
    values = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        batches = pool.map(lambda i: fetch(f'batches/{i:06d}.json', BatchCheckpoints.MAX_BYTES), range(50))
        for index, value in enumerate(batches):
            values.update(validator.validate_batch(value, index))
    require(len(values) == len(manifest['records']) == COUNT, 'Incomplete results')
    for row, entry in zip(rows, manifest['records']):
        require(entry == validator.reference(row, values[row['id']]), 'Manifest checksum/order differs')
    performance = validate_performance(fetch('performance.json'), contract, 16)
    require(performance['resumed'] == 0 and performance['analyzed_this_attempt'] == COUNT,
            'Retried/partial attempt is not comparable')
    require(len(performance['workers']) == 16, 'Not all 16 engines started')
    memory = performance['metrics']['memory']
    require(memory['worker']['samples'] > 0 and memory['host_available']['samples'] > 0,
            'Missing memory telemetry')
    baseline = json.loads((directory / 'baseline-report.json').read_bytes())['platforms']['batch']
    old_values = {}
    for index in range(50):
        payload = json.loads((BASELINE / f'download/batch/batches/{index:06d}.json').read_bytes())
        old_values.update({r['id']: r for r in payload['records']})
    require(semantic_digest(rows, old_values) == baseline['semantic_sha256'], 'Baseline results changed')
    changed = [row['id'] for row in rows if semantic_digest([row], values) != semantic_digest([row], old_values)]
    save(directory, 'changed-results.json', {'count': len(changed), 'ids': changed,
        'note': 'Hash is different; score/PV differences are reported, not silently treated as equivalent quality.'})
    report = {'id': plan['id'], 'count': COUNT, 'database_writes': 0, 'performance': performance,
        'baseline_performance': baseline['performance'], 'changed_chess_results': len(changed),
        'speedup': baseline['performance']['metrics']['worker_seconds'] / performance['metrics']['worker_seconds'],
        'note': 'CPU count AND hash changed. This does not isolate CPU scaling. Price actual VM lifetime separately.'}
    save(directory, 'completion-status.json', observed)
    save(directory, 'report.json', report)
    print(json.dumps({'report': str(directory / 'report.json'), 'speedup': report['speedup'],
                      'changed_chess_results': len(changed), 'memory': memory}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'launch', 'status', 'collect'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--id')
    parser.add_argument('--local-image', default='chessism-stockfish-batch:vm16-memory-v1')
    parser.add_argument('--gcloud', default='gcloud')
    args = parser.parse_args()
    directory = args.directory.resolve()
    client = Client(args.gcloud)
    if args.action == 'prepare':
        prepare(directory, client, args.id, args.local_image)
    else:
        {'launch': launch, 'status': status, 'collect': collect}[args.action](directory, client)


if __name__ == '__main__':
    main()
