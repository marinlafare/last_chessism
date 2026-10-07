"""Save sizing/cost evidence, then optionally clean only this verified benchmark.

Use inspect before cleanup. Cleanup preserves the complete local downloads,
shared infrastructure, unrelated logs, and Google-required audit/billing records.
"""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote

from benchmark_platforms.log_cleanup import inspect as inspect_log, idle
from benchmark_platforms.run import require, save
from cleaning_job.cleanup import cleaning_job, validate_plan, verify
from cleaning_job.shared import TEST_LOGS
from cloud_job.launch import Client
from .run import BASELINE, COUNT, PARENT, PROJECT, REGION, load


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def owners(uid, instances):
    require(re.fullmatch(r'[a-z0-9-]+', uid), 'Unsafe Batch UID')
    clauses = ['(resource.type="batch.googleapis.com/Job" AND resource.labels.resource_container='
               + json.dumps(PROJECT) + ' AND resource.labels.job_id=' + json.dumps(uid) + ')']
    require(instances, 'No verified VM identity')
    for instance in instances:
        require(re.fullmatch(r'us-central1-[a-z]/[0-9]+', instance), 'Unsafe VM identity')
        zone, ident = instance.split('/')
        clauses.append('(resource.type="gce_instance" AND resource.labels.project_id=' + json.dumps(PROJECT)
                       + ' AND resource.labels.zone=' + json.dumps(zone)
                       + ' AND resource.labels.instance_id=' + json.dumps(ident) + ')')
    return '(' + ' OR '.join(clauses) + ')'


def compare_results(directory):
    keys = ('score', 'pv', 'wdl', 'depth', 'seldepth', 'nodes')
    counts = {key: 0 for key in keys}
    best_changed = top_score_changed = 0
    score_deltas = []
    for index in range(50):
        filename = f'batches/{index:06d}.json'
        new = json.loads((directory / 'download' / filename).read_bytes())['records']
        old = json.loads((BASELINE / 'download/batch' / filename).read_bytes())['records']
        require([r['id'] for r in new] == [r['id'] for r in old], 'Comparison order differs')
        for first, second in zip(old, new):
            a, b = first['engine_result']['analysis'], second['engine_result']['analysis']
            if not isinstance(a, list) or not isinstance(b, list):
                require(a == b, 'Terminal result differs')
                continue
            for key in keys:
                counts[key] += [v.get(key) for v in a] != [v.get(key) for v in b]
            best_changed += a[0].get('pv', [])[:1] != b[0].get('pv', [])[:1]
            if a[0]['score'] != b[0]['score']:
                top_score_changed += 1
                score_deltas.append(abs(a[0]['score'] - b[0]['score']))
    return {'positions_with_changed_fields': counts, 'best_move_changed': best_changed,
            'top_score_changed': top_score_changed,
            'largest_top_score_delta': max(score_deltas, default=0),
            'note': 'Score deltas use local serialized scores (mate uses a numeric sentinel); not a strength assessment.'}


def inspect(directory, client):
    plan = load(directory)
    report = json.loads((directory / 'report.json').read_bytes())
    job = client.job(plan['job'])
    require(job and job['status']['state'] == 'SUCCEEDED', 'Inspect before deleting the succeeded job')
    client.check_job(job, plan['specification'])
    task = client.request('batch', f"{PARENT}/jobs/{plan['job']}/taskGroups/group0/tasks/0")
    require(task['status']['state'] == 'SUCCEEDED', 'Task not successful')
    assigned = set(re.findall(r'zones/(us-central1-[a-z])/instances/([0-9]+)', json.dumps(task)))
    require(len(assigned) == 1, 'Expected one task VM without retries')
    zone, instance_id = next(iter(assigned))
    result = subprocess.run([client.gcloud, 'compute', 'operations', 'list', '--project=' + PROJECT,
        '--filter=targetLink:' + job['uid'], '--format=json'], capture_output=True,
        text=True, check=True, timeout=40)
    operations = json.loads(result.stdout)
    vm_ops = [op for op in operations if str(op.get('targetId')) == instance_id
              and f'/zones/{zone}/instances/{job["uid"]}-group' in op.get('targetLink', '')]
    starts = [op for op in vm_ops if op['operationType'] == 'insert']
    ends = [op for op in vm_ops if op['operationType'] == 'delete']
    require(len(starts) == len(ends) == 1 and all(op['status'] == 'DONE' and not op.get('error') for op in vm_ops),
            'VM lifecycle incomplete or ambiguous')
    lifetime = (timestamp(ends[0]['endTime']) - timestamp(starts[0]['insertTime'])).total_seconds()
    require(lifetime > 0, 'Invalid VM lifecycle')
    instances = [f'{zone}/{instance_id}']
    owner = owners(job['uid'], instances)
    warnings = list(client.log_entries(owner + ' AND severity>=WARNING'))
    save(directory, 'task-status.json', task)
    save(directory, 'vm-operations.json', operations)
    save(directory, 'owned-vm-identities.json', instances)
    save(directory, 'warning-logs.json', warnings)
    save(directory, 'pre-cleanup-job.json', job)
    rates = json.loads((directory / 'pricing-reference.json').read_bytes())
    baseline = json.loads((BASELINE / 'comparison-summary.json').read_bytes())['platforms']['batch']
    cost = lifetime * rates['n2d_highcpu_16_spot_vm_hour'] / 3600
    perf = report['performance']
    metrics = perf['metrics']
    workers = perf['workers']
    elapsed = metrics['worker_seconds']
    summary = {'id': plan['id'], 'positions': COUNT, 'state': 'SUCCEEDED', 'retries': 0,
        'worker_seconds': elapsed, 'fens_per_second': metrics['fen_per_second'],
        'submission_to_success_seconds': (timestamp(job['status']['statusEvents'][-1]['eventTime'])
                                         - timestamp(job['createTime'])).total_seconds(),
        'speedup_over_baseline': report['speedup'], 'baseline': baseline,
        'vm_lifecycle_seconds': lifetime, 'estimated_vm_compute_usd': cost,
        'estimated_compute_savings_percent': 100 * (1 - cost / baseline['estimated_vm_compute_usd']),
        'cost_note': rates['note'], 'memory': metrics['memory'],
        'vm_cpu_busy_percent': metrics['vm_cpu_busy_percent'],
        'worker_position_range': [min(w['positions'] for w in workers), max(w['positions'] for w in workers)],
        'last_search_finish_spread_seconds': max(w['last_search_seconds'] for w in workers)
                                            - min(w['last_search_seconds'] for w in workers),
        'mean_worker_result_queue_wait_seconds': metrics['engine_save_wait_seconds_sum'] / 16,
        'mean_worker_result_queue_wait_fraction': metrics['engine_save_wait_seconds_sum'] / (16 * elapsed),
        'changed_result_count': report['changed_chess_results'], 'result_differences': compare_results(directory),
        'warning_log_count': len(warnings), 'database_writes': 0,
        'comparison_note': 'One placement per configuration. Both hash and CPU count changed; not pure CPU scaling.'}
    save(directory, 'comparison-summary.json', summary)
    print(json.dumps(summary, indent=2))


def cleanup(directory, client):
    plan = load(directory)
    require((directory / 'comparison-summary.json').exists(), 'Inspect and save results before cleanup')
    job = json.loads((directory / 'pre-cleanup-job.json').read_bytes())
    require(job['status']['state'] == 'SUCCEEDED' and job['name'].split('/')[-1] == plan['job'], 'Wrong saved job')
    instances = json.loads((directory / 'owned-vm-identities.json').read_bytes())
    owner = owners(job['uid'], instances)
    receipt_path = directory / 'cleanup-result.json'
    previous = json.loads(receipt_path.read_bytes()) if receipt_path.exists() else None
    if previous:
        saved_plan = json.loads(Path(previous['plan']).read_bytes())
        validate_plan(saved_plan)
        require([(j['id'], j['uid']) for j in saved_plan['jobs']] == [(plan['job'], job['uid'])],
                'Saved cleanup belongs to another job')
        result = cleaning_job(plan=saved_plan, execute=True, plan_directory=directory / 'cleanup',
                              wait_seconds=15, cloud=client)
    else:
        result = cleaning_job([plan['job']], execute=True, plan_directory=directory / 'cleanup',
                              wait_seconds=15, cloud=client)
    save(directory, 'cleanup-result.json', result)
    print(json.dumps({'resource_cleanup': result}), flush=True)
    if not result.get('complete'):
        return
    idle(client)
    logs = {unquote(n.split('/logs/', 1)[1]) for n in client.logs()} & (TEST_LOGS | {'diagnostic-log', 'ping'})
    inspection = {log: inspect_log(client, log, owner) for log in sorted(logs)}
    save(directory, 'log-cleanup-plan.json', {'owner': owner, 'streams': inspection})
    previous_logs = directory / 'log-cleanup-result.json'
    log_result = json.loads(previous_logs.read_bytes()) if previous_logs.exists() else {'deleted': []}
    log_result['retained_unrelated'] = [log for log, value in inspection.items() if value == 'retained_unrelated']
    for log, value in inspection.items():
        if value != 'eligible':
            continue
        idle(client)
        require(inspect_log(client, log, owner) != 'retained_unrelated', 'Log acquired unrelated entries')
        client.delete_log(log)
        log_result['deleted'].append(log)
        save(directory, 'log-cleanup-result.json', log_result)
    save(directory, 'log-cleanup-result.json', log_result)
    final = verify(client, json.loads(Path(result['plan']).read_bytes()))
    final['log_cleanup'] = log_result
    save(directory, 'final-verification.json', final)
    print(json.dumps(final, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'cleanup'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--gcloud', default='gcloud')
    args = parser.parse_args()
    {'inspect': inspect, 'cleanup': cleanup}[args.action](args.directory.resolve(), Client(args.gcloud))


if __name__ == '__main__':
    main()
