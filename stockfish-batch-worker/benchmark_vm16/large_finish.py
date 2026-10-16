"""Save the 200k report, then clean only after verified normal database import."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time

from benchmark_platforms.run import require, save
from benchmark_vm16 import large, large_database as database
from benchmark_vm16.finish import cleanup_verified, owners, timestamp
from cleaning_job.cloud import PROJECT
from cloud_job.launch import Client


def summary(plan, report, job, lifetime, warnings):
    require(report.get('all_result_checksums_and_legal_pvs_verified') is True
            and report['id'] == plan['id'] and report['positions'] == plan['count'], 'Unverified report')
    first, second, perf = report['interrupted'], report['resumed'], report['performance']
    require(first['child_exit_code'] == -9 and second['checkpoint_recovery_verified'] is True
            and perf['resumed'] == 25000 and perf['analyzed_this_attempt'] == 175000,
            'Unexpected recovery results')
    metrics, workers = perf['metrics'], perf['workers']
    phases = [first, second]
    seconds = sum(p['elapsed_seconds'] for p in phases)
    return {'id': plan['id'], 'positions': plan['count'], 'state': job['status']['state'],
        'machine_type': 'n2d-highcpu-16', 'vcpus': 16, 'vm_memory_gib': 16,
        'worker_memory_limit_gib': 12, 'hash_mib_per_engine': 256, 'nodes_per_fen': 100000,
        'phase_seconds': {p['phase']: p['elapsed_seconds'] for p in phases},
        'total_phase_seconds': seconds, 'overall_fens_per_second': plan['count'] / seconds,
        'submission_to_success_seconds': (timestamp(job['status']['statusEvents'][-1]['eventTime'])
                                         - timestamp(job['createTime'])).total_seconds(),
        'vm_lifecycle_seconds': lifetime, 'preserved_fens': perf['resumed'],
        'recovery_scope': report['recovery_scope'],
        'resume_vm_cpu_busy_percent': metrics['vm_cpu_busy_percent'],
        'resume_fens_per_second': metrics['fen_per_second'],
        'resume_mean_engine_queue_wait_seconds': metrics['engine_save_wait_seconds_sum'] / len(workers),
        'resume_worker_fen_range': [min(w['positions'] for w in workers), max(w['positions'] for w in workers)],
        'resume_last_search_finish_spread_seconds': max(w['last_search_seconds'] for w in workers)
                                                  - min(w['last_search_seconds'] for w in workers),
        'result_pipeline': metrics['result_pipeline'],
        'peak_vm_used_bytes': max(p['supervisor_metrics']['memory']['host_used_excluding_reclaimable']['max_bytes'] for p in phases),
        'peak_container_bytes': max(p['supervisor_metrics']['memory']['cgroup_peak_bytes'] for p in phases),
        'oom_kills': sum(p['supervisor_metrics']['memory']['cgroup_oom_kills_delta'] for p in phases),
        'peak_swap_bytes': max(p['supervisor_metrics']['memory']['host_swap_used']['max_bytes'] for p in phases),
        'memory_snapshots': [{'saved_fens': snap['saved'],
            'cumulative_peak_vm_used_bytes': snap['metrics']['memory']['host_used_excluding_reclaimable']['max_bytes'],
            'cumulative_peak_container_bytes': snap['metrics']['memory']['cgroup_peak_bytes']}
            for p in phases for snap in p['memory_snapshots']],
        'warning_log_count': len(warnings),
        'comparison_note': 'Different FEN sample from the 25k benchmark; throughput is not a controlled pipeline speedup comparison.'}


def inspect(directory, client):
    plan = large.load(directory, validate_input=False)
    report = json.loads((directory / 'report.json').read_bytes())
    job = client.job(plan['job'])
    submitted = json.loads((directory / 'submission.json').read_bytes())['response']
    require(job and job['uid'] == submitted['uid'] and job['status']['state'] == 'SUCCEEDED', 'Wrong/unfinished job')
    large.check_job(job, plan['specification'])
    task = client.request('batch', f"{large.PARENT}/jobs/{plan['job']}/taskGroups/group0/tasks/0")
    require(task['status']['state'] == 'SUCCEEDED', 'Task not successful')
    assigned = set(re.findall(r'zones/(us-central1-[a-z])/instances/([0-9]+)', json.dumps(task)))
    require(len(assigned) == 1, 'Expected exactly one task VM')
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
    require(lifetime > 0, 'Invalid VM lifetime')
    instances = [f'{zone}/{instance_id}']
    warnings = list(client.log_entries(owners(job['uid'], instances) + ' AND severity>=WARNING'))
    for name, value in (('task-status', task), ('vm-operations', operations),
                        ('owned-vm-identities', instances), ('warning-logs', warnings), ('pre-cleanup-job', job)):
        save(directory, name + '.json', value)
    result = summary(plan, report, job, lifetime, warnings)
    save(directory, 'performance-summary.json', result)
    print(json.dumps(result, indent=2), flush=True)
    return result


def cleanup(directory, client):
    plan = large.load(directory, validate_input=False)
    require((directory / 'performance-summary.json').is_file(), 'Save the report before cleanup')
    # No deletion at all unless the durable receipt AND actual DB scores match.
    database.execute(database.verify_import, directory, plan)
    job = json.loads((directory / 'pre-cleanup-job.json').read_bytes())
    submitted = json.loads((directory / 'submission.json').read_bytes())['response']
    require(job['uid'] == submitted['uid'], 'Saved job UID mismatch')
    large.check_job(job, plan['specification'])
    instances = json.loads((directory / 'owned-vm-identities.json').read_bytes())
    result = cleanup_verified(directory, client, plan, job, instances)
    if result.get('complete'):
        receipt = database.execute(database.verify_import, directory, plan, cleanup=result)
        save(directory, 'database-completion.json', receipt)
        print(json.dumps({'database_completion': receipt}), flush=True)
    return result


def wait_cleanup(directory, client, *, wait_seconds=1800):
    """Bounded local finisher; no cloud calls until a complete DB receipt exists."""
    require(1 <= wait_seconds <= 1800, 'Wait must be between 1 and 1800 seconds')
    plan = large.load(directory, validate_input=False)
    require((directory / 'performance-summary.json').is_file(), 'Save report before waiting for cleanup')
    deadline = time.monotonic() + wait_seconds
    def state(value, **extra):
        save(directory, 'finish-status.json', {'state': value, 'benchmark_id': plan['id'], **extra})
        print(json.dumps({'state': value, **extra}), flush=True)
    state('waiting_for_import_receipt')
    try:
        while time.monotonic() < deadline:
            path = directory / 'import-receipt.json'
            if path.is_file() and database.execute(database.import_receipt_ready, plan, json.loads(path.read_bytes())):
                state('cleaning')
                result = cleanup(directory, client)
                if result.get('complete'):
                    state('complete')
                    return result
                state('waiting_for_cloud_resource_deletion')
            time.sleep(min(10, max(0, deadline - time.monotonic())))
        raise TimeoutError('Finalization timed out; results remain protected. Inspect the importer and rerun cleanup.')
    except Exception as error:
        state('failed', error_type=type(error).__name__)
        raise


def finalize_import(directory, client):
    """Retry failed summary refreshes without reimporting scores or running engines."""
    plan = large.load(directory, validate_input=False)
    def state(value, **extra):
        save(directory, 'finish-status.json', {'state': value, 'benchmark_id': plan['id'], **extra})
        print(json.dumps({'state': value, **extra}), flush=True)
    state('refreshing_database_summaries')
    try:
        database.execute(database.finalize_import, directory, plan)
        state('cleaning')
        result = cleanup(directory, client)
        if not result.get('complete'):
            return wait_cleanup(directory, client)
        state('complete')
        return result
    except Exception as error:
        state('failed', error_type=type(error).__name__)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'cleanup', 'wait-cleanup', 'finalize-import'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--gcloud', default='gcloud')
    args = parser.parse_args()
    result = {'inspect': inspect, 'cleanup': cleanup, 'wait-cleanup': wait_cleanup,
              'finalize-import': finalize_import}[args.action](args.directory.resolve(), Client(args.gcloud))
    if result.get('complete') is False:
        raise SystemExit(2)


if __name__ == '__main__':
    main()
