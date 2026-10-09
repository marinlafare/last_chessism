"""Bounded work units, sequential containers per VM, existing Spot supervisor."""
from copy import deepcopy

from . import batch_spot

MAX_JOB_FENS = 500_000
UNIT_FENS = 50_000


def ranges(count, n_vms):
    if type(count) is not int or not 1 <= count <= MAX_JOB_FENS:
        raise ValueError('Batch work-unit request requires 1–500,000 FENs')
    if type(n_vms) is not int or not 1 <= n_vms <= batch_spot.MAX_VMS:
        raise ValueError('Invalid VM count')
    n_vms = min(count, n_vms)
    size, extra = divmod(count, n_vms)
    result = []
    for vm in range(n_vms):
        remaining = size + (vm < extra)
        while remaining:
            take = min(UNIT_FENS, remaining)
            result.append((vm, take))
            remaining -= take
    return result


def spec_for(units, image):
    from stockfish_batch.config import Config
    if not units or len(units) > 10 or any(not 1 <= u['count'] <= UNIT_FENS for u in units):
        raise ValueError('Invalid bounded VM work units')
    if len({u['run_id'] for u in units}) != len(units):
        raise ValueError('Repeated work unit')
    specs = []
    for unit in units:
        config = Config(**unit['config']).validate()
        if config != batch_spot.config_for(unit['run_id'], unit['count']):
            raise ValueError('Work-unit configuration differs from the fixed research profile')
        specs.append(batch_spot.spec_for(config, image))
    spec = deepcopy(specs[0])
    runnables = []
    for source in specs:
        runnable = deepcopy(source['taskGroups'][0]['taskSpec']['runnables'][0])
        # Default worker entrypoint is retained. This opt-in only skips units
        # with a complete, contract-bound manifest AND performance report.
        runnable['container']['commands'].append('--skip-completed-unit')
        runnables.append(runnable)
    spec['taskGroups'][0]['taskSpec']['runnables'] = runnables
    return spec


def verify_job(job, spec):
    from .launch import Client
    Client.check_job(job, spec)
    # Reuse resource/retry checks with a single runnable projection; exact
    # ordered runnable equivalence was checked above, including exit flags.
    actual, expected = deepcopy(job), deepcopy(spec)
    for item in (actual, expected):
        task = item['taskGroups'][0]['taskSpec']
        task['runnables'] = task['runnables'][:1]
    batch_spot.verify_job(actual, expected)
