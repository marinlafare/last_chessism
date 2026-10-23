"""Pure CPU validation. Child processes never open a DB or cloud connection."""
import atexit
import asyncio
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import json
import multiprocessing
import time

from . import runtime
from stockfish_batch.checkpoints import BatchCheckpoints, digest, encode


def validate_batch(raw, index, positions, contract, task=None):
    if type(index) is not int or index < 0:
        raise ValueError('Invalid checkpoint index')
    if task and (task['start'] < 0 or task['count'] < 1 or task['start'] + task['count'] > len(positions)):
        raise ValueError('Invalid task range')
    if len(raw) > BatchCheckpoints.MAX_BYTES:
        raise ValueError('Oversized checkpoint')
    offset, end = (task['start'], task['start'] + task['count']) if task else (0, len(positions))
    group = positions[offset + index * 500:min(end, offset + (index + 1) * 500)]
    if not group:
        raise ValueError('Checkpoint index outside selected positions')
    checkpoints = BatchCheckpoints(None, '', task['contract'] if task else contract, group, 500)
    records = checkpoints.validate_batch(json.loads(raw), 0)
    name = (f"tasks/{task['index']:06d}/" if task else '') + f'batches/{index:06d}.json'
    receipt = {'sha256': digest(raw), 'records': [
        {'id': record['id'], 'object': name, 'sha256': record['result_sha256']}
        for record in records.values()]}
    return name, receipt, [record['engine_result'] for record in records.values()]


def input_signature(positions, contract):
    return digest(encode({'positions': positions, 'contract': contract}))


@dataclass(frozen=True)
class ValidatedBatch:
    input_sha256: str
    receipt: dict
    results: list
    seconds: float


def prepare_batch(raw, positions, contract):
    started = time.monotonic()
    if not 1 <= len(positions) <= 500:
        raise ValueError('Validation requires one bounded batch')
    _, receipt, results = validate_batch(raw, 0, positions, contract)
    return ValidatedBatch(input_signature(positions, contract), receipt, results, time.monotonic() - started)


def checked_batch(prepared, raw, positions, contract):
    # The writer re-reads the authoritative reservation/contract under its run
    # lock. Never let prevalidation authorize changed inputs or bytes.
    if not isinstance(prepared, ValidatedBatch) or prepared.input_sha256 != input_signature(positions, contract) \
            or prepared.receipt['sha256'] != digest(raw):
        raise ValueError('Prevalidated batch no longer matches its reserved input/settings/bytes')
    return prepared.receipt, prepared.results, prepared.seconds


_pool = None


async def prepare_in_process(raw, positions, contract):
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('spawn'))
    return await asyncio.get_running_loop().run_in_executor(_pool, prepare_batch, raw, positions, contract)


def shutdown_pool():
    global _pool
    pool, _pool = _pool, None
    if pool:
        pool.shutdown(wait=True, cancel_futures=True)


atexit.register(shutdown_pool)
