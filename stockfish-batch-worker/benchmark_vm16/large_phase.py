"""Test-only container supervisor, embedded in the two sequential Batch runnables.

Interrupt phase kills ONLY its own analyzer process group after durable progress.
Resume phase is a fresh container. Neither phase submits jobs, modifies IAM, or
imports into the database. This is process-loss recovery, NOT Spot code 50001.
"""
import asyncio
import json
import os
from pathlib import Path
import re
import signal
import sys
import time

from stockfish_batch.checkpoints import digest, encode
from stockfish_batch.config import parse_args
from stockfish_batch.metrics import Sampler
from stockfish_batch.storage import Storage, child


def require(ok, message):
    if not ok:
        raise ValueError(message)


def inventory(store, prefix):
    """Only immutable batch objects; generations/checksums must survive restart."""
    base = child(prefix, 'batches/')
    if prefix.startswith('gs://'):
        blob = store._blob(base)
        from google.cloud.storage.retry import DEFAULT_RETRY
        objects = list(blob.bucket.list_blobs(prefix=blob.name, max_results=401,
                       timeout=10, retry=DEFAULT_RETRY.with_timeout(20)))
        require(len(objects) <= 400, 'Too many benchmark objects')
        result = {obj.name.removeprefix(blob.name):
                  {'generation': str(obj.generation), 'size': obj.size, 'crc32c': obj.crc32c}
                  for obj in objects}
    else:
        result = {p.name: {'sha256': digest(p.read_bytes()), 'size': p.stat().st_size}
                  for p in Path(base).glob('*.json')}
    require(all(re.fullmatch(r'[0-9]{6}\.json', name) for name in result), 'Unexpected checkpoint name')
    return result


def unchanged(before, after):
    require(bool(before) and all(after.get(key) == value for key, value in before.items()),
            'Previously committed checkpoint changed or disappeared')


def stop_owned(process):
    """PID comes only from our own start_new_session subprocess, never user input."""
    if process is not None and process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


async def phase(mode, cut, argv):
    require(mode in ('interrupt', 'resume'), 'Invalid benchmark phase')
    config = parse_args(argv)
    require(config.compact_results and config.batch_size > 1, 'Checkpoint batches required')
    require(0 < cut < config.max_positions and cut % config.batch_size == 0, 'Invalid interruption boundary')
    store, sampler = Storage(), Sampler()
    marker_uri = child(config.output, 'recovery/interruption.json')
    marker = await asyncio.to_thread(store.read, marker_uri)
    before = None
    if mode == 'interrupt':
        require(marker is None and not await asyncio.to_thread(inventory, store, config.output),
                'Interruption phase requires a fresh result prefix')
    else:
        require(marker is not None, 'Missing interruption evidence')
        before = json.loads(marker)
        require(before['kind'] == 'owned_analyzer_sigkill' and before['total'] == config.max_positions
                and before['signal'] == 9 and before['child_exit_code'] == -9,
                'Invalid interruption evidence')
        unchanged(before['checkpoints'], await asyncio.to_thread(inventory, store, config.output))
    stop = asyncio.Event()
    sampling = asyncio.create_task(sampler.sample(stop))
    process = None
    started = complete = None
    cut_event = None
    snapshots, saved_batches = [], []
    began = time.monotonic()
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, task.cancel)
    try:
        async with asyncio.timeout(config.run_timeout + 30):
            process = await asyncio.create_subprocess_exec(sys.executable, '-m', 'stockfish_batch', *argv,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, start_new_session=True)
            while line := await process.stdout.readline():
                print(line.decode(errors='replace').rstrip(), flush=True)
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get('event') == 'started':
                    require(started is None and event['total'] == config.max_positions, 'Unexpected worker start')
                    started = event
                    if before:
                        require(event['fingerprint'] == before['fingerprint']
                                and len(before['checkpoints']) * config.batch_size <= event['resumed'] < event['total'],
                                'Worker did not resume the interrupted run')
                    else:
                        require(event['resumed'] == 0, 'Fresh test unexpectedly resumed')
                elif event.get('event') == 'saved':
                    index = event['batch']
                    require(index not in saved_batches, 'Batch saved twice in one phase')
                    if before:
                        require(f'{index:06d}.json' not in before['checkpoints'], 'Worker redid a saved batch')
                    saved_batches.append(index)
                    if event['saved'] % 25000 == 0 or event['saved'] == event['total']:
                        snapshots.append({'saved': event['saved'], 'metrics': sampler.finish()})
                    if mode == 'interrupt' and event['saved'] >= cut:
                        cut_event = event
                        stop_owned(process)
                        break
                elif event.get('event') == 'complete':
                    complete = event
            code = await process.wait()
        require(started is not None, 'Missing worker start')
        current = await asyncio.to_thread(inventory, store, config.output)
        report = {'phase': mode, 'total': config.max_positions, 'fingerprint': started['fingerprint'],
                  'worker_started': started, 'elapsed_seconds': time.monotonic() - began,
                  'saved_batches': saved_batches, 'memory_snapshots': snapshots,
                  'supervisor_metrics': sampler.finish(), 'child_exit_code': code}
        if mode == 'interrupt':
            require(cut_event is not None and code == -9 and complete is None, 'Planned interruption not observed')
            require(cut // config.batch_size <= len(current) < config.max_positions // config.batch_size,
                    'No valid partial work to recover')
            require(await asyncio.to_thread(store.read, child(config.output, 'manifest.json')) is None,
                    'Interrupted run unexpectedly completed')
            report.update(kind='owned_analyzer_sigkill', signal=9, cut_event=cut_event, checkpoints=current,
                          note='Not a VM shutdown or genuine Google Spot preemption signal.')
            destination = marker_uri
        else:
            require(code == 0 and complete is not None and complete['saved'] == config.max_positions,
                    'Recovery worker failed or incomplete')
            require(complete['resumed'] == started['resumed']
                    and complete['analyzed_this_attempt'] == config.max_positions - started['resumed'],
                    'Recovery counts do not reconcile')
            unchanged(before['checkpoints'], current)
            require(len(current) * config.batch_size == config.max_positions, 'Incomplete final batch set')
            report.update(checkpoint_recovery_verified=True, complete=complete,
                          preserved_batches=len(before['checkpoints']))
            destination = child(config.output, 'recovery/resume.json')
        require(await asyncio.to_thread(store.create, destination, encode(report)), 'Phase report already exists')
        print(json.dumps({'event': 'benchmark_phase_complete', 'phase': mode,
                          'saved': len(current) * config.batch_size}), flush=True)
    finally:
        stop_owned(process)
        if process is not None:
            await process.wait()
        stop.set()
        await sampling
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)


if __name__ == '__main__':
    asyncio.run(phase(sys.argv[1], int(sys.argv[2]), sys.argv[3:]))
