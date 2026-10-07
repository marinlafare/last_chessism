"""Bounded one-off recorder. Redis/HTTP/sysfs reads only; output files are local."""

import argparse
import asyncio
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import time

from arq.jobs import Job
import httpx
from redis.asyncio import Redis

from .report import Summary, flat
from .system import SystemSampler, read


def utc():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, allow_nan=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def progress_rate(previous, current):
    if not previous:
        return None, False
    before = previous.get('job', {}).get('progress') or {}
    after = current.get('job', {}).get('progress') or {}
    delta = current['elapsed_seconds'] - previous['elapsed_seconds']
    if delta <= 0 or before.get('total') != after.get('total') or not isinstance(before.get('processed'), (int, float)) or not isinstance(after.get('processed'), (int, float)):
        return None, True
    count = after['processed'] - before['processed']
    return (count / delta if count >= 0 else None), before.get('phase') != after.get('phase')


async def record(args, *, stop=None):
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)  # Never overwrite an earlier run.
    groups = dict(item.split('=', 1) for item in args.container)
    sampler = SystemSampler(groups)
    summary = Summary()
    if stop is None:
        stop = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    redis = Redis(host=os.getenv('REDIS_HOST', 'redis'), socket_connect_timeout=3, socket_timeout=3)
    job = Job(args.job_id, redis, _queue_name='analysis_queue')
    started = time.monotonic()
    metadata = {'format_version': 1, 'job_id': args.job_id, 'recording_started_utc': utc(),
                'interval_seconds': args.interval, 'maximum_hours': args.max_hours,
                'finish_grace_seconds': args.finish_grace, 'containers': groups,
                'host_boot_id': read('/proc/sys/kernel/random/boot_id'), 'read_only_observer': True,
                'cpu_usage_scope': 'whole_host_and_per_logical_cpu',
                'container_cpu_usage_available': bool(groups)}
    atomic_json(output / 'metadata.json', metadata)
    previous, terminal_at, missing_at, result_summary = None, None, None, None
    reason = 'recorder_error'
    last_log_phase = None
    async with httpx.AsyncClient(timeout=3) as client:
        with (output / 'samples.jsonl').open('x', buffering=1) as raw, (output / 'metrics.csv').open('x', newline='') as csv_file:
            writer = None
            try:
                while time.monotonic() - started < args.max_hours * 3600:
                    tick = time.monotonic()
                    sample = {'timestamp_utc': utc(), 'elapsed_seconds': tick - started, 'errors': []}
                    try:
                        sample['system'] = sampler.sample()
                    except Exception as error:
                        sample['errors'].append(f'system: {type(error).__name__}: {error}')
                    try:
                        response = await client.get('http://stockfish-service:9999/status')
                        response.raise_for_status()
                        sample['engine'] = response.json()
                    except Exception as error:
                        sample['errors'].append(f'engine: {type(error).__name__}: {error}')
                    try:
                        state = (await job.status()).value
                        progress = await redis.get(f'chessism:job_progress:{args.job_id}')
                        sample['job'] = {'status': state, 'progress': json.loads(progress) if progress else None}
                        if 'job' not in metadata:
                            info = await job.info()
                            if info:
                                metadata['job'] = {'function': info.function, 'enqueue_time': str(info.enqueue_time),
                                                   'parameters': {key: value for key, value in info.kwargs.items() if key in (
                                                       'player_name', 'planned_fens', 'batch_size', 'nodes_limit', 'cool_off',
                                                       'chunk_size', 'selection_mode', 'selected_games', 'positions_per_run',
                                                       'runs', 'batches', 'total_fens_to_process', 'scope')}}
                                atomic_json(output / 'metadata.json', metadata)
                        if state == 'complete':
                            result = await job.result_info()
                            if result:
                                result_summary = {'success': result.success, 'start_time': str(result.start_time),
                                                  'finish_time': str(result.finish_time),
                                                  'duration_seconds': (result.finish_time - result.start_time).total_seconds(),
                                                  'result': result.result if isinstance(result.result, (dict, list, int, str, type(None))) else str(result.result)}
                            terminal_at = terminal_at if terminal_at is not None else tick
                        elif state == 'not_found':
                            missing_at = missing_at if missing_at is not None else tick
                        else:
                            missing_at = None
                    except Exception as error:
                        sample['errors'].append(f'job: {type(error).__name__}: {error}')
                    sample['fens_per_second'], sample['rate_interval_mixed_phase'] = progress_rate(previous, sample)
                    summary.add(sample)
                    raw.write(json.dumps(sample, allow_nan=False) + '\n')
                    raw.flush()
                    os.fsync(raw.fileno())
                    row = flat(sample)
                    if writer is None:
                        writer = csv.DictWriter(csv_file, fieldnames=list(row))
                        writer.writeheader()
                    writer.writerow(row)
                    csv_file.flush()
                    os.fsync(csv_file.fileno())
                    atomic_json(output / 'summary.json', {**summary.payload(), 'recording_status': 'recording',
                                                          'updated_at': utc(), 'job_result': result_summary})
                    phase = (row['status'], row['phase'])
                    if phase != last_log_phase or len(summary.rows) % 20 == 0:
                        print(json.dumps(row), flush=True)
                        last_log_phase = phase
                    previous = sample
                    if terminal_at is not None and tick - terminal_at >= args.finish_grace:
                        reason = 'job_finished'; break
                    if missing_at is not None and tick - missing_at >= 600:
                        reason = 'job_missing_for_10_minutes'; break
                    if stop.is_set():
                        reason = 'recorder_stopped'; break
                    try:
                        await asyncio.wait_for(stop.wait(), max(.1, args.interval - (time.monotonic() - tick)))
                    except asyncio.TimeoutError:
                        pass
                    if stop.is_set():
                        reason = 'recorder_stopped'; break
                else:
                    reason = 'time_limit'
            finally:
                atomic_json(output / 'summary.json', {**summary.payload(), 'recording_status': 'finished',
                                                      'stop_reason': reason, 'updated_at': utc(), 'job_result': result_summary})
                await redis.aclose()
    print(json.dumps({'recording_finished': reason, 'output': str(output)}), flush=True)
    return reason


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job-id', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--container', action='append', default=[], help='label=full-container-id, for read-only cgroup metrics')
    parser.add_argument('--interval', type=float, default=15)
    parser.add_argument('--max-hours', type=float, default=10)
    parser.add_argument('--finish-grace', type=float, default=120)
    args = parser.parse_args()
    if not 1 <= args.interval <= 300 or not 0 < args.max_hours <= 24 or not 0 <= args.finish_grace <= 600:
        parser.error('Use interval 1–300 seconds, up to 24 hours and grace 0–600 seconds.')
    asyncio.run(record(args))


if __name__ == '__main__':
    main()
