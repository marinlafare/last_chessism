"""Compose-managed, read-only observer of the single analysis worker's jobs.

Records each running job in its own timestamped folder, including jobs already
running on observer startup. Polls every 15 seconds; queued jobs do not create
recordings. Never enqueues, cancels, modifies Redis, or connects to PostgreSQL.
CPU usage is for the whole host and each logical CPU, not individual containers:
no Docker socket or fixed container IDs are required across Compose restarts.
An interrupted/retried job gets a new recording segment rather than overwriting
prior data. Each segment is bounded to 24 hours. No thermal control is applied.
"""

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
from types import SimpleNamespace

from arq.jobs import Job, JobStatus
from redis.asyncio import Redis

from .recorder import atomic_json, record, utc


ANALYSIS_FUNCTIONS = frozenset((
    'run_analysis_job', 'run_player_analysis_job',
    'run_analysis_loop_job', 'run_player_games_analysis_job',
))


async def active_analysis_job(redis):
    # Only the analysis queue; paginate to keep reads bounded even if many jobs wait.
    offset = 0
    while True:
        batch = await redis.zrange('analysis_queue', offset, offset + 99)
        for raw_id in batch:
            job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            job = Job(job_id, redis, _queue_name='analysis_queue')
            if await job.status() != JobStatus.in_progress:
                continue
            info = await job.info()
            if info and info.function in ANALYSIS_FUNCTIONS:
                return job_id
        if len(batch) < 100:
            return None
        offset += len(batch)


def recording_args(output_root, job_id, interval):
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')
    # Job IDs normally are UUID hex, but must never be trusted as file paths.
    safe_id = ''.join(char for char in job_id if char.isascii() and (char.isalnum() or char in '-_'))[:128] or 'job'
    return SimpleNamespace(job_id=job_id, container=[], interval=interval,
                           max_hours=24, finish_grace=0,
                           output_dir=str(Path(output_root) / f'job-{safe_id}-{stamp}'))


async def watch(output_root, *, interval=15, stop=None):
    output = Path(output_root)
    output.mkdir(parents=True, exist_ok=True)
    if stop is None:
        stop = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    redis = Redis(host=os.getenv('REDIS_HOST', 'redis'), socket_connect_timeout=3, socket_timeout=3)
    previous_state = None

    def state(phase, **extra):
        nonlocal previous_state
        value = {'status': phase, **extra}
        if value != previous_state:
            value_with_time = {**value, 'updated_at': utc()}
            atomic_json(output / 'watcher.json', value_with_time)
            print(json.dumps(value_with_time), flush=True)
            previous_state = value

    try:
        state('waiting_for_analysis')
        while not stop.is_set():
            try:
                job_id = await active_analysis_job(redis)
                if job_id:
                    args = recording_args(output, job_id, interval)
                    state('recording', job_id=job_id, output_dir=args.output_dir)
                    await record(args, stop=stop)
                else:
                    state('waiting_for_analysis')
            except Exception as error:
                # A telemetry outage must never affect the analysis worker.
                state('retrying', error=f'{type(error).__name__}: {error}')
            if not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
    finally:
        state('stopped')
        await redis.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', default='/recordings')
    parser.add_argument('--interval', type=float, default=15)
    args = parser.parse_args()
    if not 1 <= args.interval <= 300:
        parser.error('Use an interval between 1 and 300 seconds.')
    asyncio.run(watch(args.output_root, interval=args.interval))


if __name__ == '__main__':
    main()
