"""Bounded prefetch, fair across shards, with one transactional async DB writer.

At most four downloads plus the file being imported: <=80 MiB of result bytes.
Nothing is acknowledged until the import transaction commits. Restart simply
rediscovers immutable files whose durable receipt is missing.
"""
import asyncio
from collections import deque
import json
import threading
import time

from .importer import import_batch
from stockfish_batch.checkpoints import BatchCheckpoints


async def ingest_ready(storage, run, tasks, *, storage_factory=None, concurrency=4, writer=None):
    if type(concurrency) is not int or not 1 <= concurrency <= 4:
        raise ValueError('Ingestion concurrency must be 1–4')
    writer = writer or import_batch
    thread_state = threading.local()
    def read(uri, limit):
        if not hasattr(thread_state, 'storage'):
            thread_state.storage = storage_factory() if storage_factory else storage
        started = time.monotonic()
        raw = thread_state.storage.read(uri, limit)
        return raw, time.monotonic() - started

    states = deque({'task': task, 'next': 0, 'checked': False, 'checking': False, 'done': False}
                   for task in tasks)
    pending = deque()
    imported = 0
    def fill():
        misses = 0
        while len(pending) < concurrency and states and misses < len(states):
            state = states[0]
            states.rotate(-1)
            task = state['task']
            if state['done'] or state['checking']:
                misses += 1
                continue
            if not state['checked']:
                state['checking'] = True
                index, uri, limit = None, task['output'] + '/contract.json', 2 * 1024 * 1024
            else:
                prefix = f"tasks/{task['index']:06d}/" if task.get('index') is not None else ''
                while state['next'] < (task['count'] + 499) // 500 and (
                    prefix + f"batches/{state['next']:06d}.json") in run.receipts:
                    state['next'] += 1
                if state['next'] >= (task['count'] + 499) // 500:
                    state['done'] = True
                    misses += 1
                    continue
                index = state['next']
                state['next'] += 1
                uri, limit = task['output'] + f'/batches/{index:06d}.json', BatchCheckpoints.MAX_BYTES
            misses = 0
            pending.append((state, index, asyncio.create_task(asyncio.to_thread(read, uri, limit))))

    try:
        fill()
        while pending:
            state, index, future = pending.popleft()
            raw, elapsed = await future
            task = state['task']
            if raw is None:
                state['done'] = True
            elif index is None:
                if json.loads(raw) != task['contract']:
                    raise ValueError('Worker contract differs from its reserved FENs/settings')
                state['checking'], state['checked'] = False, True
            # Refill BEFORE awaiting the database, overlapping I/O with COMMIT.
            fill()
            if raw is not None and index is not None:
                imported += await writer(run.id, raw, index, task_index=task.get('index'), download_seconds=elapsed)
    finally:
        # to_thread cannot stop a running HTTP request. Drain its bounded,
        # timeout-protected reads; never leave a detached writer behind.
        if pending:
            await asyncio.gather(*(f for _, _, f in pending), return_exceptions=True)
    return imported
