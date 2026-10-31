"""Bounded, persistent, spawn-only Syzygy processes; never database writers."""
import asyncio
import math
import multiprocessing
import os
from pathlib import Path
from multiprocessing.util import Finalize

import chess.syzygy
from .tablebase_probe import _probe_batch_sync

_tables = None


def worker_count(requested=None):
    count = max(1, min(4, int(requested if requested is not None else os.getenv('TABLEBASE_PROBE_WORKERS', '4'))))
    count = min(count, len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count() or 1)
    try:
        quota, period = Path('/sys/fs/cgroup/cpu.max').read_text().split()
        if quota != 'max': count = min(count, max(1, math.floor(int(quota) / int(period))))
    except (OSError, ValueError):
        try:
            quota = int(Path('/sys/fs/cgroup/cpu/cpu.cfs_quota_us').read_text())
            period = int(Path('/sys/fs/cgroup/cpu/cpu.cfs_period_us').read_text())
            if quota > 0: count = min(count, max(1, quota // period))
        except (OSError, ValueError): pass
    return count


def _initialize(path):
    global _tables
    _tables = chess.syzygy.open_tablebase(path)
    Finalize(_tables, _tables.close, exitpriority=10)


def _probe(rows):
    return _probe_batch_sync(_tables, rows)


class TablebaseProber:
    def __init__(self, path, workers=None, *, timeout=120):
        self.path, self.workers, self.timeout = path, worker_count(workers), timeout
        self.pool = self.tables = None

    async def __aenter__(self): return self

    async def probe(self, rows):
        if not rows: return [], []
        if self.workers == 1:
            if self.tables is None: self.tables = chess.syzygy.open_tablebase(self.path)
            # Shield this read-only thread: don't close mmap files while it probes.
            task = asyncio.create_task(asyncio.to_thread(_probe_batch_sync, self.tables, rows))
            try: return await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        if self.pool is None:
            self.pool = multiprocessing.get_context('spawn').Pool(self.workers, _initialize, (self.path,))
        size = max(1, (len(rows) + self.workers - 1) // self.workers)
        result = self.pool.map_async(_probe, [rows[i:i+size] for i in range(0, len(rows), size)])
        async def wait_result():
            while not result.ready(): await asyncio.sleep(.02)
            return result.get()
        try:
            parts = await asyncio.wait_for(wait_result(), self.timeout)
            return ([row for solved, _ in parts for row in solved],
                    [row for _, unavailable in parts for row in unavailable])
        except BaseException:
            await self.close(abort=True)
            raise

    async def close(self, *, abort=False):
        pool, self.pool = self.pool, None
        if pool is not None:
            pool.terminate() if abort else pool.close()
            task = asyncio.create_task(asyncio.to_thread(pool.join))
            try: await asyncio.shield(task)
            except asyncio.CancelledError:
                pool.terminate()
                await task
                raise
        if self.tables is not None:
            self.tables.close(); self.tables = None

    async def __aexit__(self, kind, error, traceback):
        await self.close(abort=kind is not None)
