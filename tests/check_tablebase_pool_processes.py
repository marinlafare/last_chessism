"""Actual spawn/reuse/cancellation check with read-only local Syzygy files."""
import asyncio
import multiprocessing
from chessism_api.operations.tablebase_parallel import TablebaseProber
from .benchmark_tablebase_parallel import positions


def children():return {p.pid for p in multiprocessing.active_children()}


async def main():
    rows=positions(300);initial=children()
    async with TablebaseProber('/syzygy',2) as prober:
        assert prober.workers==2,'Test needs two CPU allowance'
        first=await prober.probe(rows);pids=children()-initial
        assert len(pids)==2
        assert await prober.probe(rows)==first
        assert children()-initial==pids
    assert children()==initial
    async with TablebaseProber('/syzygy',2) as prober:
        task=asyncio.create_task(prober.probe(rows*5))
        await asyncio.sleep(.25);task.cancel()
        try:await task
        except asyncio.CancelledError:pass
        else:raise AssertionError('Expected cancellation')
        assert prober.pool is None and children()==initial
    async with TablebaseProber('/syzygy',2,timeout=.03) as prober:
        try:await prober.probe(rows*5)
        except asyncio.TimeoutError:pass
        else:raise AssertionError('Expected timeout')
        assert prober.pool is None and children()==initial
    async with TablebaseProber('/syzygy',2) as prober:
        try:await prober.probe([None])
        except TypeError:pass
        else:raise AssertionError('Worker failures must propagate')
        assert prober.pool is None and children()==initial
    print('PASS real spawned pool reuse, result parity, cancellation, timeout, failure and no leftover child processes',flush=True)


if __name__=='__main__':asyncio.run(main())
