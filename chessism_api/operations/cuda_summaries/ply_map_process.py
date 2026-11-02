"""One killable bulk CUDA build; the parent retains checkpoints and heartbeats.

Use spawn, not fork: the parent already has a CUDA context. IPC contains only
objects produced by this local child, not user-supplied serialized files.
"""
import asyncio
import multiprocessing
import time
import traceback

BUILD_TIMEOUT = 180


def child(sender, games, device_bytes):
    try:
        from ..research_algorithms.cuda_runtime import cuda_backend
        from . import batch_budget
        from chessism_api.database.neo4j import ply_map_compute as compute
        xp, hardware = cuda_backend(0)
        if hardware['name'] != 'NVIDIA GeForce RTX 4060':
            raise RuntimeError('Ply-map summaries require the RTX 4060.')
        budget = batch_budget.plan(*xp.cuda.runtime.memGetInfo())
        xp.get_default_memory_pool().set_limit(size=min(device_bytes,budget.device_bytes))
        started = time.monotonic()
        metrics = dict(implementation=compute.IMPLEMENTATION,hardware=hardware['name'])

        def progress(event):
            if event['stage'] in ('grouping','layout'):
                metrics.update({k:v for k,v in event.items() if k!='stage'})
            sender.send(('progress',event))

        result = compute.build(games,xp,progress)
        metrics['compute_seconds'] = time.monotonic()-started
        sender.send(('result',(result,metrics)))
    except BaseException as error:
        traceback.print_exc()
        sender.send(('error',f'{type(error).__name__}: {error}'[:1000]))
    finally:
        sender.close()


async def execute(games, device_bytes, progress, *, timeout=BUILD_TIMEOUT, _target=None):
    context = multiprocessing.get_context('spawn')
    receiver,sender = context.Pipe(duplex=False)
    process = context.Process(target=_target or child,args=(sender,games,device_bytes),daemon=True)
    pending = None
    started = time.monotonic()
    try:
        # Spawning serializes the cohort once; keep it off the event loop too.
        start = asyncio.create_task(asyncio.to_thread(process.start))
        try:
            await asyncio.shield(start)
        except asyncio.CancelledError:
            await start
            raise
        sender.close()
        while True:
            remaining = timeout-(time.monotonic()-started)
            if remaining <= 0:
                raise TimeoutError(f'Ply-map calculation exceeded {timeout:g}s; saved inputs retained for retry.')
            pending = asyncio.create_task(asyncio.to_thread(receiver.recv))
            try:
                kind,payload = await asyncio.wait_for(asyncio.shield(pending),remaining)
            except asyncio.TimeoutError:
                raise TimeoutError(f'Ply-map calculation exceeded {timeout:g}s; saved inputs retained for retry.') from None
            except EOFError:
                raise RuntimeError(f'Ply-map calculation process exited without a result (exit {process.exitcode}); saved inputs retained.') from None
            pending = None
            if kind == 'progress':
                await progress(payload)
            elif kind == 'result':
                return payload
            elif kind == 'error':
                raise RuntimeError(payload)
            else:
                raise RuntimeError('Invalid ply-map calculation process message.')
    finally:
        # On timeout/cancellation stop the compute process, not the worker or DB.
        sender.close()
        if process.pid is not None:
            if process.is_alive(): process.terminate()
            await asyncio.to_thread(process.join,3)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join,3)
            process.close()
        # Closing the child end releases any pending blocking recv safely.
        if pending is not None:
            await asyncio.gather(pending,return_exceptions=True)
        receiver.close()
