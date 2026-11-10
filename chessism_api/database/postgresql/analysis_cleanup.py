"""Cancellation-safe cleanup for sessions owned by analysis selections/workers."""
import asyncio
from typing import Awaitable


async def finish_analysis_cleanup(operation: Awaitable[None]) -> None:
    """Finish cleanup before propagating its caller's first cancellation."""
    task = asyncio.ensure_future(operation)
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
        except Exception:
            break  # Retrieve the cleanup failure below, after observing completion.

    try:
        task.result()
    except BaseException as error:
        if cancellation is None:
            raise
        print(f"Analysis cleanup failed during cancellation: {error!r}", flush=True)
    if cancellation is not None:
        raise cancellation
