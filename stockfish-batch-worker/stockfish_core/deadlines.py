"""Cancellation-preserving deadlines for the HTTP analysis execution path."""
import asyncio


async def wait_with_timeout(operation, timeout):
    """Own and drain an operation before propagating timeout or cancellation.

    Unlike wait_for on Python 3.10/3.11, wait does not return a child's result
    when external cancellation races its completion. If the result is an owned
    resource, the caller must reclaim it from the supplied task on cancellation.
    """
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=timeout)
        if not done:
            raise asyncio.TimeoutError()
        return task.result()
    except BaseException as error:
        task.cancel()
        cleanup = asyncio.gather(task, return_exceptions=True)
        cancellation = error if isinstance(error, asyncio.CancelledError) else None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as cancelled:
                cancellation = cancellation or cancelled
        if cancellation is not None:
            raise cancellation
        raise
