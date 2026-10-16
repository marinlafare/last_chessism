"""An inactivity deadline, reset only by meaningful application progress."""
import asyncio
from contextlib import asynccontextmanager


class AnalysisStalled(TimeoutError):
    """No FEN completed during the analysis-phase inactivity window."""


@asynccontextmanager
async def progress_timeout(seconds, *, completed_fens_only=False):
    loop = asyncio.get_running_loop()
    deadline = asyncio.timeout(seconds or None)

    def advanced():
        if seconds and not deadline.expired():
            deadline.reschedule(loop.time() + seconds)

    try:
        async with deadline:
            yield advanced
    except TimeoutError:
        if deadline.expired():
            if completed_fens_only:
                raise AnalysisStalled(f"No new FEN completed for {seconds:g} seconds during analysis") from None
            raise TimeoutError(f"No meaningful progress for {seconds:g} seconds") from None
        raise
