"""Normal Spot cleanup with buffered substep timings, never new deletion scopes."""
import asyncio

from cleaning_job import cleaning_job
from . import runtime
from .preparation import StageTimings


async def apply(cloud, plan, *, job_id, run_id):
    timings = StageTimings()
    try:
        return await asyncio.to_thread(cleaning_job, plan=plan, execute=True, cloud=cloud,
            wait_seconds=0, plan_directory=runtime.CLEANUP_ROOT, timings=timings)
    finally:
        await timings.save(job_id, run_id)
