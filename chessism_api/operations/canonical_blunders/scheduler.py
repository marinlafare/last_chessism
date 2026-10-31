"""One durable batch at a time; cron repairs wakeups, not the batch cadence."""
from ..research_algorithms.config import QUEUE
from .worker import canonical_tick

BATCH_ID='canonical-blunder-batch'


async def schedule(ctx):
    return await ctx['redis'].enqueue_job('canonical_blunder_batch',_queue_name=QUEUE,_job_id=BATCH_ID)


async def run_batch(ctx):
    result=await canonical_tick(ctx)
    ctx['canonical_continue']=result.get('status')=='building' and result.get('remaining',0)>0
    return result


async def after_job_end(ctx):
    # ARQ has removed this ordinary job's key before this hook. A stable ID
    # prevents duplicate wakeups while another batch or research job is queued.
    if ctx.get('job_id')==BATCH_ID and ctx.get('canonical_continue'):
        await ctx['redis'].enqueue_job('canonical_blunder_batch',_queue_name=QUEUE,_job_id=BATCH_ID,_defer_by=1)
