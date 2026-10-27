"""Dedicated, single-job algorithm worker. Never consumes Stockfish queues."""

import os

from arq.worker import func
from arq import cron
from ..canonical_blunders import scheduler as canonical
from sqlalchemy import select, update

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.runtime import init_runtime_db
from chessism_api.database.models import AlgorithmRun
from chessism_api.redis_client import redis_settings
from .config import QUEUE
from .jobs import run_algorithm_job
from .repository import now
from .storage import acquire_worker_lock, cleanup_interrupted_workspaces


async def startup(ctx):
    handle = acquire_worker_lock()
    ctx["algorithm_workspace_lock"] = handle
    try:
        await init_runtime_db(os.environ["DATABASE_URL"])
        removed = cleanup_interrupted_workspaces()
        async with AsyncDBSession() as session, session.begin():
            await session.execute(update(AlgorithmRun).where(AlgorithmRun.status == "running").values(
                status="failed", finished_at=now(), error="Worker was interrupted. Temporary inputs were cleaned; run again manually.",
                progress={"phase": "interrupted", "detail": "Worker restarted; temporary data removed."}))
            queued = list((await session.execute(select(AlgorithmRun.id, AlgorithmRun.config).where(AlgorithmRun.status == "queued"))))
        from ..canonical_blunders.benchmark_store import recover_interrupted
        await recover_interrupted()
        # Recover only durable requests previously submitted by a user. ARQ's
        # stable IDs prevent duplicates when Redis already has the request.
        for run_id, config in queued:
            function = 'run_large_algorithm_job' if config.get('operation') == 'full_development_rating_blunder_v1' else 'run_algorithm_job'
            await ctx["redis"].enqueue_job(function, run_id=run_id, _queue_name=QUEUE, _job_id=f"algorithm-{run_id}")
        print(f"Algorithm worker ready; removed {len(removed)} interrupted workspaces; {len(queued)} pending requests.", flush=True)
    except BaseException:
        handle.close()
        raise


async def shutdown(ctx):
    handle = ctx.pop("algorithm_workspace_lock", None)
    if handle:
        handle.close()


class WorkerSettings:
    functions = [func(run_algorithm_job, timeout=3600, max_tries=1),
                 func(run_algorithm_job, name='run_large_algorithm_job', timeout=21600, max_tries=1),
                 func(canonical.run_batch,name='canonical_blunder_batch',timeout=600,max_tries=1,keep_result=0)]
    cron_jobs = [cron(canonical.schedule,name='cron:canonical_tick',second=0, run_at_startup=True,
        job_id='canonical-blunder-refresh', timeout=600, keep_result=0)]
    after_job_end = canonical.after_job_end
    redis_settings = redis_settings
    queue_name = QUEUE
    max_jobs = 1
    on_startup = startup
    on_shutdown = shutdown
    health_check_interval = 30
