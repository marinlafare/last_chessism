"""Shared resource guards for background research workloads."""

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus


ANALYSIS_JOB_FUNCTIONS = {
    "run_analysis_job",
    "run_player_analysis_job",
    "run_analysis_loop_job",
    "run_player_games_analysis_job",
}


async def analysis_jobs_active(redis: ArqRedis) -> bool:
    """Return true when a queued or running job consumes Stockfish resources."""
    queued_rows = await redis.zrange("analysis_queue", 0, -1)
    for raw_job_id in queued_rows:
        job_id = raw_job_id.decode("utf-8") if isinstance(raw_job_id, bytes) else str(raw_job_id)
        job = Job(job_id, redis, _queue_name="analysis_queue")
        status = await job.status()
        if status not in (JobStatus.queued, JobStatus.deferred, JobStatus.in_progress):
            continue
        info = await job.info()
        if info and info.function in ANALYSIS_JOB_FUNCTIONS:
            return True
    return False
