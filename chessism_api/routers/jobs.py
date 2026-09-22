from datetime import datetime
import json
from typing import Any

from arq.jobs import Job, JobStatus
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from arq.connections import ArqRedis

from chessism_api.redis_client import get_redis_pool

router = APIRouter()

KNOWN_QUEUES = ("pipeline_queue", "fen_queue", "analysis_queue", "games_queue", "arq:queue")
ANALYSIS_JOB_FUNCTIONS = {
    "run_analysis_job",
    "run_player_analysis_job",
    "run_analysis_loop_job",
    "run_player_games_analysis_job",
}

DELETE_QUEUED_JOB_SCRIPT = """
if redis.call('EXISTS', KEYS[2]) == 1 then
    return -1
end
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
for index = 3, #KEYS do
    redis.call('DEL', KEYS[index])
end
return 1
"""


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, BaseException):
        return {
            "type": value.__class__.__name__,
            "message": str(value),
        }
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, set):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    return str(value)


def _serialize_job_info(info: Any) -> dict[str, Any] | None:
    if info is None:
        return None

    serialized_kwargs = _serialize_value(info.kwargs)
    if info.function == "run_player_games_analysis_job" and isinstance(serialized_kwargs, dict):
        game_links = serialized_kwargs.pop("game_links", [])
        serialized_kwargs["game_count"] = len(game_links) if isinstance(game_links, list) else 0

    return {
        "function": info.function,
        "args": _serialize_value(info.args),
        "kwargs": serialized_kwargs,
        "job_try": info.job_try,
        "enqueue_time": _serialize_value(info.enqueue_time),
        "score": info.score,
    }


def _serialize_result(result: Any) -> dict[str, Any] | None:
    if result is None:
        return None

    return {
        "success": result.success,
        "result": _serialize_value(result.result),
        "start_time": _serialize_value(result.start_time),
        "finish_time": _serialize_value(result.finish_time),
        "queue_name": result.queue_name,
    }


async def _read_progress(redis: ArqRedis, job_id: str) -> dict[str, Any] | None:
    raw = await redis.get(f"chessism:job_progress:{job_id}")
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


@router.get("/active")
async def api_get_active_jobs(redis: ArqRedis = Depends(get_redis_pool)) -> JSONResponse:
    """
    Returns jobs with live progress payloads that have not completed yet.
    This lets the UI recover a running job after navigation or refresh.
    """
    jobs = []
    keys = await redis.keys("chessism:job_progress:*")
    for key in keys:
        key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        job_id = key_text.rsplit(":", 1)[-1]
        progress = await _read_progress(redis, job_id)
        if not progress:
            continue

        phase = str(progress.get("phase") or "")
        if phase in ("complete", "failed"):
            continue

        status_payload = None
        for queue_name in KNOWN_QUEUES:
            job = Job(job_id, redis, _queue_name=queue_name)
            status = await job.status()
            if status == JobStatus.not_found:
                continue
            status_payload = {
                "job_id": job_id,
                "queue_name": queue_name,
                "status": status.value if isinstance(status, JobStatus) else str(status),
                "info": _serialize_job_info(await job.info()),
                "result": _serialize_result(await job.result_info()),
                "progress": progress,
            }
            break

        jobs.append(status_payload or {
            "job_id": job_id,
            "queue_name": None,
            "status": "in_progress",
            "info": None,
            "result": None,
            "progress": progress,
        })

    jobs.sort(key=lambda item: item.get("progress", {}).get("updated_at", 0), reverse=True)
    return JSONResponse(content={"jobs": jobs})


@router.get("/analysis")
async def api_get_analysis_jobs(redis: ArqRedis = Depends(get_redis_pool)) -> JSONResponse:
    """Return every running or queued job from the dedicated analysis queue."""
    queue_name = "analysis_queue"
    queued_rows = await redis.zrange(queue_name, 0, -1, withscores=True)
    jobs = []

    for raw_job_id, score in queued_rows:
        job_id = raw_job_id.decode("utf-8") if isinstance(raw_job_id, bytes) else str(raw_job_id)
        job = Job(job_id, redis, _queue_name=queue_name)
        status = await job.status()
        if status not in (JobStatus.queued, JobStatus.deferred, JobStatus.in_progress):
            continue

        info = await job.info()
        if not info or info.function not in ANALYSIS_JOB_FUNCTIONS:
            continue

        serialized_info = _serialize_job_info(info)

        jobs.append({
            "job_id": job_id,
            "queue_name": queue_name,
            "status": status.value if isinstance(status, JobStatus) else str(status),
            "score": score,
            "info": serialized_info,
            "progress": await _read_progress(redis, job_id),
        })

    jobs.sort(key=lambda item: (
        0 if item["status"] == JobStatus.in_progress.value else 1,
        float(item.get("score") or 0),
    ))
    return JSONResponse(content={"jobs": jobs})


@router.delete("/{job_id}/queued")
async def api_delete_queued_analysis_job(
    job_id: str,
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Delete an analysis job only while it is still waiting in a queue."""
    for queue_name in KNOWN_QUEUES:
        job = Job(job_id, redis, _queue_name=queue_name)
        status = await job.status()
        if status == JobStatus.not_found:
            continue
        if status not in (JobStatus.queued, JobStatus.deferred):
            raise HTTPException(
                status_code=409,
                detail="Only a queued analysis can be deleted; this job has already started.",
            )

        info = await job.info()
        if not info or info.function not in ANALYSIS_JOB_FUNCTIONS:
            raise HTTPException(status_code=400, detail="This is not an analysis job")

        deleted = await redis.eval(
            DELETE_QUEUED_JOB_SCRIPT,
            8,
            queue_name,
            f"arq:in-progress:{job_id}",
            f"arq:job:{job_id}",
            f"arq:result:{job_id}",
            f"arq:retry:{job_id}",
            f"chessism:job_progress:{job_id}",
            f"chessism:job_progress_fens:{job_id}",
            f"chessism:job_progress_failed_fens:{job_id}",
            job_id,
        )
        if int(deleted) != 1:
            raise HTTPException(
                status_code=409,
                detail="The job started before it could be deleted.",
            )

        return JSONResponse(content={
            "job_id": job_id,
            "status": "deleted",
            "message": "Queued analysis deleted.",
        })

    raise HTTPException(status_code=404, detail="Queued analysis not found")


@router.get("/{job_id}")
async def api_get_job_status(job_id: str, redis: ArqRedis = Depends(get_redis_pool)) -> JSONResponse:
    """
    Returns ARQ job state for pipeline and analysis jobs.
    """
    last_payload = None

    for queue_name in KNOWN_QUEUES:
        job = Job(job_id, redis, _queue_name=queue_name)
        status = await job.status()
        info = await job.info()
        result = await job.result_info()

        payload = {
            "job_id": job_id,
            "queue_name": queue_name,
            "status": status.value if isinstance(status, JobStatus) else str(status),
            "info": _serialize_job_info(info),
            "result": _serialize_result(result),
            "progress": await _read_progress(redis, job_id),
        }
        last_payload = payload

        if status != JobStatus.not_found:
            return JSONResponse(content=payload)

    return JSONResponse(status_code=404, content=last_payload or {"job_id": job_id, "status": "not_found"})
