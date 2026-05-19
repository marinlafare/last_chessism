from datetime import datetime
import json
from typing import Any

from arq.jobs import Job, JobStatus
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from arq.connections import ArqRedis

from chessism_api.redis_client import get_redis_pool

router = APIRouter()

KNOWN_QUEUES = ("pipeline_queue", "fen_queue", "analysis_queue", "games_queue", "arq:queue")


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

    return {
        "function": info.function,
        "args": _serialize_value(info.args),
        "kwargs": _serialize_value(info.kwargs),
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
