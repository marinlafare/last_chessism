"""Atomic Redis counters for parallel ingestion workers."""

import json
import time
from typing import Any

from arq.connections import ArqRedis

PROGRESS_TTL_SECONDS = 60 * 60 * 24

INCREMENT_STAGE_LUA = """
local total = redis.call('HINCRBY', KEYS[1], 'total', ARGV[1])
local processed = redis.call('HINCRBY', KEYS[1], 'processed', ARGV[2])
local failed = redis.call('HINCRBY', KEYS[1], 'failed', ARGV[3])
local redis_time = redis.call('TIME')
local updated_at = tonumber(redis_time[1]) + (tonumber(redis_time[2]) / 1000000)
local payload = cjson.encode({
    job_id = ARGV[4],
    kind = ARGV[5],
    ingestion_run_id = ARGV[6],
    phase = ARGV[7],
    detail = ARGV[8],
    total = total,
    processed = processed,
    failed = failed,
    updated_at = updated_at
})
redis.call('SET', KEYS[2], payload, 'EX', ARGV[9])
redis.call('EXPIRE', KEYS[1], ARGV[9])
return {total, processed, failed}
"""


def _counter_key(job_id: str, phase: str) -> str:
    return f"chessism:ingestion_progress:{job_id}:{phase}"


def _progress_key(job_id: str) -> str:
    return f"chessism:job_progress:{job_id}"


async def reset_stage_progress(
    redis: ArqRedis,
    *,
    job_id: str,
    ingestion_run_id: str | None,
    phase: str,
    total: int = 0,
    processed: int = 0,
    failed: int = 0,
    detail: str,
    kind: str = "fen_extraction",
) -> dict[str, Any]:
    """Reset a stage counter and publish its first progress snapshot."""
    counter_key = _counter_key(job_id, phase)
    await redis.delete(counter_key)
    await redis.hset(counter_key, mapping={
        "total": max(0, int(total)),
        "processed": max(0, int(processed)),
        "failed": max(0, int(failed)),
    })
    await redis.expire(counter_key, PROGRESS_TTL_SECONDS)
    payload = {
        "job_id": job_id,
        "kind": kind,
        "ingestion_run_id": ingestion_run_id,
        "total": max(0, int(total)),
        "processed": max(0, int(processed)),
        "failed": max(0, int(failed)),
        "phase": phase,
        "detail": detail,
        "updated_at": time.time(),
    }
    await redis.set(
        _progress_key(job_id),
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS,
    )
    return payload


async def increment_stage_progress(
    redis: ArqRedis,
    *,
    job_id: str,
    ingestion_run_id: str | None,
    phase: str,
    total_delta: int = 0,
    processed_delta: int = 0,
    failed_delta: int = 0,
    detail: str,
    kind: str = "fen_extraction",
) -> dict[str, int]:
    """Atomically increment a shared parallel stage and its public snapshot."""
    values = await redis.eval(
        INCREMENT_STAGE_LUA,
        2,
        _counter_key(job_id, phase),
        _progress_key(job_id),
        int(total_delta),
        int(processed_delta),
        int(failed_delta),
        job_id,
        kind,
        str(ingestion_run_id or ""),
        phase,
        detail,
        PROGRESS_TTL_SECONDS,
    )
    return {
        "total": int(values[0]),
        "processed": int(values[1]),
        "failed": int(values[2]),
    }
