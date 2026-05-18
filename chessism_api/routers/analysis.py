from typing import Any

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from arq.connections import ArqRedis
import math

from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.analysis import ENGINE_URL
from chessism_api.operations.analysis_times import record_analysis_times

router = APIRouter()


class AnalysisJobRequest(BaseModel):
    total_fens_to_process: int = Field(1_000_000, ge=1)
    batch_size: int = Field(500, ge=1)
    nodes_limit: int = Field(1_000_000, ge=1)


class PlayerAnalysisJobRequest(AnalysisJobRequest):
    total_fens_to_process: int = Field(100_000, ge=1)
    player_name: str = Field(..., min_length=1)


class FenAnalysisRequest(BaseModel):
    fens: list[str] = Field(..., min_length=1, max_length=20)
    nodes_limit: int = Field(1_000_000, ge=1, le=100_000_000)
    multipv: int = Field(4, ge=1, le=10)


def _analysis_timeout(total_fens: int) -> int:
    return math.ceil(total_fens * 2.1) + 600


def _engine_elapsed_ms(result: dict[str, Any]) -> float | None:
    analysis = result.get("analysis")
    if isinstance(analysis, list) and analysis:
        value = analysis[0].get("time")
    elif isinstance(analysis, dict):
        value = analysis.get("time")
    else:
        value = None
    try:
        return float(value) * 1000
    except (TypeError, ValueError):
        return None


@router.post("/fen")
async def api_analyze_fen(data: FenAnalysisRequest = Body(...)) -> list[dict[str, Any]]:
    """
    Runs immediate Stockfish analysis for user-supplied FEN positions.
    """
    payload = {
        "fens": data.fens,
        "nodes_limit": data.nodes_limit,
        "multipv": data.multipv,
    }

    try:
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.post(ENGINE_URL, json=payload)
            response.raise_for_status()
            result = response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=exc.response.text or "Stockfish analysis failed",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail=f"Stockfish service unavailable: {exc}") from exc

    if not isinstance(result, list):
        raise HTTPException(status_code=502, detail="Unexpected Stockfish response")

    timing_rows = []
    for fen, item in zip(data.fens, result):
        elapsed_ms = _engine_elapsed_ms(item)
        if elapsed_ms is None:
            continue
        timing_rows.append({
            "fen": fen,
            "source": "manual",
            "nodes_limit": data.nodes_limit,
            "multipv": data.multipv,
            "elapsed_ms": elapsed_ms,
            "engine_result": item,
        })
    await record_analysis_times(timing_rows)

    return result


@router.post("/run_job")
async def api_run_analysis_job(
    data: AnalysisJobRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
):
    """
    Enqueues a job to analyze FENs from the main database pool.
    
    Payload:
    {
        "total_fens_to_process": 100000,
        "batch_size": 100,
        "nodes_limit": 1000000
    }
    """
    total_fens = data.total_fens_to_process
    queue_name = "analysis_queue"
    calculated_timeout = _analysis_timeout(total_fens)
    
    print(f"Enqueuing analysis job on {queue_name}. Target: {total_fens} FENs. Calculated timeout: {calculated_timeout}s")
    
    job = await redis.enqueue_job(
        'run_analysis_job',
        total_fens_to_process=total_fens,
        batch_size=data.batch_size,
        nodes_limit=data.nodes_limit,
        _queue_name=queue_name,
        _job_timeout=calculated_timeout
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202,
        content={
            "message": f"Batch analysis job enqueued on {queue_name}.",
            "total_fens_to_process": total_fens,
            "job_id": job_id
        }
    )


@router.post("/run_player_job")
async def api_run_player_analysis_job(
    data: PlayerAnalysisJobRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
):
    """
    Enqueues a job to analyze FENs for a specific player.
    
    Payload:
    {
        "player_name": "hikaru",
        "total_fens_to_process": 1000,
        "batch_size": 50,
        "nodes_limit": 1000000
    }
    """
    player_name = data.player_name.lower()
    total_fens = data.total_fens_to_process
    queue_name = "analysis_queue"
    calculated_timeout = _analysis_timeout(total_fens)

    print(f"Enqueuing PLAYER analysis job for '{player_name}' on {queue_name}. Calculated timeout: {calculated_timeout}s")
    
    job = await redis.enqueue_job(
        'run_player_analysis_job',
        player_name=player_name,
        total_fens_to_process=total_fens,
        batch_size=data.batch_size,
        nodes_limit=data.nodes_limit,
        _queue_name=queue_name,
        _job_timeout=calculated_timeout
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202,
        content={
            "message": f"Batch player analysis job for '{player_name}' enqueued on {queue_name}.",
            "player_name": player_name,
            "job_id": job_id
        }
    )
