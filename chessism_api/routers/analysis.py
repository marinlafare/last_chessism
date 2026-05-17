from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from arq.connections import ArqRedis
import math

from chessism_api.redis_client import get_redis_pool

router = APIRouter()


class AnalysisJobRequest(BaseModel):
    total_fens_to_process: int = Field(1_000_000, ge=1)
    batch_size: int = Field(500, ge=1)
    nodes_limit: int = Field(1_000_000, ge=1)


class PlayerAnalysisJobRequest(AnalysisJobRequest):
    total_fens_to_process: int = Field(100_000, ge=1)
    player_name: str = Field(..., min_length=1)


def _analysis_timeout(total_fens: int) -> int:
    return math.ceil(total_fens * 2.1) + 600


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
