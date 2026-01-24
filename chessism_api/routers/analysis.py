# chessism_api/routers/analysis.py

# --- REMOVED BackgroundTasks, Added Depends ---
from fastapi import APIRouter, Body, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Dict, Any
from arq.connections import ArqRedis
import math # <-- NEW: Import math for ceiling

# --- Import the job functions (they are now just for reference) ---
from chessism_api.operations.analysis import (
    run_analysis_job,
    run_player_analysis_job
)
# --- NEW: Import the Redis client ---
from chessism_api.redis_client import get_redis_pool

router = APIRouter()

@router.post("/run_job")
async def api_run_analysis_job(
    data: Dict[str, Any] = Body(...),
    redis: ArqRedis = Depends(get_redis_pool) # <-- NEW: Get Redis client
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
    try:
        total_fens = int(data.get("total_fens_to_process", 1000000))
        batch_size = int(data.get("batch_size", 500))
        nodes_limit = int(data.get("nodes_limit", 1000000))
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload.")

    queue_name = "analysis_queue"

    # --- NEW: Calculate dynamic timeout based on your formula ---
    # (total_fens * 2.1)
    # We add a 600-second (10 min) buffer for safety and DB operations.
    calculated_timeout = math.ceil(total_fens * 2.1) + 600
    
    print(f"Enqueuing analysis job on {queue_name}. Target: {total_fens} FENs. Calculated timeout: {calculated_timeout}s")
    
    # --- NEW: Enqueue the job instead of running it ---
    job = await redis.enqueue_job(
        'run_analysis_job', # This must match the function name in worker.py
        total_fens_to_process=total_fens,
        batch_size=batch_size,
        nodes_limit=nodes_limit,
        _queue_name=queue_name, # Tell arq which queue to use
        _job_timeout=calculated_timeout # <-- THIS IS THE FIX
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202, # Accepted
        content={
            "message": f"Batch analysis job enqueued on {queue_name}.",
            "total_fens_to_process": total_fens,
            "job_id": job_id
        }
    )


@router.post("/run_player_job")
async def api_run_player_analysis_job(
    data: Dict[str, Any] = Body(...),
    redis: ArqRedis = Depends(get_redis_pool) # <-- NEW: Get Redis client
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
    try:
        player_name = str(data["player_name"]).lower()
        total_fens = int(data.get("total_fens_to_process", 100000))
        batch_size = int(data.get("batch_size", 500))
        nodes_limit = int(data.get("nodes_limit", 1000000))
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Required: 'player_name' (str).")

    queue_name = "analysis_queue"

    # --- NEW: Calculate dynamic timeout based on your formula ---
    # (total_fens * 2.1)
    # We add a 600-second (10 min) buffer for safety and DB operations.
    calculated_timeout = math.ceil(total_fens * 2.1) + 600

    print(f"Enqueuing PLAYER analysis job for '{player_name}' on {queue_name}. Calculated timeout: {calculated_timeout}s")
    
    # --- NEW: Enqueue the job ---
    job = await redis.enqueue_job(
        'run_player_analysis_job', # Function name
        player_name=player_name,
        total_fens_to_process=total_fens,
        batch_size=batch_size,
        nodes_limit=nodes_limit,
        _queue_name=queue_name, # The specific queue to use
        _job_timeout=calculated_timeout # <-- THIS IS THE FIX
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202, # Accepted
        content={
            "message": f"Batch player analysis job for '{player_name}' enqueued on {queue_name}.",
            "player_name": player_name,
            "job_id": job_id
        }
    )

