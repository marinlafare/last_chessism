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

# --- Define queue names ---
QUEUE_NAMES = {
    0: "gpu_0_queue",
    1: "gpu_1_queue"
}

@router.post("/run_job")
async def api_run_analysis_job(
    data: Dict[str, Any] = Body(...),
    redis: ArqRedis = Depends(get_redis_pool) # <-- NEW: Get Redis client
):
    """
    Enqueues a job to analyze FENs from the main database pool.
    
    Payload:
    {
        "gpu_index": 0,
        "total_fens_to_process": 100000,
        "batch_size": 100,
        "nodes_limit": 50000
    }
    """
    try:
        gpu_index = int(data["gpu_index"])
        total_fens = int(data.get("total_fens_to_process", 1000000))
        batch_size = int(data.get("batch_size", 100))
        nodes_limit = int(data.get("nodes_limit", 50000))
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Required: 'gpu_index' (int).")

    if gpu_index not in [0, 1]:
        raise HTTPException(status_code=400, detail="'gpu_index' must be 0 or 1.")

    queue_name = QUEUE_NAMES[gpu_index]

    # --- NEW: Calculate dynamic timeout based on your formula ---
    # (total_fens * 2.1)
    # We add a 600-second (10 min) buffer for safety and DB operations.
    calculated_timeout = math.ceil(total_fens * 2.1) + 600
    
    print(f"Enqueuing analysis job on {queue_name}. Target: {total_fens} FENs. Calculated timeout: {calculated_timeout}s")
    
    # --- NEW: Enqueue the job instead of running it ---
    await redis.enqueue_job(
        'run_analysis_job', # This must match the function name in worker.py
        gpu_index=gpu_index,
        total_fens_to_process=total_fens,
        batch_size=batch_size,
        nodes_limit=nodes_limit,
        _queue_name=queue_name, # Tell arq which queue to use
        _job_timeout=calculated_timeout # <-- THIS IS THE FIX
    )
    
    return JSONResponse(
        status_code=202, # Accepted
        content={
            "message": f"Batch analysis job enqueued on {queue_name}.",
            "gpu_index": gpu_index,
            "total_fens_to_process": total_fens,
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
        "gpu_index": 0,
        "total_fens_to_process": 1000,
        "batch_size": 50,
        "nodes_limit": 50000
    }
    """
    try:
        player_name = str(data["player_name"]).lower()
        gpu_index = int(data["gpu_index"])
        total_fens = int(data.get("total_fens_to_process", 100000))
        batch_size = int(data.get("batch_size", 50))
        nodes_limit = int(data.get("nodes_limit", 50000))
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Required: 'player_name' (str) and 'gpu_index' (int).")

    if gpu_index not in [0, 1]:
        raise HTTPException(status_code=400, detail="'gpu_index' must be 0 or 1.")

    queue_name = QUEUE_NAMES[gpu_index]

    # --- NEW: Calculate dynamic timeout based on your formula ---
    # (total_fens * 2.1)
    # We add a 600-second (10 min) buffer for safety and DB operations.
    calculated_timeout = math.ceil(total_fens * 2.1) + 600

    print(f"Enqueuing PLAYER analysis job for '{player_name}' on {queue_name}. Calculated timeout: {calculated_timeout}s")
    
    # --- NEW: Enqueue the job ---
    await redis.enqueue_job(
        'run_player_analysis_job', # Function name
        player_name=player_name,
        gpu_index=gpu_index,
        total_fens_to_process=total_fens,
        batch_size=batch_size,
        nodes_limit=nodes_limit,
        _queue_name=queue_name, # The specific queue to use
        _job_timeout=calculated_timeout # <-- THIS IS THE FIX
    )
    
    return JSONResponse(
        status_code=202, # Accepted
        content={
            "message": f"Batch player analysis job for '{player_name}' enqueued on {queue_name}.",
            "player_name": player_name,
            "gpu_index": gpu_index,
        }
    )