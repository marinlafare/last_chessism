# chessism_api/routers/analysis.py

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any

# --- NEW IMPORTS ---
from chessism_api.operations.analysis import (
    run_analysis_job,
    run_player_analysis_job
)
# ---

router = APIRouter()

@router.post("/run_job")
async def api_run_analysis_job(
    background_tasks: BackgroundTasks, 
    data: Dict[str, Any] = Body(...)
):
    """
    Triggers a background task to analyze FENs from the main database pool.
    
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
        nodes_limit = int(data.get("nodes_limit", 50000)) # Req 3
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Required: 'gpu_index' (int).")

    if gpu_index not in [0, 1]:
        raise HTTPException(status_code=400, detail="'gpu_index' must be 0 or 1.")

    print(f"Received analysis job request for GPU {gpu_index}. Target: {total_fens} FENs.")
    
    # Start the job in a background task
    background_tasks.add_task(
        run_analysis_job,
        gpu_index=gpu_index,
        total_fens_to_process=total_fens,
        batch_size=batch_size,
        nodes_limit=nodes_limit
    )
    
    return JSONResponse(
        status_code=202, # Accepted
        content={
            "message": f"Batch analysis job started on GPU {gpu_index}.",
            "gpu_index": gpu_index,
            "total_fens_to_process": total_fens,
            "batch_size": batch_size,
            "nodes_limit": nodes_limit
        }
    )

@router.post("/run_player_job")
async def api_run_player_analysis_job(
    background_tasks: BackgroundTasks, 
    data: Dict[str, Any] = Body(...)
):
    """
    Triggers a background task to analyze FENs specifically from
    a single player's games.
    
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
        nodes_limit = int(data.get("nodes_limit", 50000)) # Req 3
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Required: 'player_name' (str) and 'gpu_index' (int).")

    if gpu_index not in [0, 1]:
        raise HTTPException(status_code=400, detail="'gpu_index' must be 0 or 1.")

    print(f"Received PLAYER analysis job request for '{player_name}' on GPU {gpu_index}.")
    
    # Start the job in a background task
    background_tasks.add_task(
        run_player_analysis_job,
        player_name=player_name,
        gpu_index=gpu_index,
        total_fens_to_process=total_fens,
        batch_size=batch_size,
        nodes_limit=nodes_limit
    )
    
    return JSONResponse(
        status_code=202, # Accepted
        content={
            "message": f"Batch player analysis job for '{player_name}' started on GPU {gpu_index}.",
            "player_name": player_name,
            "gpu_index": gpu_index,
            "total_fens_to_process": total_fens,
            "batch_size": batch_size,
            "nodes_limit": nodes_limit
        }
    )