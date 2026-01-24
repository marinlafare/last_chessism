#chessism_api/routers/fens.py

# --- MODIFIED: Imports ---
from fastapi import APIRouter, Body, HTTPException, Depends
from fastapi.responses import JSONResponse
from arq.connections import ArqRedis
import math # <-- Import math

# --- Import the job function (for reference) and redis client ---
# --- MODIFIED: Import the "boss" job ---
from chessism_api.operations.fens import run_fen_pipeline
from chessism_api.redis_client import get_redis_pool
# ---

# --- REMOVED: Admin Task Imports ---
# from chessism_api.database.db_interface import reset_all_game_fens_done_to_false
# from chessism_api.database.ask_db import delete_analysis_tables
# ---

from chessism_api.database.ask_db import (
    get_top_fens, 
    get_sum_n_games, 
    get_top_fens_unscored
)
from typing import Dict, Any 

router = APIRouter()

# --- THIS IS THE FIX ---
# This endpoint now enqueues ONE "boss" job
@router.post("/generate")
async def api_generate_fens(
    data: Dict[str, int] = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
):
    """
    Triggers the FEN generation pipeline.
    This enqueues a single 'boss' job which then coordinates
    the 3 parallel 'fen-worker' jobs.
    
    Payload: {"total_games_to_process": 400000, "batch_size": 1000}
    """
    try:
        total_games = data.get("total_games_to_process", 1000000)
        batch_size = data.get("batch_size", 1000)
        num_workers = 3
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload format.")
    
    print(f"Enqueuing FEN Pipeline job for {total_games} games.")
    
    # Enqueue 1 "boss" job
    await redis.enqueue_job(
        'run_fen_pipeline', # The "boss" job
        total_games_to_process=total_games,
        batch_size=batch_size,
        num_workers=num_workers,
        _queue_name='pipeline_queue' # Send to the pipeline worker
    )
    
    return JSONResponse(
        status_code=202, # Accepted
        content={"message": f"FEN Generation Pipeline started for {total_games} total games."}
    )
# --- END FIX ---


# --- (The rest of the router file is unchanged) ---
@router.get("/top")
async def api_get_top_fens(limit: int = 20) -> JSONResponse:
    """
    Retrieves the top N FENs based on the highest count of n_games.
    """
    fens = await get_top_fens(limit)
    
    if not fens:
        return JSONResponse(content={"results": "No FENs found in the database."})
    
    # Format the output for readability
    results_str = f"--- Top {limit} FENs by n_games ---\n"
    for i, fen_data in enumerate(fens):
        results_str += (
            f"{i+1: >3}. "
            f"n_games: {fen_data['n_games']: >6} | "
            f"Score: {fen_data['score']: >6} | "
            f"FEN: {fen_data['fen']}\n"
        )
    
    return JSONResponse(content={"results": results_str})

@router.get("/top_unscored")
async def api_get_top_fens_unscored(limit: int = 20) -> JSONResponse:
    """
    Retrieves the top N unscored FENs based on the highest count of n_games.
    """
    fens = await get_top_fens_unscored(limit)
    
    if not fens:
        return JSONResponse(content={"results": "No unscored FENs found in the database."})
    
    results_str = f"--- Top {limit} UNSCORED FENs by n_games ---\n"
    for i, fen_data in enumerate(fens):
        results_str += (
            f"{i+1: >3}. "
            f"n_games: {fen_data['n_games']: >6} | "
            f"FEN: {fen_data['fen']}\n"
        )
    
    return JSONResponse(content={"results": results_str})


@router.get("/sum_n_games")
async def api_get_sum_n_games(threshold: int = 10) -> JSONResponse:
    """
    Calculates the sum of all n_games where n_games > threshold.
    """
    total_sum = await get_sum_n_games(threshold)
    
    if total_sum is None:
        return JSONResponse(content={
            "threshold": threshold,
            "total_sum_n_games": 0
        })
        
    return JSONResponse(content={
        "threshold": threshold,
        "total_sum_n_games": total_sum
    })

# --- REMOVED: ADMIN ENDPOINTS ---
