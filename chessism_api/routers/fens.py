from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from arq.connections import ArqRedis

from chessism_api.redis_client import get_redis_pool
from chessism_api.database.ask_db import (
    get_top_fens, 
    get_sum_n_games, 
    get_top_fens_unscored,
    get_fen_analysis_counts,
    _get_remaining_fens_count_committed
)

router = APIRouter()


class FenGenerationRequest(BaseModel):
    total_games_to_process: int = Field(1_000_000, ge=1)
    batch_size: int = Field(1_000, ge=1)
    num_workers: int = Field(3, ge=1, le=16)


@router.post("/generate")
async def api_generate_fens(
    data: FenGenerationRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
):
    """
    Triggers the FEN generation pipeline.
    This enqueues a single 'boss' job which then coordinates
    the 3 parallel 'fen-worker' jobs.
    
    Payload: {"total_games_to_process": 400000, "batch_size": 1000}
    """
    total_games = data.total_games_to_process
    
    print(f"Enqueuing FEN Pipeline job for {total_games} games.")
    
    job = await redis.enqueue_job(
        'run_fen_pipeline',
        total_games_to_process=total_games,
        batch_size=data.batch_size,
        num_workers=data.num_workers,
        _queue_name='pipeline_queue'
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202,
        content={
            "message": f"FEN Generation Pipeline started for {total_games} total games.",
            "job_id": job_id
        }
    )


@router.get("/remaining_games")
async def api_get_remaining_games_needing_fens() -> JSONResponse:
    """
    Returns how many games still need FEN extraction.
    """
    remaining = await _get_remaining_fens_count_committed()
    return JSONResponse(content={"remaining_games": int(remaining or 0)})


@router.get("/analysis_counts")
async def api_get_fen_analysis_counts() -> JSONResponse:
    """
    Returns FEN analysis coverage counts.
    """
    counts = await get_fen_analysis_counts()
    return JSONResponse(content=counts)


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
        "total_sum_n_games": int(total_sum)
    })

# --- REMOVED: ADMIN ENDPOINTS ---
