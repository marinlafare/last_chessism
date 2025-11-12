#chessism_api/routers/fens.py

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException
from fastapi.responses import JSONResponse

# --- FIXED IMPORTS ---
from chessism_api.operations.fens import run_fen_generation_job
# --- UPDATED IMPORT ---
from chessism_api.database.ask_db import get_top_fens, get_sum_n_games
# ---
from typing import Dict, Any 

router = APIRouter()

# --- MODIFIED: Re-ordered parameters ---
# background_tasks (no default) must come BEFORE data (has default)
@router.post("/generate")
async def api_generate_fens(background_tasks: BackgroundTasks, data: Dict[str, int] = Body(...)):
    """
    Triggers a background task to generate FENs from un-processed games.
    
    Payload: {"total_games_to_process": 10000, "batch_size": 1000}
    """
    try:
        total_games = data.get("total_games_to_process", 1000000)
        batch_size = data.get("batch_size", 1000)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload format.")

    print(f"Received request to generate FENs. Target: {total_games} games.")
    
    # Start the job in a background task
    background_tasks.add_task(
        run_fen_generation_job,
        total_games_to_process=total_games,
        batch_size=batch_size
    )
    
    return JSONResponse(
        status_code=202, # Accepted
        content={"message": f"Batch job started: Generating FENs for {total_games} games."}
    )

# --- FINAL FIX: Route changed to /top to avoid internal conflicts with FastAPI's path handling ---
@router.get("/top")
async def api_get_top_fens(limit: int = 20) -> JSONResponse:
    """
    Retrieves the top N FENs with the highest n_games count and formats them as a string.
    """
    if limit <= 0 or limit > 100:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 100.")
        
    top_fens = await get_top_fens(limit)
    
    if not top_fens:
        return JSONResponse(content={"message": "No FEN data available in the database."}, status_code=200)

    # Format the data into the requested string format
    output_string = ""
    for index, fen_data in enumerate(top_fens):
        line = f"{index + 1}.- n_games: {fen_data['n_games']} (FEN: {fen_data['fen']})"
        if index < len(top_fens) - 1:
            line += "\n"
        output_string += line
        
    return JSONResponse(content={"results": output_string})


# --- NEW ENDPOINT ---
@router.get("/sum_n_games")
async def api_get_sum_n_games(threshold: int = 10) -> JSONResponse:
    """
    Calculates the sum of all n_games in the Fen table
    where n_games > threshold.
    """
    if threshold < 0:
        raise HTTPException(status_code=400, detail="Threshold must be a non-negative integer.")
        
    try:
        total_sum = await get_sum_n_games(threshold)
        
        return JSONResponse(content={
            "threshold": threshold,
            "total_n_games_sum": total_sum
        })
    except Exception as e:
        print(f"Error in /sum_n_games endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while calculating sum.")