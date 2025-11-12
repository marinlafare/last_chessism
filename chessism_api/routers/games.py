# chessism_api/routers/games.py

# chessism_api/routers/games.py

from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body, HTTPException # <-- Added HTTPException
from typing import Dict, Any # <-- Added typing

# --- FIXED IMPORTS ---
from chessism_api.operations.games import create_games, read_game, update_player_games
from chessism_api.database.ask_db import open_async_request # <-- Fixed import
# ---

router = APIRouter()


@router.get("/{link}") # --- FIX: Removed '/games' prefix ---
async def api_read_game(link: str) -> JSONResponse:
    """
    Retrieves game information by its link.
    """
    print(f'api call for link: {link}')
    try:
        game = await read_game(link)
        if not game: # Check if game list is empty
            raise HTTPException(status_code=404, detail=f"Game with link '{link}' not found.")
        # Return the first game found (links should be unique)
        return JSONResponse(content=game[0])
    except Exception as e:
        print(f"Error fetching game {link}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("") # --- FIX: Changed route from "/" to "" ---
async def api_create_game(data: Dict[str, Any] = Body(...)) -> JSONResponse: # <-- Use Dict
    """
    data = {"player_name": "some_player_name"}
    
    Fetches every available game for the player_name
    formats them and inserts them into DB.
    """
    try:
        player_name = data["player_name"]
    except KeyError:
        raise HTTPException(status_code=400, detail="Payload must include 'player_name'.")
        
    congratulation = await create_games(data)
    return JSONResponse(content={"message": congratulation})


# --- NEW ENDPOINT ---
@router.post("/update")
async def api_update_player_games(data: Dict[str, Any] = Body(...)) -> JSONResponse:
    """
    data = {"player_name": "some_player_name"}
    
    Fetches games from the last recorded month to the present.
    """
    try:
        player_name = data["player_name"]
    except KeyError:
        raise HTTPException(status_code=400, detail="Payload must include 'player_name'.")
        
    message = await update_player_games(data)
    return JSONResponse(content={"message": message})