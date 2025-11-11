# chessism_api/routers/games.py

from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body, HTTPException # <-- FIXED IMPORTS
from typing import Dict, Any

# --- FIXED IMPORTS ---
# Removed update_player_games (does not exist in operations/games.py)
from chessism_api.operations.games import create_games, read_game
# Removed get_principal_players (does not exist in database/ask_db.py)
# ---

router = APIRouter()

@router.get("/games/{link}")
async def api_read_game(link: str) -> JSONResponse:
    """
    Retrieves game information by its link.
    """
    print(f'api call for link: {link}')
    
    # --- FIX: Removed unreachable 'return' and enabled try/except ---
    try:
        game = await read_game(link)
        if not game: # Check if game list is empty or None
            raise HTTPException(status_code=404, detail=f"Game with link '{link}' not found.")
        # read_game returns a list, so we return the first item or the list
        return JSONResponse(content=game[0] if isinstance(game, list) and len(game) > 0 else game)
    except Exception as e:
        print(f"Error fetching game {link}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# --- FIX: Changed path from "/games/{player_name}" to "/games" ---
# This matches the function signature expecting a JSON Body.
@router.post("/games")
async def api_create_game(data: Dict[str, Any] = Body(...)) -> JSONResponse:
    """
    data = {"player_name": "some_player_name"}
    
    It fetches every available game for the player_name
    formats them and inserts them into DB.
    """
    if 'player_name' not in data:
        raise HTTPException(status_code=400, detail="Missing 'player_name' in request body.")

    # create_games expects the dict, e.g., {"player_name": "hikaru"}
    congratulation_message = await create_games(data) 
    
    # create_games returns a string, wrap it in a JSON response
    return JSONResponse(content={"message": congratulation_message})