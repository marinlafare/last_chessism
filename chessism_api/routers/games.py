# chessism_api/routers/games.py

from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body, HTTPException
from typing import Dict, Any

from chessism_api.operations.games import create_games, read_game
# (Removed unused imports)

router = APIRouter()

# --- NO CHANGE NEEDED HERE ---
# FastAPI is smart enough to combine /games + /{link}
@router.get("/{link}")
async def api_read_game(link: str) -> JSONResponse:
    """
    Retrieves game information by its link.
    """
    print(f'api call for link: {link}')
    
    try:
        game = await read_game(link)
        if not game: 
            raise HTTPException(status_code=404, detail=f"Game with link '{link}' not found.")
        
        # read_game returns a list, so we return the first item
        return JSONResponse(content=game[0] if isinstance(game, list) and len(game) > 0 else game)
    except Exception as e:
        print(f"Error fetching game {link}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# --- FIX: Path changed from "/" to "" ---
# This makes the full path "/games" instead of "/games/"
@router.post("")
async def api_create_game(data: Dict[str, Any] = Body(...)) -> JSONResponse:
    """
    data = {"player_name": "some_player_name"}
    
    It fetches every available game for the player_name
    formats them and inserts them into DB.
    """
    if 'player_name' not in data:
        raise HTTPException(status_code=400, detail="Missing 'player_name' in request body.")

    congratulation_message = await create_games(data) 
    
    return JSONResponse(content={"message": congratulation_message})