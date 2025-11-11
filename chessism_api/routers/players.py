# chessism_api/router/players.py

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
# --- FIXED: Import the new function ---
from chessism_api.operations.players import get_current_players_with_games_in_db
from typing import Dict, Any

router = APIRouter()

@router.get("/current_players")
async def api_get_current_players_with_games():
    # --- FIXED: Uncommented and activated this endpoint ---
    result = await get_current_players_with_games_in_db()
    return result