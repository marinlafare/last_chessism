# chessism_api/router/players.py

from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import JSONResponse
from typing import Dict, Any

# --- UPDATED IMPORTS ---
from chessism_api.operations.players import (
    get_current_players_with_games_in_db, 
    read_player, 
    insert_player,
    create_and_store_player_stats # <-- NEW
)
# ---

router = APIRouter()

@router.get("/current_players")
async def api_get_current_players_with_games():
    """
    Fetches all players that have a full profile (joined != 0).
    """
    result = await get_current_players_with_games_in_db()
    return JSONResponse(content=result)


@router.get("/{player_name}")
async def api_get_player_profile(player_name: str) -> JSONResponse:
    """
    Fetches a player's profile.
    1. Tries to read from the local database.
    2. If not found, attempts to fetch from Chess.com and save.
    """
    # 1. Try to read from the database first
    player_data = await read_player(player_name)
    
    if player_data:
        print(f"Found player {player_name} in database.")
        return JSONResponse(content=player_data)

    # 2. If not in DB, try to fetch from Chess.com (which also saves it)
    print(f"Player {player_name} not in DB. Fetching from Chess.com...")
    try:
        new_player_data = await insert_player({"player_name": player_name})
        
        if new_player_data:
            print(f"Successfully fetched and saved {player_name}.")
            # .model_dump() converts the Pydantic object to a dict for the JSON response
            return JSONResponse(content=new_player_data.model_dump())
        else:
            # This means get_profile() returned None (e.g., ConnectTimeout or 404 from Chess.com)
            raise HTTPException(status_code=404, detail="Player not found in database or on Chess.com (or connection failed).")
            
    except Exception as e:
        print(f"Error during insert_player fetch for {player_name}: {repr(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching the player.")


# --- NEW ENDPOINT for Player Stats ---
@router.get("/{player_name}/stats")
async def api_get_player_stats(player_name: str) -> JSONResponse:
    """
    Fetches a player's stats.
    1. Tries to read from the local database.
    2. If not found, attempts to fetch from Chess.com, save, and return.
    """
    player_name_lower = player_name.lower()
    
    # 1. Try to read from the database first
    stats_data = await read_player_stats(player_name_lower)
    
    if stats_data:
        print(f"Found stats for {player_name_lower} in database.")
        return JSONResponse(content=stats_data)

    # 2. If not in DB, try to fetch, store, and return
    print(f"Stats for {player_name_lower} not in DB. Fetching from Chess.com...")
    try:
        new_stats_data = await create_and_store_player_stats(player_name_lower)
        
        if new_stats_data:
            print(f"Successfully fetched and saved stats for {player_name_lower}.")
            # .model_dump() converts the Pydantic object to a dict
            return JSONResponse(content=new_stats_data.model_dump())
        else:
            # This means get_player_stats() returned None
            raise HTTPException(status_code=404, detail="Stats not found on Chess.com (or connection failed).")
            
    except Exception as e:
        print(f"Error during stats fetch for {player_name_lower}: {repr(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching stats.")