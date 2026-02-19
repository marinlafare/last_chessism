# chessism_api/routers/players.py

from fastapi import APIRouter, HTTPException, Body, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from typing import Dict, Any, List # <-- Import List
# --- MODIFIED: Import the new ask_db function ---
from chessism_api.database.ask_db import (
    get_player_fen_score_counts,
    get_player_performance_summary,
    get_player_modes_stats,
    get_player_mode_chart
)
# ---
# --- UPDATED IMPORTS ---
from chessism_api.operations.players import (
    get_current_players_with_games_in_db, 
    read_player, 
    insert_player,
    create_and_store_player_stats,
    read_player_stats,
    update_stats_for_all_primary_players,
    get_main_character_time_control_counts_payload,
    get_top_main_characters_by_time_control_payload
)
# ---

router = APIRouter()


@router.get("/main_characters/time_controls")
async def api_get_main_character_time_controls() -> JSONResponse:
    """
    Returns normalized game counts for bullet, blitz and rapid considering
    games with at least one main character.
    """
    result = await get_main_character_time_control_counts_payload()
    return JSONResponse(content=result)


@router.get("/main_characters/top")
async def api_get_top_main_characters(
    time_control: str = Query(..., pattern="^(bullet|blitz|rapid)$"),
    limit: int = Query(200, ge=1, le=5000)
) -> JSONResponse:
    """
    Returns top main characters for one time control.
    """
    result = await get_top_main_characters_by_time_control_payload(
        time_control=time_control,
        limit=limit
    )
    return JSONResponse(content=result)

# --- NEW ENDPOINT ---
@router.get("/{player_name}/game_count")
async def api_get_player_game_count(player_name: str) -> JSONResponse:
    """
    Returns the total number of games a player has in the database.
    """
    player_name_lower = player_name.lower()
    summary = await get_player_performance_summary(player_name_lower)
    
    if not summary or summary.get('total_games') is None:
        return JSONResponse(content={
            "player_name": player_name_lower,
            "total_games": 0,
            "message": "No games found for this player."
        })
        
    return JSONResponse(content={
        "player_name": player_name_lower,
        "total_games": summary['total_games']
    })
# --- END NEW ENDPOINT ---

@router.get("/{player_name}/fen_counts")
async def api_get_player_fen_counts(player_name: str) -> JSONResponse:
    """
    Returns the count of FENs with score 0, score != 0, and unscored (NULL) for a player.
    """
    player_name_lower = player_name.lower()
    counts = await get_player_fen_score_counts(player_name_lower)
    return JSONResponse(content=counts)

@router.get("/current_players")
async def api_get_current_players_with_games():
    """
    Fetches all players that have a full profile (joined != 0).
    """
    result = await get_current_players_with_games_in_db()
    
    # --- THIS IS THE FIX ---
    # The 'result' is a list of RowMapping objects, which are not
    # directly JSON serializable. We must convert them to plain dicts.
    content = [dict(row) for row in result]
    return JSONResponse(content=content)
    # --- END FIX ---

# --- RE-ORDERED: Specific routes first ---

@router.post("/update-all-stats")
async def api_update_all_stats(background_tasks: BackgroundTasks):
    """
    Triggers a long-running background task to update the stats
    for EVERY primary player in the database.
    
    Responds immediately with a "Job Started" message.
    """
    print("Received request to update all player stats.")
    background_tasks.add_task(update_stats_for_all_primary_players)
    return JSONResponse(
        status_code=202, # Accepted
        content={"message": "Batch job started: Updating stats for all primary players in the background."}
    )

@router.get("/{player_name}/stats")
async def api_get_player_stats(player_name: str) -> JSONResponse:
    """
    Fetches a player's stats from Chess.com and updates the local database.
    1. Always attempts to fetch fresh data from the Chess.com API.
    2. Saves the data to the DB (inserting or updating).
    3. Returns the fresh data.
    """
    player_name_lower = player_name.lower()
    
    print(f"Fetching fresh stats for {player_name_lower} from Chess.com...")
    try:
        # This function handles the full "fetch and upsert" logic
        new_stats_data = await create_and_store_player_stats(player_name_lower)
        
        if new_stats_data:
            print(f"Successfully fetched and upserted stats for {player_name_lower}.")
            # .model_dump() converts the Pydantic object to a dict
            return JSONResponse(content=new_stats_data.model_dump())
        else:
            # This means get_player_stats() returned None
            raise HTTPException(status_code=404, detail="Stats not found on Chess.com (or connection failed).")
            
    except Exception as e:
        print(f"Error during stats fetch for {player_name_lower}: {repr(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching stats.")


@router.get("/{player_name}/modes_stats")
async def api_get_modes_stats(player_name: str) -> JSONResponse:
    """
    Returns normalized mode statistics for the player.
    """
    player_name_lower = player_name.lower()
    data = await get_player_modes_stats(player_name_lower)
    return JSONResponse(content=data)


@router.get("/{player_name}/mode_chart")
async def api_get_mode_chart(
    player_name: str,
    mode: str = Query(..., min_length=1),
    range_type: str = Query("all"),
    years: int | None = Query(None, ge=1, le=20)
) -> JSONResponse:
    """
    Returns chart payload for one normalized mode of the player.
    """
    player_name_lower = player_name.lower()
    payload = await get_player_mode_chart(
        player_name_lower,
        mode,
        range_type=range_type,
        years=years
    )
    return JSONResponse(content=payload)


# --- RE-ORDERED: General route last ---

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
