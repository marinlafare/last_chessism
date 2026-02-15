# chessism_api/routers/games.py

# chessism_api/routers/games.py

from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body, HTTPException, Query # <-- Added HTTPException
from typing import Dict, Any # <-- Added typing

# --- FIXED IMPORTS ---
from chessism_api.operations.games import create_games, read_game, update_player_games
from chessism_api.database.ask_db import (
    get_player_performance_summary,
    get_player_games_page,
    get_player_game_summary,
    get_games_database_generalities,
    get_time_control_mode_counts,
    get_time_control_top_moves,
    get_time_control_top_openings
)
from chessism_api.database.ask_db import open_async_request # <-- Fixed import
# ---

router = APIRouter()

@router.get("/database/generalities")
async def api_get_games_database_generalities() -> JSONResponse:
    """
    Returns overall games/player summary values for the games dashboard.
    """
    result = await get_games_database_generalities()
    return JSONResponse(content=result)

@router.get("/generalities")
async def api_get_games_generalities_alias() -> JSONResponse:
    """
    Backward-compatible alias for games database generalities.
    """
    result = await get_games_database_generalities()
    return JSONResponse(content=result)

@router.get("/_database_generalities")
async def api_get_games_database_generalities_safe_alias() -> JSONResponse:
    """
    Safe alias that avoids collisions with dynamic routes.
    """
    result = await get_games_database_generalities()
    return JSONResponse(content=result)

@router.get("/time_controls")
async def api_get_time_control_mode_counts() -> JSONResponse:
    """
    Returns normalized game counts for bullet, blitz and rapid.
    """
    result = await get_time_control_mode_counts()
    return JSONResponse(content=result)

@router.get("/time_controls/{mode}/top_moves")
async def api_get_time_control_top_moves(
    mode: str,
    move_color: str = Query("white", regex="^(white|black)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=10),
    max_move: int = Query(10, ge=1, le=30)
) -> JSONResponse:
    """
    Returns the most played move by move number for a normalized mode.
    """
    result = await get_time_control_top_moves(
        mode=mode,
        move_color=move_color,
        page=page,
        page_size=page_size,
        max_move=max_move
    )
    return JSONResponse(content=result)

@router.get("/time_controls/{mode}/top_openings")
async def api_get_time_control_top_openings(
    mode: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=25),
    opening_depth_moves: int = Query(10, ge=1, le=20)
) -> JSONResponse:
    """
    Returns top openings for a normalized mode using move sequences.
    """
    result = await get_time_control_top_openings(
        mode=mode,
        page=page,
        page_size=page_size,
        opening_depth_moves=opening_depth_moves
    )
    return JSONResponse(content=result)


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
    
    Fetches every available game for the player_name,
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


@router.get("/{player_name}/count")
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


@router.get("/{player_name}/recent")
async def api_get_recent_player_games(
    player_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
) -> JSONResponse:
    """
    Returns paginated recent games with date/time, player color, and result.
    """
    player_name_lower = player_name.lower()
    result = await get_player_games_page(player_name_lower, page=page, page_size=page_size)
    return JSONResponse(content=result)


@router.get("/{player_name}/summary")
async def api_get_player_game_summary(player_name: str) -> JSONResponse:
    """
    Returns aggregate stats (wins/losses/draws), date range, and time control counts for a player.
    """
    player_name_lower = player_name.lower()
    result = await get_player_game_summary(player_name_lower)
    return JSONResponse(content=result)
