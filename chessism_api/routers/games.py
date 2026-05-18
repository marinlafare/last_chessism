import asyncio
import json
import time
from typing import Any, Dict

from arq.connections import ArqRedis
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.games import (
    create_games,
    read_game,
    get_time_control_result_color_matrix_payload,
    get_time_control_game_length_analytics_payload,
    get_time_control_activity_trend_payload
)
from chessism_api.database.ask_db import (
    get_player_performance_summary,
    get_player_games_page,
    get_player_game_summary,
    get_player_mode_games,
    get_player_hours_played,
    get_player_time_control_top_moves,
    get_player_time_control_top_openings,
    get_player_time_control_results,
    get_player_time_control_lengths,
    get_player_time_control_activity_trend,
    get_games_database_generalities,
    get_time_control_mode_counts,
    get_rating_time_control_chart,
    get_time_control_top_moves,
    get_time_control_top_openings
)

router = APIRouter()
GAME_PIPELINE_LOCK = asyncio.Lock()
GAME_QUEUE_NAME = "games_queue"
PROGRESS_TTL_SECONDS = 60 * 60 * 24


def _payload_player_name(data: Dict[str, Any]) -> str:
    player_name = str(data.get("player_name", "")).strip().lower()
    if not player_name:
        raise HTTPException(status_code=400, detail="Payload must include 'player_name'.")
    return player_name


async def _has_active_game_job(redis: ArqRedis) -> bool:
    keys = await redis.keys("chessism:job_progress:*")
    for key in keys:
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
        except Exception:
            continue

        if payload.get("kind") != "game_update":
            continue
        if payload.get("phase") not in ("complete", "failed"):
            return True
    return False


async def _write_queued_game_job(redis: ArqRedis, job_id: str, player_name: str) -> None:
    payload = {
        "job_id": job_id,
        "kind": "game_update",
        "player_name": player_name,
        "total": 1,
        "processed": 0,
        "failed": 0,
        "phase": "queued",
        "detail": f"Queued update for {player_name}.",
        "result": None,
        "updated_at": time.time(),
    }
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS
    )

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
    move_color: str = Query("white", pattern="^(white|black)$", alias="player_color"),
    min_rating: int = Query(None),
    max_rating: int = Query(None),
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
        min_rating=min_rating,
        max_rating=max_rating,
        page=page,
        page_size=page_size,
        max_move=max_move
    )
    return JSONResponse(content=result)

@router.get("/time_controls/{mode}/top_openings")
async def api_get_time_control_top_openings(
    mode: str,
    min_rating: int = Query(None),
    max_rating: int = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=25),
    n_moves: int = Query(3, ge=3, le=10)
) -> JSONResponse:
    """
    Returns top openings for a normalized mode using the first n_moves full moves.
    """
    result = await get_time_control_top_openings(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating,
        page=page,
        page_size=page_size,
        n_moves=n_moves
    )
    return JSONResponse(content=result)

@router.get("/rating_time_control_chart")
async def api_get_rating_time_control_chart(
    time_control: str = Query(..., pattern="^(bullet|blitz|rapid)$")
) -> JSONResponse:
    """
    Returns histogram-ready ratings for a given normalized time control.
    """
    result = await get_rating_time_control_chart(time_control=time_control)
    return JSONResponse(content=result)


@router.get("/time_controls/{mode}/result_color_matrix")
async def api_get_time_control_result_color_matrix(
    mode: str,
    min_rating: int = Query(None),
    max_rating: int = Query(None)
) -> JSONResponse:
    """
    Returns white/black result matrix for mode and rating range.
    """
    result = await get_time_control_result_color_matrix_payload(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating
    )
    return JSONResponse(content=result)


@router.get("/time_controls/{mode}/game_length_analytics")
async def api_get_time_control_game_length_analytics(
    mode: str,
    min_rating: int = Query(None),
    max_rating: int = Query(None)
) -> JSONResponse:
    """
    Returns game-length summary and histograms for mode and rating range.
    """
    result = await get_time_control_game_length_analytics_payload(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating
    )
    return JSONResponse(content=result)


@router.get("/time_controls/{mode}/activity_trend")
async def api_get_time_control_activity_trend(
    mode: str,
    min_rating: int = Query(None),
    max_rating: int = Query(None)
) -> JSONResponse:
    """
    Returns activity heat data by month/day/hour for mode and rating range.
    """
    result = await get_time_control_activity_trend_payload(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating
    )
    return JSONResponse(content=result)


@router.get("/{link}")
async def api_read_game(link: str) -> JSONResponse:
    """
    Retrieves game information by its link.
    """
    print(f'api call for link: {link}')
    game = await read_game(link)
    if not game:
        raise HTTPException(status_code=404, detail=f"Game with link '{link}' not found.")
    return JSONResponse(content=game[0])

@router.post("")
async def api_create_game(
    data: Dict[str, Any] = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
) -> JSONResponse:
    """
    data = {"player_name": "some_player_name"}
    
    Fetches every available game for the player_name,
    formats them and inserts them into DB.
    """
    player_name = _payload_player_name(data)
    data["player_name"] = player_name
        
    if GAME_PIPELINE_LOCK.locked() or await _has_active_game_job(redis):
        return JSONResponse(
            status_code=409,
            content={"message": "Another download/update is running. Wait until it finishes."}
        )

    async with GAME_PIPELINE_LOCK:
        try:
            congratulation = await create_games(data)
            return JSONResponse(content={"message": congratulation})
        except Exception as error:
            print(f"Error creating games for {player_name}: {error}")
            return JSONResponse(
                status_code=500,
                content={"message": f"Failed to download games for {player_name}."}
            )


@router.post("/update")
async def api_update_player_games(
    data: Dict[str, Any] = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
) -> JSONResponse:
    """
    data = {"player_name": "some_player_name"}
    
    Fetches games from the last recorded month to the present.
    """
    player_name = _payload_player_name(data)
    data["player_name"] = player_name
        
    if GAME_PIPELINE_LOCK.locked() or await _has_active_game_job(redis):
        return JSONResponse(
            status_code=409,
            content={"message": "Another download/update is running. Wait until it finishes."}
        )

    try:
        job = await redis.enqueue_job(
            "run_update_player_games_job",
            data,
            _queue_name=GAME_QUEUE_NAME,
            _job_timeout=60 * 60 * 6
        )
        job_id = str(getattr(job, "job_id", job))
        await _write_queued_game_job(redis, job_id, player_name)
        return JSONResponse(
            status_code=202,
            content={
                "message": f"Games update for {player_name} enqueued on {GAME_QUEUE_NAME}.",
                "player_name": player_name,
                "job_id": job_id
            }
        )
    except Exception as error:
        print(f"Error enqueueing games update for {player_name}: {error}")
        return JSONResponse(
            status_code=500,
            content={"message": f"Failed to enqueue games update for {player_name}."}
        )


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


@router.get("/{player_name}/mode_games")
async def api_get_player_mode_games(
    player_name: str,
    mode: str = Query(..., pattern="^(bullet|blitz|rapid)$")
) -> JSONResponse:
    """
    Returns all games for a player in one normalized mode plus mode summary.
    """
    player_name_lower = player_name.lower()
    result = await get_player_mode_games(player_name_lower, mode=mode)
    return JSONResponse(content=result)


@router.get("/{player_name}/hours_played")
async def api_get_player_hours_played(player_name: str) -> JSONResponse:
    """
    Returns total played hours and per-mode played hours for a player.
    """
    player_name_lower = player_name.lower()
    result = await get_player_hours_played(player_name_lower)
    return JSONResponse(content=result)


@router.get("/{player_name}/time_controls/{mode}/top_moves")
async def api_get_player_time_control_top_moves(
    player_name: str,
    mode: str,
    move_color: str = Query("white", pattern="^(white|black)$", alias="player_color"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=10),
    max_move: int = Query(10, ge=1, le=30)
) -> JSONResponse:
    player_name_lower = player_name.lower()
    result = await get_player_time_control_top_moves(
        player_name=player_name_lower,
        mode=mode,
        move_color=move_color,
        page=page,
        page_size=page_size,
        max_move=max_move
    )
    return JSONResponse(content=result)


@router.get("/{player_name}/time_controls/{mode}/top_openings")
async def api_get_player_time_control_top_openings(
    player_name: str,
    mode: str,
    result_filter: str = Query("win", pattern="^(win|loss|draw)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=25),
    n_moves: int = Query(3, ge=3, le=10)
) -> JSONResponse:
    player_name_lower = player_name.lower()
    result = await get_player_time_control_top_openings(
        player_name=player_name_lower,
        mode=mode,
        result_filter=result_filter,
        page=page,
        page_size=page_size,
        n_moves=n_moves
    )
    return JSONResponse(content=result)


@router.get("/{player_name}/time_controls/{mode}/results")
async def api_get_player_time_control_results(player_name: str, mode: str) -> JSONResponse:
    player_name_lower = player_name.lower()
    result = await get_player_time_control_results(player_name=player_name_lower, mode=mode)
    return JSONResponse(content=result)


@router.get("/{player_name}/time_controls/{mode}/lengths")
async def api_get_player_time_control_lengths(player_name: str, mode: str) -> JSONResponse:
    player_name_lower = player_name.lower()
    result = await get_player_time_control_lengths(player_name=player_name_lower, mode=mode)
    return JSONResponse(content=result)


@router.get("/{player_name}/time_controls/{mode}/activity_trend")
async def api_get_player_time_control_activity_trend(player_name: str, mode: str) -> JSONResponse:
    player_name_lower = player_name.lower()
    result = await get_player_time_control_activity_trend(player_name=player_name_lower, mode=mode)
    return JSONResponse(content=result)
