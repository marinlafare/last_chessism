import json
from datetime import date

from arq.connections import ArqRedis
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Body, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from chessism_api.redis_client import get_redis_pool
from chessism_api.database.ask_db import (
    get_player_fen_score_counts,
    get_player_neighbors,
    get_player_performance_summary,
    get_player_modes_stats,
    get_player_mode_chart
)
from chessism_api.operations.players import (
    get_current_players_with_games_in_db, 
    read_player, 
    insert_player,
    create_and_store_player_stats,
    update_stats_for_all_primary_players,
    get_main_character_time_control_counts_payload,
    get_top_main_characters_by_time_control_payload
)
from chessism_api.operations.player_deletion import (
    find_player_deletion_conflict,
    get_player_deletion_preview,
    write_player_deletion_progress,
)
from chessism_api.operations.player_analytics import (
    PLAYER_ANALYTICS_CACHE_SECONDS,
    get_player_engine_insights,
    get_player_playing_patterns,
    normalize_player_analytics_filters,
    player_analytics_cache_key,
)

router = APIRouter()
PLAYER_DELETION_QUEUE = "games_queue"


class PlayerDeletionRequest(BaseModel):
    confirmation: str = Field(..., min_length=1)
    expected_exclusive_games: int = Field(..., ge=0)
    expected_shared_games: int = Field(..., ge=0)


async def _get_cached_player_analysis(
    redis: ArqRedis,
    kind: str,
    filters: dict,
) -> dict:
    cache_key = player_analytics_cache_key(kind, filters)
    cached = await redis.get(cache_key)
    if cached:
        if isinstance(cached, bytes):
            cached = cached.decode("utf-8")
        try:
            payload = json.loads(cached)
            if isinstance(payload, dict):
                payload["cache_hit"] = True
                return payload
        except (TypeError, ValueError):
            pass

    if kind == "engine":
        payload = await get_player_engine_insights(filters)
    else:
        payload = await get_player_playing_patterns(filters)
    await redis.set(
        cache_key,
        json.dumps(payload),
        ex=PLAYER_ANALYTICS_CACHE_SECONDS,
    )
    payload["cache_hit"] = False
    return payload


def _analysis_filters(
    player_name: str,
    mode: str,
    date_from: date | None,
    date_to: date | None,
) -> dict:
    try:
        return normalize_player_analytics_filters(
            player_name,
            mode,
            date_from,
            date_to,
        )
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


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
    return JSONResponse(content=result)


@router.get("/{player_name}/neighbors")
async def api_get_player_neighbors(player_name: str) -> JSONResponse:
    """Return the previous and next full player profiles alphabetically."""
    result = await get_player_neighbors(player_name.strip().lower())
    return JSONResponse(content=result)


@router.get("/{player_name}/analysis/engine")
async def api_get_player_engine_insights(
    player_name: str,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Return bounded, engine-derived aggregates for the player dashboard."""
    filters = _analysis_filters(player_name, mode, date_from, date_to)
    payload = await _get_cached_player_analysis(redis, "engine", filters)
    return JSONResponse(content=payload)


@router.get("/{player_name}/analysis/patterns")
async def api_get_player_playing_patterns(
    player_name: str,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Return bounded game, rating, opening and clock aggregates."""
    filters = _analysis_filters(player_name, mode, date_from, date_to)
    payload = await _get_cached_player_analysis(redis, "patterns", filters)
    return JSONResponse(content=payload)


@router.get("/{player_name}/deletion-preview")
async def api_get_player_deletion_preview(player_name: str) -> JSONResponse:
    """Preview the exact exclusive/shared split without changing data."""
    normalized_player = player_name.strip().lower()
    try:
        preview = await get_player_deletion_preview(normalized_player)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    if preview["already_deleted"]:
        raise HTTPException(status_code=409, detail="This player was already deleted.")
    if not preview["is_main_player"]:
        raise HTTPException(status_code=409, detail="Only a main player can be deleted.")
    return JSONResponse(content=preview)


@router.delete("/{player_name}")
async def api_delete_player(
    player_name: str,
    data: PlayerDeletionRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Queue a confirmed, preview-checked player deletion."""
    normalized_player = player_name.strip().lower()
    if data.confirmation.strip().lower() != normalized_player:
        raise HTTPException(status_code=422, detail="Type the exact player username to confirm deletion.")

    try:
        preview = await get_player_deletion_preview(normalized_player)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    if not preview["is_main_player"]:
        raise HTTPException(status_code=409, detail="Only an active main player can be deleted.")
    if (
        preview["exclusive_games"] != data.expected_exclusive_games
        or preview["shared_games"] != data.expected_shared_games
    ):
        raise HTTPException(status_code=409, detail="Player game counts changed; review a new deletion preview.")

    conflict = await find_player_deletion_conflict(redis, normalized_player)
    if conflict:
        raise HTTPException(status_code=409, detail=conflict)

    job = await redis.enqueue_job(
        "run_delete_player_job",
        player_name=normalized_player,
        expected_exclusive_games=data.expected_exclusive_games,
        expected_shared_games=data.expected_shared_games,
        _queue_name=PLAYER_DELETION_QUEUE,
        _job_timeout=60 * 60 * 24,
    )
    job_id = str(getattr(job, "job_id", job))
    await write_player_deletion_progress(
        redis,
        job_id,
        player_name=normalized_player,
        total=data.expected_exclusive_games,
        processed=0,
        phase="queued",
        detail=f"Queued deletion for {normalized_player}.",
    )
    return JSONResponse(status_code=202, content={
        "message": f"Player deletion for {normalized_player} was queued.",
        "job_id": job_id,
        "player_name": normalized_player,
        "exclusive_games": data.expected_exclusive_games,
        "shared_games": data.expected_shared_games,
        "backup_location": preview["backup_location"],
    })

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
            return JSONResponse(content=new_stats_data.model_dump())
        else:
            raise HTTPException(status_code=404, detail="Stats not found on Chess.com (or connection failed).")
    except HTTPException:
        raise
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
            return JSONResponse(content=new_player_data.model_dump())
        else:
            raise HTTPException(status_code=404, detail="Player not found in database or on Chess.com (or connection failed).")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error during insert_player fetch for {player_name}: {repr(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching the player.")
