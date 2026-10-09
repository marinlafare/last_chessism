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
    get_tracked_player_names,
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
from chessism_api.operations.player_hero_analytics import (
    get_player_behavioural_activity,
    get_player_behavioural_day,
    get_player_behavioural_ratings,
    get_player_game_measures,
    get_player_hour_measures,
    get_player_daily_game_cp,
    get_player_quality_calendar,
    get_player_range_game_scores,
)
from chessism_api.operations.player_hero_efficiency import get_player_daily_efficiency
from chessism_api.operations.player_game_explorer import explore_player_games
from chessism_api.operations.player_salience import get_player_daily_salience_accuracy
from chessism_api.operations.player_move_salience import get_player_game_salience

router = APIRouter()
PLAYER_DELETION_QUEUE = "games_queue"


class PlayerDeletionRequest(BaseModel):
    confirmation: str = Field(..., min_length=1)
    expected_exclusive_games: int = Field(..., ge=0)
    expected_shared_games: int = Field(..., ge=0)


class RangeGamesScoreRequest(BaseModel):
    game_ids: list[int] | None = Field(None, max_length=5_000)
    date_from: date | None = None
    date_to: date | None = None
    mode: str = Field("all", pattern="^(all|bullet|blitz|rapid)$")
    page: int = Field(1, ge=1)
    page_size: int = Field(100, ge=1, le=500)


class ExploreGamesRequest(BaseModel):
    scope: str = Field(..., pattern="^(date|hour|weekday|weekday_hour)$")
    modes: list[str] = Field(..., min_length=1, max_length=3)
    date: str | None = Field(None, min_length=10, max_length=10)
    weekday: int | None = Field(None, ge=1, le=7)
    hour: int | None = Field(None, ge=0, le=23)
    analyzed_only: bool = True
    minimum_game_moves_exclusive: int | None = Field(None, ge=0, le=1_000)
    limit: int = Field(30, ge=1, le=100)
    cursor: str | None = Field(None, max_length=512)


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


async def _hero_analytics_response(operation, *args, **kwargs) -> JSONResponse:
    try:
        payload = await operation(*args, **kwargs)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    return JSONResponse(content=payload)


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


@router.get("/navigation")
async def api_get_player_navigation() -> JSONResponse:
    """Return the alphabetical active-player list used by hero navigation."""
    return JSONResponse(content={"players": await get_tracked_player_names()})


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


@router.get("/{player_name}/analysis/behavioural/activity")
async def api_get_player_behavioural_activity(
    player_name: str,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
) -> JSONResponse:
    """Return dense weekday/hour distributions in the player's resolved timezone."""
    return await _hero_analytics_response(
        get_player_behavioural_activity,
        player_name,
        mode,
        date_from,
        date_to,
    )


@router.get("/{player_name}/analysis/behavioural/ratings")
async def api_get_player_behavioural_ratings(
    player_name: str,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
) -> JSONResponse:
    """Return dense daily last-rating series in the player's resolved timezone."""
    return await _hero_analytics_response(
        get_player_behavioural_ratings,
        player_name,
        mode,
        date_from,
        date_to,
    )


@router.get("/{player_name}/analysis/behavioural/days/{target_date}")
async def api_get_player_behavioural_day(
    player_name: str,
    target_date: date,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
) -> JSONResponse:
    """Return rating observations and results for one resolved local calendar day."""
    return await _hero_analytics_response(
        get_player_behavioural_day,
        player_name,
        target_date,
        mode,
    )


@router.get("/{player_name}/analysis/measures/quality-calendar")
async def api_get_player_quality_calendar(
    player_name: str,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    timezone: str | None = Query(None, min_length=1, max_length=64),
) -> JSONResponse:
    """Return player-perspective CP gains/losses grouped by local play time."""
    return await _hero_analytics_response(
        get_player_quality_calendar,
        player_name,
        mode,
        date_from,
        date_to,
        timezone,
    )


@router.get("/{player_name}/analysis/measures/daily-game-cp")
async def api_get_player_daily_game_cp(
    player_name: str,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    timezone: str | None = Query(None, min_length=1, max_length=64),
) -> JSONResponse:
    """Return the summed player-oriented game CP for every active local day."""
    return await _hero_analytics_response(
        get_player_daily_game_cp,
        player_name,
        mode,
        date_from,
        date_to,
        timezone,
    )


@router.get("/{player_name}/analysis/measures/daily-efficiency")
async def api_get_player_daily_efficiency(
    player_name: str,
    mode: str = Query("all", min_length=1, max_length=32),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    timezone: str | None = Query(None, min_length=1, max_length=64),
) -> JSONResponse:
    """Return average Lichess-style game efficiency per active local day."""
    return await _hero_analytics_response(
        get_player_daily_efficiency,
        player_name,
        mode,
        date_from,
        date_to,
        timezone,
    )


@router.get("/{player_name}/analysis/measures/daily-salience-accuracy")
async def api_get_player_daily_salience_accuracy(
    player_name: str,
    mode: str = Query("all", min_length=1, max_length=32),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    timezone: str | None = Query(None, min_length=1, max_length=64),
) -> JSONResponse:
    """Return salience-weighted Accuracy and effective games per local day."""
    return await _hero_analytics_response(
        get_player_daily_salience_accuracy,
        player_name,
        mode,
        date_from,
        date_to,
        timezone,
    )


@router.get("/{player_name}/analysis/salience/games/{game_id}")
async def api_get_player_game_salience(
    player_name: str,
    game_id: int,
) -> JSONResponse:
    """Return every occurrence-level salience contribution in one player game."""
    return await _hero_analytics_response(
        get_player_game_salience,
        player_name,
        game_id,
    )


@router.post("/{player_name}/analysis/measures/range-games-score")
async def api_get_player_range_game_scores(
    player_name: str,
    request: RangeGamesScoreRequest,
) -> JSONResponse:
    """Return one compact player-perspective score for every selected game."""
    return await _hero_analytics_response(
        get_player_range_game_scores,
        player_name,
        game_ids=request.game_ids,
        date_from=request.date_from,
        date_to=request.date_to,
        mode=request.mode,
        page=request.page,
        page_size=request.page_size,
    )


@router.post("/{player_name}/analysis/measures/explore-games")
async def api_explore_player_games(
    player_name: str,
    request: ExploreGamesRequest,
) -> JSONResponse:
    """Return compact analyzed games from one player chart bin."""
    return await _hero_analytics_response(
        explore_player_games,
        player_name,
        scope_kind=request.scope,
        modes=request.modes,
        target_date=request.date,
        weekday=request.weekday,
        hour=request.hour,
        analyzed_only=request.analyzed_only,
        minimum_game_moves_exclusive=request.minimum_game_moves_exclusive,
        limit=request.limit,
        cursor=request.cursor,
    )


@router.get("/{player_name}/analysis/measures/games/{game_id}")
async def api_get_player_game_measures(
    player_name: str,
    game_id: int,
    timezone: str | None = Query(None, min_length=1, max_length=64),
) -> JSONResponse:
    """Return the complete ordered, player-perspective score series for a game."""
    return await _hero_analytics_response(
        get_player_game_measures,
        player_name,
        game_id,
        timezone,
    )


@router.get("/{player_name}/analysis/measures/days/{target_date}/hours/{hour}")
async def api_get_player_hour_measures(
    player_name: str,
    target_date: date,
    hour: int,
    mode: str = Query("all", pattern="^(all|bullet|blitz|rapid)$"),
    timezone: str | None = Query(None, min_length=1, max_length=64),
    limit_games: int = Query(20, ge=1, le=100),
    cursor: str | None = Query(None, max_length=512),
) -> JSONResponse:
    """Return paginated score sequences for games started in one local hour."""
    return await _hero_analytics_response(
        get_player_hour_measures,
        player_name,
        target_date,
        hour,
        mode,
        timezone,
        limit_games,
        cursor,
    )


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
