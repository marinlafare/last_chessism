from datetime import date, datetime, time as datetime_time, timedelta, timezone
import json
from typing import Any
from uuid import uuid4

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from arq.connections import ArqRedis
import math

from chessism_api.redis_client import get_redis_pool
from chessism_api.operations.analysis import (
    ENGINE_URL,
    MAX_ANALYSIS_BATCH_SIZE,
    MAX_LOOP_ANALYSIS_BATCH_SIZE,
    timing_rows_from_engine_results,
)
from chessism_api.operations.analysis_backups import (
    BACKUP_DISPLAY_DIR,
    list_fen_analysis_backups,
)
from chessism_api.operations.analysis_times import record_analysis_times
from chessism_api.operations.tablebase import ensure_tablebase_analysis_enqueued
from chessism_api.database.ask_db import (
    get_player_game_analysis_scope,
    preview_player_games_for_analysis,
)

router = APIRouter()
PLAYER_GAME_PLAN_TTL_SECONDS = 60 * 60
PLAYER_GAME_ANALYSIS_RATE = 2.2
PLAYER_GAME_ANALYSIS_CHUNK_SIZE = 5_000


class AnalysisJobRequest(BaseModel):
    total_fens_to_process: int = Field(1_000_000, ge=1)
    batch_size: int = Field(
        MAX_ANALYSIS_BATCH_SIZE,
        ge=1,
        le=MAX_ANALYSIS_BATCH_SIZE,
    )
    nodes_limit: int = Field(1_000_000, ge=1)


class PlayerAnalysisJobRequest(AnalysisJobRequest):
    total_fens_to_process: int = Field(100_000, ge=1)
    player_name: str = Field(..., min_length=1)


class AnalysisLoopJobRequest(BaseModel):
    scope: str = Field("all", pattern="^(all|player)$")
    player_name: str | None = Field(None)
    positions_per_run: int = Field(20_000, ge=1, le=1_000_000)
    runs: int = Field(4, ge=1, le=100)
    batches: int = Field(500, ge=1, le=MAX_LOOP_ANALYSIS_BATCH_SIZE)
    cool_off: int = Field(300, ge=0, le=3_600)
    nodes_limit: int = Field(1_000_000, ge=1)


class FenAnalysisRequest(BaseModel):
    fens: list[str] = Field(..., min_length=1, max_length=20)
    nodes_limit: int = Field(1_000_000, ge=1, le=100_000_000)
    multipv: int = Field(4, ge=1, le=10)


class TablebaseAnalysisRequest(BaseModel):
    max_positions: int | None = Field(None, ge=1)
    batch_size: int = Field(1_000, ge=1, le=5_000)


class PlayerGameScopeRequest(BaseModel):
    player_name: str = Field(..., min_length=1)
    selection_mode: str = Field("latest", pattern="^(latest|oldest|range|fair_range)$")
    date_from: date | None = None
    date_to: date | None = None


class PlayerGamePreviewRequest(PlayerGameScopeRequest):
    game_limit: int | None = Field(None, ge=1, le=50_000)
    use_all: bool = False
    range_order: str = Field("latest", pattern="^(latest|oldest)$")


class PlayerGameAnalysisConfirmRequest(BaseModel):
    plan_id: str = Field(..., min_length=1)
    batch_size: int = Field(500, ge=1, le=MAX_LOOP_ANALYSIS_BATCH_SIZE)
    cool_off: int = Field(120, ge=0, le=3_600)
    nodes_limit: int = Field(1_000_000, ge=1)


def _analysis_timeout(total_fens: int) -> int:
    return math.ceil(total_fens * 2.1) + 600


def _enqueued_job_id(job: Any) -> str:
    if job is None:
        raise HTTPException(status_code=503, detail="The job could not be queued")
    return str(getattr(job, "job_id", job))


def _player_game_date_bounds(
    data: PlayerGameScopeRequest,
) -> tuple[datetime | None, datetime | None]:
    if data.selection_mode != "range":
        return None, None
    if data.date_from is None or data.date_to is None:
        raise HTTPException(status_code=422, detail="Start and end dates are required for a date range")
    if data.date_from > data.date_to:
        raise HTTPException(status_code=422, detail="Start date must be on or before end date")
    date_from = datetime.combine(data.date_from, datetime_time.min, tzinfo=timezone.utc)
    date_to_exclusive = datetime.combine(
        data.date_to + timedelta(days=1),
        datetime_time.min,
        tzinfo=timezone.utc,
    )
    return date_from, date_to_exclusive


@router.post("/fen")
async def api_analyze_fen(data: FenAnalysisRequest = Body(...)) -> list[dict[str, Any]]:
    """
    Runs immediate Stockfish analysis for user-supplied FEN positions.
    """
    payload = {
        "fens": data.fens,
        "nodes_limit": data.nodes_limit,
        "multipv": data.multipv,
    }

    try:
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.post(ENGINE_URL, json=payload)
            response.raise_for_status()
            result = response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=exc.response.text or "Stockfish analysis failed",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail=f"Stockfish service unavailable: {exc}") from exc

    if not isinstance(result, list):
        raise HTTPException(status_code=502, detail="Unexpected Stockfish response")

    await record_analysis_times(timing_rows_from_engine_results(
        data.fens,
        result,
        source="manual",
        nodes_limit=data.nodes_limit,
        multipv=data.multipv,
    ))

    return result


@router.post("/tablebase")
async def api_queue_tablebase_analysis(
    data: TablebaseAnalysisRequest = Body(default=TablebaseAnalysisRequest()),
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Cache exact Syzygy results for eligible, unscored game positions."""
    result = await ensure_tablebase_analysis_enqueued(
        redis,
        max_positions=data.max_positions,
        batch_size=data.batch_size,
    )
    if result["status"] == "up_to_date":
        return JSONResponse(content={
            "message": "Every eligible endgame position is already cached.",
            **result,
        })
    return JSONResponse(status_code=202, content={
        "message": (
            f"Syzygy caching queued for {result['pending']} positions."
            if result["status"] == "queued"
            else "Syzygy caching is already running."
        ),
        **result,
    })


@router.post("/run_job")
async def api_run_analysis_job(
    data: AnalysisJobRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
):
    """
    Enqueues a job to analyze FENs from the main database pool.
    
    Payload:
    {
        "total_fens_to_process": 100000,
        "batch_size": 100,
        "nodes_limit": 1000000
    }
    """
    total_fens = data.total_fens_to_process
    queue_name = "analysis_queue"
    calculated_timeout = _analysis_timeout(total_fens)
    
    print(f"Enqueuing analysis job on {queue_name}. Target: {total_fens} FENs. Calculated timeout: {calculated_timeout}s")
    
    job = await redis.enqueue_job(
        'run_analysis_job',
        total_fens_to_process=total_fens,
        batch_size=data.batch_size,
        nodes_limit=data.nodes_limit,
        _queue_name=queue_name,
        _job_timeout=calculated_timeout
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202,
        content={
            "message": f"Batch analysis job enqueued on {queue_name}.",
            "total_fens_to_process": total_fens,
            "job_id": job_id
        }
    )


@router.post("/run_player_job")
async def api_run_player_analysis_job(
    data: PlayerAnalysisJobRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool)
):
    """
    Enqueues a job to analyze FENs for a specific player.
    
    Payload:
    {
        "player_name": "hikaru",
        "total_fens_to_process": 1000,
        "batch_size": 50,
        "nodes_limit": 1000000
    }
    """
    player_name = data.player_name.lower()
    total_fens = data.total_fens_to_process
    queue_name = "analysis_queue"
    calculated_timeout = _analysis_timeout(total_fens)

    print(f"Enqueuing PLAYER analysis job for '{player_name}' on {queue_name}. Calculated timeout: {calculated_timeout}s")
    
    job = await redis.enqueue_job(
        'run_player_analysis_job',
        player_name=player_name,
        total_fens_to_process=total_fens,
        batch_size=data.batch_size,
        nodes_limit=data.nodes_limit,
        _queue_name=queue_name,
        _job_timeout=calculated_timeout
    )
    job_id = str(getattr(job, "job_id", job))
    
    return JSONResponse(
        status_code=202,
        content={
            "message": f"Batch player analysis job for '{player_name}' enqueued on {queue_name}.",
            "player_name": player_name,
            "job_id": job_id
        }
    )


@router.post("/run_loop_job")
async def api_run_analysis_loop_job(
    data: AnalysisLoopJobRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Enqueue several sequential all-position or player-position analysis runs."""
    player_name = str(data.player_name or "").strip().lower() or None
    if data.scope == "player" and not player_name:
        raise HTTPException(status_code=422, detail="Player name is required for player loops")

    total_fens = data.positions_per_run * data.runs
    total_cool_off = data.cool_off * max(0, data.runs - 1)
    calculated_timeout = _analysis_timeout(total_fens) + total_cool_off
    queue_name = "analysis_queue"
    job = await redis.enqueue_job(
        "run_analysis_loop_job",
        scope=data.scope,
        player_name=player_name,
        positions_per_run=data.positions_per_run,
        runs=data.runs,
        batches=data.batches,
        cool_off=data.cool_off,
        nodes_limit=data.nodes_limit,
        _queue_name=queue_name,
        _job_timeout=calculated_timeout,
    )
    job_id = _enqueued_job_id(job)

    return JSONResponse(
        status_code=202,
        content={
            "message": f"{data.runs} analysis runs enqueued on {queue_name}.",
            "job_id": job_id,
            "scope": data.scope,
            "player_name": player_name,
            "runs": data.runs,
            "positions_per_run": data.positions_per_run,
            "total_fens_to_process": total_fens,
            "batches": data.batches,
            "cool_off": data.cool_off,
        },
    )


@router.post("/player_games/scope")
async def api_get_player_game_analysis_scope(
    data: PlayerGameScopeRequest = Body(...),
) -> dict[str, Any]:
    """Inspect how many complete and incomplete games exist in a player selection."""
    player_name = data.player_name.strip().lower()
    if not player_name:
        raise HTTPException(status_code=422, detail="Player name is required")
    date_from, date_to_exclusive = _player_game_date_bounds(data)
    scope = await get_player_game_analysis_scope(
        player_name,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
    )
    return {
        **scope,
        "selection_mode": data.selection_mode,
        "date_from": data.date_from.isoformat() if data.date_from else None,
        "date_to": data.date_to.isoformat() if data.date_to else None,
    }


@router.post("/player_games/preview")
async def api_preview_player_game_analysis(
    data: PlayerGamePreviewRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool),
) -> dict[str, Any]:
    """Select and freeze exact incomplete games, then report their Stockfish workload."""
    player_name = data.player_name.strip().lower()
    if not player_name:
        raise HTTPException(status_code=422, detail="Player name is required")
    date_from, date_to_exclusive = _player_game_date_bounds(data)

    if data.selection_mode == "range":
        order = data.range_order
        game_limit = None if data.use_all else data.game_limit
        if not data.use_all and game_limit is None:
            raise HTTPException(status_code=422, detail="Choose all games or enter a game limit")
    elif data.selection_mode == "fair_range":
        order = "fair_range"
        game_limit = data.game_limit
        if game_limit is None:
            raise HTTPException(status_code=422, detail="Enter the number of games to sample")
    else:
        order = data.selection_mode
        game_limit = data.game_limit
        if game_limit is None:
            raise HTTPException(status_code=422, detail="Enter the number of games to select")

    scope = await get_player_game_analysis_scope(
        player_name,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
    )
    if int(scope.get("incomplete_games") or 0) <= 0:
        raise HTTPException(status_code=409, detail="No incomplete games with FENs were found in this selection")

    preview = await preview_player_games_for_analysis(
        player_name,
        order=order,
        game_limit=game_limit,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
    )
    if int(preview.get("selected_games") or 0) <= 0:
        raise HTTPException(status_code=409, detail="No games could be selected")

    plan_id = uuid4().hex
    stockfish_fens = max(
        0,
        int(preview.get("fens_to_analyze") or 0)
        - int(preview.get("tablebase_fens") or 0),
    )
    estimated_seconds = math.ceil(stockfish_fens / PLAYER_GAME_ANALYSIS_RATE)
    plan = {
        **preview,
        "plan_id": plan_id,
        "selection_mode": data.selection_mode,
        "selection_order": order,
        "date_from": data.date_from.isoformat() if data.date_from else None,
        "date_to": data.date_to.isoformat() if data.date_to else None,
        "requested_games": game_limit,
        "stockfish_fens": stockfish_fens,
        "estimated_seconds": estimated_seconds,
    }
    await redis.set(
        f"chessism:player_game_analysis_plan:{plan_id}",
        json.dumps(plan),
        ex=PLAYER_GAME_PLAN_TTL_SECONDS,
    )
    public_plan = {key: value for key, value in plan.items() if key != "game_links"}
    return {
        **public_plan,
        **scope,
        "plan_expires_in": PLAYER_GAME_PLAN_TTL_SECONDS,
    }


@router.post("/player_games/run_job")
async def api_run_player_game_analysis_job(
    data: PlayerGameAnalysisConfirmRequest = Body(...),
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Confirm a frozen preview and queue its exact game set for analysis."""
    plan_key = f"chessism:player_game_analysis_plan:{data.plan_id}"
    raw_plan = await redis.get(plan_key)
    if not raw_plan:
        raise HTTPException(status_code=404, detail="This preview expired; create a new preview")
    if isinstance(raw_plan, bytes):
        raw_plan = raw_plan.decode("utf-8")
    try:
        plan = json.loads(raw_plan)
    except (TypeError, ValueError) as error:
        raise HTTPException(status_code=409, detail="The saved preview is invalid") from error

    game_links = [int(link) for link in plan.get("game_links") or []]
    if not game_links:
        raise HTTPException(status_code=409, detail="The preview contains no games")
    planned_fens = max(0, int(plan.get("fens_to_analyze") or 0))
    planned_stockfish_fens = max(0, int(plan.get("stockfish_fens") or planned_fens))
    cool_off_count = max(0, math.ceil(planned_stockfish_fens / PLAYER_GAME_ANALYSIS_CHUNK_SIZE) - 1)
    calculated_timeout = _analysis_timeout(max(1, planned_stockfish_fens)) + (cool_off_count * data.cool_off)
    job = await redis.enqueue_job(
        "run_player_games_analysis_job",
        player_name=str(plan.get("player_name") or "").lower(),
        game_links=game_links,
        planned_fens=planned_fens,
        batch_size=data.batch_size,
        nodes_limit=data.nodes_limit,
        cool_off=data.cool_off,
        chunk_size=PLAYER_GAME_ANALYSIS_CHUNK_SIZE,
        selection_mode=plan.get("selection_mode"),
        selection_order=plan.get("selection_order"),
        selected_games=int(plan.get("selected_games") or len(game_links)),
        _queue_name="analysis_queue",
        _job_timeout=calculated_timeout,
    )
    job_id = _enqueued_job_id(job)
    await redis.delete(plan_key)
    return JSONResponse(status_code=202, content={
        "message": f"Game-completion analysis for {len(game_links)} games was queued.",
        "job_id": job_id,
        "player_name": plan.get("player_name"),
        "selected_games": int(plan.get("selected_games") or len(game_links)),
        "planned_fens": planned_fens,
        "batch_size": data.batch_size,
        "cool_off": data.cool_off,
    })


@router.get("/backups")
async def api_list_fen_analysis_backups() -> dict[str, Any]:
    """List durable Stockfish-result exports available for restoration."""
    try:
        backups = list_fen_analysis_backups()
    except OSError as error:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to read the FEN-analysis backup directory: {error}",
        ) from error
    return {
        "storage_location": BACKUP_DISPLAY_DIR,
        "backups": backups,
    }


@router.post("/backups")
async def api_create_fen_analysis_backup(
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Queue creation or an incremental update of the durable FEN snapshot."""
    job = await redis.enqueue_job(
        "run_fen_analysis_backup_job",
        _queue_name="pipeline_queue",
        _job_timeout=86_400,
    )
    return JSONResponse(
        status_code=202,
        content={
            "message": "FEN-analysis backup update queued.",
            "job_id": _enqueued_job_id(job),
            "storage_location": BACKUP_DISPLAY_DIR,
        },
    )


@router.post("/backups/{filename}/restore")
async def api_restore_fen_analysis_backup(
    filename: str,
    redis: ArqRedis = Depends(get_redis_pool),
) -> JSONResponse:
    """Queue restoration of an export into matching regenerated FEN rows."""
    try:
        available_filenames = {
            backup["filename"] for backup in list_fen_analysis_backups()
        }
    except OSError as error:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to read the FEN-analysis backup directory: {error}",
        ) from error
    if filename not in available_filenames:
        raise HTTPException(status_code=404, detail="FEN-analysis backup not found")

    job = await redis.enqueue_job(
        "run_fen_analysis_restore_job",
        filename=filename,
        _queue_name="pipeline_queue",
        _job_timeout=86_400,
    )
    return JSONResponse(
        status_code=202,
        content={
            "message": f"Restore queued for {filename}.",
            "job_id": _enqueued_job_id(job),
            "filename": filename,
        },
    )
