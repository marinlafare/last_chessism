"""ARQ entry points and lifecycle orchestration for player game ingestion."""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from sqlalchemy import select

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Game, to_dict
from chessism_api.operations.ingestion_pipeline import PARSING_GAMES
from chessism_api.operations.ingestion_pipeline.chesscom import download_months
from chessism_api.operations.ingestion_pipeline.fen_orchestrator import (
    ensure_fen_pipeline_enqueued,
)
from chessism_api.operations.ingestion_pipeline.game_importer import (
    format_games,
    insert_games_months_moves_and_players,
)
from chessism_api.operations.ingestion_pipeline.game_repository import (
    sync_player_months,
)
from chessism_api.operations.ingestion_pipeline.months import (
    missing_archive_months,
    update_archive_months,
)
from chessism_api.operations.ingestion_pipeline.timing import (
    finish_ingestion_run,
    finish_ingestion_stage,
    start_ingestion_run,
    start_ingestion_stage,
)


PROGRESS_TTL_SECONDS = 24 * 60 * 60
GAME_UPDATE_PROGRESS_KIND = "game_update"
ProgressCallback = Callable[[str, int, int, str | None], Awaitable[None]]
IngestionOperation = Callable[[dict[str, Any], ProgressCallback | None], Awaitable[str]]


async def _write_game_job_progress(
    ctx: dict[str, Any],
    job_id: str,
    *,
    player_name: str,
    total: int,
    processed: int,
    failed: int = 0,
    phase: str,
    detail: str | None = None,
    result: str | None = None,
    ingestion_run_id: str | None = None,
) -> None:
    redis = ctx.get("redis")
    if redis is None:
        return
    payload = {
        "job_id": job_id,
        "kind": GAME_UPDATE_PROGRESS_KIND,
        "ingestion_run_id": ingestion_run_id,
        "player_name": player_name,
        "total": int(total),
        "processed": int(processed),
        "failed": int(failed),
        "phase": phase,
        "detail": detail,
        "result": result,
        "updated_at": time.time(),
    }
    try:
        await redis.set(
            f"chessism:job_progress:{job_id}",
            json.dumps(payload),
            ex=PROGRESS_TTL_SECONDS,
        )
    except Exception as error:
        print(f"Could not publish game-job progress: {error!r}", flush=True)


async def read_game(link: int | str) -> list[dict[str, Any]]:
    async with AsyncDBSession() as session:
        result = await session.execute(select(Game).where(Game.link == int(link)))
        return [to_dict(game) for game in result.scalars().all()]


def _successful_archive_months(
    archives: dict[int, dict[int, list[dict[str, Any]]]],
) -> set[tuple[int, int]]:
    return {
        (int(year), int(month))
        for year, year_archives in archives.items()
        for month in year_archives
    }


async def _download_format_and_insert(
    player_name: str,
    months: Sequence[str],
    progress_callback: ProgressCallback | None,
    *,
    no_games_message: str,
) -> str | None:
    """Download serially, parse, persist, and record every successful archive."""
    total_steps = max(1, len(months) + 2)
    if progress_callback:
        await progress_callback(
            "downloading",
            total_steps,
            0,
            f"Downloading {len(months)} months.",
        )

    async def on_month_downloaded(index: int, month: str) -> None:
        if progress_callback:
            await progress_callback(
                "downloading",
                total_steps,
                index,
                f"Downloaded {month}.",
            )

    archives = await download_months(
        player_name,
        list(months),
        progress_callback=on_month_downloaded,
    )
    fetched_months = _successful_archive_months(archives)
    downloaded_count = sum(
        len(month_games)
        for year_archives in archives.values()
        for month_games in year_archives.values()
    )
    if downloaded_count == 0:
        await sync_player_months(player_name, fetched_months)
        return no_games_message

    if progress_callback:
        await progress_callback(
            "formatting",
            total_steps,
            len(months),
            f"Parsing {downloaded_count} downloaded games.",
        )
    formatted_games = await format_games(archives, player_name)
    if isinstance(formatted_games, str):
        await sync_player_months(player_name, fetched_months)
        return formatted_games

    if progress_callback:
        await progress_callback(
            "inserting",
            total_steps,
            len(months) + 1,
            "Saving games to the database.",
        )
    result = await insert_games_months_moves_and_players(
        formatted_games,
        player_name,
    )
    await sync_player_months(player_name, fetched_months)
    if progress_callback:
        await progress_callback(
            "complete",
            total_steps,
            total_steps,
            f"Saved {len(formatted_games)} games and queued position extraction.",
        )
    return result


async def create_games(
    data: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
) -> str:
    player_name = str(data["player_name"]).strip().lower()
    months = await missing_archive_months(player_name)
    if not months:
        return "ALL MONTHS IN DB ALREADY"
    result = await _download_format_and_insert(
        player_name,
        months,
        progress_callback,
        no_games_message=f"No games found for {player_name}.",
    )
    return result or f"DATA READY FOR {player_name}"


async def update_player_games(
    data: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
) -> str:
    player_name = str(data["player_name"]).strip().lower()
    if progress_callback:
        await progress_callback(
            "preparing",
            1,
            0,
            f"Checking existing months for {player_name}.",
        )
    months = await update_archive_months(player_name)
    if not months:
        return f"Player {player_name} is already up to date."
    result = await _download_format_and_insert(
        player_name,
        months,
        progress_callback,
        no_games_message=f"No new games found for {player_name}.",
    )
    return result or f"DATA UPDATED FOR {player_name}"


async def _run_game_job(
    ctx: dict[str, Any],
    data: dict[str, Any],
    *,
    operation: IngestionOperation,
    fallback_job_id: str,
    queued_detail: str,
    trigger: str = "automatic",
) -> str:
    """Run one import operation with durable timings and transient progress."""
    job_id = str(ctx.get("job_id") or fallback_job_id)
    player_name = str(data.get("player_name", "")).strip().lower()
    latest_total = 1
    latest_processed = 0
    ingestion_run_id = await start_ingestion_run(
        run_id=data.get("ingestion_run_id"),
        player_name=player_name,
        trigger=trigger,
    )
    await start_ingestion_stage(
        ingestion_run_id,
        PARSING_GAMES,
        detail=queued_detail.format(player_name=player_name),
    )

    async def progress_callback(
        phase: str,
        total: int,
        processed: int,
        detail: str | None = None,
    ) -> None:
        nonlocal latest_total, latest_processed
        latest_total = max(1, int(total or 1))
        latest_processed = max(0, min(latest_total, int(processed or 0)))
        await _write_game_job_progress(
            ctx,
            job_id,
            player_name=player_name,
            total=latest_total,
            processed=latest_processed,
            phase=phase,
            detail=detail,
            ingestion_run_id=ingestion_run_id,
        )

    await _write_game_job_progress(
        ctx,
        job_id,
        player_name=player_name,
        total=1,
        processed=0,
        phase="queued",
        detail=queued_detail.format(player_name=player_name),
        ingestion_run_id=ingestion_run_id,
    )
    try:
        message = await operation(data, progress_callback)
        await finish_ingestion_stage(
            ingestion_run_id,
            PARSING_GAMES,
            processed=latest_processed,
            total=latest_total,
            detail=message,
        )
        fen_pipeline = await ensure_fen_pipeline_enqueued(
            ctx["redis"],
            ingestion_run_id=ingestion_run_id,
            player_name=player_name,
            trigger=trigger,
        )
        if fen_pipeline["status"] == "queued":
            fen_detail = (
                " Automatic FEN extraction queued for "
                f"{fen_pipeline['pending_games']:,} games."
            )
        elif fen_pipeline["status"] == "already_active":
            fen_detail = " Automatic FEN extraction is already running."
        else:
            fen_detail = " FEN extraction is up to date."
            await finish_ingestion_run(ingestion_run_id)
        completion_message = f"{message}{fen_detail}"
        await _write_game_job_progress(
            ctx,
            job_id,
            player_name=player_name,
            total=latest_total,
            processed=latest_total,
            phase="complete",
            detail=completion_message,
            result=completion_message,
            ingestion_run_id=ingestion_run_id,
        )
        return completion_message
    except Exception as error:
        await finish_ingestion_stage(
            ingestion_run_id,
            PARSING_GAMES,
            processed=latest_processed,
            total=latest_total,
            status="failed",
            detail=str(error),
        )
        await finish_ingestion_run(
            ingestion_run_id,
            status="failed",
            error=str(error),
        )
        await _write_game_job_progress(
            ctx,
            job_id,
            player_name=player_name,
            total=latest_total,
            processed=latest_processed,
            failed=1,
            phase="failed",
            detail=str(error),
            ingestion_run_id=ingestion_run_id,
        )
        raise


async def run_create_player_games_job(
    ctx: dict[str, Any],
    data: dict[str, Any],
    **_kwargs: Any,
) -> str:
    return await _run_game_job(
        ctx,
        data,
        operation=create_games,
        fallback_job_id="game-download",
        queued_detail="Queued full download for {player_name}.",
        trigger="player_create",
    )


async def run_update_player_games_job(
    ctx: dict[str, Any],
    data: dict[str, Any],
    **_kwargs: Any,
) -> str:
    return await _run_game_job(
        ctx,
        data,
        operation=update_player_games,
        fallback_job_id="game-update",
        queued_detail="Queued update for {player_name}.",
        trigger="player_update",
    )
