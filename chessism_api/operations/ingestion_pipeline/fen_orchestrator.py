"""Coordination and stage orchestration for automatic position ingestion."""

from __future__ import annotations

import json
import time
from typing import Any

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus

from chessism_api.database.ask_db import (
    _get_remaining_fens_count_committed,
    get_fen_ingestion_summary,
    increment_fen_ingestion_summaries,
    refresh_database_summary_fen_counts,
    refresh_fen_pipeline_summary,
    refresh_game_analysis_summary,
    refresh_scored_position_summary,
    refresh_scored_rating_summary,
)
from chessism_api.operations.ingestion_pipeline import (
    FEN_EXTRACTION,
    LINKING_POSITIONS,
    SAVING_POSITIONS,
    TABLEBASE_MARKING,
    UPDATING_SUMMARIES,
)
from chessism_api.operations.ingestion_pipeline.fen_core import (
    aggregate_fen_data,
    split_balanced,
)
from chessism_api.operations.ingestion_pipeline.fen_repository import (
    finalize_fen_game_states,
    refresh_fen_occurrence_counts,
    release_fen_processing_claims,
)
from chessism_api.operations.ingestion_pipeline.fen_workers import (
    run_association_insertion_job,
    run_fen_generation_job,
    run_fen_insertion_job,
)
from chessism_api.operations.ingestion_pipeline.progress import (
    increment_stage_progress,
    reset_stage_progress,
)
from chessism_api.operations.ingestion_pipeline.timing import (
    fail_running_ingestion_stages,
    finish_ingestion_run,
    finish_ingestion_stage,
    start_ingestion_run,
    start_ingestion_stage,
)
from chessism_api.operations.player_salience import enqueue_stale_player_salience_jobs
from chessism_api.operations.tablebase import ensure_tablebase_analysis_enqueued


FEN_PIPELINE_COORDINATION_KEY = "chessism:automatic_fen_pipeline"
FEN_PIPELINE_PROGRESS_KIND = "fen_extraction"
FEN_PIPELINE_BATCH_SIZE = 1_000
FEN_PIPELINE_WORKERS = 3
FEN_PIPELINE_LOCK_TTL_SECONDS = 24 * 60 * 60
FEN_PIPELINE_RESERVATION_TTL_SECONDS = 60
PROGRESS_TTL_SECONDS = 24 * 60 * 60
ACTIVE_JOB_STATUSES = {
    JobStatus.queued,
    JobStatus.deferred,
    JobStatus.in_progress,
}


def _decode_redis_text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value or "")


async def _write_fen_pipeline_progress(
    redis: ArqRedis,
    job_id: str,
    *,
    total: int,
    processed: int,
    phase: str,
    detail: str,
    failed: int = 0,
    ingestion_run_id: str | None = None,
) -> None:
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps({
            "job_id": job_id,
            "kind": FEN_PIPELINE_PROGRESS_KIND,
            "ingestion_run_id": ingestion_run_id,
            "total": max(0, int(total)),
            "processed": max(0, int(processed)),
            "failed": max(0, int(failed)),
            "phase": phase,
            "detail": detail,
            "updated_at": time.time(),
        }),
        ex=PROGRESS_TTL_SECONDS,
    )


async def _release_fen_pipeline_coordination(redis: ArqRedis, job_id: str) -> None:
    owner = _decode_redis_text(await redis.get(FEN_PIPELINE_COORDINATION_KEY))
    if owner == job_id:
        await redis.delete(FEN_PIPELINE_COORDINATION_KEY)


async def ensure_fen_pipeline_enqueued(
    redis: ArqRedis,
    *,
    total_games_to_process: int | None = None,
    batch_size: int = FEN_PIPELINE_BATCH_SIZE,
    num_workers: int = FEN_PIPELINE_WORKERS,
    ingestion_run_id: str | None = None,
    player_name: str | None = None,
    trigger: str = "automatic",
) -> dict[str, Any]:
    """Atomically ensure one coordinator is draining all pending games."""
    pending_games = int(await _get_remaining_fens_count_committed() or 0)
    if pending_games <= 0:
        return {"status": "up_to_date", "job_id": None, "pending_games": 0}

    current_job_id = _decode_redis_text(
        await redis.get(FEN_PIPELINE_COORDINATION_KEY)
    )
    if current_job_id == "reserving":
        return {
            "status": "already_active",
            "job_id": None,
            "pending_games": pending_games,
        }
    if current_job_id:
        current_job = Job(current_job_id, redis, _queue_name="pipeline_queue")
        if await current_job.status() in ACTIVE_JOB_STATUSES:
            return {
                "status": "already_active",
                "job_id": current_job_id,
                "pending_games": pending_games,
            }
        await redis.delete(FEN_PIPELINE_COORDINATION_KEY)

    reserved = await redis.set(
        FEN_PIPELINE_COORDINATION_KEY,
        "reserving",
        ex=FEN_PIPELINE_RESERVATION_TTL_SECONDS,
        nx=True,
    )
    if not reserved:
        owner = _decode_redis_text(await redis.get(FEN_PIPELINE_COORDINATION_KEY))
        return {
            "status": "already_active",
            "job_id": owner if owner != "reserving" else None,
            "pending_games": pending_games,
        }

    stable_run_id = ingestion_run_id
    try:
        released_claims = await release_fen_processing_claims()
        if released_claims:
            print(f"Released {released_claims} stale FEN claims.", flush=True)
        pending_games = int(await _get_remaining_fens_count_committed() or 0)
        if pending_games <= 0:
            await redis.delete(FEN_PIPELINE_COORDINATION_KEY)
            return {"status": "up_to_date", "job_id": None, "pending_games": 0}

        requested_games = min(
            pending_games,
            max(1, int(total_games_to_process or pending_games)),
        )
        stable_run_id = await start_ingestion_run(
            run_id=ingestion_run_id,
            player_name=player_name,
            trigger=trigger,
        )
        job = await redis.enqueue_job(
            "run_fen_pipeline",
            total_games_to_process=requested_games,
            batch_size=max(1, int(batch_size)),
            num_workers=max(1, int(num_workers)),
            ingestion_run_id=stable_run_id,
            player_name=player_name,
            trigger=trigger,
            _queue_name="pipeline_queue",
        )
        if job is None:
            raise RuntimeError("Redis did not create the FEN pipeline job.")
        job_id = str(job.job_id)
        await redis.set(
            FEN_PIPELINE_COORDINATION_KEY,
            job_id,
            ex=FEN_PIPELINE_LOCK_TTL_SECONDS,
        )
        await _write_fen_pipeline_progress(
            redis,
            job_id,
            total=requested_games,
            processed=0,
            phase="queued",
            detail="Waiting for automatic FEN extraction.",
            ingestion_run_id=stable_run_id,
        )
        return {
            "status": "queued",
            "job_id": job_id,
            "pending_games": pending_games,
        }
    except Exception as error:
        if _decode_redis_text(
            await redis.get(FEN_PIPELINE_COORDINATION_KEY)
        ) == "reserving":
            await redis.delete(FEN_PIPELINE_COORDINATION_KEY)
        await fail_running_ingestion_stages(stable_run_id, str(error))
        await finish_ingestion_run(stable_run_id, status="failed", error=str(error))
        raise


async def _enqueue_child(
    redis: ArqRedis,
    function: str,
    **kwargs: Any,
) -> Job:
    job = await redis.enqueue_job(function, _queue_name="fen_queue", **kwargs)
    if job is None:
        raise RuntimeError(f"Redis did not create child job {function}.")
    return job


async def _wait_for_write_jobs(jobs: list[Job], label: str) -> None:
    errors: list[str] = []
    for job in jobs:
        try:
            await job.result(timeout=None)
        except Exception as error:
            errors.append(repr(error))
    if errors:
        raise RuntimeError(f"{len(errors)} {label} worker(s) failed: {errors[0]}")


async def _recover_from_write_failure(
    claimed_game_links: set[int],
    affected_fens: list[str],
) -> None:
    await release_fen_processing_claims(sorted(claimed_game_links))
    await refresh_fen_occurrence_counts(affected_fens)
    await refresh_database_summary_fen_counts()
    await refresh_scored_position_summary()


async def _run_fen_pipeline(
    ctx: dict[str, Any],
    total_games_to_process: int,
    batch_size: int,
    num_workers: int,
    ingestion_run_id: str | None = None,
    **_kwargs: Any,
) -> dict[str, Any]:
    """Run extraction, persistence, and summary stages for one bounded batch."""
    redis: ArqRedis = ctx["redis"]
    progress_job_id = str(ctx.get("job_id") or "unknown")
    games_remaining = int(await _get_remaining_fens_count_committed() or 0)
    if games_remaining <= 0:
        return {"claimed_games": 0, "successful_games": 0, "failed_games": 0}

    games_to_process = min(max(1, total_games_to_process), games_remaining)
    worker_count = min(max(1, num_workers), games_to_process)
    await start_ingestion_stage(
        ingestion_run_id,
        FEN_EXTRACTION,
        detail=f"Extracting positions from {games_to_process} games.",
    )
    await reset_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="extracting",
        total=0,
        processed=0,
        detail=f"Extracting positions from {games_to_process} games.",
    )

    per_worker, extra = divmod(games_to_process, worker_count)
    generation_jobs = [
        await _enqueue_child(
            redis,
            "run_fen_generation_job",
            total_games_to_process=per_worker + (1 if index < extra else 0),
            batch_size=max(1, batch_size),
            parent_job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
        )
        for index in range(worker_count)
    ]

    associations: list[dict[str, Any]] = []
    claimed_links: set[int] = set()
    successful_links: set[int] = set()
    failed_links: set[int] = set()
    expected_positions = 0
    generation_errors: list[str] = []
    for job in generation_jobs:
        try:
            payload = await job.result(timeout=None)
            worker_associations = list(payload.get("associations") or [])
            associations.extend(worker_associations)
            claimed_links.update(int(link) for link in payload.get("claimed_game_links") or [])
            successful_links.update(int(link) for link in payload.get("successful_game_links") or [])
            failed_links.update(int(link) for link in payload.get("failed_game_links") or [])
            expected_positions += int(payload.get("expected_positions") or 0)
        except Exception as error:
            generation_errors.append(repr(error))
    if generation_errors:
        await release_fen_processing_claims()
        raise RuntimeError(
            f"{len(generation_errors)} FEN generation worker(s) failed: "
            f"{generation_errors[0]}"
        )

    await finish_ingestion_stage(
        ingestion_run_id,
        FEN_EXTRACTION,
        processed=len(associations),
        total=expected_positions,
        detail=f"Extracted {len(associations)} position occurrences.",
    )
    successful_links.update(
        int(row["game_link"])
        for row in associations
        if row.get("game_link") is not None
    )
    failed_links.update(claimed_links - successful_links - failed_links)

    if not associations:
        await finalize_fen_game_states([], sorted(claimed_links))
        await refresh_fen_pipeline_summary()
        for stage in (SAVING_POSITIONS, LINKING_POSITIONS, UPDATING_SUMMARIES):
            await start_ingestion_stage(ingestion_run_id, stage)
            await finish_ingestion_stage(ingestion_run_id, stage)
        return {
            "claimed_games": len(claimed_links),
            "successful_games": 0,
            "failed_games": len(claimed_links),
        }

    fens_to_insert, associations_to_insert = aggregate_fen_data(associations)
    affected_fens = [str(row["fen"]) for row in fens_to_insert]
    summary_before = await get_fen_ingestion_summary(affected_fens)

    await start_ingestion_stage(
        ingestion_run_id,
        SAVING_POSITIONS,
        detail=f"Saving {len(fens_to_insert)} unique positions.",
    )
    await reset_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="saving_fens",
        total=len(fens_to_insert),
        processed=0,
        failed=len(failed_links),
        detail="Saving unique positions.",
    )
    fen_jobs = [
        await _enqueue_child(
            redis,
            "run_fen_insertion_job",
            fens_to_insert=chunk,
            parent_job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
        )
        for chunk in split_balanced(fens_to_insert, worker_count)
        if chunk
    ]
    try:
        await _wait_for_write_jobs(fen_jobs, "FEN insertion")
    except Exception:
        await _recover_from_write_failure(claimed_links, affected_fens)
        raise
    await finish_ingestion_stage(
        ingestion_run_id,
        SAVING_POSITIONS,
        processed=len(fens_to_insert),
        total=len(fens_to_insert),
        detail=f"Saved {len(fens_to_insert)} unique positions.",
    )

    await start_ingestion_stage(
        ingestion_run_id,
        LINKING_POSITIONS,
        detail=f"Saving {len(associations_to_insert)} game-position links.",
    )
    await reset_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="saving_games",
        total=len(associations_to_insert),
        processed=0,
        failed=len(failed_links),
        detail="Linking positions back to their games.",
    )
    association_jobs = [
        await _enqueue_child(
            redis,
            "run_association_insertion_job",
            associations_to_insert=chunk,
            parent_job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
        )
        for chunk in split_balanced(associations_to_insert, worker_count)
        if chunk
    ]
    try:
        await _wait_for_write_jobs(association_jobs, "association insertion")
    except Exception:
        await _recover_from_write_failure(claimed_links, affected_fens)
        raise
    await finish_ingestion_stage(
        ingestion_run_id,
        LINKING_POSITIONS,
        processed=len(associations_to_insert),
        total=len(associations_to_insert),
        detail=f"Saved {len(associations_to_insert)} game-position links.",
    )

    await start_ingestion_stage(
        ingestion_run_id,
        UPDATING_SUMMARIES,
        detail="Updating five ingestion projections.",
    )
    await reset_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        total=5,
        processed=0,
        failed=len(failed_links),
        detail="Recounting affected position occurrences.",
    )
    await refresh_fen_occurrence_counts(affected_fens)
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Publishing completed game extraction state.",
    )
    await finalize_fen_game_states(sorted(successful_links), sorted(failed_links))
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Updating aggregate ingestion counters.",
    )
    try:
        summary_after = await get_fen_ingestion_summary(affected_fens)
        await increment_fen_ingestion_summaries(summary_before, summary_after)
    except Exception as error:
        print(
            f"Incremental summary update failed ({error!r}); reconciling globally.",
            flush=True,
        )
        await refresh_database_summary_fen_counts()
        await refresh_scored_position_summary()
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Updating per-game analysis summaries.",
    )
    await refresh_game_analysis_summary(tuple(sorted(successful_links)))
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Updating pipeline coverage summary.",
    )
    await refresh_fen_pipeline_summary()
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Ingestion summaries are current.",
    )
    await finish_ingestion_stage(
        ingestion_run_id,
        UPDATING_SUMMARIES,
        processed=5,
        total=5,
        detail="All ingestion summaries are current.",
    )
    return {
        "claimed_games": len(claimed_links),
        "successful_games": len(successful_links),
        "failed_games": len(failed_links),
        "successful_game_links": sorted(successful_links),
    }


async def _finish_drained_chain(
    redis: ArqRedis,
    job_id: str,
    ingestion_run_id: str | None,
) -> None:
    await increment_stage_progress(
        redis,
        job_id=job_id,
        ingestion_run_id=ingestion_run_id,
        phase="discovering_tablebase",
        detail="Refreshing the rating projection for affected games.",
    )
    await refresh_scored_rating_summary()
    await increment_stage_progress(
        redis,
        job_id=job_id,
        ingestion_run_id=ingestion_run_id,
        phase="discovering_tablebase",
        processed_delta=1,
        detail="Tablebase and rating projections are current.",
    )
    try:
        salience_jobs = await enqueue_stale_player_salience_jobs(redis)
        if salience_jobs:
            print(f"Queued {len(salience_jobs)} player salience refreshes.", flush=True)
    except Exception as error:
        print(f"Could not queue player salience refreshes: {error!r}", flush=True)
    await finish_ingestion_stage(
        ingestion_run_id,
        TABLEBASE_MARKING,
        processed=2,
        total=2,
        detail="No new tablebase positions required marking.",
    )
    await finish_ingestion_run(ingestion_run_id)


async def run_fen_pipeline(
    ctx: dict[str, Any],
    total_games_to_process: int,
    batch_size: int,
    num_workers: int,
    ingestion_run_id: str | None = None,
    player_name: str | None = None,
    trigger: str = "automatic",
    **kwargs: Any,
) -> dict[str, Any]:
    """Run one bounded batch and schedule tablebase/follow-up work."""
    redis: ArqRedis = ctx["redis"]
    job_id = str(ctx.get("job_id") or "unknown")
    coordination_released = False
    try:
        result = await _run_fen_pipeline(
            ctx,
            total_games_to_process=total_games_to_process,
            batch_size=batch_size,
            num_workers=num_workers,
            ingestion_run_id=ingestion_run_id,
            **kwargs,
        )
        result_stats = result if isinstance(result, dict) else {}
        successful_games = int(result_stats.get("successful_games") or 0)
        failed_games = int(result_stats.get("failed_games") or 0)
        successful_links = [
            int(link)
            for link in result_stats.get("successful_game_links") or []
        ]

        tablebase_job: dict[str, Any] = {
            "status": "blocked",
            "job_id": None,
            "pending": 0,
        }
        if not (failed_games and not successful_games):
            await start_ingestion_stage(
                ingestion_run_id,
                TABLEBASE_MARKING,
                detail="Finding tablebase positions in newly extracted games.",
            )
            await reset_stage_progress(
                redis,
                job_id=job_id,
                ingestion_run_id=ingestion_run_id,
                phase="discovering_tablebase",
                total=2,
                processed=0,
                detail="Finding tablebase positions in newly extracted games.",
                failed=failed_games,
            )
            tablebase_job = await ensure_tablebase_analysis_enqueued(
                redis,
                game_links=successful_links,
                ingestion_run_id=ingestion_run_id,
            )
            await increment_stage_progress(
                redis,
                job_id=job_id,
                ingestion_run_id=ingestion_run_id,
                phase="discovering_tablebase",
                processed_delta=1,
                detail=f"Found {int(tablebase_job.get('pending') or 0)} tablebase candidates.",
            )

        await _release_fen_pipeline_coordination(redis, job_id)
        coordination_released = True
        if failed_games and not successful_games:
            follow_up = {"status": "blocked"}
        else:
            follow_up = await ensure_fen_pipeline_enqueued(
                redis,
                batch_size=batch_size,
                num_workers=num_workers,
                ingestion_run_id=ingestion_run_id,
                player_name=player_name,
                trigger=trigger,
            )

        if (
            follow_up["status"] == "up_to_date"
            and tablebase_job["status"] == "up_to_date"
        ):
            await _finish_drained_chain(redis, job_id, ingestion_run_id)
        elif failed_games and not successful_games:
            error = f"{failed_games} games failed FEN validation."
            await fail_running_ingestion_stages(ingestion_run_id, error)
            await finish_ingestion_run(ingestion_run_id, status="failed", error=error)

        await _write_fen_pipeline_progress(
            redis,
            job_id,
            total=max(0, int(total_games_to_process)),
            processed=successful_games,
            phase="complete",
            detail=(
                f"Automatic FEN extraction finished for {successful_games} games; "
                f"{failed_games} failed validation."
            ),
            failed=failed_games,
            ingestion_run_id=ingestion_run_id,
        )
        return result_stats
    except Exception as error:
        await release_fen_processing_claims()
        await fail_running_ingestion_stages(ingestion_run_id, str(error))
        await finish_ingestion_run(ingestion_run_id, status="failed", error=str(error))
        await _write_fen_pipeline_progress(
            redis,
            job_id,
            total=max(0, int(total_games_to_process)),
            processed=0,
            phase="failed",
            detail=f"Automatic FEN extraction failed: {error}",
            ingestion_run_id=ingestion_run_id,
        )
        raise
    finally:
        if not coordination_released:
            await _release_fen_pipeline_coordination(redis, job_id)


__all__ = [
    "ensure_fen_pipeline_enqueued",
    "run_fen_pipeline",
    "run_fen_generation_job",
    "run_fen_insertion_job",
    "run_association_insertion_job",
]
