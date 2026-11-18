"""Coordination and stage orchestration for automatic position ingestion."""

from __future__ import annotations

from typing import Any

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus

from chessism_api.operations.scored_rating_summary import refresh_scored_rating_summary
from chessism_api.operations.ingestion_pipeline import TABLEBASE_MARKING
from chessism_api.operations.ingestion_pipeline.fen_repository import (
    count_pending_games, release_fen_processing_claims,
)
from chessism_api.operations.ingestion_pipeline.fen_batches import run_bounded_pipeline
from chessism_api.operations.ingestion_pipeline.fen_workers import (
    run_association_insertion_job,
    run_fen_generation_job,
    run_fen_insertion_job,
)
from chessism_api.operations.ingestion_pipeline.progress import (
    increment_stage_progress,
    reset_stage_progress,
    write_job_progress,
)
from chessism_api.operations.redis_ownership import release_owned
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
    await write_job_progress(
        redis, job_id, kind=FEN_PIPELINE_PROGRESS_KIND,
        ingestion_run_id=ingestion_run_id, total=total, processed=processed,
        failed=failed, phase=phase, detail=detail,
    )


async def _release_fen_pipeline_coordination(redis: ArqRedis, job_id: str) -> None:
    await release_owned(redis, FEN_PIPELINE_COORDINATION_KEY, job_id)


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
    pending_games = int(await count_pending_games() or 0)
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
        await release_owned(redis, FEN_PIPELINE_COORDINATION_KEY, current_job_id)

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
        pending_games = int(await count_pending_games() or 0)
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


async def _run_fen_pipeline(ctx, total_games_to_process, batch_size, num_workers,
                            ingestion_run_id=None, **_kwargs):
    games_remaining = await count_pending_games()
    if games_remaining <= 0:
        return {"claimed_games": 0, "successful_games": 0, "failed_games": 0}
    return await run_bounded_pipeline(ctx, min(max(1, total_games_to_process), games_remaining),
        batch_size, num_workers, ingestion_run_id=ingestion_run_id)


async def _finish_drained_chain(
    redis: ArqRedis,
    job_id: str,
    ingestion_run_id: str | None,
) -> None:
    from .steps import StepReporter

    await increment_stage_progress(
        redis,
        job_id=job_id,
        ingestion_run_id=ingestion_run_id,
        phase="discovering_tablebase",
        detail="Refreshing the rating projection for affected games.",
    )
    async with StepReporter({"redis": redis, "job_id": job_id}, ingestion_run_id) as reporter:
        async with reporter.step("rating_projection", 1, "refreshes"):
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
        async with StepReporter({"redis": redis, "job_id": job_id}, ingestion_run_id) as reporter:
            async with reporter.step("queue_player_summaries", None, "jobs"):
                salience_jobs = await enqueue_stale_player_salience_jobs(redis)
                await reporter.advance(len(salience_jobs or []))
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
            from .steps import StepReporter
            async with StepReporter(ctx, ingestion_run_id) as reporter:
                async with reporter.step("count_endgames", 1, "checks"):
                    tablebase_job = await ensure_tablebase_analysis_enqueued(
                        redis, game_links=successful_links, ingestion_run_id=ingestion_run_id,
                    )
            await increment_stage_progress(
                redis,
                job_id=job_id,
                ingestion_run_id=ingestion_run_id,
                phase="discovering_tablebase",
                processed_delta=1,
                detail=(f"Found {int(tablebase_job['pending'])} tablebase candidates."
                    if tablebase_job.get('pending') is not None else
                    "Endgame discovery queued; exact count will appear when ready."),
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
