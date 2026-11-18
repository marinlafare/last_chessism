import asyncio
import json
import os
import sys
from typing import Any, Sequence

import chess
import chess.syzygy
from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus

from chessism_api.operations.game_analysis_summary import (
    increment_game_analysis_summary_for_scored_fens,
    refresh_game_analysis_summary,
)
from chessism_api.operations.scored_rating_summary import refresh_scored_rating_summary
from chessism_api.database.postgresql.ask_db import (
    increment_scored_position_summary_for_scored_fens,
    refresh_database_summary_fen_counts,
    refresh_fen_pipeline_summary,
    refresh_scored_position_summary,
)
from chessism_api.operations.player_salience import enqueue_stale_player_salience_jobs
from chessism_api.operations.ingestion_pipeline.steps import StepReporter
from chessism_api.operations.ingestion_pipeline.progress import write_job_progress
from chessism_api.operations.redis_ownership import release_owned
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup
from chessism_api.database.postgresql.analysis_selection import release_analysis_session
from chessism_api.database.postgresql.tablebase_repository import (
    CandidateSpool, clean_game_links, count_tablebase_candidates, has_tablebase_candidates,
    read_tablebase_batch, lease_tablebase_batch, stage_tablebase_results,
)
from chessism_api.operations.tablebase_probe import (
    _best_tablebase_move, _move_rank, _tablebase_score_payload,
    _probe_batch_sync as probe_batch,
)
from chessism_api.operations.tablebase_parallel import TablebaseProber
from chessism_api.operations.ingestion_pipeline import TABLEBASE_MARKING
from chessism_api.operations.ingestion_pipeline.timing import (
    fail_running_ingestion_stages,
    finish_ingestion_run,
    finish_ingestion_stage,
    start_ingestion_stage,
)


TABLEBASE_PATH = os.getenv("STOCKFISH_SYZYGY_PATH", "/syzygy")
TABLEBASE_BATCH_SIZE = 1_000
TABLEBASE_PROGRESS_KIND = "tablebase_analysis"
TABLEBASE_COORDINATION_KEY = "chessism:automatic_tablebase_analysis"
COORDINATION_TTL_SECONDS = 60 * 60 * 24
RESERVATION_TTL_SECONDS = 60
ACTIVE_JOB_STATUSES = {
    JobStatus.queued,
    JobStatus.deferred,
    JobStatus.in_progress,
}

def _redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value or "")


async def _write_progress(
    redis: ArqRedis | None,
    job_id: str,
    *,
    total: int,
    processed: int,
    failed: int,
    phase: str,
    detail: str,
    ingestion_run_id: str | None = None,
) -> None:
    if redis is None:
        return
    await write_job_progress(
        redis, job_id, kind=TABLEBASE_PROGRESS_KIND,
        ingestion_run_id=ingestion_run_id, total=total, processed=processed,
        failed=failed, phase=phase, detail=detail,
    )


async def _read_progress(redis: ArqRedis | None, job_id: str) -> dict[str, Any]:
    if redis is None:
        return {}
    raw = await redis.get(f"chessism:job_progress:{job_id}")
    if not raw:
        return {}
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _probe_batch_sync(tablebase, rows):
    # Preserve this shared entry point for callers and selection parity tests.
    return probe_batch(tablebase, rows, best_move=_best_tablebase_move)


async def _analyze_tablebase_positions(
    ctx: dict,
    *,
    reporter,
    game_links: Sequence[int] | None = None,
    candidate_count: int | None = None,
    max_positions: int | None = None,
    batch_size: int = TABLEBASE_BATCH_SIZE,
    progress_job_id: str | None = None,
    refresh_projections: bool = True,
    ingestion_run_id: str | None = None,
) -> dict[str, int]:
    """Solve tablebase-eligible positions directly and persist exact results."""
    if not os.path.isdir(TABLEBASE_PATH):
        raise RuntimeError(f"Syzygy tablebase directory is unavailable: {TABLEBASE_PATH}")

    redis = ctx.get("redis")
    job_id = str(progress_job_id or ctx.get("job_id") or "tablebase-analysis")
    # candidate_count is a stale compatibility hint, not a cap on discovery.
    target = 0
    await _write_progress(
        redis,
        job_id,
        total=0,
        processed=0,
        failed=0,
        phase="discovering_tablebase",
        detail="Finding eligible Syzygy positions; count not known yet.",
        ingestion_run_id=ingestion_run_id,
    )

    processed = solved_total = unavailable_total = skipped_total = 0
    safe_batch_size = max(1, min(int(batch_size), 5_000))
    prober = TablebaseProber(TABLEBASE_PATH)
    candidates = CandidateSpool()
    try:
        async with reporter.step("discover_endgames", None, "positions"):
            limit = max(0, int(max_positions)) if max_positions is not None else None
            await candidates.discover(game_links, limit, reporter.advance)
        target = candidates.count
        reporter.goal("claim_endgames", target)
        reporter.goal("probe_endgames", target)
        reporter.goal("save_endgames", target)
        progress_total = target + (1 if refresh_projections else 0)
        await _write_progress(redis, job_id, total=progress_total, processed=0, failed=0,
            phase="tablebase", detail=f"Probing {target} positions with {prober.workers} Syzygy processes.",
            ingestion_run_id=ingestion_run_id)
        while identifiers := candidates.read(safe_batch_size):
            rows = await read_tablebase_batch(identifiers)
            async with reporter.step("probe_endgames", len(rows), "positions"):
                solved, unavailable_rows = await prober.probe(rows)
            # No locks or database connection were held during CPU/file work.
            # A cloud claim or score that arrived meanwhile wins; never overwrite.
            session = None
            try:
                async with reporter.step("claim_endgames", len(identifiers), "positions"):
                    session, eligible = await lease_tablebase_batch(
                        game_links, len(identifiers), candidate_ids=identifiers,
                    ) if rows else (None, [])
                allowed = {row['fen'] for row in eligible}
                solved = [row for row in solved if row['fen'] in allowed]
                unavailable_rows = [row for row in unavailable_rows if row['fen'] in allowed]
                skipped_total += len(identifiers) - len(solved) - len(unavailable_rows)
                if session is None:
                    processed += len(identifiers)
                    await _write_progress(redis, job_id, total=progress_total, processed=processed,
                        failed=unavailable_total, phase="tablebase",
                        detail=f"Solved {solved_total}; {skipped_total} already scored or reserved.",
                        ingestion_run_id=ingestion_run_id)
                    continue
                async with reporter.step("save_endgames", len(solved)+len(unavailable_rows), "positions"):
                    await stage_tablebase_results(session, solved, unavailable_rows)
                    from chessism_api.database.postgresql.evaluation_results import increment_new_fen_counts
                    await increment_new_fen_counts(session, solved)
                    await session.commit()
            except BaseException:
                if session is not None:
                    try:
                        await release_analysis_session(session)
                    except BaseException as error:
                        print(f"Failed to release tablebase write session: {error!r}", flush=True)
                raise
            else:
                await finish_analysis_cleanup(session.close())

            if solved:
                try:
                    async with reporter.step("endgame_summaries", len(solved), "positions"):
                        await increment_scored_position_summary_for_scored_fens(solved)
                        await increment_game_analysis_summary_for_scored_fens(solved)
                except Exception:
                    # FEN scores were committed immediately before these cached
                    # summaries. Reconcile them before surfacing the failure so a
                    # retry cannot leave counters or per-game coverage incomplete.
                    await refresh_database_summary_fen_counts()
                    await refresh_scored_position_summary()
                    await refresh_game_analysis_summary(
                        clean_game_links(game_links) if game_links is not None else None
                    )
                    raise

            processed += len(identifiers)
            solved_total += len(solved)
            unavailable_total += len(unavailable_rows)
            await _write_progress(
                redis,
                job_id,
                total=progress_total,
                processed=processed,
                failed=unavailable_total,
                phase="tablebase",
                detail=(f"Solved {solved_total}; {unavailable_total} require Stockfish; "
                        f"{skipped_total} already scored or reserved."),
                ingestion_run_id=ingestion_run_id,
            )
    finally:
        # Terminating an unfinished read-only probe is safe; no partial result
        # was published. The next run rediscovers only still-unscored positions.
        try: await prober.close(abort=sys.exc_info()[0] is not None)
        finally: candidates.close()

    # A scoped automatic run follows freshly inserted game associations. Those
    # associations change rating projections even when every endgame probe is
    # unavailable, so the handoff still needs one projection refresh.
    if refresh_projections and (solved_total > 0 or game_links is not None):
        await _write_progress(
            redis,
            job_id,
            total=progress_total,
            processed=processed,
            failed=unavailable_total,
            phase="refreshing_statistics",
            detail="Refreshing rating statistics for the solved endgames.",
            ingestion_run_id=ingestion_run_id,
        )
        async with reporter.step("rating_projection", 1, "refreshes"):
            await refresh_scored_rating_summary()
        await _write_progress(
            redis,
            job_id,
            total=progress_total,
            processed=processed + 1,
            failed=unavailable_total,
            phase="tablebase",
            detail="Tablebase and rating projections are current.",
            ingestion_run_id=ingestion_run_id,
        )

    return {
        "processed": processed,
        "solved": solved_total,
        "unavailable": unavailable_total,
        "target": target,
        "skipped": skipped_total,
    }


async def analyze_tablebase_positions(ctx, **kwargs):
    async with StepReporter(ctx, kwargs.get("ingestion_run_id")) as reporter:
        return await _analyze_tablebase_positions(ctx, reporter=reporter, **kwargs)


async def _release_coordination(redis: ArqRedis, job_id: str) -> None:
    await release_owned(redis, TABLEBASE_COORDINATION_KEY, job_id)


async def _enqueue_tablebase_job(redis, *, game_links, max_positions, batch_size, ingestion_run_id):
    """One normalized queue contract for shared jobs and scoped finalization."""
    job = await redis.enqueue_job(
        'run_tablebase_analysis_job',
        max_positions=max_positions,
        batch_size=max(1, min(int(batch_size), 5_000)),
        candidate_count=None,
        game_links=clean_game_links(game_links) if game_links is not None else None,
        ingestion_run_id=ingestion_run_id,
        _queue_name='pipeline_queue',
    )
    if job is None:
        raise RuntimeError('Redis did not create the tablebase job.')
    return job


async def _queue_ingestion_followup(redis, *, game_links, max_positions, batch_size, ingestion_run_id):
    """A shared job cannot finish another ingestion's durable progress record.

    The single pipeline worker executes this scoped follow-up after its current
    work. Discovery only selects still-unscored positions, so shared-job results
    are reused, never recalculated. Do not replace the shared coordination owner.
    """
    job = await _enqueue_tablebase_job(redis, game_links=game_links, max_positions=max_positions,
        batch_size=batch_size, ingestion_run_id=ingestion_run_id)
    job_id = str(job.job_id)
    await _write_progress(redis, job_id, total=0, processed=0, failed=0,
        phase='queued', ingestion_run_id=ingestion_run_id,
        detail='Waiting for shared endgame work; then checking this ingestion and finalizing its progress.')
    return {'status':'queued', 'job_id':job_id, 'pending':None}


async def ensure_tablebase_analysis_enqueued(
    redis: ArqRedis,
    *,
    game_links: Sequence[int] | None = None,
    max_positions: int | None = None,
    batch_size: int = TABLEBASE_BATCH_SIZE,
    ingestion_run_id: str | None = None,
) -> dict[str, Any]:
    async def already_active(job_id=None, pending=None):
        if ingestion_run_id:
            return await _queue_ingestion_followup(redis, game_links=game_links,
                max_positions=max_positions, batch_size=batch_size,
                ingestion_run_id=ingestion_run_id)
        return {'status':'already_active', 'job_id':job_id, 'pending':pending}

    current_job_id = _redis_text(await redis.get(TABLEBASE_COORDINATION_KEY))
    if current_job_id and current_job_id != "reserving":
        status = await Job(
            current_job_id,
            redis,
            _queue_name="pipeline_queue",
        ).status()
        if status in ACTIVE_JOB_STATUSES:
            return await already_active(current_job_id)
        await _release_coordination(redis, current_job_id)
    elif current_job_id == "reserving":
        return await already_active()

    clean_links = clean_game_links(game_links)
    has_work = await has_tablebase_candidates(
        clean_links if game_links is not None else None
    )
    if not has_work:
        if game_links is None:
            await refresh_fen_pipeline_summary(mark_tablebase_current=True)
        return {"status": "up_to_date", "job_id": None, "pending": 0}
    pending = None  # Exact count comes from discovery, not a second whole scan.

    reserved = await redis.set(
        TABLEBASE_COORDINATION_KEY,
        "reserving",
        ex=RESERVATION_TTL_SECONDS,
        nx=True,
    )
    if not reserved:
        return await already_active(pending=pending)

    try:
        job = await _enqueue_tablebase_job(
            redis,
            max_positions=max_positions,
            batch_size=batch_size,
            game_links=clean_links if game_links is not None else None,
            ingestion_run_id=ingestion_run_id,
        )
        job_id = str(job.job_id)
        await redis.set(
            TABLEBASE_COORDINATION_KEY,
            job_id,
            ex=COORDINATION_TTL_SECONDS,
        )
        await _write_progress(
            redis,
            job_id,
            total=0,
            processed=0,
            failed=0,
            phase="queued",
            detail="Waiting to cache exact Syzygy endgames.",
            ingestion_run_id=ingestion_run_id,
        )
        return {"status": "queued", "job_id": job_id, "pending": pending}
    except Exception:
        await _release_coordination(redis, "reserving")
        raise


async def run_tablebase_analysis_job(
    ctx: dict,
    max_positions: int | None = None,
    batch_size: int = TABLEBASE_BATCH_SIZE,
    candidate_count: int | None = None,
    game_links: Sequence[int] | None = None,
    ingestion_run_id: str | None = None,
    **kwargs,
) -> dict[str, int]:
    redis: ArqRedis = ctx["redis"]
    job_id = str(ctx.get("job_id") or "tablebase-analysis")
    try:
        await start_ingestion_stage(
            ingestion_run_id,
            TABLEBASE_MARKING,
            detail="Probing exact Syzygy endgames and refreshing projections.",
        )
        result = await analyze_tablebase_positions(
            ctx,
            game_links=game_links,
            candidate_count=candidate_count,
            max_positions=max_positions,
            batch_size=batch_size,
            progress_job_id=job_id,
            ingestion_run_id=ingestion_run_id,
        )
        if game_links is None and not await has_tablebase_candidates():
            await refresh_fen_pipeline_summary(mark_tablebase_current=True)
        await _write_progress(
            redis,
            job_id,
            total=result["target"] + 1,
            processed=result["processed"] + 1,
            failed=result["unavailable"],
            phase="complete",
            detail=f"Cached {result['solved']} exact Syzygy positions.",
            ingestion_run_id=ingestion_run_id,
        )
        try:
            async with StepReporter(ctx, ingestion_run_id) as reporter:
                async with reporter.step("queue_player_summaries", None, "jobs"):
                    salience_jobs = await enqueue_stale_player_salience_jobs(redis)
                    await reporter.advance(len(salience_jobs or []))
            if salience_jobs:
                print(
                    f"[TABLEBASE {job_id[:6]}] Queued {len(salience_jobs)} "
                    "player salience refreshes.",
                    flush=True,
                )
        except Exception as error:
            print(
                f"[TABLEBASE {job_id[:6]}] Could not queue player salience "
                f"refreshes: {error!r}",
                flush=True,
            )
        await finish_ingestion_stage(
            ingestion_run_id,
            TABLEBASE_MARKING,
            processed=result["processed"] + 1,
            total=result["target"] + 1,
            detail=f"Cached {result['solved']} exact Syzygy positions.",
        )
        await finish_ingestion_run(ingestion_run_id)
        return result
    except Exception as error:
        previous_progress = await _read_progress(redis, job_id)
        await _write_progress(
            redis,
            job_id,
            total=int(previous_progress.get("total") or 0),
            processed=int(previous_progress.get("processed") or 0),
            failed=max(1, int(previous_progress.get("failed") or 0)),
            phase="failed",
            detail=str(error),
            ingestion_run_id=ingestion_run_id,
        )
        await fail_running_ingestion_stages(ingestion_run_id, str(error))
        await finish_ingestion_run(
            ingestion_run_id,
            status="failed",
            error=str(error),
        )
        raise
    finally:
        await _release_coordination(redis, job_id)
