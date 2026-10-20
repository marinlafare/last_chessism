"""Bounded extraction/persistence workers: queue messages contain IDs, not FENs."""
import asyncio
import logging
import random

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.ask_db import refresh_game_analysis_summary, refresh_fen_pipeline_summary
from .fen_bulk import persist_positions
from .fen_core import process_single_game_sync, aggregate_fen_data
from .fen_repository import (
    claim_games_needing_fens, read_moves_for_games, set_game_fen_state,
    finalize_fen_game_states, release_fen_processing_claims,
)
from .progress import increment_stage_progress, reset_stage_progress
from .steps import StepReporter
from .timing import start_ingestion_stage, finish_ingestion_stage

BATCH_GAMES = 300
MAX_BATCH_ATTEMPTS = 4
RETRYABLE_SQLSTATES = {"40P01", "40001"}  # Deadlock / serialization rollback.
logger = logging.getLogger(__name__)


def _retryable_transaction_error(error):
    """Handle both raw asyncpg errors and SQLAlchemy's wrapped DB errors."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if (getattr(current, "sqlstate", None) in RETRYABLE_SQLSTATES
                or getattr(current, "pgcode", None) in RETRYABLE_SQLSTATES):
            return True
        for attribute in ("orig", "__cause__", "__context__"):
            nested = getattr(current, attribute, None)
            if isinstance(nested, BaseException):
                pending.append(nested)
    return False


async def _commit_batch(limit, reporter):
    """Fresh session/transaction per attempt, including claims and COMMIT."""
    for attempt in range(1, MAX_BATCH_ATTEMPTS + 1):
        checkpoint = reporter.checkpoint()
        try:
            async with AsyncDBSession() as session, session.begin():
                links = await claim_games_needing_fens(session, limit)
                if not links:
                    return None
                moves = await read_moves_for_games(session, links)
                good, bad, associations = [], [], []
                async with reporter.step("extract_positions", len(links), "games"):
                    for link in links:
                        rows, errors = process_single_game_sync((link, moves.get(link, [])))
                        if errors or not rows:
                            bad.append(link)
                        else:
                            good.append(link)
                            associations.extend(rows)
                async with reporter.step("combine_positions", len(associations), "occurrences"):
                    fens, durable = aggregate_fen_data(associations)
                counts = {"new_positions": 0, "new_links": 0}
                if durable:
                    counts = await persist_positions(session, fens, durable, reporter)
                # Keep claimed until the coordinator completes coverage.
                # Replay is safe: inserted links alone drive the counter deltas.
                await set_game_fen_state(session, links, processing=True)
            # Return accounting only AFTER the transaction successfully commits.
            return links, good, bad, len(associations), counts
        except Exception as error:
            retry = _retryable_transaction_error(error) and attempt < MAX_BATCH_ATTEMPTS
            detail = (f"Database contention; retrying rolled-back batch "
                      f"(attempt {attempt + 1}/{MAX_BATCH_ATTEMPTS}).") if retry else None
            await reporter.rollback_attempt(checkpoint, retry_detail=detail)
            if not retry:
                raise
            delay = min(2.0, 0.25 * 2 ** (attempt - 1)) + random.uniform(0, 0.25)
            logger.warning("FEN batch transaction rolled back; retry %s/%s in %.2fs",
                           attempt + 1, MAX_BATCH_ATTEMPTS, delay)
            # Transaction and session are closed before waiting/reclaiming games.
            await asyncio.sleep(delay)


async def run_fen_batch_job(ctx, total_games_to_process, batch_size=BATCH_GAMES,
                            parent_job_id=None, ingestion_run_id=None, **_kwargs):
    successful, failed = [], []
    positions = new_positions = new_links = 0
    remaining = int(total_games_to_process)
    async with StepReporter(ctx, ingestion_run_id) as reporter:
        reporter.goal("extract_positions", remaining)
        while remaining > 0:
            result = await _commit_batch(min(remaining, batch_size, BATCH_GAMES), reporter)
            if result is None:
                break
            links, good, bad, batch_positions, counts = result
            successful.extend(good)
            failed.extend(bad)
            positions += batch_positions
            new_positions += counts["new_positions"]
            new_links += counts["new_links"]
            remaining -= len(links)
            if parent_job_id:
                await increment_stage_progress(ctx["redis"], job_id=parent_job_id,
                    ingestion_run_id=ingestion_run_id, phase="processing_batches",
                    processed_delta=len(links), failed_delta=len(bad),
                    detail="Extracting and committing bounded game batches.")
    return {"successful_game_links": successful, "failed_game_links": failed,
            "positions": positions, "new_positions": new_positions, "new_links": new_links}


async def run_bounded_pipeline(ctx, total_games_to_process, batch_size, num_workers,
                               ingestion_run_id=None, **_kwargs):
    redis, job_id = ctx["redis"], str(ctx["job_id"])
    await start_ingestion_stage(ingestion_run_id, "fen_extraction",
        detail="Extracting, saving and counting bounded position batches.")
    await reset_stage_progress(redis, job_id=job_id, ingestion_run_id=ingestion_run_id,
        phase="processing_batches", total=total_games_to_process, processed=0,
        detail="Extracting and committing bounded game batches.")
    count = min(max(1, num_workers), total_games_to_process)
    per_worker, extra = divmod(total_games_to_process, count)
    children, errors = [], []
    for index in range(count):
        try:
            child = await redis.enqueue_job("run_fen_batch_job",
                total_games_to_process=per_worker + (index < extra),
                batch_size=min(batch_size, BATCH_GAMES), parent_job_id=job_id,
                ingestion_run_id=ingestion_run_id, _queue_name="fen_queue")
            if child is None:
                raise RuntimeError("Could not queue bounded FEN worker")
            children.append(child)
        except Exception as error:
            errors.append(error)
            break
    results = []
    # Always join every writer before releasing claims after a failure.
    for child in children:
        try:
            results.append(await child.result(timeout=None))
        except Exception as error:
            errors.append(error)
    if errors:
        await release_fen_processing_claims()
        raise RuntimeError(f"Bounded FEN worker failed: {errors[0]}")
    successful = sorted({link for result in results for link in result["successful_game_links"]})
    failed = sorted({link for result in results for link in result["failed_game_links"]})
    await finish_ingestion_stage(ingestion_run_id, "fen_extraction",
        processed=sum(r["positions"] for r in results),
        total=sum(r["positions"] for r in results),
        detail="Position batches and exact appearance deltas committed.")
    try:
        async with StepReporter(ctx, ingestion_run_id) as reporter:
            await reset_stage_progress(redis, job_id=job_id, ingestion_run_id=ingestion_run_id,
                phase="refreshing_statistics", total=len(successful), processed=0,
                detail="Refreshing coverage for imported games.")
            async with reporter.step("coverage", len(successful), "games"):
                for offset in range(0, len(successful), 1000):
                    chunk = successful[offset:offset + 1000]
                    await refresh_game_analysis_summary(tuple(chunk))
                    await reporter.advance(len(chunk))
                    await increment_stage_progress(redis, job_id=job_id,
                        ingestion_run_id=ingestion_run_id, phase="refreshing_statistics",
                        processed_delta=len(chunk), detail="Refreshing coverage for imported games.")
            async with reporter.step("ingestion_summaries", 1, "refreshes"):
                await finalize_fen_game_states(successful, failed)
                await refresh_fen_pipeline_summary()
    except BaseException:
        await release_fen_processing_claims(successful + failed)
        raise
    return {"claimed_games": len(successful) + len(failed),
            "successful_games": len(successful), "failed_games": len(failed),
            "successful_game_links": successful}
