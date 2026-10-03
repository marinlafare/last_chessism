# chessism_api/operations/analysis.py

import asyncio
import httpx
import json
import math
import time
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, insert

# --- FIXED IMPORTS ---
from chessism_api.database.models import Fen, FenContinuation
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.ask_db import (
    count_game_set_fens_for_analysis,
    get_game_set_analysis_completion,
    get_game_set_fens_for_analysis,
    get_fens_for_analysis,
    get_player_fens_for_analysis,
    increment_game_analysis_summary_for_scored_fens,
    increment_database_summary_fen_counts,
    refresh_game_analysis_summary,
    refresh_scored_position_summary,
    refresh_scored_rating_summary
)
from chessism_api.operations.analysis_times import record_analysis_times
from chessism_api.operations.tablebase import analyze_tablebase_positions
# ---

# --- Engine service URL ---
ENGINE_URL = "http://stockfish-service:9999/analyze"

# Initialize the DBInterface for Fen
fen_interface = DBInterface(Fen)

# Concurrency for analysis batches (number of parallel workers per job)
ANALYSIS_CONCURRENCY = max(1, int(os.getenv("ANALYSIS_CONCURRENCY", "4")))
MAX_ANALYSIS_BATCH_SIZE = 500
MAX_LOOP_ANALYSIS_BATCH_SIZE = 1000
PROGRESS_TTL_SECONDS = 60 * 60 * 24

JOB_PROGRESS_UPDATE_SCRIPT = """
local incoming = cjson.decode(ARGV[1])
local current = redis.call('GET', KEYS[1])
if current then
    local ok, decoded = pcall(cjson.decode, current)
    if ok then
        incoming['processed'] = math.max(
            tonumber(incoming['processed']) or 0,
            tonumber(decoded['processed']) or 0
        )
        incoming['failed'] = math.max(
            tonumber(incoming['failed']) or 0,
            tonumber(decoded['failed']) or 0
        )
    end
end
redis.call('SET', KEYS[1], cjson.encode(incoming), 'EX', tonumber(ARGV[2]))
return incoming['processed']
"""


class EngineServiceRequestError(RuntimeError):
    """A permanent request error that must fail the queued analysis job."""


async def _call_engine_service(
    client: httpx.AsyncClient,
    url: str,
    fens: List[str],
    nodes: int,
    *,
    progress_job_id: str | None = None,
    progress_total: int | None = None,
    progress_detail_prefix: str | None = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Sends a batch of FENs to the specified analysis engine service.
    """
    payload = {
        "fens": fens,
        "nodes_limit": nodes
    }
    if progress_job_id:
        payload.update({
            "progress_job_id": progress_job_id,
            "progress_total": progress_total or len(fens),
            "progress_detail_prefix": progress_detail_prefix,
        })
    try:
        response = await client.post(url, json=payload, timeout=None) # Disable timeout
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        message = (
            "HTTP error calling engine service: "
            f"{e.response.status_code} - {e.response.text}"
        )
        print(message, flush=True)
        if 400 <= e.response.status_code < 500:
            raise EngineServiceRequestError(message) from e
        return None
    except httpx.RequestError as e:
        print(f"Request error calling engine service: {repr(e)}", flush=True)
        return None
    except Exception as e:
        print(f"Unexpected error in _call_engine_service: {repr(e)}", flush=True)
        return None


async def _write_job_progress(
    ctx: dict,
    job_id: str,
    *,
    total: int,
    processed: int,
    failed: int,
    phase: str,
    detail: str | None = None
) -> None:
    redis = ctx.get("redis")
    if not redis:
        return

    payload = {
        "job_id": job_id,
        "total": int(total),
        "processed": int(processed),
        "failed": int(failed),
        "phase": phase,
        "detail": detail,
        "updated_at": time.time(),
    }
    try:
        await redis.eval(
            JOB_PROGRESS_UPDATE_SCRIPT,
            1,
            f"chessism:job_progress:{job_id}",
            json.dumps(payload),
            PROGRESS_TTL_SECONDS,
        )
    except Exception as e:
        print(f"Failed to write job progress for {job_id}: {repr(e)}", flush=True)


async def _reset_job_progress(ctx: dict, job_id: str) -> None:
    redis = ctx.get("redis")
    if not redis:
        return
    try:
        await redis.delete(
            f"chessism:job_progress:{job_id}",
            f"chessism:job_progress_fens:{job_id}",
            f"chessism:job_progress_failed_fens:{job_id}",
        )
    except Exception as e:
        print(f"Failed to reset job progress for {job_id}: {repr(e)}", flush=True)

def _format_engine_results(
    engine_output: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parses the raw JSON output from the engine into the DB format.
    """
    formatted_results = []
    continuations = []
    for item in engine_output:
        if not item or not item.get("is_valid"):
            continue

        fen_str = item.get("fen")
        analysis = item.get("analysis", {})
        if isinstance(analysis, list):
            best_analysis = analysis[0] if analysis else {}
            other_lines = analysis[1:4]
        else:
            best_analysis = analysis
            other_lines = []
        
        # Extract score (in centipawns)
        # We use .get('score') which is the simplified PovScore
        score_cp = best_analysis.get("score")

        # Extract next_moves (the principal variation)
        pv = best_analysis.get("pv", [])
        
        # Convert PV list of moves to a single string
        next_moves_str = " ".join(pv) if pv else None

        wdl = best_analysis.get("wdl")
        wdl_win = wdl_draw = wdl_loss = None
        if isinstance(wdl, list) and len(wdl) >= 3:
            wdl_win, wdl_draw, wdl_loss = wdl[0], wdl[1], wdl[2]

        if fen_str and score_cp is not None:
            formatted_results.append({
                "fen": fen_str,
                "piece_count": sum(
                    character.isalpha()
                    for character in str(fen_str).split(" ", 1)[0]
                ),
                "score": float(score_cp),
                "next_moves": next_moves_str,
                "wdl_win": wdl_win,
                "wdl_draw": wdl_draw,
                "wdl_loss": wdl_loss
            })
            for rank, line in enumerate(other_lines, start=2):
                pv_line = line.get("pv", [])
                first_move = pv_line[0] if pv_line else None
                line_score = line.get("score")
                if first_move and line_score is not None:
                    continuations.append({
                        "fen_fen": fen_str,
                        "rank": rank,
                        "move": first_move,
                        "score": float(line_score)
                    })
    return formatted_results, continuations


def _engine_elapsed_ms(result: Dict[str, Any]) -> float | None:
    analysis = result.get("analysis")
    if isinstance(analysis, list) and analysis:
        value = analysis[0].get("time")
    elif isinstance(analysis, dict):
        value = analysis.get("time")
    else:
        value = None
    try:
        return float(value) * 1000
    except (TypeError, ValueError):
        return None


def timing_rows_from_engine_results(
    fens: List[str],
    engine_results: List[Dict[str, Any]],
    *,
    source: str,
    nodes_limit: int,
    multipv: int = 4,
) -> List[Dict[str, Any]]:
    rows = []
    for fen, result in zip(fens, engine_results):
        elapsed_ms = _engine_elapsed_ms(result)
        if elapsed_ms is None:
            continue
        rows.append({
            "fen": fen,
            "source": source,
            "nodes_limit": nodes_limit,
            "multipv": multipv,
            "elapsed_ms": elapsed_ms,
            "engine_result": result,
        })
    return rows


async def _increment_summary_for_analysis_results(
    db_ready_data: List[Dict[str, Any]]
) -> Dict[str, int]:
    analyzed_delta = len(db_ready_data)
    nonzero_scored_delta = 0
    for item in db_ready_data:
        try:
            if float(item.get("score") or 0) != 0.0:
                nonzero_scored_delta += 1
        except (TypeError, ValueError):
            continue

    if analyzed_delta <= 0:
        return {"games": 0, "fens": 0, "fully_analyzed_games": 0}

    try:
        await increment_database_summary_fen_counts(
            analyzed_delta=analyzed_delta,
            nonzero_scored_delta=nonzero_scored_delta
        )
        return await increment_game_analysis_summary_for_scored_fens(db_ready_data)
    except Exception as error:
        print(f"Failed to update database summary after analysis batch: {repr(error)}", flush=True)
        return {"games": 0, "fens": 0, "fully_analyzed_games": 0}


async def _refresh_scored_projections_after_analysis(job_id: str, processed: int) -> None:
    if processed <= 0:
        return

    try:
        scored_summary = await refresh_scored_position_summary()
        print(
            f"[{job_id}] Refreshed scored position summary at job end: "
            f"{scored_summary.get('scored_positions', 0)} scored positions.",
            flush=True
        )
    except Exception as error:
        print(f"Failed to refresh scored position summary after {job_id}: {repr(error)}", flush=True)

    try:
        await refresh_scored_rating_summary()
        print(f"[{job_id}] Refreshed scored rating summary at job end.", flush=True)
    except Exception as error:
        print(f"Failed to refresh scored rating summary after {job_id}: {repr(error)}", flush=True)


FenBatchFetcher = Callable[
    [int],
    Awaitable[Tuple[Optional[AsyncSession], Optional[List[str]]]],
]


async def _run_analysis_job(
    ctx: dict,
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int,
    *,
    fetch_batch: FenBatchFetcher,
    timing_source: str,
    job_id: str,
    fallback_arq_job_id: str,
    no_more_message: str,
    max_batch_size: int = MAX_ANALYSIS_BATCH_SIZE,
    reset_progress: bool = True,
    progress_total: int | None = None,
    progress_offset: int = 0,
    failed_offset: int = 0,
    progress_detail_prefix: str | None = None,
    complete_progress: bool = True,
    refresh_projections: bool = True,
) -> Dict[str, int]:
    """Run the shared transactional Stockfish analysis loop."""
    arq_job_id = str(ctx.get("job_id") or fallback_arq_job_id)
    engine_url = ENGINE_URL
    
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    print(f"[{job_id}] Routing to: {engine_url}", flush=True) # <-- Test log

    total_processed = 0
    total_claimed = 0
    total_engine_processed = 0
    total_failed_batches = 0
    total_failed_fens = 0
    job_start_time = time.time()
    counter_lock = asyncio.Lock()
    overall_progress_total = int(progress_total or total_fens_to_process)
    if reset_progress:
        await _reset_job_progress(ctx, arq_job_id)
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=overall_progress_total,
        processed=progress_offset,
        failed=failed_offset,
        phase="queued" if progress_offset == 0 else "loop_start",
        detail=progress_detail_prefix,
    )

    async def _worker(worker_id: int):
        nonlocal total_claimed, total_processed, total_engine_processed
        nonlocal total_failed_batches, total_failed_fens
        async with httpx.AsyncClient() as client:
            while True:
                async with counter_lock:
                    remaining = total_fens_to_process - total_claimed
                    if remaining <= 0:
                        return
                    # Keep legacy or manually queued payloads compatible with the
                    # Stockfish service request limit.
                    current_batch_size = min(
                        batch_size,
                        remaining,
                        max_batch_size,
                    )
                    total_claimed += current_batch_size

                print(f"[{job_id}] Worker {worker_id} processing batch...", flush=True)

                fens_to_process = None
                session: Optional[AsyncSession] = None
                reserved_fens = current_batch_size
                committed = False

                try:
                    session, fens_to_process = await fetch_batch(current_batch_size)

                    if not fens_to_process or session is None:
                        async with counter_lock:
                            total_claimed -= reserved_fens
                        print(f"[{job_id}] {no_more_message}", flush=True)
                        return

                    if len(fens_to_process) < reserved_fens:
                        async with counter_lock:
                            total_claimed -= reserved_fens - len(fens_to_process)
                        reserved_fens = len(fens_to_process)

                    # --- MODIFIED: Start timing ---
                    batch_start_time = time.time()

                    # 2. Call engine service once per batch. Stockfish service updates per-FEN progress.
                    engine_results = await _call_engine_service(
                        client,
                        engine_url,
                        fens_to_process,
                        nodes_limit,
                        progress_job_id=arq_job_id,
                        progress_total=overall_progress_total,
                        progress_detail_prefix=progress_detail_prefix,
                    )

                    # --- MODIFIED: End timing ---
                    batch_end_time = time.time()
                    batch_duration = batch_end_time - batch_start_time

                    if not engine_results:
                        print(f"[{job_id}] Failed to get results from engine service. Skipping batch.", flush=True)
                        async with counter_lock:
                            total_failed_batches += 1
                            total_claimed -= reserved_fens
                        await session.rollback()
                        await asyncio.sleep(1)
                        continue

                    await record_analysis_times(timing_rows_from_engine_results(
                        fens_to_process,
                        engine_results,
                        source=timing_source,
                        nodes_limit=nodes_limit,
                    ))

                    # 3. Format results
                    db_ready_data, continuation_rows = _format_engine_results(engine_results)

                    if not db_ready_data:
                        print(f"[{job_id}] No valid analysis data returned from engine. Skipping batch.", flush=True)
                        async with counter_lock:
                            total_failed_batches += 1
                            total_failed_fens += len(fens_to_process)
                            total_claimed -= reserved_fens
                        await session.rollback()
                        await asyncio.sleep(1)
                        continue

                    # 4. Save results (using the same session)
                    await fen_interface.update_fen_analysis_data(session, db_ready_data)
                    fen_list = [item["fen"] for item in db_ready_data]
                    await session.execute(
                        delete(FenContinuation).where(FenContinuation.fen_fen.in_(fen_list))
                    )
                    if continuation_rows:
                        await session.execute(insert(FenContinuation.__table__), continuation_rows)

                    # 5. Commit the transaction
                    # This saves the data AND releases the 'FOR UPDATE SKIP LOCKED'
                    await session.commit()
                    committed = True
                    await _increment_summary_for_analysis_results(db_ready_data)

                    # --- MODIFIED: Calculate time per FEN ---
                    fens_in_batch = len(fens_to_process)
                    time_per_fen = (batch_duration / fens_in_batch) if fens_in_batch > 0 else 0
                    async with counter_lock:
                        total_engine_processed += len(engine_results)
                        total_processed += fens_in_batch
                        total_failed_fens += max(0, fens_in_batch - len(db_ready_data))
                        total_so_far = total_processed

                    print(f"[{job_id}] Batch complete. Total FENs: {total_so_far}. Batch Time: {batch_duration:.2f}s ({time_per_fen:.2f} s/FEN)", flush=True)
                    await _write_job_progress(
                        ctx,
                        arq_job_id,
                        total=overall_progress_total,
                        processed=progress_offset + total_engine_processed,
                        failed=failed_offset + total_failed_fens,
                        phase="committed",
                        detail=(
                            f"{progress_detail_prefix} | " if progress_detail_prefix else ""
                        ) + f"committed {total_so_far}/{total_fens_to_process}"
                    )

                except EngineServiceRequestError as e:
                    print(f"CRITICAL: Permanent error in {job_id}: {e}", flush=True)
                    async with counter_lock:
                        total_failed_batches += 1
                    if session:
                        await session.rollback()
                    await _write_job_progress(
                        ctx,
                        arq_job_id,
                        total=overall_progress_total,
                        processed=progress_offset + total_engine_processed,
                        failed=failed_offset + total_failed_fens,
                        phase="failed",
                        detail=str(e),
                    )
                    raise
                except Exception as e:
                    print(f"CRITICAL: Unhandled error in {job_id} analysis loop: {repr(e)}", flush=True)
                    async with counter_lock:
                        total_failed_batches += 1
                        if not committed:
                            total_claimed -= reserved_fens
                    if session:
                        await session.rollback() # Ensure locks are released on failure
                    await asyncio.sleep(1)
                finally:
                    if session:
                        await session.close()

    worker_count = ANALYSIS_CONCURRENCY
    print(f"[{job_id}] Concurrency: {worker_count}", flush=True)
    worker_tasks = [
        asyncio.create_task(_worker(i + 1))
        for i in range(worker_count)
    ]
    try:
        await asyncio.gather(*worker_tasks)
    except BaseException:
        for task in worker_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise

    job_end_time = time.time()
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {(job_end_time - job_start_time):.2f} seconds", flush=True)
    print(f"Total FENs processed: {total_processed}", flush=True)
    print(f"Total failed batches: {total_failed_batches}", flush=True)
    if refresh_projections:
        await _refresh_scored_projections_after_analysis(job_id, total_engine_processed)
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=overall_progress_total,
        processed=progress_offset + total_engine_processed,
        failed=failed_offset + total_failed_fens,
        phase="complete" if complete_progress else "loop_complete",
        detail=(
            f"{progress_detail_prefix} | " if progress_detail_prefix else ""
        ) + f"processed {total_processed}"
    )
    return {
        "processed": total_processed,
        "engine_processed": total_engine_processed,
        "failed": total_failed_fens,
        "failed_batches": total_failed_batches,
    }


async def run_analysis_job(
    ctx: dict,
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int,
    **kwargs,
) -> None:
    await _run_analysis_job(
        ctx,
        total_fens_to_process,
        batch_size,
        nodes_limit,
        fetch_batch=get_fens_for_analysis,
        timing_source="most_repeated",
        job_id="ANALYSIS",
        fallback_arq_job_id="analysis",
        no_more_message="No more FENs found to analyze. Stopping job.",
    )


async def run_player_analysis_job(
    ctx: dict,
    player_name: str,
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int,
    **kwargs,
) -> None:
    async def fetch_player_batch(
        limit: int,
    ) -> Tuple[Optional[AsyncSession], Optional[List[str]]]:
        return await get_player_fens_for_analysis(player_name, limit)

    await _run_analysis_job(
        ctx,
        total_fens_to_process,
        batch_size,
        nodes_limit,
        fetch_batch=fetch_player_batch,
        timing_source="character_repeated",
        job_id=f"ANALYSIS (Player: {player_name})",
        fallback_arq_job_id=f"analysis-player-{player_name}",
        no_more_message=f"No more FENs found for player {player_name}. Stopping job.",
    )


async def run_analysis_loop_job(
    ctx: dict,
    scope: str,
    runs: int,
    positions_per_run: int,
    batches: int,
    cool_off: int,
    nodes_limit: int,
    player_name: str | None = None,
    **kwargs,
) -> Dict[str, int | str]:
    """Run several sequential analysis passes as one durable ARQ job."""
    normalized_scope = str(scope or "all").strip().lower()
    safe_runs = max(1, int(runs))
    safe_positions = max(1, int(positions_per_run))
    safe_batch_size = max(1, min(int(batches), MAX_LOOP_ANALYSIS_BATCH_SIZE))
    safe_cool_off = max(0, int(cool_off))
    normalized_player = str(player_name or "").strip().lower()
    total_target = safe_runs * safe_positions
    arq_job_id = str(ctx.get("job_id") or "analysis-loop")

    if normalized_scope == "player":
        if not normalized_player:
            raise ValueError("player_name is required for player analysis loops")

        async def fetch_batch(
            limit: int,
        ) -> Tuple[Optional[AsyncSession], Optional[List[str]]]:
            return await get_player_fens_for_analysis(normalized_player, limit)

        timing_source = "character_repeated"
        loop_label = f"ANALYSIS LOOPS (Player: {normalized_player})"
        no_more_message = f"No more FENs found for player {normalized_player}."
    elif normalized_scope == "all":
        fetch_batch = get_fens_for_analysis
        timing_source = "most_repeated"
        loop_label = "ANALYSIS LOOPS (All)"
        no_more_message = "No more FENs found to analyze."
    else:
        raise ValueError(f"Unsupported analysis loop scope: {scope}")

    print(
        f"--- [START {loop_label}] {safe_runs} runs x {safe_positions} positions, "
        f"batch {safe_batch_size}, cool-off {safe_cool_off}s ---",
        flush=True,
    )
    await _reset_job_progress(ctx, arq_job_id)
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=total_target,
        processed=0,
        failed=0,
        phase="queued",
        detail=f"waiting to start run 1/{safe_runs}",
    )

    total_processed = 0
    total_failed = 0
    completed_runs = 0
    for run_number in range(1, safe_runs + 1):
        run_prefix = f"run {run_number}/{safe_runs}"
        result = await _run_analysis_job(
            ctx,
            safe_positions,
            safe_batch_size,
            nodes_limit,
            fetch_batch=fetch_batch,
            timing_source=timing_source,
            job_id=f"{loop_label} {run_prefix}",
            fallback_arq_job_id=arq_job_id,
            no_more_message=no_more_message,
            max_batch_size=MAX_LOOP_ANALYSIS_BATCH_SIZE,
            reset_progress=False,
            progress_total=total_target,
            progress_offset=total_processed,
            failed_offset=total_failed,
            progress_detail_prefix=run_prefix,
            complete_progress=False,
            refresh_projections=False,
        )
        run_processed = int(result.get("engine_processed") or 0)
        total_processed += run_processed
        total_failed += int(result.get("failed") or 0)
        completed_runs += 1

        if run_processed == 0:
            print(f"[{loop_label}] No positions processed in {run_prefix}; stopping early.", flush=True)
            break

        if run_number < safe_runs and safe_cool_off > 0:
            await _write_job_progress(
                ctx,
                arq_job_id,
                total=total_target,
                processed=total_processed,
                failed=total_failed,
                phase="cooling",
                detail=f"{run_prefix} complete; next run in {safe_cool_off}s",
            )
            await asyncio.sleep(safe_cool_off)

    await _refresh_scored_projections_after_analysis(loop_label, total_processed)
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=total_target,
        processed=total_processed,
        failed=total_failed,
        phase="complete",
        detail=f"completed {completed_runs}/{safe_runs} runs; processed {total_processed}",
    )
    print(
        f"--- [END {loop_label}] completed {completed_runs}/{safe_runs} runs, "
        f"processed {total_processed}/{total_target} positions ---",
        flush=True,
    )
    return {
        "scope": normalized_scope,
        "runs": completed_runs,
        "processed": total_processed,
        "failed": total_failed,
    }


async def run_player_games_analysis_job(
    ctx: dict,
    player_name: str,
    game_links: List[int],
    planned_fens: int,
    batch_size: int,
    nodes_limit: int,
    cool_off: int = 120,
    chunk_size: int = 5_000,
    selection_mode: str | None = None,
    selection_order: str | None = None,
    selected_games: int | None = None,
    **kwargs,
) -> Dict[str, int | str]:
    """Complete a frozen, ordered set of player games with deduplicated FEN work."""
    normalized_player = str(player_name or "").strip().lower()
    clean_links = [int(link) for link in game_links if link is not None]
    safe_batch_size = max(1, min(int(batch_size), MAX_LOOP_ANALYSIS_BATCH_SIZE))
    safe_chunk_size = max(1, int(chunk_size))
    safe_cool_off = max(0, int(cool_off))
    arq_job_id = str(ctx.get("job_id") or f"analysis-player-games-{normalized_player}")
    job_label = f"COMPLETE PLAYER GAMES ({normalized_player})"
    tablebase_result = await analyze_tablebase_positions(
        ctx,
        game_links=clean_links,
        batch_size=safe_batch_size,
        progress_job_id=arq_job_id,
        refresh_projections=False,
    )
    tablebase_solved = int(tablebase_result.get("solved") or 0)
    actual_target = await count_game_set_fens_for_analysis(clean_links)

    await _reset_job_progress(ctx, arq_job_id)
    if actual_target <= 0:
        await refresh_game_analysis_summary(tuple(clean_links))
        if tablebase_solved > 0:
            await _refresh_scored_projections_after_analysis(job_label, tablebase_solved)
        completion = await get_game_set_analysis_completion(clean_links)
        await _write_job_progress(
            ctx,
            arq_job_id,
            total=1,
            processed=1,
            failed=0,
            phase="complete",
            detail=f"{completion['fully_analyzed_games']}/{completion['selected_games']} games complete; no FENs remained",
        )
        return {
            "scope": "player_games",
            "player_name": normalized_player,
            "processed": 0,
            "tablebase_solved": tablebase_solved,
            "planned_fens": int(planned_fens),
            **completion,
        }

    pass_count = math.ceil(actual_target / safe_chunk_size)
    print(
        f"--- [START {job_label}] {len(clean_links)} frozen games, "
        f"{actual_target} Stockfish FENs after {tablebase_solved} tablebase hits, "
        f"{pass_count} passes, batch {safe_batch_size} ---",
        flush=True,
    )
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=actual_target,
        processed=0,
        failed=0,
        phase="queued",
        detail=f"preparing {len(clean_links)} games",
    )

    async def fetch_game_batch(
        limit: int,
    ) -> Tuple[Optional[AsyncSession], Optional[List[str]]]:
        return await get_game_set_fens_for_analysis(clean_links, limit)

    total_processed = 0
    total_failed = 0
    completed_passes = 0
    for pass_number in range(1, pass_count + 1):
        pass_target = min(safe_chunk_size, actual_target - total_processed)
        if pass_target <= 0:
            break
        pass_prefix = f"game completion pass {pass_number}/{pass_count}"
        result = await _run_analysis_job(
            ctx,
            pass_target,
            safe_batch_size,
            nodes_limit,
            fetch_batch=fetch_game_batch,
            timing_source="player_games",
            job_id=f"{job_label} {pass_prefix}",
            fallback_arq_job_id=arq_job_id,
            no_more_message=f"No missing FENs remain for {normalized_player}'s selected games.",
            max_batch_size=MAX_LOOP_ANALYSIS_BATCH_SIZE,
            reset_progress=False,
            progress_total=actual_target,
            progress_offset=total_processed,
            failed_offset=total_failed,
            progress_detail_prefix=pass_prefix,
            complete_progress=False,
            refresh_projections=False,
        )
        pass_processed = int(result.get("engine_processed") or 0)
        total_processed += pass_processed
        total_failed += int(result.get("failed") or 0)
        completed_passes += 1
        if pass_processed == 0:
            break

        if pass_number < pass_count and safe_cool_off > 0:
            await _write_job_progress(
                ctx,
                arq_job_id,
                total=actual_target,
                processed=total_processed,
                failed=total_failed,
                phase="cooling",
                detail=f"pass {pass_number}/{pass_count} complete; next pass in {safe_cool_off}s",
            )
            await asyncio.sleep(safe_cool_off)

    await refresh_game_analysis_summary(tuple(clean_links))
    completion = await get_game_set_analysis_completion(clean_links)
    await _refresh_scored_projections_after_analysis(
        job_label,
        total_processed + tablebase_solved,
    )
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=actual_target,
        processed=total_processed,
        failed=total_failed,
        phase="complete",
        detail=(
            f"{completion['fully_analyzed_games']}/{completion['selected_games']} games complete; "
            f"processed {total_processed} FENs"
        ),
    )
    print(
        f"--- [END {job_label}] {completion['fully_analyzed_games']}/"
        f"{completion['selected_games']} games complete, {total_processed}/{actual_target} FENs ---",
        flush=True,
    )
    return {
        "scope": "player_games",
        "player_name": normalized_player,
        "selection_mode": str(selection_mode or ""),
        "selection_order": str(selection_order or ""),
        "passes": completed_passes,
        "processed": total_processed,
        "tablebase_solved": tablebase_solved,
        "failed": total_failed,
        "planned_fens": int(planned_fens),
        **completion,
    }
