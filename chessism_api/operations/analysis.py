# chessism_api/operations/analysis.py

import asyncio
import httpx
import json
import time
import os
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, insert

# --- FIXED IMPORTS ---
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, FenContinuation
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.ask_db import (
    get_fens_for_analysis,
    get_player_fens_for_analysis
)
from chessism_api.operations.analysis_times import record_analysis_times
# ---

# --- Engine service URL ---
ENGINE_URL = "http://stockfish-service:9999/analyze"

# Initialize the DBInterface for Fen
fen_interface = DBInterface(Fen)

# Concurrency for analysis batches (number of parallel workers per job)
ANALYSIS_CONCURRENCY = max(1, int(os.getenv("ANALYSIS_CONCURRENCY", "1")))
PROGRESS_TTL_SECONDS = 60 * 60 * 24

async def _call_engine_service(
    client: httpx.AsyncClient,
    url: str,
    fens: List[str],
    nodes: int,
    *,
    progress_job_id: str | None = None,
    progress_total: int | None = None,
    progress_offset: int = 0,
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
            "progress_offset": progress_offset,
        })
    try:
        response = await client.post(url, json=payload, timeout=None) # Disable timeout
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error calling engine service: {e.response.status_code} - {e.response.text}", flush=True)
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
        await redis.set(
            f"chessism:job_progress:{job_id}",
            json.dumps(payload),
            ex=PROGRESS_TTL_SECONDS
        )
    except Exception as e:
        print(f"Failed to write job progress for {job_id}: {repr(e)}", flush=True)

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


def _timing_rows_from_engine_results(
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

async def run_analysis_job(
    ctx: dict, 
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int,
    **kwargs
):
    """
    The main background task for a general analysis job.
    '**kwargs' is added to accept extra arq arguments.
    """
    arq_job_id = str(ctx.get("job_id") or "analysis")
    job_id = "ANALYSIS"
    engine_url = ENGINE_URL
    
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    print(f"[{job_id}] Routing to: {engine_url}", flush=True) # <-- Test log

    total_processed = 0
    total_engine_processed = 0
    total_failed_batches = 0
    total_failed_fens = 0
    job_start_time = time.time()
    counter_lock = asyncio.Lock()
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=total_fens_to_process,
        processed=0,
        failed=0,
        phase="queued"
    )

    async def _worker(worker_id: int):
        nonlocal total_processed, total_engine_processed, total_failed_batches, total_failed_fens
        async with httpx.AsyncClient() as client:
            while True:
                async with counter_lock:
                    remaining = total_fens_to_process - total_processed
                if remaining <= 0:
                    return

                current_batch_size = min(batch_size, remaining)
                print(f"[{job_id}] Worker {worker_id} processing batch...", flush=True)

                fens_to_process = None
                session: Optional[AsyncSession] = None

                try:
                    # 1. Start a transaction and get FENs (with lock)
                    session, fens_to_process = await get_fens_for_analysis(current_batch_size)

                    if not fens_to_process or session is None:
                        print(f"[{job_id}] No more FENs found to analyze. Stopping job.", flush=True)
                        return

                    # --- MODIFIED: Start timing ---
                    batch_start_time = time.time()

                    async with counter_lock:
                        progress_offset = total_engine_processed

                    # 2. Call engine service once per batch. Stockfish service updates per-FEN progress.
                    engine_results = await _call_engine_service(
                        client,
                        engine_url,
                        fens_to_process,
                        nodes_limit,
                        progress_job_id=arq_job_id,
                        progress_total=total_fens_to_process,
                        progress_offset=progress_offset,
                    )

                    # --- MODIFIED: End timing ---
                    batch_end_time = time.time()
                    batch_duration = batch_end_time - batch_start_time

                    if not engine_results:
                        print(f"[{job_id}] Failed to get results from engine service. Skipping batch.", flush=True)
                        total_failed_batches += 1
                        # Rollback to release the locks
                        await session.rollback()
                        continue

                    await record_analysis_times(_timing_rows_from_engine_results(
                        fens_to_process,
                        engine_results,
                        source="most_repeated",
                        nodes_limit=nodes_limit,
                    ))

                    # 3. Format results
                    db_ready_data, continuation_rows = _format_engine_results(engine_results)

                    if not db_ready_data:
                        print(f"[{job_id}] No valid analysis data returned from engine. Skipping batch.", flush=True)
                        total_failed_batches += 1
                        # Rollback to release the locks
                        await session.rollback()
                        continue

                    # 4. Save results (using the same session)
                    await fen_interface.update_fen_analysis_data(session, db_ready_data)
                    if continuation_rows:
                        fen_list = [item["fen"] for item in db_ready_data]
                        await session.execute(
                            delete(FenContinuation).where(FenContinuation.fen_fen.in_(fen_list))
                        )
                        await session.execute(insert(FenContinuation.__table__), continuation_rows)

                    # 5. Commit the transaction
                    # This saves the data AND releases the 'FOR UPDATE SKIP LOCKED'
                    await session.commit()

                    # --- MODIFIED: Calculate time per FEN ---
                    fens_in_batch = len(fens_to_process)
                    time_per_fen = (batch_duration / fens_in_batch) if fens_in_batch > 0 else 0
                    async with counter_lock:
                        total_engine_processed += len(engine_results)
                        total_processed += fens_in_batch
                        total_so_far = total_processed

                    print(f"[{job_id}] Batch complete. Total FENs: {total_so_far}. Batch Time: {batch_duration:.2f}s ({time_per_fen:.2f} s/FEN)", flush=True)
                    await _write_job_progress(
                        ctx,
                        arq_job_id,
                        total=total_fens_to_process,
                        processed=total_engine_processed,
                        failed=total_failed_fens,
                        phase="committed",
                        detail=f"committed {total_so_far}/{total_fens_to_process}"
                    )

                except Exception as e:
                    print(f"CRITICAL: Unhandled error in {job_id} analysis loop: {repr(e)}", flush=True)
                    total_failed_batches += 1
                    if session:
                        await session.rollback() # Ensure locks are released on failure
                finally:
                    if session:
                        await session.close()

                # Small delay to prevent spamming
                await asyncio.sleep(1)

    worker_count = ANALYSIS_CONCURRENCY
    print(f"[{job_id}] Concurrency: {worker_count}", flush=True)
    await asyncio.gather(*[_worker(i + 1) for i in range(worker_count)])

    job_end_time = time.time()
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {(job_end_time - job_start_time):.2f} seconds", flush=True)
    print(f"Total FENs processed: {total_processed}", flush=True)
    print(f"Total failed batches: {total_failed_batches}", flush=True)
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=total_fens_to_process,
        processed=total_engine_processed,
        failed=total_failed_fens,
        phase="complete",
        detail=f"processed {total_processed}"
    )

async def run_player_analysis_job(
    ctx: dict, 
    player_name: str,
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int,
    **kwargs # <-- THIS IS THE FIX
):
    """
    The main background task for a player-specific analysis job.
    '**kwargs' is added to accept extra arq arguments.
    """
    arq_job_id = str(ctx.get("job_id") or f"analysis-player-{player_name}")
    job_id = f"ANALYSIS (Player: {player_name})"
    engine_url = ENGINE_URL
    
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    print(f"[{job_id}] Routing to: {engine_url}", flush=True) # <-- Test log

    total_processed = 0
    total_engine_processed = 0
    total_failed_batches = 0
    total_failed_fens = 0
    job_start_time = time.time()
    counter_lock = asyncio.Lock()
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=total_fens_to_process,
        processed=0,
        failed=0,
        phase="queued"
    )

    async def _worker(worker_id: int):
        nonlocal total_processed, total_engine_processed, total_failed_batches, total_failed_fens
        async with httpx.AsyncClient() as client:
            while True:
                async with counter_lock:
                    remaining = total_fens_to_process - total_processed
                if remaining <= 0:
                    return

                current_batch_size = min(batch_size, remaining)
                print(f"[{job_id}] Worker {worker_id} processing batch...", flush=True)

                fens_to_process = None
                session: Optional[AsyncSession] = None

                try:
                    # 1. Start a transaction and get player-specific FENs (with lock)
                    session, fens_to_process = await get_player_fens_for_analysis(
                        player_name,
                        current_batch_size
                    )

                    if not fens_to_process or session is None:
                        print(f"[{job_id}] No more FENs found for player {player_name}. Stopping job.", flush=True)
                        return

                    # --- MODIFIED: Start timing ---
                    batch_start_time = time.time()

                    async with counter_lock:
                        progress_offset = total_engine_processed

                    # 2. Call engine service once per batch. Stockfish service updates per-FEN progress.
                    engine_results = await _call_engine_service(
                        client,
                        engine_url,
                        fens_to_process,
                        nodes_limit,
                        progress_job_id=arq_job_id,
                        progress_total=total_fens_to_process,
                        progress_offset=progress_offset,
                    )

                    # --- MODIFIED: End timing ---
                    batch_end_time = time.time()
                    batch_duration = batch_end_time - batch_start_time

                    if not engine_results:
                        print(f"[{job_id}] Failed to get results from engine service. Skipping batch.", flush=True)
                        total_failed_batches += 1
                        await session.rollback()
                        continue

                    await record_analysis_times(_timing_rows_from_engine_results(
                        fens_to_process,
                        engine_results,
                        source="character_repeated",
                        nodes_limit=nodes_limit,
                    ))

                    # 3. Format results
                    db_ready_data, continuation_rows = _format_engine_results(engine_results)

                    if not db_ready_data:
                        print(f"[{job_id}] No valid analysis data from engine. Skipping batch.", flush=True)
                        total_failed_batches += 1
                        await session.rollback()
                        continue

                    # 4. Save results (using the same session)
                    await fen_interface.update_fen_analysis_data(session, db_ready_data)
                    if continuation_rows:
                        fen_list = [item["fen"] for item in db_ready_data]
                        await session.execute(
                            delete(FenContinuation).where(FenContinuation.fen_fen.in_(fen_list))
                        )
                        await session.execute(insert(FenContinuation.__table__), continuation_rows)

                    # 5. Commit the transaction
                    await session.commit()

                    # --- MODIFIED: Calculate time per FEN ---
                    fens_in_batch = len(fens_to_process)
                    time_per_fen = (batch_duration / fens_in_batch) if fens_in_batch > 0 else 0
                    async with counter_lock:
                        total_engine_processed += len(engine_results)
                        total_processed += fens_in_batch
                        total_so_far = total_processed

                    print(f"[{job_id}] Batch complete. Total FENs: {total_so_far}. Batch Time: {batch_duration:.2f}s ({time_per_fen:.2f} s/FEN)", flush=True)
                    await _write_job_progress(
                        ctx,
                        arq_job_id,
                        total=total_fens_to_process,
                        processed=total_engine_processed,
                        failed=total_failed_fens,
                        phase="committed",
                        detail=f"committed {total_so_far}/{total_fens_to_process}"
                    )

                except Exception as e:
                    print(f"CRITICAL: Unhandled error in {job_id} loop: {repr(e)}", flush=True)
                    total_failed_batches += 1
                    if session:
                        await session.rollback()
                finally:
                    if session:
                        await session.close()

                await asyncio.sleep(1)

    worker_count = ANALYSIS_CONCURRENCY
    print(f"[{job_id}] Concurrency: {worker_count}", flush=True)
    await asyncio.gather(*[_worker(i + 1) for i in range(worker_count)])

    job_end_time = time.time()
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {(job_end_time - job_start_time):.2f} seconds", flush=True)
    print(f"Total FENs processed: {total_processed}", flush=True)
    print(f"Total failed batches: {total_failed_batches}", flush=True)
    await _write_job_progress(
        ctx,
        arq_job_id,
        total=total_fens_to_process,
        processed=total_engine_processed,
        failed=total_failed_fens,
        phase="complete",
        detail=f"processed {total_processed}"
    )
