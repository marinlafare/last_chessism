# chessism_api/operations/analysis.py

# chessism_api/operations/analysis.py

import asyncio
import httpx
import time
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

# --- FIXED IMPORTS ---
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.ask_db import (
    get_fens_for_analysis,
    get_player_fens_for_analysis
)
# ---

# --- RESTORED DUAL-GPU URLS ---
LEELA_URLS = {
    0: "http://leela-service-gpu0:9999/analyze", # Port 9999
    1: "http://leela-service-gpu1:9999/analyze"  # Mapped to host 9998
}

# Initialize the DBInterface for Fen
fen_interface = DBInterface(Fen)

async def _call_leela_service(
    client: httpx.AsyncClient,
    url: str,
    fens: List[str],
    nodes: int
) -> Optional[List[Dict[str, Any]]]:
    """
    Sends a batch of FENs to the specified Leela service.
    """
    payload = {
        "fens": fens,
        "nodes_limit": nodes
    }
    try:
        response = await client.post(url, json=payload, timeout=None) # Disable timeout
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error calling Leela service: {e.response.status_code} - {e.response.text}", flush=True)
        return None
    except httpx.RequestError as e:
        print(f"Request error calling Leela service: {repr(e)}", flush=True)
        return None
    except Exception as e:
        print(f"Unexpected error in _call_leela_service: {repr(e)}", flush=True)
        return None

def _format_leela_results(
    leela_output: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Parses the raw JSON output from Leela into the DB format.
    """
    formatted_results = []
    for item in leela_output:
        if not item or not item.get("is_valid"):
            continue

        fen_str = item.get("fen")
        analysis = item.get("analysis", {})
        
        # Extract score (in centipawns)
        # We use .get('score') which is the simplified PovScore
        score_cp = analysis.get("score")

        # Extract next_moves (the principal variation)
        pv = analysis.get("pv", [])
        
        # Convert PV list of moves to a single string
        next_moves_str = " ".join(pv) if pv else None

        if fen_str and score_cp is not None:
            formatted_results.append({
                "fen": fen_str,
                "score": float(score_cp),
                "next_moves": next_moves_str
            })
    return formatted_results

async def run_analysis_job(
    gpu_index: int,
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int
):
    """
    The main background task for a general analysis job.
    """
    job_id = f"GPU-{gpu_index}"
    # This will now correctly select the URL
    leela_url = LEELA_URLS[gpu_index] 
    
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    print(f"[{job_id}] Routing to: {leela_url}", flush=True) # <-- Test log

    total_processed = 0
    total_failed_batches = 0
    job_start_time = time.time()
    
    # Use one httpx client for the life of the job
    async with httpx.AsyncClient() as client:
        while total_processed < total_fens_to_process:
            current_batch_size = min(batch_size, total_fens_to_process - total_processed)
            if current_batch_size <= 0:
                break
                
            print(f"[{job_id}] Processing batch { (total_processed // batch_size) + 1 }...", flush=True)
            
            fens_to_process = None
            session: Optional[AsyncSession] = None
            
            try:
                # 1. Start a transaction and get FENs (with lock)
                # --- THIS IS THE FIX for the TypeError ---
                session, fens_to_process = await get_fens_for_analysis(current_batch_size)
                
                if not fens_to_process or session is None:
                    print(f"[{job_id}] No more FENs found to analyze. Stopping job.", flush=True)
                    break
                
                # --- MODIFIED: Start timing ---
                batch_start_time = time.time()

                # 2. Call Leela service
                leela_results = await _call_leela_service(
                    client,
                    leela_url,
                    fens_to_process,
                    nodes_limit
                )
                
                # --- MODIFIED: End timing ---
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                
                if not leela_results:
                    print(f"[{job_id}] Failed to get results from Leela service. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    # Rollback to release the locks
                    await session.rollback()
                    continue

                # 3. Format results
                db_ready_data = _format_leela_results(leela_results)
                
                if not db_ready_data:
                    print(f"[{job_id}] No valid analysis data returned from Leela. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    # Rollback to release the locks
                    await session.rollback()
                    continue

                # 4. Save results (using the same session)
                await fen_interface.update_fen_analysis_data(session, db_ready_data)
                
                # 5. Commit the transaction
                # This saves the data AND releases the 'FOR UPDATE SKIP LOCKED'
                await session.commit()
                
                # --- MODIFIED: Calculate time per FEN ---
                fens_in_batch = len(fens_to_process)
                time_per_fen = (batch_duration / fens_in_batch) if fens_in_batch > 0 else 0
                total_processed += fens_in_batch
                
                print(f"[{job_id}] Batch complete. Total FENs: {total_processed}. Batch Time: {batch_duration:.2f}s ({time_per_fen:.2f} s/FEN)", flush=True)

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

    job_end_time = time.time()
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {(job_end_time - job_start_time):.2f} seconds", flush=True)
    print(f"Total FENs processed: {total_processed}", flush=True)
    print(f"Total failed batches: {total_failed_batches}", flush=True)

async def run_player_analysis_job(
    player_name: str,
    gpu_index: int,
    total_fens_to_process: int,
    batch_size: int,
    nodes_limit: int
):
    """
    The main background task for a player-specific analysis job.
    """
    job_id = f"GPU-{gpu_index} (Player: {player_name})"
    leela_url = LEELA_URLS[gpu_index]
    
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    print(f"[{job_id}] Routing to: {leela_url}", flush=True) # <-- Test log

    total_processed = 0
    total_failed_batches = 0
    job_start_time = time.time()

    async with httpx.AsyncClient() as client:
        while total_processed < total_fens_to_process:
            current_batch_size = min(batch_size, total_fens_to_process - total_processed)
            if current_batch_size <= 0:
                break
                
            print(f"[{job_id}] Processing batch { (total_processed // batch_size) + 1 }...", flush=True)
            
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
                    break
                
                # --- MODIFIED: Start timing ---
                batch_start_time = time.time()

                # 2. Call Leela service
                leela_results = await _call_leela_service(
                    client,
                    leela_url,
                    fens_to_process,
                    nodes_limit
                )
                
                # --- MODIFIED: End timing ---
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                
                if not leela_results:
                    print(f"[{job_id}] Failed to get results from Leela. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    await session.rollback()
                    continue

                # 3. Format results
                db_ready_data = _format_leela_results(leela_results)
                
                if not db_ready_data:
                    print(f"[{job_id}] No valid analysis data from Leela. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    await session.rollback()
                    continue

                # 4. Save results (using the same session)
                await fen_interface.update_fen_analysis_data(session, db_ready_data)
                
                # 5. Commit the transaction
                await session.commit()
                
                # --- MODIFIED: Calculate time per FEN ---
                fens_in_batch = len(fens_to_process)
                time_per_fen = (batch_duration / fens_in_batch) if fens_in_batch > 0 else 0
                total_processed += fens_in_batch

                print(f"[{job_id}] Batch complete. Total FENs: {total_processed}. Batch Time: {batch_duration:.2f}s ({time_per_fen:.2f} s/FEN)", flush=True)

            except Exception as e:
                print(f"CRITICAL: Unhandled error in {job_id} loop: {repr(e)}", flush=True)
                total_failed_batches += 1
                if session:
                    await session.rollback()
            finally:
                if session:
                    await session.close()
            
            await asyncio.sleep(1)

    job_end_time = time.time()
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {(job_end_time - job_start_time):.2f} seconds", flush=True)
    print(f"Total FENs processed: {total_processed}", flush=True)
    print(f"Total failed batches: {total_failed_batches}", flush=True)