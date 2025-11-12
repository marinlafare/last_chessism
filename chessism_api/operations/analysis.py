# chessism_api/operations/analysis.py

import asyncio
import httpx
import time
import json
from typing import List, Dict, Any, Optional

# --- NEW IMPORTS ---
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Fen
from chessism_api.database.ask_db import (
    get_fens_for_analysis,
    get_player_fens_for_analysis
)
# ---

# Define the Leela service URLs based on the docker-compose setup
# These are the *internal* Docker network URLs
LEELA_URLS = {
    0: "http://leela-service-gpu0:9999/analyze",
    1: "http://leela-service-gpu1:9999/analyze"
}

# ---
# 1. Leela API Communication
# ---

async def call_leela_service(
    fens: List[str], 
    gpu_index: int, 
    nodes_limit: int
) -> Optional[List[Dict[str, Any]]]:
    """
    Calls the appropriate Leela service with a list of FENs.
    """
    target_url = LEELA_URLS.get(gpu_index)
    if not target_url:
        print(f"Error: Invalid gpu_index {gpu_index}. No service URL found.", flush=True)
        return None

    payload = {
        "fens": fens,
        "nodes_limit": nodes_limit
    }

    # Use a long timeout as this analysis is node-limited and can take time
    timeout_config = httpx.Timeout(300.0, connect=10.0) 

    async with httpx.AsyncClient(timeout=timeout_config, http2=False) as client:
        try:
            response = await client.post(target_url, json=payload)
            response.raise_for_status() # Raise error for 4xx/5xx
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP Error calling Leela (GPU {gpu_index}): {e.response.status_code}", flush=True)
            print(f"Response: {e.response.text}", flush=True)
            return None
        except httpx.RequestError as e:
            print(f"Request Error calling Leela (GPU {gpu_index}): {repr(e)}", flush=True)
            return None
        except Exception as e:
            print(f"Unexpected Error calling Leela (GPU {gpu_index}): {repr(e)}", flush=True)
            return None

# ---
# 2. Data Formatting
# ---

def format_leela_results(
    leela_json_response: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Parses the JSON response from the leela-service into the flat
    dictionary format required by the 'fen' table.
    
    leela_json_response item:
    {
      "fen": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
      "is_valid": true,
      "analysis": {
        "depth": 20,
        "seldepth": 30,
        "score": 15,  <-- This is the centipawn score
        "pv": ["e2e4", "e7e5", "g1f3"], <-- This is the list of moves
        "nodes": 50000,
        ...
      }
    }
    
    Returns List of:
    {
        "fen": "...",
        "score": 0.15,
        "next_moves": "e2e4 e7e5 g1f3"
    }
    """
    formatted_data = []
    for item in leela_json_response:
        if not item.get("is_valid") or not item.get("analysis"):
            print(f"Warning: Invalid or failed analysis for FEN: {item.get('fen')}", flush=True)
            continue
        
        analysis = item["analysis"]
        
        # 1. Get Score
        # Convert centipawns (e.g., 15) to pawns (e.g., 0.15)
        cp_score = analysis.get("score")
        if cp_score is None:
            print(f"Warning: No score found for FEN: {item.get('fen')}", flush=True)
            continue
            
        try:
            score_in_pawns = float(cp_score) / 100.0
        except Exception:
            print(f"Warning: Could not parse score '{cp_score}' for FEN: {item.get('fen')}", flush=True)
            continue

        # 2. Get Next Moves (PV)
        # Convert list ["e2e4", "e7e5"] to string "e2e4 e7e5"
        pv_list = analysis.get("pv", [])
        if not isinstance(pv_list, list):
            pv_list = [] # Handle case where pv might be None or not a list
            
        next_moves_str = " ".join(pv_list)

        formatted_data.append({
            "fen": item["fen"],
            "score": score_in_pawns,
            "next_moves": next_moves_str
        })
        
    return formatted_data

# ---
# 3. Background Job Orchestrators
# ---

async def run_analysis_job(
    gpu_index: int, 
    total_fens_to_process: int, 
    batch_size: int, 
    nodes_limit: int
):
    """
    The main background task for a "general pool" analysis job.
    Uses 'get_fens_for_analysis' (FOR UPDATE SKIP LOCKED).
    """
    job_id = f"GPU-{gpu_index}"
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    
    fen_interface = DBInterface(Fen)
    total_processed = 0
    total_failed_batches = 0
    job_start_time = time.time()
    
    # Use a single DB session *per batch* to manage the row locks
    
    while total_processed < total_fens_to_process:
        print(f"[{job_id}] Processing batch {total_processed // batch_size + 1}...", flush=True)
        
        # 1. Fetch FENs using the FOR UPDATE SKIP LOCKED query
        # This is a new session that starts a transaction
        try:
            async with AsyncDBSession() as session:
                fens_to_analyze = await get_fens_for_analysis(session, batch_size)
                
                if not fens_to_analyze:
                    print(f"[{job_id}] No more FENs found to analyze. Job complete.", flush=True)
                    break
                
                # The FENs in 'fens_to_analyze' are NOW LOCKED in the DB
                # until this 'session' block completes.
                
                # 2. Call Leela Service (Network I/O)
                leela_results = await call_leela_service(fens_to_analyze, gpu_index, nodes_limit)
                
                if not leela_results:
                    print(f"[{job_id}] Leela service call failed for this batch. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    # We continue, and the 'session' block will exit,
                    # releasing the locks on 'fens_to_analyze' so another
                    # job can pick them up later.
                    continue 
                
                # 3. Format Results (CPU-bound, but very fast)
                formatted_results = format_leela_results(leela_results)
                
                if not formatted_results:
                    print(f"[{job_id}] Leela results were invalid or empty. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    continue

                # 4. Save results to DB
                # This uses the same session that is holding the locks.
                await fen_interface.update_fen_analysis_data(session, formatted_results)
                
                # 5. Commit
                # The analysis data is saved AND the locks are released.
                await session.commit()
                
                total_processed += len(fens_to_analyze)
                print(f"[{job_id}] Batch complete. Total FENs processed: {total_processed}", flush=True)

        except Exception as e:
            print(f"CRITICAL: Unhandled error in {job_id} analysis loop: {repr(e)}", flush=True)
            total_failed_batches += 1
            # Wait a moment before retrying to avoid spamming a failing DB
            await asyncio.sleep(5) 
            
    # --- End of while loop ---
    job_time = time.time() - job_start_time
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {job_time:.2f} seconds", flush=True)
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
    The background task for a "player-specific" analysis job.
    Uses 'get_player_fens_for_analysis' (FOR UPDATE SKIP LOCKED).
    """
    job_id = f"GPU-{gpu_index} (Player: {player_name})"
    print(f"--- [START JOB {job_id}] ---", flush=True)
    print(f"Targeting {total_fens_to_process} FENs, Batch Size: {batch_size}, Nodes: {nodes_limit}", flush=True)
    
    fen_interface = DBInterface(Fen)
    total_processed = 0
    total_failed_batches = 0
    job_start_time = time.time()
    
    while total_processed < total_fens_to_process:
        print(f"[{job_id}] Processing batch {total_processed // batch_size + 1}...", flush=True)
        
        try:
            async with AsyncDBSession() as session:
                # 1. Fetch Player FENs (FOR UPDATE SKIP LOCKED)
                fens_to_analyze = await get_player_fens_for_analysis(
                    session, 
                    player_name, 
                    batch_size
                )
                
                if not fens_to_analyze:
                    print(f"[{job_id}] No more FENs found for player '{player_name}'. Job complete.", flush=True)
                    break
                
                # 2. Call Leela Service
                leela_results = await call_leela_service(fens_to_analyze, gpu_index, nodes_limit)
                
                if not leela_results:
                    print(f"[{job_id}] Leela service call failed. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    continue 
                
                # 3. Format Results
                formatted_results = format_leela_results(leela_results)
                
                if not formatted_results:
                    print(f"[{job_id}] Leela results were invalid. Skipping batch.", flush=True)
                    total_failed_batches += 1
                    continue

                # 4. Save results to DB
                await fen_interface.update_fen_analysis_data(session, formatted_results)
                
                # 5. Commit
                await session.commit()
                
                total_processed += len(fens_to_analyze)
                print(f"[{job_id}] Batch complete. Total FENs processed: {total_processed}", flush=True)

        except Exception as e:
            print(f"CRITICAL: Unhandled error in {job_id} analysis loop: {repr(e)}", flush=True)
            total_failed_batches += 1
            await asyncio.sleep(5) 
            
    # --- End of while loop ---
    job_time = time.time() - job_start_time
    print(f"--- [END JOB {job_id}] ---", flush=True)
    print(f"Total time: {job_time:.2f} seconds", flush=True)
    print(f"Total FENs processed: {total_processed}", flush=True)
    print(f"Total failed batches: {total_failed_batches}", flush=True)