# chessism_api/operations/fens.py

import asyncio
import chess
import time
from typing import List, Dict, Any, Tuple
from sqlalchemy import select, update, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
import os
import math # Import math

# --- NEW: arq imports for the "boss" job ---
from arq import create_pool
from arq.connections import ArqRedis
from arq.jobs import Job
from chessism_api.redis_client import redis_settings
# ---

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Game, Move, Fen
from chessism_api.database.models import GameFenAssociation
from chessism_api.database.db_interface import DBInterface
# --- THIS IS THE FIX: Import the missing function ---
from chessism_api.database.ask_db import _get_remaining_fens_count_committed


# ---
# 1. HELPER FUNCTIONS (PGN PARSING)
# ---

def process_single_game_sync(game_data: Tuple[int, List[Dict[str, Any]]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Takes a game's moves, reconstructs the game, and generates a simplified FEN 
    for every half-move.
    
    Returns:
        A tuple: (associations_to_insert, failures)
    """
    link, one_game_moves = game_data
    associations_to_insert = []
    failures = []
    board = chess.Board()
    
    try:
        data = sorted(one_game_moves, key=lambda x: x['n_move'])
    except KeyError:
        failures.append({'link': link, 'move_num': -1, 'san': 'N/A', 'error': 'KeyError on move sort'})
        return ([], failures)

    try:
        for ind, move_data in enumerate(data):
            expected_move_num = ind + 1
            current_move_num = move_data.get('n_move')

            if not expected_move_num == current_move_num:
                failures.append({'link': link, 'move_num': current_move_num, 'san': 'N/A', 'error': 'Move sequence mismatch'})
                break 

            white_move_san = move_data.get('white_move')
            black_move_san = move_data.get('black_move')

            # --- Process White's Move ---
            if white_move_san and white_move_san != "--":
                try:
                    move_obj_white = board.parse_san(white_move_san)
                    board.push(move_obj_white)
                    current_fen_white = board.fen()
                    # --- FIX: Get 5th AND 6th FEN fields ---
                    fen_parts_white = current_fen_white.split(' ')
                    halfmove_clock_white = fen_parts_white[4]
                    fullmove_number_white = fen_parts_white[5]
                    assoc_data = create_association_data(
                        current_fen_white, current_move_num, "white", link, 
                        halfmove_clock_white, fullmove_number_white
                    )
                    associations_to_insert.append(assoc_data)
                except ValueError as e:
                    failures.append({
                        'link': link, 'move_num': current_move_num, 'color': 'white', 
                        'san': white_move_san, 'error': str(e)
                    })
                    break 

            # --- Process Black's Move ---
            if black_move_san and black_move_san != "--":
                try:
                    move_obj_black = board.parse_san(black_move_san)
                    board.push(move_obj_black)
                    current_fen_black = board.fen()
                    # --- FIX: Get 5th AND 6th FEN fields ---
                    fen_parts_black = current_fen_black.split(' ')
                    halfmove_clock_black = fen_parts_black[4]
                    fullmove_number_black = fen_parts_black[5]
                    assoc_data = create_association_data(
                        current_fen_black, current_move_num, "black", link, 
                        halfmove_clock_black, fullmove_number_black
                    )
                    associations_to_insert.append(assoc_data)
                except ValueError as e:
                    failures.append({
                        'link': link, 'move_num': current_move_num, 'color': 'black', 
                        'san': black_move_san, 'error': str(e)
                    })
                    break 

    except Exception as e:
        failures.append({'link': link, 'move_num': -1, 'san': 'N/A', 'error': f"Unexpected processing error: {e}"})

    return (associations_to_insert, failures)


def create_association_data(
    raw_fen: str, 
    n_move: int, 
    move_color: str, 
    link:int, 
    halfmove_clock: str, 
    fullmove_number: str
) -> Dict[str, Any]:
    """
    Creates data for the GameFenAssociation table and for Fen aggregation.
    """
    parts = raw_fen.split(' ')
    simplified_fen = ' '.join(parts[:4])
    
    # --- THIS IS THE FIX ---
    # Remove the trailing underscore
    formatted_counter = f"#{halfmove_clock}_{fullmove_number}"
    # --- END FIX ---

    # This temp object contains data for GameFenAssociation AND for Fen aggregation
    association_data = {
        # For GameFenAssociation table
        'game_link': link,
        'fen_fen': simplified_fen,
        'n_move': n_move,
        'move_color': move_color,
        
        # For Fen table aggregation
        'move_counter_string': formatted_counter
    }
    
    return association_data

# ---
# 2. DATABASE HELPER FUNCTIONS
# ---

async def _get_games_needing_fens(session: AsyncSession, batch_size: int) -> List[int]:
    """
    Queries the database for game links where FENs have not been generated yet.
    Applies a row-level lock to support concurrent workers.
    """
    stmt = (
        select(Game.link)
        .where(Game.fens_done == False)
        .limit(batch_size)
        .with_for_update(skip_locked=True) 
    )
    result = await session.execute(stmt)
    game_links = result.scalars().all()
    return game_links

async def _get_moves_for_games(session: AsyncSession, game_links: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Fetches all moves associated with a list of game links.
    """
    if not game_links:
        return {}
    stmt = (
        select(Move.link, Move.n_move, Move.white_move, Move.black_move)
        .where(Move.link.in_(game_links))
        .order_by(Move.link, Move.n_move)
    )
    result = await session.execute(stmt)
    moves_by_link: Dict[int, List[Dict[str, Any]]] = {}
    for row in result.mappings():
        link = row['link']
        if link not in moves_by_link:
            moves_by_link[link] = []
        moves_by_link[link].append(row)
    return moves_by_link

async def _mark_games_as_done_in_session(session: AsyncSession, game_links: List[int]):
    """
    Bulk updates the Game table to set fens_done=True using the provided session.
    """
    if not game_links:
        return
    
    stmt = (
        update(Game)
        .where(Game.link.in_(game_links))
        .values(fens_done=True)
    )
    await session.execute(stmt)


# --- STAGE 2 Aggregation Function (Called by Boss) ---
def _aggregate_fen_data_in_memory(all_associations: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Reads the complete list of associations from all workers and aggregates
    them in memory (as requested) to create the FEN table data.
    """
    print(f"[FEN AGGREGATOR] Aggregating {len(all_associations)} associations in memory...")
    
    fen_map: Dict[str, Dict[str, Any]] = {}
    
    for assoc in all_associations:
        fen_str = assoc['fen_fen']
        # --- THIS IS THE FIX ---
        # Use the correctly formatted string (no trailing _)
        move_counter_str = assoc.get('move_counter_string', '#0_0') 
        # --- END FIX ---
        
        if fen_str not in fen_map:
            fen_map[fen_str] = {
                'fen': fen_str,
                'n_games': 1,
                'moves_counter': move_counter_str, # e.g., "#0_1"
                'score': None,
                'next_moves': None
            }
        else:
            fen_map[fen_str]['n_games'] += 1
            # --- THIS IS THE FIX ---
            # This logic now correctly checks for uniqueness
            # e.g., if "#0_1" is not in "#0_1", this is False.
            # e.g., if "#1_1" is not in "#0_1", this is True.
            if move_counter_str not in fen_map[fen_str]['moves_counter']:
                fen_map[fen_str]['moves_counter'] += move_counter_str # Appends "#1_1" -> "#0_1#1_1"
            # --- END FIX ---

    # --- THIS IS THE FIX: Create the *correct* list for associations ---
    # The aggregated list (fen_map.values()) is for the 'fen' table.
    # The original 'all_associations' list is needed for the 'game_fen_association' table.
    # We just need to remove the temporary 'move_counter_string' key.
    
    associations_for_db = []
    for assoc in all_associations:
        # Create a copy and remove the temp key
        assoc_copy = assoc.copy()
        assoc_copy.pop('move_counter_string', None)
        associations_for_db.append(assoc_copy)
    
    # We deduplicate *this* list, not the original
    unique_associations = list({tuple(d.items()): d for d in associations_for_db}.values())

    print(f"[FEN AGGREGATOR] Found {len(fen_map)} unique FENs.")
    print(f"[FEN AGGREGATOR] Found {len(unique_associations)} unique associations.")
    
    return list(fen_map.values()), unique_associations
    # --- END FIX ---

# --- NEW: List splitter utility ---
def split_list(data: List[Any], n_chunks: int) -> List[List[Any]]:
    """Splits a list into n roughly equal chunks."""
    if n_chunks <= 0:
        return [data]
    chunk_size = math.ceil(len(data) / n_chunks)
    if chunk_size == 0:
        return [[] for _ in range(n_chunks)]
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

# ---
# 3. BACKGROUND JOBS (MODIFIED)
# ---

# --- STAGE 1: The "Generation" Job (Child) ---
async def run_fen_generation_job(
    ctx: dict, 
    total_games_to_process: int, # This is the "quota" for this worker
    batch_size: int, 
    **kwargs
) -> List[Dict[str, Any]]: # <-- MODIFIED: Returns list of associations
    """
    STAGE 1: (CHILD "MAP" JOB)
    - Fetches batches of games using SKIP LOCKED.
    - Runs CPU-bound PGN parsing.
    - Returns a list of all associations it found.
    - DOES NOT aggregate or insert FEN/Assoc data.
    """
    worker_id = ctx.get('job_id', 'unknown')[:6]
    job_log_prefix = f"[FEN GEN {worker_id}]" # <-- Changed log prefix
    print(f"--- [START] {job_log_prefix} ---", flush=True)
    print(f"{job_log_prefix} Quota: {total_games_to_process} games, in batches of {batch_size}.", flush=True)
    
    all_associations_for_job = []
    all_failures_for_job = []
    total_processed_so_far = 0
    
    job_start_time = time.time()
    
    while total_processed_so_far < total_games_to_process:
        
        batch_start_time = time.time()
        print(f"{job_log_prefix} Processing batch { (total_processed_so_far // batch_size) + 1 }...", flush=True)
        
        async with AsyncDBSession() as session:
            try:
                # 1. Fetch Games
                games_left_to_reach_target = total_games_to_process - total_processed_so_far
                current_batch_size = min(batch_size, games_left_to_reach_target)
                game_links = await _get_games_needing_fens(session, current_batch_size)
                
                if not game_links:
                    print(f"{job_log_prefix} No more games found to process. Stopping job.", flush=True)
                    break 
                
                print(f"{job_log_prefix} Fetched {len(game_links)} games for this batch.", flush=True)

                # 2. Fetch Moves
                moves_by_link = await _get_moves_for_games(session, game_links)
                
                # 3. Process in Parallel (CPU-bound)
                tasks_data = [
                    (link, moves) for link, moves in moves_by_link.items() if moves
                ]
                tasks = [
                    asyncio.to_thread(process_single_game_sync, game_data)
                    for game_data in tasks_data
                ]
                results_tuples = await asyncio.gather(*tasks) 
                
                batch_associations = []
                batch_failures = []
                
                for associations_list, failures_list in results_tuples:
                    if failures_list:
                        batch_failures.extend(failures_list)
                    else:
                        batch_associations.extend(associations_list) 
                
                all_associations_for_job.extend(batch_associations)
                all_failures_for_job.extend(batch_failures)

                # 4. Mark Games as Done
                await _mark_games_as_done_in_session(session, game_links)
                
                # 5. Commit Transaction (ONLY for Game updates)
                await session.commit()
                
                total_processed_so_far += len(game_links)
                print(f"{job_log_prefix} Batch { (total_processed_so_far // batch_size) } complete. Time: {time.time() - batch_start_time:.2f}s", flush=True)

            except Exception as e:
                print(f"CRITICAL: Error in {job_log_prefix} batch: {e}. Rolling back batch.", flush=True)
                if hasattr(e, 'orig'):
                    print(f"DBAPI Error: {e.orig}", flush=True)
                await session.rollback()
                break 
            
    # --- END OF WHILE LOOP ---
    
    # 6. Log failures (if any)
    if all_failures_for_job:
        log_file_path = "logs/illegall_fen.txt" 
        print(f"{job_log_prefix} Encountered {len(all_failures_for_job)} game failures. Writing details to '{log_file_path}'...", flush=True)
        try:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(f"\n--- FEN Job {worker_id} Run: {datetime.now().isoformat()} ---\n")
                for fail in all_failures_for_job:
                    f.write(f"{fail}\n")
                f.write(f"--- End of Job Run ---\n")
        except Exception as e:
            print(f"CRITICAL: Failed to write to '{log_file_path}': {e}", flush=True)
    
    total_job_time = time.time() - job_start_time
    print(f"--- [END] {job_log_prefix} ---", flush=True)
    print(f"--- {job_log_prefix} Total job time: {total_job_time:.2f} seconds ---", flush=True)
    
    # 7. Return the raw associations
    return all_associations_for_job


# --- NEW: STAGE 2: The "FEN Insertion" Job (Child) ---
async def run_fen_insertion_job(
    ctx: dict,
    fens_to_insert: List[Dict[str, Any]],
    **kwargs
) -> bool:
    """
    STAGE 2: (CHILD "WRITE FENs" JOB)
    - Receives a chunk of aggregated FENs.
    - Inserts them into the database in smaller batches.
    """
    worker_id = ctx.get('job_id', 'unknown')[:6]
    job_log_prefix = f"[FEN INSERT {worker_id}]"
    print(f"--- [START] {job_log_prefix} ---", flush=True)
    
    job_start_time = time.time()
    fen_interface = DBInterface(Fen)

    # --- THIS IS THE FIX ---
    # Define a smaller, safer batch size for each transaction.
    # 50,000 FENs per transaction.
    TRANSACTION_BATCH_SIZE = 50000 
    
    total_fens = len(fens_to_insert)
    num_batches = math.ceil(total_fens / TRANSACTION_BATCH_SIZE)
    
    print(f"{job_log_prefix} Inserting {total_fens} FENs in {num_batches} batches of {TRANSACTION_BATCH_SIZE}...", flush=True)
    
    batches = [
        fens_to_insert[i:i + TRANSACTION_BATCH_SIZE] 
        for i in range(0, total_fens, TRANSACTION_BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        batch_start_time_inner = time.time()
        print(f"{job_log_prefix} Starting FEN batch {i+1}/{num_batches} ({len(batch)} records)...", flush=True)
        try:
            # create_all handles its *own* session and commit.
            # This is now a self-contained transaction.
            await fen_interface.create_all(batch)
            print(f"{job_log_prefix} Finished FEN batch {i+1}/{num_batches} in {time.time() - batch_start_time_inner:.2f}s.", flush=True)
        except Exception as e:
            print(f"CRITICAL: Error in {job_log_prefix} FEN batch {i+1}: {e}. Rolling back.", flush=True)
            if hasattr(e, 'orig'):
                print(f"DBAPI Error: {e.orig}", flush=True)
            raise # Re-raise error to fail the job
    # --- END FIX ---

    total_job_time = time.time() - job_start_time
    print(f"--- [END] {job_log_prefix} ---", flush=True)
    print(f"--- {job_log_prefix} Total FEN insertion time: {total_job_time:.2f} seconds ---", flush=True)
    return True

# --- NEW: STAGE 3: The "Association Insertion" Job (Child) ---
async def run_association_insertion_job(
    ctx: dict,
    associations_to_insert: List[Dict[str, Any]],
    **kwargs
) -> bool:
    """
    STAGE 3: (CHILD "WRITE ASSOCS" JOB)
    - Receives a chunk of aggregated Associations.
    - Inserts them into the database in smaller batches.
    """
    worker_id = ctx.get('job_id', 'unknown')[:6]
    job_log_prefix = f"[ASSOC INSERT {worker_id}]"
    print(f"--- [START] {job_log_prefix} ---", flush=True)

    job_start_time = time.time()
    assoc_interface = DBInterface(GameFenAssociation)

    # --- THIS IS THE FIX ---
    # Define a smaller, safer batch size for each transaction.
    # 50,000 Assocs per transaction.
    TRANSACTION_BATCH_SIZE = 50000 
    
    total_assocs = len(associations_to_insert)
    num_batches = math.ceil(total_assocs / TRANSACTION_BATCH_SIZE)
    
    print(f"{job_log_prefix} Inserting {total_assocs} associations in {num_batches} batches of {TRANSACTION_BATCH_SIZE}...", flush=True)
    
    batches = [
        associations_to_insert[i:i + TRANSACTION_BATCH_SIZE] 
        for i in range(0, total_assocs, TRANSACTION_BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        batch_start_time_inner = time.time()
        print(f"{job_log_prefix} Starting Association batch {i+1}/{num_batches} ({len(batch)} records)...", flush=True)
        try:
            # create_all handles its *own* session and commit.
            await assoc_interface.create_all(batch)
            print(f"{job_log_prefix} Finished Association batch {i+1}/{num_batches} in {time.time() - batch_start_time_inner:.2f}s.", flush=True)
        except Exception as e:
            print(f"CRITICAL: Error in {job_log_prefix} Association batch {i+1}: {e}. Rolling back.", flush=True)
            if hasattr(e, 'orig'):
                print(f"DBAPI Error: {e.orig}", flush=True)
            raise # Re-raise error to fail the job
    # --- END FIX ---

    total_job_time = time.time() - job_start_time
    print(f"--- [END] {job_log_prefix} ---", flush=True)
    print(f"--- {job_log_prefix} Total association insertion time: {total_job_time:.2f} seconds ---", flush=True)
    return True


# --- STAGE 0 "Boss" Job ---
async def run_fen_pipeline(ctx: dict, total_games_to_process: int, batch_size: int, num_workers: int, **kwargs):
    """
    STAGE 0: (BOSS "MapReduce" JOB)
    Orchestrates the entire FEN generation pipeline based on user's architecture.
    1. Enqueues 3 parallel "generation" jobs (Map).
    2. Collects all results.
    3. Performs one central aggregation (Reduce).
    4. Enqueues 3 parallel "FEN insertion" jobs (Write FENs).
    5. Awaits FEN insertion jobs.
    6. Enqueues 3 parallel "Association insertion" jobs (Write Assocs).
    7. Awaits Association insertion jobs.
    """
    job_id = ctx.get('job_id', 'unknown')[:6]
    job_log_prefix = f"[FEN PIPELINE {job_id}]"
    print(f"--- [START] {job_log_prefix} ---", flush=True)
    
    redis: ArqRedis = ctx['redis']
    
    # --- 1. Check games remaining ---
    games_remaining_in_db = await _get_remaining_fens_count_committed()
    if games_remaining_in_db == 0:
        print(f"{job_log_prefix} No games found to process. Aborting.", flush=True)
        return
        
    actual_games_to_process = min(total_games_to_process, games_remaining_in_db)
    
    print(f"{job_log_prefix} User requested {total_games_to_process}, DB has {games_remaining_in_db} remaining.", flush=True)
    print(f"{job_log_prefix} Will process {actual_games_to_process} total games. Distributing to {num_workers} workers.", flush=True)

    # --- 2. Enqueue "Generation" (Map) jobs ---
    games_per_worker = math.ceil(actual_games_to_process / num_workers)
    
    gen_jobs: List[Job] = []
    print(f"{job_log_prefix} Enqueuing {num_workers} generation jobs...", flush=True)
    for i in range(num_workers):
        job = await redis.enqueue_job(
            'run_fen_generation_job',
            total_games_to_process=games_per_worker, 
            batch_size=batch_size, 
            _queue_name='fen_queue'
        )
        gen_jobs.append(job)
        
    # --- 3. Wait for "Generation" jobs and collect results ---
    all_associations_from_workers: List[Dict[str, Any]] = []
    for i, job in enumerate(gen_jobs):
        try:
            result_list = await job.result(timeout=None) # Wait forever
            all_associations_from_workers.extend(result_list)
            print(f"{job_log_prefix} Generation job {i+1}/{num_workers} (ID: {job.job_id}) finished. Got {len(result_list)} associations.", flush=True)
        except Exception as e:
            print(f"CRITICAL: {job_log_prefix} Generation job {i+1} (ID: {job.job_id}) FAILED: {repr(e)}", flush=True)
    
    print(f"{job_log_prefix} All generation jobs complete.", flush=True)

    if not all_associations_from_workers:
        print(f"{job_log_prefix} No associations were generated. Aborting.", flush=True)
        return

    # --- 4. Perform Central Aggregation (Reduce) ---
    fens_to_insert, associations_to_insert = _aggregate_fen_data_in_memory(all_associations_from_workers)

    # --- 5. Split Aggregated Data ---
    fen_chunks = split_list(fens_to_insert, num_workers)
    assoc_chunks = split_list(associations_to_insert, num_workers)
    
    print(f"{job_log_prefix} Aggregation complete. Splitting into {num_workers} chunks.", flush=True)

    # --- 6. Enqueue "FEN Insertion" (Write FENs) jobs ---
    print(f"{job_log_prefix} Enqueuing {num_workers} FEN insertion jobs...", flush=True)
    fen_insert_jobs: List[Job] = []
    for i in range(num_workers):
        job = await redis.enqueue_job(
            'run_fen_insertion_job',
            fens_to_insert=fen_chunks[i],
            _queue_name='fen_queue'
        )
        fen_insert_jobs.append(job)

    # --- 7. Wait for "FEN Insertion" jobs to complete ---
    for i, job in enumerate(fen_insert_jobs):
        try:
            await job.result(timeout=None) # Wait forever
            print(f"{job_log_prefix} FEN insertion job {i+1}/{num_workers} (ID: {job.job_id}) finished.", flush=True)
        except Exception as e:
            print(f"CRITICAL: {job_log_prefix} FEN insertion job {i+1} (ID: {job.job_id}) FAILED: {repr(e)}", flush=True)
            # If FEN insertion fails, we must not continue to associations.
            print(f"CRITICAL: {job_log_prefix} Aborting pipeline due to FEN insertion failure.", flush=True)
            return

    print(f"{job_log_prefix} All FENs inserted. Proceeding to associations.", flush=True)

    # --- 8. Enqueue "Association Insertion" (Write Assocs) jobs ---
    print(f"{job_log_prefix} Enqueuing {num_workers} Association insertion jobs...", flush=True)
    assoc_insert_jobs: List[Job] = []
    for i in range(num_workers):
        job = await redis.enqueue_job(
            'run_association_insertion_job',
            associations_to_insert=assoc_chunks[i],
            _queue_name='fen_queue'
        )
        assoc_insert_jobs.append(job)

    # --- 9. Wait for "Association Insertion" jobs to complete ---
    for i, job in enumerate(assoc_insert_jobs):
        try:
            await job.result(timeout=None) # Wait forever
            print(f"{job_log_prefix} Association insertion job {i+1}/{num_workers} (ID: {job.job_id}) finished.", flush=True)
        except Exception as e:
            # This is the error you saw in your logs
            print(f"CRITICAL: {job_log_prefix} Association insertion job {i+1} (ID: {job.job_id}) FAILED: {repr(e)}", flush=True)

    print(f"{job_log_prefix} All association insertion jobs complete.", flush=True)
    print(f"--- [END] {job_log_prefix} ---", flush=True)