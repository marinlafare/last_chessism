# chessism_api/operations/fens.py

import asyncio
import chess
import time
from typing import List, Dict, Any, Tuple
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
import os

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Game, Move, Fen
from chessism_api.database.db_interface import DBInterface

# ---
# 1. YOUR HELPER FUNCTIONS
# ---

def process_single_game_sync(game_data: Tuple[int, List[Dict[str, Any]]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Takes a game's moves, reconstructs the game, and generates a simplified FEN 
    for every half-move.
    
    This function is CPU-bound and is intended to run in a thread executor.
    
    Args:
        game_data: A tuple (game_link: int, moves: List[Dict])
        
    Returns:
        A tuple: (List[Successful FENs], List[Failure Details])
    """
    link, one_game_moves = game_data
    fens_to_insert = []
    failures = [] # <-- NEW: To store failure info
    board = chess.Board()
    
    # Sort moves by move number (n_move)
    try:
        data = sorted(one_game_moves, key=lambda x: x['n_move'])
    except KeyError:
        # print(f"KeyError sorting moves for game {link}. Skipping game.") # <-- SILENCED
        failures.append({'link': link, 'move_num': -1, 'san': 'N/A', 'error': 'KeyError on move sort'})
        return ([], failures) # Return failure

    try:
        for ind, move_data in enumerate(data):
            # Check for data integrity: moves should be sequential
            expected_move_num = ind + 1
            current_move_num = move_data.get('n_move')

            if not expected_move_num == current_move_num:
                # print(f"Move sequence mismatch in game {link}. Skipping game.") # <-- SILENCED
                failures.append({'link': link, 'move_num': current_move_num, 'san': 'N/A', 'error': 'Move sequence mismatch'})
                break # Stop processing this game

            white_move_san = move_data.get('white_move')
            black_move_san = move_data.get('black_move')

            # --- Process White's Move ---
            if white_move_san and white_move_san != "--":
                try:
                    move_obj_white = board.parse_san(white_move_san)
                    board.push(move_obj_white)
                    current_fen_white = board.fen()
                    # Use float 'ind' for half-move counter
                    to_insert_white = simplify_fen(current_fen_white, float(ind) + 0.0, link)
                    fens_to_insert.append(to_insert_white)
                except ValueError as e:
                    # --- MODIFIED: Log error details instead of printing ---
                    failures.append({
                        'link': link, 
                        'move_num': current_move_num, 
                        'color': 'white', 
                        'san': white_move_san, 
                        'error': str(e)
                    })
                    break # Stop processing this game

            # --- Process Black's Move ---
            if black_move_san and black_move_san != "--":
                try:
                    move_obj_black = board.parse_san(black_move_san)
                    board.push(move_obj_black)
                    current_fen_black = board.fen()
                    # Use float 'ind + 0.5' for half-move counter
                    to_insert_black = simplify_fen(current_fen_black, float(ind) + 0.5, link)
                    fens_to_insert.append(to_insert_black)
                except ValueError as e:
                    # --- MODIFIED: Log error details instead of printing ---
                    failures.append({
                        'link': link, 
                        'move_num': current_move_num, 
                        'color': 'black', 
                        'san': black_move_san, 
                        'error': str(e)
                    })
                    break # Stop processing this game

    except Exception as e:
        # print(f"Unexpected error processing game {link}: {e}") # <-- SILENCED
        failures.append({'link': link, 'move_num': -1, 'san': 'N/A', 'error': f"Unexpected processing error: {e}"})

    return (fens_to_insert, failures) # <-- NEW: Return tuple


def simplify_fen(raw_fen: str, n_move: float, link:int) -> Dict[str, Any]:
    """
    Simplifies the FEN to the first 4 components (board, side, castling, en passant target).
    Adds metadata for insertion into the Fen table.
    """
    parts = raw_fen.split(' ')
    
    simplified_fen = ' '.join(parts[:4])
    
    # moves_counter stores the halfmove clock and fullmove number for tracking purposes
    moves_counter = f"#{parts[4]}#{parts[5]}_" 
    
    return {'link':link, # This will be filtered out by DBInterface
            'fen':simplified_fen,
            'n_games':1, # Initial count is 1 for the game being processed
            'moves_counter': moves_counter,
            'n_move' : n_move, # This will be filtered out by DBInterface
            'next_moves' : None,
            'score' : None}

# ---
# 2. NEW DATABASE HELPER FUNCTIONS
# ---

async def _get_games_needing_fens(session: AsyncSession, batch_size: int) -> List[int]:
    """
    Queries the database for game links where FENs have not been generated yet.
    """
    # print(f"Fetching new batch of {batch_size} games...", flush=True) # <-- SILENCED
    stmt = (
        select(Game.link)
        .where(Game.fens_done == False)
        .limit(batch_size)
    )
    result = await session.execute(stmt)
    game_links = result.scalars().all()
    # print(f"Found {len(game_links)} games to process.", flush=True) # <-- SILENCED
    return game_links

async def _get_moves_for_games(session: AsyncSession, game_links: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Fetches all moves associated with a list of game links.
    """
    if not game_links:
        return {}
        
    # print(f"Fetching moves for {len(game_links)} games...", flush=True) # <-- SILENCED
    stmt = (
        select(Move.link, Move.n_move, Move.white_move, Move.black_move)
        .where(Move.link.in_(game_links))
        .order_by(Move.link, Move.n_move)
    )
    result = await session.execute(stmt)
    
    # Group moves by game link
    moves_by_link: Dict[int, List[Dict[str, Any]]] = {}
    for row in result.mappings(): # .mappings() returns dictionaries
        link = row['link']
        if link not in moves_by_link:
            moves_by_link[link] = []
        moves_by_link[link].append(row)
        
    # print(f"Found moves for {len(moves_by_link)} games.", flush=True) # <-- SILENCED
    return moves_by_link

async def _get_remaining_fens_count(session: AsyncSession) -> int:
    """
    Counts how many games still have fens_done = False.
    Uses the provided session to read the current transaction state.
    """
    stmt = select(func.count(Game.link)).where(Game.fens_done == False)
    result = await session.execute(stmt)
    count = result.scalar()
    return count or 0

async def _get_remaining_fens_count_committed() -> int:
    """
    Counts how many games still have fens_done = False using a NEW session.
    This reads the last COMMITTED state of the database.
    """
    async with AsyncDBSession() as session:
        stmt = select(func.count(Game.link)).where(Game.fens_done == False)
        result = await session.execute(stmt)
        count = result.scalar()
        return count or 0

# ---
# 3. ORCHESTRATOR / BACKGROUND JOB (MODIFIED)
# ---

async def run_fen_generation_job(total_games_to_process: int = 1000000, batch_size: int = 1000):
    """
    The main background task.
    - Loops until 'total_games_to_process' is met or no games are left.
    - Pre-aggregates FENs in Python to avoid CardinalityViolationError.
    - Logs total job time and failures at the end.
    - MODIFIED: Each batch is now its own atomic transaction.
    """
    
    print(f"--- [START] FEN Generation Job ---")
    print(f"Targeting {total_games_to_process} games, in batches of {batch_size}.")
    
    fen_interface = DBInterface(Fen)
    total_processed_so_far = 0
    total_games_failed = 0 
    total_job_failures_list = [] 
    
    job_start_time = time.time()
    
    # --- MODIFIED: Session is now created *inside* the loop ---
    while total_processed_so_far < total_games_to_process:
        
        # Each loop is a new session and a new atomic transaction
        async with AsyncDBSession() as session:
            try:
                # Calculate how many games to fetch in this batch
                games_left_to_reach_target = total_games_to_process - total_processed_so_far
                current_batch_size = min(batch_size, games_left_to_reach_target)
                
                # 1. Fetch Games (uses this batch's session)
                game_links = await _get_games_needing_fens(session, current_batch_size)
                if not game_links:
                    break # No more games left
                
                # 2. Fetch Moves (uses this batch's session)
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
                
                batch_fens_to_aggregate = []
                
                for fens_list, failures_list in results_tuples:
                    if failures_list:
                        total_games_failed += 1
                        total_job_failures_list.extend(failures_list)
                    else:
                        batch_fens_to_aggregate.extend(fens_list)
                
                # 4. Pre-aggregate FENs
                aggregated_fens: Dict[str, Dict[str, Any]] = {}
                for fen_data in batch_fens_to_aggregate: 
                    fen_str = fen_data['fen']
                    if fen_str not in aggregated_fens:
                        aggregated_fens[fen_str] = {
                            'fen': fen_str,
                            'n_games': 1,
                            'moves_counter': fen_data['moves_counter'],
                            'next_moves': None,
                            'score': None
                        }
                    else:
                        aggregated_fens[fen_str]['n_games'] += 1
                        aggregated_fens[fen_str]['moves_counter'] += fen_data['moves_counter']
                all_fens_to_insert = list(aggregated_fens.values())

                # 5. Bulk-Upsert FENs (uses this batch's session)
                if all_fens_to_insert:
                    # --- MODIFIED: Pass the session to create_all ---
                    await fen_interface.create_all_with_session(session, all_fens_to_insert)

                # 6. Mark Games as Done (uses this batch's session)
                await _mark_games_as_done_in_session(session, game_links)
                
                # 7. Commit Transaction
                # If all steps above succeeded, commit the transaction
                await session.commit()
                
                total_processed_so_far += len(game_links)

            except Exception as e:
                # If anything in this batch failed, roll back the transaction
                print(f"CRITICAL: Error during batch processing: {e}. Rolling back batch.", flush=True)
                if hasattr(e, 'orig'):
                    print(f"DBAPI Error: {e.orig}", flush=True)
                await session.rollback()
                break # Stop the job
            
        # --- END OF WHILE LOOP ---
    
    # 7. Write failures (if any)
    if total_job_failures_list:
        log_file_path = "logs/illegall_fen.txt" 
        print(f"Encountered {total_games_failed} game failures (illegal SAN). Writing details to '{log_file_path}'...", flush=True)
        try:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(f"\n--- FEN Generation Job Run: {datetime.now().isoformat()} ---\n")
                for fail in total_job_failures_list:
                    f.write(f"{fail}\n")
                f.write(f"--- End of Job Run ---\n")
        except Exception as e:
            print(f"CRITICAL: Failed to write to '{log_file_path}': {e}", flush=True)
    
    
    # 8. Log final summary (using a new session for a committed read)
    total_job_time = time.time() - job_start_time
    remaining_games_count = await _get_remaining_fens_count_committed()
    
    print(f"--- [END] FEN Generation Job ---", flush=True)
    print(f"--- Total job time: {total_job_time:.2f} seconds ---", flush=True)
    print(f"--- Total games processed in this run: {total_processed_so_far} ---", flush=True)
    print(f"--- Games failed (illegal SAN): {total_games_failed} ---", flush=True)
    print(f"--- Games still needing FENs in DB: {remaining_games_count} ---", flush=True)


async def _mark_games_as_done_in_session(session: AsyncSession, game_links: List[int]):
    """
    (Helper for run_fen_generation_job)
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
    # The commit is now handled by the main job loop
    # print(f"Successfully marked {len(game_links)} games as fens_done=True (pending commit).", flush=True) # <-- SILENCED