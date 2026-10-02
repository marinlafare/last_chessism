# chessism_api/operations/fens.py

import asyncio
import chess
import json
import time
from typing import List, Dict, Any, Tuple
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
import os
import math # Import math

# --- NEW: arq imports for the "boss" job ---
from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Game, Move, Fen
from chessism_api.database.models import GameFenAssociation
from chessism_api.database.db_interface import DBInterface
# --- THIS IS THE FIX: Import the missing function ---
from chessism_api.database.ask_db import (
    _get_remaining_fens_count_committed,
    get_fen_ingestion_summary,
    increment_fen_ingestion_summaries,
    refresh_game_analysis_summary,
    refresh_database_summary_fen_counts,
    refresh_fen_pipeline_summary,
    refresh_scored_position_summary,
    refresh_scored_rating_summary
)
from chessism_api.operations.tablebase import ensure_tablebase_analysis_enqueued
from chessism_api.operations.player_salience import enqueue_stale_player_salience_jobs
from chessism_api.operations.ingestion_pipeline import (
    FEN_EXTRACTION,
    LINKING_POSITIONS,
    SAVING_POSITIONS,
    TABLEBASE_MARKING,
    UPDATING_SUMMARIES,
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


FEN_PIPELINE_COORDINATION_KEY = "chessism:automatic_fen_pipeline"
FEN_PIPELINE_PROGRESS_KIND = "fen_extraction"
FEN_PIPELINE_BATCH_SIZE = 1_000
FEN_PIPELINE_WORKERS = 3
FEN_PIPELINE_LOCK_TTL_SECONDS = 60 * 60 * 24
FEN_PIPELINE_RESERVATION_TTL_SECONDS = 60
PROGRESS_TTL_SECONDS = 60 * 60 * 24
ACTIVE_JOB_STATUSES = {
    JobStatus.queued,
    JobStatus.deferred,
    JobStatus.in_progress,
}


def count_fen_pieces(fen: str) -> int:
    """Count chessmen in a FEN without constructing another board object."""
    board_field = str(fen or "").split(" ", 1)[0]
    return sum(character.isalpha() for character in board_field)


def _decode_redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value or "")


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
    payload = {
        "job_id": job_id,
        "kind": FEN_PIPELINE_PROGRESS_KIND,
        "ingestion_run_id": ingestion_run_id,
        "total": max(0, int(total)),
        "processed": max(0, int(processed)),
        "failed": max(0, int(failed)),
        "phase": phase,
        "detail": detail,
        "updated_at": time.time(),
    }
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps(payload),
        ex=PROGRESS_TTL_SECONDS,
    )


async def ensure_fen_pipeline_enqueued(
    redis: ArqRedis,
    *,
    total_games_to_process: int | None = None,
    batch_size: int = FEN_PIPELINE_BATCH_SIZE,
    num_workers: int = FEN_PIPELINE_WORKERS,
    ingestion_run_id: str | None = None,
    player_name: str | None = None,
    trigger: str = "automatic",
) -> Dict[str, Any]:
    """Ensure one automatic FEN pipeline is draining all pending games."""
    pending_games = int(await _get_remaining_fens_count_committed() or 0)
    if pending_games <= 0:
        return {
            "status": "up_to_date",
            "job_id": None,
            "pending_games": 0,
        }

    current_value = await redis.get(FEN_PIPELINE_COORDINATION_KEY)
    current_job_id = _decode_redis_text(current_value)
    if current_job_id and current_job_id != "reserving":
        current_job = Job(current_job_id, redis, _queue_name="pipeline_queue")
        current_status = await current_job.status()
        if current_status in ACTIVE_JOB_STATUSES:
            return {
                "status": "already_active",
                "job_id": current_job_id,
                "pending_games": pending_games,
            }
        await redis.delete(FEN_PIPELINE_COORDINATION_KEY)
    elif current_job_id == "reserving":
        return {
            "status": "already_active",
            "job_id": None,
            "pending_games": pending_games,
        }

    reserved = await redis.set(
        FEN_PIPELINE_COORDINATION_KEY,
        "reserving",
        ex=FEN_PIPELINE_RESERVATION_TTL_SECONDS,
        nx=True,
    )
    if not reserved:
        active_value = await redis.get(FEN_PIPELINE_COORDINATION_KEY)
        active_job_id = _decode_redis_text(active_value)
        return {
            "status": "already_active",
            "job_id": active_job_id if active_job_id != "reserving" else None,
            "pending_games": pending_games,
        }

    stable_run_id = ingestion_run_id
    try:
        # Holding the reservation makes it safe to recover claims left behind
        # by a crashed coordinator without racing a newly-started pipeline.
        released_claims = await _release_fen_processing_claims()
        if released_claims:
            print(
                f"Released {released_claims} stale FEN extraction claims.",
                flush=True,
            )
        pending_games = int(await _get_remaining_fens_count_committed() or 0)
        if pending_games <= 0:
            await redis.delete(FEN_PIPELINE_COORDINATION_KEY)
            return {
                "status": "up_to_date",
                "job_id": None,
                "pending_games": 0,
            }

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
        if _decode_redis_text(await redis.get(FEN_PIPELINE_COORDINATION_KEY)) == "reserving":
            await redis.delete(FEN_PIPELINE_COORDINATION_KEY)
        await fail_running_ingestion_stages(stable_run_id, str(error))
        await finish_ingestion_run(
            stable_run_id,
            status="failed",
            error=str(error),
        )
        raise


async def _release_fen_pipeline_coordination(redis: ArqRedis, job_id: str) -> None:
    current_job_id = _decode_redis_text(await redis.get(FEN_PIPELINE_COORDINATION_KEY))
    if current_job_id == job_id:
        await redis.delete(FEN_PIPELINE_COORDINATION_KEY)


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


def count_expected_fen_positions(moves: List[Dict[str, Any]]) -> int:
    """Count the legal-move slots that FEN extraction will attempt."""
    return sum(
        1
        for move in moves
        for field in ("white_move", "black_move")
        if move.get(field) and move.get(field) != "--"
    )


async def _extract_one_game(
    link: int,
    moves: List[Dict[str, Any]],
) -> tuple[int, tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
    return int(link), await asyncio.to_thread(process_single_game_sync, (link, moves))


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
        .where(
            Game.fens_done == False,
            Game.fens_processing == False,
            Game.rules == "chess",
        )
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

async def _set_games_fen_state_in_session(
    session: AsyncSession,
    game_links: List[int],
    *,
    done: bool | None = None,
    processing: bool | None = None,
) -> None:
    """Bulk-update durable completion and transient extraction claim state."""
    if not game_links:
        return

    values: Dict[str, bool] = {}
    if done is not None:
        values["fens_done"] = bool(done)
    if processing is not None:
        values["fens_processing"] = bool(processing)
    if not values:
        return

    stmt = (
        update(Game)
        .where(Game.link.in_(game_links))
        .values(**values)
    )
    await session.execute(stmt)


async def _release_fen_processing_claims(game_links: List[int] | None = None) -> int:
    """Release claims after a finished/failed pipeline or a stale coordinator."""
    async with AsyncDBSession() as session:
        stmt = update(Game).where(Game.fens_processing == True)
        if game_links is not None:
            clean_links = list(dict.fromkeys(int(link) for link in game_links))
            if not clean_links:
                return 0
            stmt = stmt.where(Game.link.in_(clean_links))
        result = await session.execute(stmt.values(fens_processing=False))
        await session.commit()
        return int(result.rowcount or 0)


async def _finalize_fen_game_states(
    successful_game_links: List[int],
    failed_game_links: List[int],
) -> None:
    """Publish completion only after every association insert has committed."""
    async with AsyncDBSession() as session:
        try:
            await _set_games_fen_state_in_session(
                session,
                successful_game_links,
                done=True,
                processing=False,
            )
            await _set_games_fen_state_in_session(
                session,
                failed_game_links,
                done=False,
                processing=False,
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def _refresh_fen_occurrence_counts(fen_values: List[str]) -> None:
    """Recount affected FENs from committed associations, keeping retries idempotent."""
    unique_fens = list(dict.fromkeys(str(fen) for fen in fen_values if fen))
    if not unique_fens:
        return

    query = text("""
        WITH target(fen) AS (
            SELECT unnest(CAST(:fen_values AS VARCHAR[]))
        ), counts AS (
            SELECT target.fen, COUNT(association.id)::bigint AS occurrences
            FROM target
            LEFT JOIN game_fen_association association
              ON association.fen_fen = target.fen
            GROUP BY target.fen
        )
        UPDATE fen
        SET n_games = counts.occurrences
        FROM counts
        WHERE fen.fen = counts.fen;
    """)
    async with AsyncDBSession() as session:
        try:
            for start in range(0, len(unique_fens), 5_000):
                await session.execute(
                    query,
                    {"fen_values": unique_fens[start:start + 5_000]},
                )
            await session.commit()
        except Exception:
            await session.rollback()
            raise


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
                'piece_count': count_fen_pieces(fen_str),
                'n_games': 1,
                'moves_counter': move_counter_str, # e.g., "#0_1"
                '_move_counters': {move_counter_str},
                'score': None,
                'next_moves': None
            }
        else:
            fen_map[fen_str]['n_games'] += 1
            if move_counter_str not in fen_map[fen_str]['_move_counters']:
                fen_map[fen_str]['_move_counters'].add(move_counter_str)
                fen_map[fen_str]['moves_counter'] += move_counter_str # Appends "#1_1" -> "#0_1#1_1"

    # --- THIS IS THE FIX: Create the *correct* list for associations ---
    # The aggregated list (fen_map.values()) is for the 'fen' table.
    # The original 'all_associations' list is needed for the 'game_fen_association' table.
    # We just need to remove the temporary 'move_counter_string' key.
    
    unique_associations = []
    seen_associations = set()
    for assoc in all_associations:
        key = (
            assoc['game_link'],
            assoc['fen_fen'],
            assoc['n_move'],
            assoc['move_color'],
        )
        if key in seen_associations:
            continue
        seen_associations.add(key)
        unique_associations.append({
            'game_link': assoc['game_link'],
            'fen_fen': assoc['fen_fen'],
            'n_move': assoc['n_move'],
            'move_color': assoc['move_color'],
        })

    for fen_data in fen_map.values():
        fen_data.pop('_move_counters')

    print(f"[FEN AGGREGATOR] Found {len(fen_map)} unique FENs.")
    print(f"[FEN AGGREGATOR] Found {len(unique_associations)} unique associations.")
    
    return list(fen_map.values()), unique_associations
    # --- END FIX ---

# --- NEW: List splitter utility ---
def split_list(data: List[Any], n_chunks: int) -> List[List[Any]]:
    """Split data into exactly ``n_chunks`` balanced chunks."""
    if n_chunks <= 0:
        return [data]
    base_size, remainder = divmod(len(data), n_chunks)
    chunks = []
    start = 0
    for index in range(n_chunks):
        chunk_size = base_size + (1 if index < remainder else 0)
        chunks.append(data[start:start + chunk_size])
        start += chunk_size
    return chunks

# ---
# 3. BACKGROUND JOBS (MODIFIED)
# ---

# --- STAGE 1: The "Generation" Job (Child) ---
async def run_fen_generation_job(
    ctx: dict, 
    total_games_to_process: int, # This is the "quota" for this worker
    batch_size: int, 
    parent_job_id: str | None = None,
    ingestion_run_id: str | None = None,
    **kwargs
) -> Dict[str, Any]:
    """
    STAGE 1: (CHILD "MAP" JOB)
    - Fetches batches of games using SKIP LOCKED.
    - Runs CPU-bound PGN parsing.
    - Returns associations plus the exact successful/failed game claims.
    - DOES NOT aggregate or insert FEN/Assoc data.
    """
    worker_id = ctx.get('job_id', 'unknown')[:6]
    job_log_prefix = f"[FEN GEN {worker_id}]" # <-- Changed log prefix
    print(f"--- [START] {job_log_prefix} ---", flush=True)
    print(f"{job_log_prefix} Quota: {total_games_to_process} games, in batches of {batch_size}.", flush=True)
    
    all_associations_for_job = []
    all_failures_for_job = []
    claimed_game_links: set[int] = set()
    successful_game_links: set[int] = set()
    failed_game_links: set[int] = set()
    total_processed_so_far = 0
    expected_positions_total = 0
    
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
                expected_by_link = {
                    int(link): count_expected_fen_positions(moves)
                    for link, moves in tasks_data
                }
                batch_expected_positions = sum(expected_by_link.values())
                expected_positions_total += batch_expected_positions
                if parent_job_id:
                    await increment_stage_progress(
                        ctx["redis"],
                        job_id=parent_job_id,
                        ingestion_run_id=ingestion_run_id,
                        phase="extracting",
                        total_delta=batch_expected_positions,
                        detail="Extracting positions from game moves.",
                    )
                tasks = [
                    asyncio.create_task(_extract_one_game(link, moves))
                    for link, moves in tasks_data
                ]
                
                batch_associations = []
                batch_failures = []

                missing_move_links = set(game_links) - set(moves_by_link)
                for link in missing_move_links:
                    batch_failures.append({
                        "link": link,
                        "move_num": -1,
                        "san": "N/A",
                        "error": "No stored moves",
                    })
                    failed_game_links.add(int(link))

                for completed in asyncio.as_completed(tasks):
                    link, (associations_list, failures_list) = await completed
                    expected_positions = expected_by_link.get(int(link), 0)
                    extracted_positions = len(associations_list)
                    if parent_job_id:
                        await increment_stage_progress(
                            ctx["redis"],
                            job_id=parent_job_id,
                            ingestion_run_id=ingestion_run_id,
                            phase="extracting",
                            processed_delta=extracted_positions,
                            failed_delta=max(0, expected_positions - extracted_positions),
                            detail="Extracting positions from game moves.",
                        )
                    if failures_list or not associations_list:
                        batch_failures.extend(failures_list)
                        if not failures_list:
                            batch_failures.append({
                                "link": link,
                                "move_num": -1,
                                "san": "N/A",
                                "error": "No FEN associations generated",
                            })
                        failed_game_links.add(int(link))
                    else:
                        batch_associations.extend(associations_list)
                        successful_game_links.add(int(link))
                
                all_associations_for_job.extend(batch_associations)
                all_failures_for_job.extend(batch_failures)

                # 4. Claim games so sibling workers cannot select them. Durable
                # completion is published by the coordinator only after every
                # FEN and association transaction has committed.
                await _set_games_fen_state_in_session(
                    session,
                    game_links,
                    processing=True,
                )
                claimed_game_links.update(int(link) for link in game_links)
                
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
        log_file_path = "/app/illegal_fen.txt"
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
    
    # 7. Return the raw associations and their claim outcome. ARQ serializes
    # this payload before the coordinator moves to the insertion stages.
    return {
        "associations": all_associations_for_job,
        "claimed_game_links": sorted(claimed_game_links),
        "successful_game_links": sorted(successful_game_links),
        "failed_game_links": sorted(failed_game_links),
        "failure_count": len(all_failures_for_job),
        "expected_positions": expected_positions_total,
    }


# --- NEW: STAGE 2: The "FEN Insertion" Job (Child) ---
async def run_fen_insertion_job(
    ctx: dict,
    fens_to_insert: List[Dict[str, Any]],
    parent_job_id: str | None = None,
    ingestion_run_id: str | None = None,
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
    # Smaller committed chunks make progress observable without sacrificing
    # the parallel bulk-insert path.
    TRANSACTION_BATCH_SIZE = 5_000
    
    total_fens = len(fens_to_insert)
    num_batches = math.ceil(total_fens / TRANSACTION_BATCH_SIZE)
    
    print(f"{job_log_prefix} Inserting {total_fens} FENs in {num_batches} batches of {TRANSACTION_BATCH_SIZE}...", flush=True)
    
    for i, start in enumerate(range(0, total_fens, TRANSACTION_BATCH_SIZE)):
        batch = fens_to_insert[start:start + TRANSACTION_BATCH_SIZE]
        batch_start_time_inner = time.time()
        print(f"{job_log_prefix} Starting FEN batch {i+1}/{num_batches} ({len(batch)} records)...", flush=True)
        try:
            # create_all handles its *own* session and commit.
            # This is now a self-contained transaction.
            await fen_interface.create_all(batch)
            if parent_job_id:
                await increment_stage_progress(
                    ctx["redis"],
                    job_id=parent_job_id,
                    ingestion_run_id=ingestion_run_id,
                    phase="saving_fens",
                    processed_delta=len(batch),
                    detail="Saving unique positions.",
                )
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
    parent_job_id: str | None = None,
    ingestion_run_id: str | None = None,
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
    # Smaller committed chunks make progress observable without sacrificing
    # the parallel bulk-insert path.
    TRANSACTION_BATCH_SIZE = 5_000
    
    total_assocs = len(associations_to_insert)
    num_batches = math.ceil(total_assocs / TRANSACTION_BATCH_SIZE)
    
    print(f"{job_log_prefix} Inserting {total_assocs} associations in {num_batches} batches of {TRANSACTION_BATCH_SIZE}...", flush=True)
    
    for i, start in enumerate(range(0, total_assocs, TRANSACTION_BATCH_SIZE)):
        batch = associations_to_insert[start:start + TRANSACTION_BATCH_SIZE]
        batch_start_time_inner = time.time()
        print(f"{job_log_prefix} Starting Association batch {i+1}/{num_batches} ({len(batch)} records)...", flush=True)
        try:
            # create_all handles its *own* session and commit.
            await assoc_interface.create_all(batch)
            if parent_job_id:
                await increment_stage_progress(
                    ctx["redis"],
                    job_id=parent_job_id,
                    ingestion_run_id=ingestion_run_id,
                    phase="saving_games",
                    processed_delta=len(batch),
                    detail="Linking positions back to games.",
                )
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
async def _run_fen_pipeline(
    ctx: dict,
    total_games_to_process: int,
    batch_size: int,
    num_workers: int,
    ingestion_run_id: str | None = None,
    **kwargs,
):
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
    progress_job_id = str(ctx.get("job_id") or "unknown")
    job_log_prefix = f"[FEN PIPELINE {progress_job_id[:6]}]"
    print(f"--- [START] {job_log_prefix} ---", flush=True)
    
    redis: ArqRedis = ctx['redis']
    
    # --- 1. Check games remaining ---
    games_remaining_in_db = await _get_remaining_fens_count_committed()
    if games_remaining_in_db == 0:
        print(f"{job_log_prefix} No games found to process. Aborting.", flush=True)
        return {"claimed_games": 0, "successful_games": 0, "failed_games": 0}
        
    actual_games_to_process = min(total_games_to_process, games_remaining_in_db)
    await start_ingestion_stage(
        ingestion_run_id,
        FEN_EXTRACTION,
        detail=f"Extracting positions from {actual_games_to_process} games.",
    )
    await reset_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="extracting",
        total=0,
        processed=0,
        detail=f"Extracting positions from {actual_games_to_process} games.",
    )
    
    print(f"{job_log_prefix} User requested {total_games_to_process}, DB has {games_remaining_in_db} remaining.", flush=True)
    print(f"{job_log_prefix} Will process {actual_games_to_process} total games. Distributing to {num_workers} workers.", flush=True)

    # --- 2. Enqueue "Generation" (Map) jobs ---
    games_per_worker, extra_games = divmod(actual_games_to_process, num_workers)
    generation_quotas = [
        games_per_worker + (1 if index < extra_games else 0)
        for index in range(num_workers)
    ]
    generation_quotas = [quota for quota in generation_quotas if quota > 0]
    
    gen_jobs: List[Job] = []
    print(f"{job_log_prefix} Enqueuing {len(generation_quotas)} generation jobs...", flush=True)
    for quota in generation_quotas:
        job = await redis.enqueue_job(
            'run_fen_generation_job',
            total_games_to_process=quota,
            batch_size=batch_size,
            parent_job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
            _queue_name='fen_queue'
        )
        gen_jobs.append(job)
        
    # --- 3. Wait for "Generation" jobs and collect results ---
    all_associations_from_workers: List[Dict[str, Any]] = []
    claimed_game_links: set[int] = set()
    successful_game_links: set[int] = set()
    failed_game_links: set[int] = set()
    generation_errors: List[str] = []
    expected_positions = 0
    for i, job in enumerate(gen_jobs):
        try:
            payload = await job.result(timeout=None) # Wait forever
            if isinstance(payload, dict):
                worker_associations = list(payload.get("associations") or [])
                claimed_game_links.update(
                    int(link) for link in payload.get("claimed_game_links") or []
                )
                successful_game_links.update(
                    int(link) for link in payload.get("successful_game_links") or []
                )
                failed_game_links.update(
                    int(link) for link in payload.get("failed_game_links") or []
                )
                expected_positions += int(payload.get("expected_positions") or 0)
            else:
                # Compatibility for a child that started immediately before a
                # rolling deployment. New workers always return a mapping.
                worker_associations = list(payload or [])
                legacy_links = {
                    int(item["game_link"])
                    for item in worker_associations
                    if item.get("game_link") is not None
                }
                claimed_game_links.update(legacy_links)
                successful_game_links.update(legacy_links)
            all_associations_from_workers.extend(worker_associations)
            print(
                f"{job_log_prefix} Generation job {i+1}/{len(gen_jobs)} "
                f"(ID: {job.job_id}) finished. Got "
                f"{len(worker_associations)} associations.",
                flush=True,
            )
        except Exception as e:
            print(f"CRITICAL: {job_log_prefix} Generation job {i+1} (ID: {job.job_id}) FAILED: {repr(e)}", flush=True)
            generation_errors.append(repr(e))
    
    print(f"{job_log_prefix} All generation jobs complete.", flush=True)

    if generation_errors:
        await _release_fen_processing_claims()
        raise RuntimeError(
            f"{len(generation_errors)} FEN generation worker(s) failed."
        )

    await finish_ingestion_stage(
        ingestion_run_id,
        FEN_EXTRACTION,
        processed=len(all_associations_from_workers),
        total=expected_positions,
        detail=f"Extracted {len(all_associations_from_workers)} position occurrences.",
    )

    successful_game_links.update({
        int(association["game_link"])
        for association in all_associations_from_workers
        if association.get("game_link") is not None
    })
    failed_game_links.update(
        claimed_game_links - successful_game_links - failed_game_links
    )

    if not all_associations_from_workers:
        await _finalize_fen_game_states([], sorted(claimed_game_links))
        await refresh_fen_pipeline_summary()
        for empty_stage in (SAVING_POSITIONS, LINKING_POSITIONS, UPDATING_SUMMARIES):
            await start_ingestion_stage(ingestion_run_id, empty_stage)
            await finish_ingestion_stage(ingestion_run_id, empty_stage)
        print(
            f"{job_log_prefix} No associations were generated; "
            f"released {len(claimed_game_links)} claims.",
            flush=True,
        )
        return {
            "claimed_games": len(claimed_game_links),
            "successful_games": 0,
            "failed_games": len(claimed_game_links),
        }

    # --- 4. Perform Central Aggregation (Reduce) ---
    fens_to_insert, associations_to_insert = _aggregate_fen_data_in_memory(all_associations_from_workers)
    affected_fen_values = [str(item["fen"]) for item in fens_to_insert]
    ingestion_summary_before = await get_fen_ingestion_summary(affected_fen_values)

    # --- 5. Split Aggregated Data ---
    fen_chunks = split_list(fens_to_insert, num_workers)
    assoc_chunks = split_list(associations_to_insert, num_workers)
    
    print(f"{job_log_prefix} Aggregation complete. Splitting into {num_workers} chunks.", flush=True)

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
        failed=len(failed_game_links),
        detail="Saving unique positions.",
    )

    # --- 6. Enqueue "FEN Insertion" (Write FENs) jobs ---
    print(f"{job_log_prefix} Enqueuing {num_workers} FEN insertion jobs...", flush=True)
    fen_insert_jobs: List[Job] = []
    for i in range(num_workers):
        job = await redis.enqueue_job(
            'run_fen_insertion_job',
            fens_to_insert=fen_chunks[i],
            parent_job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
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
            await _release_fen_processing_claims(sorted(claimed_game_links))
            await _refresh_fen_occurrence_counts(affected_fen_values)
            await refresh_database_summary_fen_counts()
            await refresh_scored_position_summary()
            raise RuntimeError("FEN insertion failed; game claims were released.") from e

    await finish_ingestion_stage(
        ingestion_run_id,
        SAVING_POSITIONS,
        processed=len(fens_to_insert),
        total=len(fens_to_insert),
        detail=f"Saved {len(fens_to_insert)} unique positions.",
    )

    print(f"{job_log_prefix} All FENs inserted. Proceeding to associations.", flush=True)
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
        detail="Linking positions back to their games.",
        failed=len(failed_game_links),
    )

    # --- 8. Enqueue "Association Insertion" (Write Assocs) jobs ---
    print(f"{job_log_prefix} Enqueuing {num_workers} Association insertion jobs...", flush=True)
    assoc_insert_jobs: List[Job] = []
    for i in range(num_workers):
        job = await redis.enqueue_job(
            'run_association_insertion_job',
            associations_to_insert=assoc_chunks[i],
            parent_job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
            _queue_name='fen_queue'
        )
        assoc_insert_jobs.append(job)

    # --- 9. Wait for "Association Insertion" jobs to complete ---
    association_errors: List[str] = []
    for i, job in enumerate(assoc_insert_jobs):
        try:
            await job.result(timeout=None) # Wait forever
            print(f"{job_log_prefix} Association insertion job {i+1}/{num_workers} (ID: {job.job_id}) finished.", flush=True)
        except Exception as e:
            print(f"CRITICAL: {job_log_prefix} Association insertion job {i+1} (ID: {job.job_id}) FAILED: {repr(e)}", flush=True)
            association_errors.append(repr(e))

    if association_errors:
        await _release_fen_processing_claims(sorted(claimed_game_links))
        await _refresh_fen_occurrence_counts(affected_fen_values)
        await refresh_database_summary_fen_counts()
        await refresh_scored_position_summary()
        raise RuntimeError(
            f"{len(association_errors)} association insertion worker(s) failed; "
            "game claims were released."
        )

    await finish_ingestion_stage(
        ingestion_run_id,
        LINKING_POSITIONS,
        processed=len(associations_to_insert),
        total=len(associations_to_insert),
        detail=f"Saved {len(associations_to_insert)} game-position links.",
    )

    print(f"{job_log_prefix} All association insertion jobs complete.", flush=True)
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
        failed=len(failed_game_links),
        detail="Recounting affected position occurrences.",
    )
    await _refresh_fen_occurrence_counts(affected_fen_values)
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Publishing completed game extraction state.",
    )
    await _finalize_fen_game_states(
        sorted(successful_game_links),
        sorted(failed_game_links),
    )
    await increment_stage_progress(
        redis,
        job_id=progress_job_id,
        ingestion_run_id=ingestion_run_id,
        phase="refreshing_statistics",
        processed_delta=1,
        detail="Updating aggregate ingestion counters.",
    )
    try:
        ingestion_summary_after = await get_fen_ingestion_summary(affected_fen_values)
        summary_delta = await increment_fen_ingestion_summaries(
            ingestion_summary_before,
            ingestion_summary_after,
        )
        print(
            f"{job_log_prefix} Incremented FEN summaries: {summary_delta}",
            flush=True,
        )
    except Exception as e:
        print(
            f"CRITICAL: {job_log_prefix} Failed to increment FEN summaries: "
            f"{repr(e)}. Falling back to a full reconciliation.",
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
    try:
        game_links = tuple(sorted(successful_game_links))
        game_summary_counts = await refresh_game_analysis_summary(game_links)
        print(f"{job_log_prefix} Refreshed game analysis summary: {game_summary_counts}", flush=True)
        await increment_stage_progress(
            redis,
            job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
            phase="refreshing_statistics",
            processed_delta=1,
            detail="Updating pipeline coverage summary.",
        )
        pipeline_summary = await refresh_fen_pipeline_summary()
        print(f"{job_log_prefix} Refreshed FEN pipeline summary: {pipeline_summary}", flush=True)
        await increment_stage_progress(
            redis,
            job_id=progress_job_id,
            ingestion_run_id=ingestion_run_id,
            phase="refreshing_statistics",
            processed_delta=1,
            detail="Ingestion summaries are current.",
        )
    except Exception as e:
        print(f"CRITICAL: {job_log_prefix} Failed to refresh game analysis summary: {repr(e)}", flush=True)
        raise
    await finish_ingestion_stage(
        ingestion_run_id,
        UPDATING_SUMMARIES,
        processed=5,
        total=5,
        detail="All ingestion summaries are current.",
    )
    print(f"--- [END] {job_log_prefix} ---", flush=True)
    return {
        "claimed_games": len(claimed_game_links),
        "successful_games": len(successful_game_links),
        "failed_games": len(failed_game_links),
        "successful_game_links": sorted(successful_game_links),
    }


async def run_fen_pipeline(
    ctx: dict,
    total_games_to_process: int,
    batch_size: int,
    num_workers: int,
    ingestion_run_id: str | None = None,
    player_name: str | None = None,
    trigger: str = "automatic",
    **kwargs,
):
    """Run the position stages of one measured ingestion pipeline."""
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
        successful_game_links = [
            int(link) for link in result_stats.get("successful_game_links") or []
        ]

        tablebase_job = {"status": "blocked", "job_id": None, "pending": 0}
        if not (failed_games > 0 and successful_games == 0):
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
                detail="Finding tablebase positions in the newly extracted games.",
                failed=failed_games,
            )
            tablebase_job = await ensure_tablebase_analysis_enqueued(
                redis,
                game_links=successful_game_links,
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
            if tablebase_job["status"] == "queued":
                print(
                    f"[FEN PIPELINE {job_id[:6]}] Queued Syzygy job "
                    f"{tablebase_job['job_id']} for {tablebase_job['pending']} positions.",
                    flush=True,
                )

        await _release_fen_pipeline_coordination(redis, job_id)
        coordination_released = True

        if failed_games > 0 and successful_games == 0:
            print(
                f"[FEN PIPELINE {job_id[:6]}] Stopped automatic retries: "
                f"{failed_games} games need data repair.",
                flush=True,
            )
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

        if follow_up["status"] == "queued":
            print(
                f"[FEN PIPELINE {job_id[:6]}] Queued follow-up job "
                f"{follow_up['job_id']} for {follow_up['pending_games']} games.",
                flush=True,
            )

        chain_drained = (
            follow_up["status"] == "up_to_date"
            and tablebase_job["status"] == "up_to_date"
        )
        if chain_drained:
            await increment_stage_progress(
                redis,
                job_id=job_id,
                ingestion_run_id=ingestion_run_id,
                phase="discovering_tablebase",
                processed_delta=0,
                detail="Refreshing the rating projection for the affected games.",
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
                    print(
                        f"[FEN PIPELINE {job_id[:6]}] Queued "
                        f"{len(salience_jobs)} player salience refreshes.",
                        flush=True,
                    )
            except Exception as error:
                print(
                    f"[FEN PIPELINE {job_id[:6]}] Could not queue player "
                    f"salience refreshes: {error!r}",
                    flush=True,
                )
            await finish_ingestion_stage(
                ingestion_run_id,
                TABLEBASE_MARKING,
                processed=2,
                total=2,
                detail="No new tablebase positions required marking.",
            )
            await finish_ingestion_run(ingestion_run_id)
        elif failed_games > 0 and successful_games == 0:
            error = f"{failed_games} games failed FEN validation."
            await fail_running_ingestion_stages(ingestion_run_id, error)
            await finish_ingestion_run(
                ingestion_run_id,
                status="failed",
                error=error,
            )

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
        return result
    except Exception as error:
        await _release_fen_processing_claims()
        await fail_running_ingestion_stages(ingestion_run_id, str(error))
        await finish_ingestion_run(
            ingestion_run_id,
            status="failed",
            error=str(error),
        )
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
