import json
import time
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Sequence, Union

from sqlalchemy import select

from chessism_api.operations.format_games import format_games, insert_games_months_moves_and_players
from chessism_api.operations.chess_com_api import download_months
from chessism_api.database.ask_db import open_async_request
from chessism_api.database.models import Month
from chessism_api.database.db_interface import DBInterface
from chessism_api.operations import players as players_ops
from chessism_api.operations import months as months_ops
from chessism_api.operations.fens import ensure_fen_pipeline_enqueued

PROGRESS_TTL_SECONDS = 60 * 60 * 24
GAME_UPDATE_PROGRESS_KIND = "game_update"
ProgressCallback = Callable[[str, int, int, str | None], Awaitable[None]]


async def _write_game_job_progress(
    ctx: dict,
    job_id: str,
    *,
    player_name: str,
    total: int,
    processed: int,
    failed: int = 0,
    phase: str,
    detail: str | None = None,
    result: str | None = None
) -> None:
    redis = ctx.get("redis")
    if not redis:
        return

    payload = {
        "job_id": job_id,
        "kind": GAME_UPDATE_PROGRESS_KIND,
        "player_name": player_name,
        "total": int(total),
        "processed": int(processed),
        "failed": int(failed),
        "phase": phase,
        "detail": detail,
        "result": result,
        "updated_at": time.time(),
    }
    try:
        await redis.set(
            f"chessism:job_progress:{job_id}",
            json.dumps(payload),
            ex=PROGRESS_TTL_SECONDS
        )
    except Exception as error:
        print(f"Failed to write game job progress for {job_id}: {repr(error)}", flush=True)

async def read_game(data):
    params = {"link": int(data)}
    sql_query =  """SELECT * FROM game
                    WHERE link = :link;"""
    result = await open_async_request(sql_query, params,fetch_as_dict=True)
    return result

async def get_joined_and_current_date(player_name: str) -> Dict[str, Any]:
    """
    Fetches player profile (inserting if new) and extracts the date they joined.
    """
    existing_player = await players_ops.read_player(player_name)
    if existing_player and existing_player.get("joined") not in (None, 0):
        joined_ts = existing_player.get("joined")
    else:
        profile = await players_ops.insert_player({"player_name": player_name})

        # Handle case where profile fetch failed
        if not profile:
            return {"error": f"Could not fetch or create profile for {player_name}."}

        joined_ts = getattr(profile, "joined", None)

    current_date = datetime.now()

    if joined_ts is None or joined_ts == 0:
        return {"error": "Joined date not found or is zero in player profile."}

    try:
        joined_date = datetime.fromtimestamp(joined_ts)
    except (TypeError, ValueError) as e:
        print(f"Error converting joined timestamp {joined_ts} for {player_name}: {e}")
        return {"error": f"Invalid joined date format for {player_name}"}
        
    return {"joined_date": joined_date, "current_date": current_date}
    
async def full_range(player_name: str) -> Union[List[str], Dict[str, Any]]:
    """
    Generates a list of 'YYYY-M' month strings
    from player's joined date to current date.
    """
    dates_info = await get_joined_and_current_date(player_name)

    if "error" in dates_info:
        return dates_info # Pass the error up

    joined_date = dates_info["joined_date"]
    current_date = dates_info["current_date"]

    all_months = []
    # Start from the 1st of the joined month
    current_month_iter = joined_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Ensure we include the current month
    end_date_iter = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


    while current_month_iter <= end_date_iter:
        # Use YYYY-M format
        month_str = f"{current_month_iter.year}-{current_month_iter.month}"
        all_months.append(month_str)
        
        if current_month_iter.month == 12:
            current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
        else:
            current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)
            
    return all_months

async def just_new_months(player_name: str) -> Union[List[str], Dict[str, Any], bool]:
    """
    Fetches all possible months for a player and filters out those already in the DB.
    """
    
    all_possible_months_strs = await full_range(player_name)
    
    if isinstance(all_possible_months_strs, dict) and "error" in all_possible_months_strs:
        return all_possible_months_strs # Pass error up

    month_db_interface = DBInterface(Month)
    
    # --- FIX: Use .get_session() ---
    async with month_db_interface.get_session() as session:
    
        player_db_months = select(month_db_interface.db_class).filter_by(player_name=player_name)
        result = await session.execute(player_db_months)
        existing_months_for_player = {
            f"{month.year}-{month.month}"
            for month in result.scalars().all()
        }
    
    new_months_to_fetch = [
        month_str for month_str in all_possible_months_strs
        if month_str not in existing_months_for_player
    ]

    if not new_months_to_fetch:
        return False
    
    return new_months_to_fetch


async def _download_format_and_insert(
    player_name: str,
    months: Sequence[str],
    progress_callback: ProgressCallback | None,
    *,
    no_games_message: str,
) -> str | None:
    """Run the shared download, formatting, and insertion stages."""
    total_steps = max(1, len(months) + 2)
    if progress_callback:
        await progress_callback(
            "downloading",
            total_steps,
            0,
            f"Downloading {len(months)} months.",
        )

    async def on_month_downloaded(index: int, month_str: str) -> None:
        if progress_callback:
            await progress_callback(
                "downloading",
                total_steps,
                index,
                f"Downloaded {month_str}.",
            )

    print("... Starting DOWNLOAD ...")
    downloaded_games = await download_months(
        player_name,
        list(months),
        progress_callback=on_month_downloaded,
    )
    downloaded_count = sum(
        len(month_games)
        for year_games in downloaded_games.values()
        for month_games in year_games.values()
    )
    print(f"Processed {len(months)} months. Downloaded games: {downloaded_count}")
    if downloaded_count == 0:
        return no_games_message

    if progress_callback:
        await progress_callback(
            "formatting",
            total_steps,
            len(months),
            "Formatting downloaded games.",
        )

    format_started = time.time()
    formatted_games = await format_games(downloaded_games, player_name)
    if isinstance(formatted_games, str):
        print(formatted_games)
        return formatted_games

    print(f"Formatted {len(formatted_games)} games in {time.time() - format_started:.2f}s")
    if progress_callback:
        await progress_callback(
            "inserting",
            total_steps,
            len(months) + 1,
            "Saving games to the database.",
        )

    await insert_games_months_moves_and_players(formatted_games, player_name)
    return None

async def create_games(data: dict, progress_callback: ProgressCallback | None = None) -> str:
    """
    Fetches all games for a player for all new months.
    """
    player_name = data['player_name'].lower()
    start_create_games = time.time()
    start_new_months = time.time()
    
    new_months = await just_new_months(player_name)
    
    if new_months is False:
        print('#####')
        print("MONTHS found: 0", 'time elapsed: ',time.time()-start_new_months)
        return 'ALL MONTHS IN DB ALREADY'
    elif isinstance(new_months, dict): # Handle error case
        print(f"Error finding new months: {new_months.get('error')}")
        return f"Error finding new months: {new_months.get('error')}"
    else:
        print('#####')
        print(f"MONTHS found: {len(new_months)}", 'time elapsed: ',time.time()-start_new_months)

    early_result = await _download_format_and_insert(
        player_name,
        new_months,
        progress_callback,
        no_games_message=f"No games found for {player_name}.",
    )
    if early_result:
        return early_result
    
    end_create_games = time.time()
    print('Format done in: ',(end_create_games-start_create_games)/60)
    return f"DATA READY FOR {player_name}"


# --- NEW FUNCTION ---
async def update_player_games(data: dict, progress_callback: ProgressCallback | None = None) -> str:
    """
    Fetches games only from the most recent month in the DB up to the current month.
    This includes re-downloading the most recent month to catch games played
    after the last download.
    """
    player_name = data['player_name'].lower()
    start_update_games = time.time()
    if progress_callback:
        await progress_callback("preparing", 1, 0, f"Checking existing months for {player_name}.")

    # 1. Get the most recent month from the DB
    month_db_interface = DBInterface(Month)
    db_months_list = await month_db_interface.read(player_name=player_name)
    
    most_recent_month_dict = months_ops.get_most_recent_month(db_months_list)

    if not most_recent_month_dict:
        print(f"No existing months found for {player_name}. Running full 'create_games' instead.")
        return await create_games(data, progress_callback=progress_callback)

    # 2. Generate month strings from that date until now
    # This will include the most_recent_month itself
    months_to_fetch = months_ops.generate_months_from_date_to_now(most_recent_month_dict)
    
    if not months_to_fetch:
        print(f"Player {player_name} is already up to date.")
        return f"Player {player_name} is already up to date."

    print('#####')
    print(f"UPDATING {len(months_to_fetch)} months (from {months_to_fetch[0]} to present)...")
    early_result = await _download_format_and_insert(
        player_name,
        months_to_fetch,
        progress_callback,
        no_games_message=f"No new games found for {player_name}.",
    )
    if early_result:
        return early_result
    
    end_update_games = time.time()
    print('Update done in: ',(end_update_games-start_update_games)/60)
    return f"DATA UPDATED FOR {player_name}"


async def _run_game_job(
    ctx: dict,
    data: dict,
    *,
    operation: Callable[[dict, ProgressCallback | None], Awaitable[str]],
    fallback_job_id: str,
    queued_detail: str,
) -> str:
    """Run a game ingestion operation with the shared ARQ progress lifecycle."""
    arq_job_id = str(ctx.get("job_id") or fallback_job_id)
    player_name = str(data.get("player_name", "")).strip().lower()
    latest_total = 1
    latest_processed = 0

    async def progress_callback(phase: str, total: int, processed: int, detail: str | None = None) -> None:
        nonlocal latest_total, latest_processed
        latest_total = max(1, int(total or 1))
        latest_processed = max(0, min(latest_total, int(processed or 0)))
        await _write_game_job_progress(
            ctx,
            arq_job_id,
            player_name=player_name,
            total=latest_total,
            processed=latest_processed,
            phase=phase,
            detail=detail
        )

    await _write_game_job_progress(
        ctx,
        arq_job_id,
        player_name=player_name,
        total=1,
        processed=0,
        phase="queued",
        detail=queued_detail.format(player_name=player_name),
    )

    try:
        message = await operation(data, progress_callback)
        fen_pipeline = await ensure_fen_pipeline_enqueued(ctx["redis"])
        if fen_pipeline["status"] == "queued":
            fen_detail = (
                f" Automatic FEN extraction queued for "
                f"{fen_pipeline['pending_games']:,} games."
            )
        elif fen_pipeline["status"] == "already_active":
            fen_detail = " Automatic FEN extraction is already running."
        else:
            fen_detail = " FEN extraction is up to date."
        completion_message = f"{message}{fen_detail}"
        await _write_game_job_progress(
            ctx,
            arq_job_id,
            player_name=player_name,
            total=latest_total,
            processed=latest_total,
            phase="complete",
            detail=completion_message,
            result=completion_message
        )
        return completion_message
    except Exception as error:
        await _write_game_job_progress(
            ctx,
            arq_job_id,
            player_name=player_name,
            total=latest_total,
            processed=latest_processed,
            failed=1,
            phase="failed",
            detail=str(error)
        )
        raise


async def run_create_player_games_job(ctx: dict, data: dict, **kwargs) -> str:
    return await _run_game_job(
        ctx,
        data,
        operation=create_games,
        fallback_job_id="game-download",
        queued_detail="Queued full download for {player_name}.",
    )

async def run_update_player_games_job(ctx: dict, data: dict, **kwargs) -> str:
    return await _run_game_job(
        ctx,
        data,
        operation=update_player_games,
        fallback_job_id="game-update",
        queued_detail="Queued update for {player_name}.",
    )
