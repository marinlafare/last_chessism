# chessism_api/operations/games.py

# chessism_api/operations/games.py

import time
from datetime import datetime
from typing import List, Dict, Any, Union # <-- Added Union

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy import select # <-- Added select

# --- FIXED IMPORTS ---
from chessism_api.operations.format_games import format_games, insert_games_months_moves_and_players
from chessism_api.operations.chess_com_api import download_months
from chessism_api.database.ask_db import (
    open_async_request,
    get_time_control_result_color_matrix,
    get_time_control_game_length_analytics,
    get_time_control_activity_trend
)
from chessism_api.database.models import Month # <-- Added Month
from chessism_api.database.db_interface import DBInterface # <-- Added DBInterface

# --- Import operations modules correctly ---
from chessism_api.operations import players as players_ops
from chessism_api.operations import months as months_ops
# ---

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

    existing_months_for_player = []
    month_db_interface = DBInterface(Month)
    
    # --- FIX: Use .get_session() ---
    async with month_db_interface.get_session() as session:
    
        if not hasattr(month_db_interface.db_class, 'player_name') or \
           not hasattr(month_db_interface.db_class, 'year') or \
           not hasattr(month_db_interface.db_class, 'month'):
            print("Error: Month model missing expected attributes.")
            return {"error": "Month model definition issue."}

        player_db_months = select(month_db_interface.db_class).filter_by(player_name=player_name)
        result = await session.execute(player_db_months)
        # --- FIX: Use YYYY-M format ---
        existing_months_for_player = [f"{m.year}-{m.month}" for m in result.scalars().all()]
    
    new_months_to_fetch = [
        month_str for month_str in all_possible_months_strs
        if month_str not in existing_months_for_player
    ]

    if not new_months_to_fetch:
        return False
    
    return new_months_to_fetch

async def create_games(data: dict) -> str:
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
    
    print('... Starting DOWNLOAD ...')
    downloaded_games_by_month = await download_months(player_name, new_months)    
    
    num_downloaded_games = sum(len(v) for y in downloaded_games_by_month.values()
                                for v in y.values()) if downloaded_games_by_month else 0
    
    print(f"Processed {len(new_months)} months. Downloaded games: {num_downloaded_games}")
    print('#####')
    print('#####')
    print('Start the formating of the games')
    start_format = time.time()
    
    formatted_games_results = await format_games(downloaded_games_by_month, player_name)
    
    if isinstance(formatted_games_results, str): # Handle "All games already in DB"
        print(formatted_games_results)
        return formatted_games_results
        
    print(f'FORMAT of {len(formatted_games_results)} games in: {time.time()-start_format}')
    
    await insert_games_months_moves_and_players(formatted_games_results, player_name)
    
    end_create_games = time.time()
    print('Format done in: ',(end_create_games-start_create_games)/60)
    return f"DATA READY FOR {player_name}"


# --- NEW FUNCTION ---
async def update_player_games(data: dict) -> str:
    """
    Fetches games only from the most recent month in the DB up to the current month.
    This includes re-downloading the most recent month to catch games played
    after the last download.
    """
    player_name = data['player_name'].lower()
    start_update_games = time.time()

    # 1. Get the most recent month from the DB
    month_db_interface = DBInterface(Month)
    db_months_list = await month_db_interface.read(player_name=player_name)
    
    most_recent_month_dict = months_ops.get_most_recent_month(db_months_list)

    if not most_recent_month_dict:
        print(f"No existing months found for {player_name}. Running full 'create_games' instead.")
        return await create_games(data)

    # 2. Generate month strings from that date until now
    # This will include the most_recent_month itself
    months_to_fetch = months_ops.generate_months_from_date_to_now(most_recent_month_dict)
    
    if not months_to_fetch:
        print(f"Player {player_name} is already up to date.")
        return f"Player {player_name} is already up to date."

    print('#####')
    print(f"UPDATING {len(months_to_fetch)} months (from {months_to_fetch[0]} to present)...")

    # 3. Download games for these months
    print('... Starting DOWNLOAD ...')
    downloaded_games_by_month = await download_months(player_name, months_to_fetch)    
    
    num_downloaded_games = sum(len(v) for y in downloaded_games_by_month.values()
                                for v in y.values()) if downloaded_games_by_month else 0
    
    print(f"Processed {len(months_to_fetch)} months. Downloaded games: {num_downloaded_games}")
    if num_downloaded_games == 0:
        return f"No new games found for {player_name}."

    # 4. Format and insert
    print('#####')
    print('Start the formating of the games')
    start_format = time.time()
    
    formatted_games_results = await format_games(downloaded_games_by_month, player_name)
    
    if isinstance(formatted_games_results, str): # Handle "All games already in DB"
        print(formatted_games_results)
        return formatted_games_results
        
    print(f'FORMAT of {len(formatted_games_results)} games in: {time.time()-start_format}')
    
    await insert_games_months_moves_and_players(formatted_games_results, player_name)
    
    end_update_games = time.time()
    print('Update done in: ',(end_update_games-start_update_games)/60)
    return f"DATA UPDATED FOR {player_name}"


async def get_time_control_result_color_matrix_payload(
    mode: str,
    min_rating: int = None,
    max_rating: int = None
) -> Dict[str, Any]:
    """
    Operations-layer wrapper for result matrix analytics.
    """
    return await get_time_control_result_color_matrix(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating
    )


async def get_time_control_game_length_analytics_payload(
    mode: str,
    min_rating: int = None,
    max_rating: int = None
) -> Dict[str, Any]:
    """
    Operations-layer wrapper for game-length analytics.
    """
    return await get_time_control_game_length_analytics(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating
    )


async def get_time_control_activity_trend_payload(
    mode: str,
    min_rating: int = None,
    max_rating: int = None
) -> Dict[str, Any]:
    """
    Operations-layer wrapper for activity-trend analytics.
    """
    return await get_time_control_activity_trend(
        mode=mode,
        min_rating=min_rating,
        max_rating=max_rating
    )
