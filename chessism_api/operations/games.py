# chessism_api/operations/games.py

import time
from datetime import datetime
from typing import List, Dict, Any, Union
from sqlalchemy import select

# --- UPDATED: Removed month stub imports, as the file now exists ---
from chessism_api.operations.format_games import format_games, insert_games_months_moves_and_players
# (Note: This file does not use the functions from months.py, so the stub was safely removed)
# ---

from chessism_api.operations.chess_com_api import download_months
from chessism_api.database.ask_db import open_async_request
import chessism_api.operations.players as players_ops
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Month

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

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
    profile = await players_ops.insert_player({"player_name": player_name})

    if profile is None:
        return {"error": f"Could not fetch or create profile for {player_name}."}

    joined_ts = profile.joined
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
    Generates a list of 'YYYY-MM' month strings
    from player's joined date to current date.
    """
    dates_info = await get_joined_and_current_date(player_name)

    if "error" in dates_info:
        return dates_info

    joined_date = dates_info["joined_date"]
    current_date = dates_info["current_date"]

    all_months = []
    current_month_iter = joined_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current_month_iter <= current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        month_str = current_month_iter.strftime('%Y-%m')
        all_months.append(month_str)
        
        if current_month_iter.month == 12:
            current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
        else:
            current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)
            
    return all_months

async def just_new_months(player_name: str) -> Union[List[str], Dict[str, Any], bool]:
    """
    Filters the list of all possible months against months already in the DB.
    """
    
    all_possible_months_strs = await full_range(player_name)
    
    if isinstance(all_possible_months_strs, dict) and "error" in all_possible_months_strs:
        return all_possible_months_strs

    if not isinstance(all_possible_months_strs, list):
         return {"error": "Failed to generate full month range."}

    existing_months_for_player = []
    month_db_interface = DBInterface(Month)
    
    async with month_db_interface.get_session() as session:
    
        if not hasattr(Month, 'player_name') or \
           not hasattr(Month, 'year') or \
           not hasattr(Month, 'month'):
            print("Error: Month model missing expected attributes for querying existing months.")
            return {"error": "Month model definition issue. Idk seriously?"}

        player_db_months = select(Month).filter_by(player_name=player_name)
        result = await session.execute(player_db_months)
        existing_months_for_player = [f"{m.year}-{m.month:02d}" for m in result.scalars().all()]
    
    new_months_to_fetch = [
        month_str for month_str in all_possible_months_strs
        if month_str not in existing_months_for_player
    ]

    if not new_months_to_fetch:
        return False
    
    return new_months_to_fetch

async def create_games(data: dict) -> str:
    player_name = data['player_name'].lower()
    start_create_games = time.time()
    start_new_months = time.time()
    
    new_months = await just_new_months(player_name)
    
    if new_months is False:
        print('#####')
        print("MONTHS found: 0", 'time elapsed: ',time.time()-start_new_months)
        return 'ALL MONTHS IN DB ALREADY'
    
    if isinstance(new_months, dict) and "error" in new_months:
        return f"Error finding new months: {new_months['error']}"

    if not isinstance(new_months, list):
        return "Error: Did not receive a valid list of new months."
        
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
    
    # Handle non-list returns from format_games (e.g., error strings)
    if not isinstance(formatted_games_results, list):
        print(f"Formatting returned an unexpected value: {formatted_games_results}")
        return f"Formatting failed for {player_name}."

    print(f'FORMAT of {len(formatted_games_results)} games in: {time.time()-start_format}')
    
    await insert_games_months_moves_and_players(formatted_games_results, player_name)
    
    end_create_games = time.time()
    print('Format done in: ',(end_create_games-start_create_games)/60)
    return f"DATA READY FOR {player_name}"