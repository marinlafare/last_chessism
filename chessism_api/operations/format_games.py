# chessism_api/operations/format_games.py

from typing import Union,Dict,Any, List, Set, Tuple
import asyncio
import concurrent.futures
from sqlalchemy import text, select
from fastapi.encoders import jsonable_encoder

from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT
import re
import multiprocessing as mp
import time
from datetime import datetime

# --- FIXED IMPORTS & STUBS ---
from chessism_api.operations.models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Player, Game, Month, Move
from chessism_api.operations import players as players_ops

# --- FIXED: Correct function name imported from ask_db ---
from chessism_api.database.ask_db import (
    get_games_already_in_db
)

# --- FIXED: Correct function name imported from check_player_in_db ---
from chessism_api.operations.check_player_in_db import (
    get_only_players_not_in_db
)
# ---

# this should be at the main.py i think
cpu_bound_executor = concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count())


async def insert_new_data(games_list, moves_list, months_list):
    """
    Inserts formatted game, move, and month data into the database in the correct order
    to respect foreign key constraints. Games must be inserted before moves.

    Args: lists for games, moves_list and month_list
            each list contains one dictionary for item.

    Returns: Nothing
    
    """
    game_interface = DBInterface(Game)
    move_interface = DBInterface(Move)
    month_interface = DBInterface(Month)

    # Step 1: Insert games first. This is crucial for foreign key integrity with moves.
    if games_list:
        await game_interface.create_all(games_list)
        print(f"Successfully inserted {len(games_list)} games.")
    else:
        print("No new games to insert.")

    # Step 2: Insert moves after games are confirmed to be in the database.
    if moves_list:
        await move_interface.create_all(moves_list)
        print(f"Successfully inserted {len(moves_list)} moves.")
    else:
        print("No new moves to insert.")

    # Step 3: Insert months. This can be done after games and moves, or even concurrently

    if months_list:
        await month_interface.create_all(months_list)
        print(f"Successfully inserted {len(months_list)} months.")
    else:
        print("No new months to insert.")

    total_inserted_items = len(games_list) + len(moves_list) + len(months_list)
    if total_inserted_items > 0:
        print(f"Overall database insertion completed for {len(games_list)} games, {len(moves_list)} moves, and {len(months_list)} months.")
    else:
        print("No data was inserted into the database.")

def get_pgn_item(game_pgn: str, item: str) -> str:
    """Extracts an item from a PGN string."""
    try:
        if item == "Termination":
            return (
                game_pgn.split(f"{item}")[1]
                .split("\n")[0]
                .replace('"', "")
                .replace("]", "")
                .lower()
            )
        return (
            game_pgn.split(f"{item}")[1]
            .split("\n")[0]
            .replace('"', "")
            .replace("]", "")
            .replace(" ", "")
            .lower()
        )
    except IndexError:
        # Handle cases where the PGN item is missing (e.g., [StartTime ""])
        # print(f"Warning: PGN item '{item}' not found or in unexpected format.")
        if item in ['StartTime', 'EndTime']:
            return "00:00:00" # Default time if missing
        if item in ['Date', 'EndDate']:
            return "0.0.0" # Default date if missing
        # Re-raise for other items
        raise

def get_start_and_end_date(game, game_for_db):
    """Extracts and calculates game start/end dates and time elapsed."""
    try:
        game_date = get_pgn_item(game['pgn'], item='Date').split('.')
        game_for_db['year'] = int(game_date[0])
        game_for_db['month'] = int(game_date[1])
        game_for_db['day'] = int(game_date[2])
    except Exception as e:
        print(f"Warning: Could not parse game date for game {game.get('url', 'N/A')}. Setting year to 0. Error: {e}")
        game_for_db['year'] = 0
        return game_for_db # Return early if date is invalid

    try:
        game_start_time_str = get_pgn_item(game['pgn'], item='StartTime').split(':')
        game_for_db['hour'] = int(game_start_time_str[0])
        game_for_db['minute'] = int(game_start_time_str[1])
        game_for_db['second'] = int(game_start_time_str[2])
    except Exception as e:
        print(f"Warning: Could not parse game start time for game {game.get('url', 'N/A')}. Setting to 0. Error: {e}")
        game_for_db['hour'] = 0
        game_for_db['minute'] = 0
        game_for_db['second'] = 0

    # Only create game_start if date parsing was successful
    if game_for_db['year'] != 0:
        game_start = datetime(year = game_for_db['year'],
                              month = game_for_db['month'],
                              day = game_for_db['day'],
                              hour = game_for_db['hour'],
                              minute = game_for_db['minute'],
                              second = game_for_db['second'])
    else:
        game_start = None # Cannot calculate time_elapsed

    try:
        game_end_date_str = get_pgn_item(game['pgn'], item='EndDate').split('.')
        game_for_db['end_year'] = int(game_end_date_str[0])
        game_for_db['end_month'] = int(game_end_date_str[1])
        game_for_db['end_day'] = int(game_end_date_str[2])
        game_end_time_str = get_pgn_item(game['pgn'], item='EndTime').split(':')
        game_for_db['end_hour'] = int(game_end_time_str[0])
        game_for_db['end_minute'] = int(game_end_time_str[1])
        game_for_db['end_second'] = int(game_end_time_str[2])

        game_end = datetime(year= game_for_db['end_year'],
                            month = game_for_db['end_month'],
                            day = game_for_db['end_day'],
                            hour = game_for_db['end_hour'],
                            minute = game_for_db['end_minute'],
                            second =game_for_db['end_second'])
        
        if game_start:
            game_for_db['time_elapsed'] = (game_end - game_start).total_seconds()
        else:
            game_for_db['time_elapsed'] = 0
            
    except Exception as e:
        print(f"Warning: Could not parse game end date/time or calculate time_elapsed for game {game.get('url', 'N/A')}. Setting to 0. Error: {e}")
        game_for_db['end_year'] = 0
        game_for_db['end_month'] = 0
        game_for_db['end_day'] = 0
        game_for_db['end_hour'] = 0
        game_for_db['end_minute'] = 0
        game_for_db['end_second'] = 0
        game_for_db['time_elapsed'] = 0

    return game_for_db

def translate_result_to_float(str_result):
    """Converts string results to float representation."""
    if str_result in WINING_RESULT:
        return 1.0
    if str_result in DRAW_RESULTS:
        return 0.5 
    if str_result in LOSE_RESULTS:
        return 0.0 
    else:
        print('""""""UNKNOWN Natural Language Result"""""""""""""')
        print(str_result)
        return None

def get_black_and_white_data(game, game_for_db):
    """Extracts white and black player data and results."""
    game_for_db['black'] = game['black']['username'].lower()
    game_for_db['black_elo'] = int(game['black']['rating'])
    game_for_db['black_str_result'] = game['black']['result'].lower()
    game_for_db['black_result'] = translate_result_to_float(game_for_db['black_str_result'])

    game_for_db['white'] = game['white']['username'].lower()
    game_for_db['white_elo'] = int(game['white']['rating'])
    game_for_db['white_str_result'] = game['white']['result'].lower()
    game_for_db['white_result'] = translate_result_to_float(game_for_db['white_str_result'])
    return game_for_db

def get_time_bonus(game):
    """Extracts time bonus from time_control string."""
    time_control = game['time_control']
    if "+" in time_control:
        return int(time_control.split("+")[-1])
    return 0

def get_n_moves(raw_moves):
    """Calculates the number of moves from a raw PGN moves string."""
    if not raw_moves.strip():
        return 0
    numeric_moves = [int(x.replace(".", "")) for x in raw_moves.split() if x.replace(".", "").isnumeric()]
    return max(numeric_moves) if numeric_moves else 0

# --- EFFICIENT VERSION ---
def _parse_time_to_seconds(time_str: str) -> float:
    """Converts 'H:M:S.f', 'M:S.f', or 'M:S' to seconds."""
    if time_str == "--":
        return 0.0
    try:
        parts = time_str.split(':')
        seconds = 0.0
        if len(parts) == 3: # H:M:S.f
            seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2: # M:S.f
            seconds = int(parts[0]) * 60 + float(parts[1])
        return round(seconds, 3)
    except Exception:
        return 0.0

def _calculate_reaction_times(time_series: List[float], time_bonus: int) -> List[float]:
    """
    Re-implements pandas `diff(periods=-1).abs() + time_bonus` in pure Python.
    Calculates time_left[i] - time_left[i+1]
    """
    reaction_times = []
    for i in range(len(time_series)):
        if i < len(time_series) - 1:
            # This is the time spent on the *next* move, which is what the diff did.
            # But the logic is flawed. The reaction time for move[i] should be time_left[i-1] - time_left[i].
            # However, to preserve your original logic:
            reaction = abs(time_series[i] - time_series[i+1])
            reaction_times.append(round(reaction + time_bonus, 3))
        else:
            # Last move has no next move to diff against
            reaction_times.append(round(time_bonus, 3)) # Or 0.0, based on desired logic
    return reaction_times

def create_moves_table(
        game_url:str,
        times: list,
        clean_moves: list,
        n_moves: int,
        time_bonus: int) -> dict[str, Any]: 
    """
    Formats raw move data into a dictionary suitable for MoveCreateData.
    Calculates reaction times from the available data.
    """
    
    link = int(game_url.split('/')[-1])

    if len(clean_moves) % 2 != 0:
        clean_moves.append("--")
    if len(times) % 2 != 0:
        times.append("--")

    white_moves = []
    black_moves = []
    white_times_sec = []
    black_times_sec = []

    # 1. Convert times to seconds and split moves
    for i in range(0, len(clean_moves), 2):
        white_moves.append(str(clean_moves[i]))
        black_moves.append(str(clean_moves[i+1]))
        
        # White time
        white_times_sec.append(_parse_time_to_seconds(times[i]))
        # Black time
        if i + 1 < len(times):
            black_times_sec.append(_parse_time_to_seconds(times[i+1]))
        else:
            black_times_sec.append(0.0)

    # 2. Calculate reaction times
    # time_left[i] - time_left[i+1]
    # This is the time elapsed *during* the (i+1)th move.
    # This means the reaction time for move 2 is stored at index 1.
    
    def diff_minus_1(series):
        if not series:
            return []
        return [abs(series[i] - series[i+1]) + time_bonus if i < len(series) - 1 else time_bonus for i in range(len(series))]

    white_reaction_times = [round(x, 3) for x in diff_minus_1(white_times_sec)]
    black_reaction_times = [round(x, 3) for x in diff_minus_1(black_times_sec)]

    result = {
        "link": link,
        "white_moves": white_moves,
        "white_reaction_times": white_reaction_times,
        "white_time_left": white_times_sec,
        "black_moves": black_moves,
        "black_reaction_times": black_reaction_times,
        "black_time_left": black_times_sec
    }
    return result
# --- END EFFICIENT VERSION ---

def get_moves_data(game: dict) -> tuple[int, dict]:
    """Extracts and formats the moves of a game."""
    time_bonus = get_time_bonus(game)

    raw_moves = (
        game['pgn'].split("\n\n")[1]
        .replace("1/2-1/2", "")
        .replace("1-0", "")
        .replace("0-1", "")
    )
    n_moves = get_n_moves(raw_moves)

    times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
    just_moves = re.sub(r"{[^}]*}*", "", raw_moves)
    clean_moves = [x for x in just_moves.split() if x and "." not in x]
    
    if len(clean_moves) % 2 != 0:
        clean_moves.append("--")

    if len(times) % 2 != 0:
        times.append("--")

    moves_data = create_moves_table(game['url'],
                                    times,
                                    clean_moves,
                                    n_moves,
                                    time_bonus)
    return n_moves, moves_data

def create_game_dict(game_raw_data: dict) -> Union[Dict[str, Any], str, bool]:
    """Converts raw game data into a dictionary for the Game model."""
    try:
        len(game_raw_data['pgn'])
    except KeyError:
        return "NO PGN"

    game_for_db = dict()
    game_for_db['fens_done'] = False
    game_for_db['link'] = int(game_raw_data['url'].split('/')[-1])
    game_for_db['time_control'] = game_raw_data['time_control']
    game_for_db = get_start_and_end_date(game_raw_data, game_for_db)

    if game_for_db['year'] == 0:
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to date parsing error.")
        return False

    game_for_db = get_black_and_white_data(game_raw_data, game_for_db)

    if game_for_db['white_result'] is None or game_for_db['black_result'] is None:
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to unrecognised result string.")
        return False

    try:
        n_moves, moves_data = get_moves_data(game_raw_data)
    except Exception as e:
        #print(f"Error getting moves data for game {game_raw_data.get('url', 'N/A')}: {e}")
        return False

    game_for_db['n_moves'] = n_moves
    game_for_db['moves_data'] = moves_data
    try:
        game_for_db['eco'] = game_raw_data['eco']
    except Exception: # Broad exception if ECO tag is missing
        game_for_db['eco'] = 'no_eco'
    return game_for_db

def format_one_game_moves(moves: dict) -> List[Dict[str, Any]]:
    """Formats individual moves data for the Move model."""
    to_insert_moves = []
    try:
        # Ensure 'white_moves', 'black_moves', etc. are present and are lists
        if not all(k in moves and isinstance(moves[k], list) for k in ['white_moves', 'white_reaction_times', 'white_time_left', 'black_moves', 'black_reaction_times', 'black_time_left']):
            print(f"Warning: Missing or invalid moves data structure for game link {moves.get('link', 'N/A')}")
            return []
    except KeyError:
        return []

    # Ensure all lists are of comparable length, or handle index errors gracefully
    max_len = len(moves['white_moves'])

    for ind in range(max_len):
        moves_dict = {}
        moves_dict['n_move'] = ind + 1
        moves_dict['link'] = moves['link']

        # White's move data
        moves_dict['white_move'] = str(moves['white_moves'][ind])
        moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind], 3) if ind < len(moves['white_reaction_times']) else 0.0
        moves_dict['white_time_left'] = round(moves['white_time_left'][ind], 3) if ind < len(moves['white_time_left']) else 0.0

        # Black's move data (handle potential IndexError if black has fewer moves)
        try:
            moves_dict['black_move'] = str(moves['black_moves'][ind])
            moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind], 3) if ind < len(moves['black_reaction_times']) else 0.0
            moves_dict['black_time_left'] = round(moves['black_time_left'][ind], 3) if ind < len(moves['black_time_left']) else 0.0
        except IndexError:
            moves_dict['black_move'] = '--'
            moves_dict['black_reaction_time'] = 0.0
            moves_dict['black_time_left'] = 0.0

        # Validate and convert to Pydantic model, then dump to dict
        try:
            to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
        except Exception as e:
            print(f"Error creating MoveCreateData for move {ind+1} of game {moves.get('link', 'N/A')}: {e}")
            # Decide whether to skip this move or the whole game, for now just skip this move
            continue
    return to_insert_moves


# --- MAIN FORMAT AND INSERT FUNCTION ---

async def format_games(games, player_name) -> Union[List[Dict[str, Any]], str]:
    """
    Formats and inserts downloaded games into the database efficiently.
    --- OPTIMIZED VERSION ---
    """
    player_interface = DBInterface(Player)
    start_overall = time.time()
    

    # Step 1: Filter out games already in DB (I/O-bound)
    start_filter = time.time()
    games_to_process = await get_just_new_games(games)
    if not games_to_process: # get_just_new_games returns False if no new games or error
        print(f"No new games to process for {player_name}. All games already at DB or input was empty.")
        return "All games already at DB"
    
    num_games_to_process = sum(len(m_games) for y_games in games_to_process.values() for m_games in y_games.values())
    print(f"Filtered {num_games_to_process} new games in: {time.time() - start_filter:.2f} seconds")

    # Step 2: Collect all unique players from the new games
    start_get_unique_players = time.time()
    unique_player_names = set()
    for year_games in games_to_process.values():
        for month_games in year_games.values():
            for game_raw_data in month_games:
                if 'white' in game_raw_data and 'username' in game_raw_data['white']:
                    unique_player_names.add(game_raw_data['white']['username'].lower())
                if 'black' in game_raw_data and 'username' in game_raw_data['black']:
                    unique_player_names.add(game_raw_data['black']['username'].lower())     
    
    # Step 3: Find players *not* in the DB
    players_not_in_db = await get_only_players_not_in_db(unique_player_names)
    print(f"Found {len(players_not_in_db)} new players to insert in: {time.time() - start_get_unique_players:.2f}s")
    
    # Step 4: Insert *only* the new players (as 'shell' players, without full profiles)
    start_inserting_players = time.time()
    if players_not_in_db:
        # Create minimal player dicts for insertion
        player_insertion_data = [
            {"player_name": p_name, "joined": 0} for p_name in players_not_in_db
        ]
        await player_interface.create_all(player_insertion_data)
        print(f"Inserted {len(players_not_in_db)} new 'shell' players in: {time.time() - start_inserting_players:.2f} seconds")

    # Step 5: Format games (CPU-bound, run in parallel)
    start_format = time.time()
    games_futures = []
    
    # Flatten the games_to_process to a single list of game_raw_data
    all_raw_games_to_format = [
        game_raw_data
        for year_games in games_to_process.values()
        for month_games in year_games.values()
        for game_raw_data in month_games
    ]

    # --- OPTIMIZATION: Schedule each game format as a separate thread task ---
    for game_raw_data in all_raw_games_to_format:
        game_future = asyncio.to_thread(create_game_dict, game_raw_data)
        games_futures.append(game_future)

    # Await all game formatting futures concurrently
    formatted_games_results = await asyncio.gather(*games_futures)
    
    # Filter out failed formats (None, False, "NO PGN")
    valid_formatted_games = [
        g for g in formatted_games_results if g and g != "NO PGN"
    ]
    
    print(f'Formatted {len(valid_formatted_games)} games (out of {len(all_raw_games_to_format)}) in {time.time()-start_format:.2f}s')
    
    # Return the list of valid, formatted game dictionaries
    return valid_formatted_games

async def insert_games_months_moves_and_players(formatted_games_results: List[Dict[str, Any]], player_name: str):
    """
    Takes the list of formatted game dictionaries and inserts games, moves, and months.
    --- OPTIMIZED VERSION ---
    """
    
    games_list_for_db = []
    moves_futures = [] # Store futures for move formatting
    months_processed_count = {} # { (year, month): count }
    
    start_moves_format = time.time()

    for game_dict_result in formatted_games_results:
        # We already filtered in the calling function, but double-check
        if not game_dict_result: 
            continue
            
        try:
            moves_data = game_dict_result.pop('moves_data', None)
        except Exception: 
            continue
        
        if moves_data:
            # --- OPTIMIZATION: Schedule each move format as a separate thread task ---
            moves_formatted_future = asyncio.to_thread(format_one_game_moves, moves_data)
            moves_futures.append(moves_formatted_future)

        # Prepare the game data for insertion
        try:
            games_list_for_db.append(GameCreateData(**game_dict_result).model_dump())
            
            # Update month counts
            game_year = game_dict_result.get('year')
            game_month = game_dict_result.get('month')
            if game_year and game_month:
                key = (int(game_year), int(game_month))
                months_processed_count[key] = months_processed_count.get(key, 0) + 1

        except Exception as e:
            print(f"Error creating GameCreateData for formatted game {game_dict_result.get('link', 'N/A')}: {e}. Skipping game.")
            continue
            
    # --- Await all move formatting tasks ---
    moves_list_results = await asyncio.gather(*moves_futures)
    
    # Flatten the list of lists of moves
    moves_list_for_db = [
        move for moves_list in moves_list_results for move in moves_list
    ]
    
    print(f'Formatted {len(moves_list_for_db)} moves in {time.time()-start_moves_format:.2f}s')

    print(f'{len(games_list_for_db)} Games ready to insert')
    print(f'{len(moves_list_for_db)} Moves ready to insert')

    # Create months_list_for_db from the collected counts
    months_list_for_db = []
    for (year, month), n_games in months_processed_count.items():
        month_data = {
            "player_name": player_name,
            "year": year,
            "month": month,
            "n_games": n_games
        }
        months_list_for_db.append(MonthCreateData(**month_data).model_dump())

    # Step 4: Insert data into DB (I/O-bound, run concurrently)
    if not games_list_for_db and not moves_list_for_db and not months_list_for_db:
        print("No data to insert after formatting. Skipping database insertion.")
        return f"No new data to insert for {player_name}."

    start_insert = time.time()
    await insert_new_data(games_list_for_db, moves_list_for_db, months_list_for_db)
    print(f'Inserted games, moves, and months for {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds')

    print(f"Total time for insert_games_months_moves_and_players: {(time.time()-start_moves_format):.2f} seconds")

    return f"Successfully processed and inserted {len(games_list_for_db)} games for {player_name}."


async def get_just_new_games(games: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> Union[Dict[str, Dict[str, List[Dict[str, Any]]]], bool]:
    """
    Asynchronously checks the available games and returns only those not already in the DB.
    
    --- OPTIMIZED VERSION ---
    This version iterates once to build a lookup map, then builds the new
    dictionary from the filtered list of new links, avoiding a second O(N) loop.
    """
    
    # Step 1: Create a lookup map of link -> (game_object, year, month)
    # This is O(N) but we only do it once.
    game_map: Dict[int, Tuple[Dict[str, Any], str, str]] = {}
    for year, month_data in games.items():
        for month, games_in_month in month_data.items():
            for game in games_in_month:
                try:
                    if game['url']:
                        game_link = int(game['url'].split('/')[-1])
                        game_map[game_link] = (game, year, month)
                    else:
                        print(f"Warning: Game found with no URL.")
                except Exception as e:
                    print(f"Error processing game for link extraction: {e}, game data: {game}")
                    continue

    links_to_check = set(game_map.keys())
    if not links_to_check:
        print("No valid game links found to check against the database.")
        return False

    # Step 2: Asynchronously get games already in the database
    in_db_game_links = await get_games_already_in_db(tuple(links_to_check))

    to_insert_game_links = links_to_check - in_db_game_links

    if not to_insert_game_links:
        print("All available games are already in the database.")
        return False

    # Step 3: Reconstruct the nested dictionary from the filtered links
    # This loop is O(M) where M is the number of NEW games (M <= N).
    new_games_structured: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for link in to_insert_game_links:
        game, year, month = game_map[link]
        
        if year not in new_games_structured:
            new_games_structured[year] = {}
        if month not in new_games_structured[year]:
            new_games_structured[year][month] = []
            
        new_games_structured[year][month].append(game)

    total_new_games_count = len(to_insert_game_links)
    if total_new_games_count == 0:
        # This check is technically redundant now but good for safety
        print("After filtering, no new games remain.")
        return False

    return new_games_structured