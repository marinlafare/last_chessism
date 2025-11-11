# chessism_api/operations/players.py
from fastapi.encoders import jsonable_encoder
from typing import Optional, Union, Dict, Any, Tuple, List
from sqlalchemy.exc import IntegrityError

from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Player, to_dict
from chessism_api.database.ask_db import open_async_request # <-- This is imported
from chessism_api.operations.models import PlayerCreateData
from chessism_api.operations.chess_com_api import get_profile

async def read_player(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Reads a player's profile from the database by player_name.
    """
    player_db_interface = DBInterface(Player)
    player_name_lower = player_name.lower()
    
    # --- FIX: DBInterface uses generic .read() which returns a LIST ---
    player_data_list: List[Dict[str, Any]] = await player_db_interface.read(player_name=player_name_lower)

    if player_data_list:
        return player_data_list[0] # Return the first (and only) item
    else:
        print(f"Player {player_name_lower} not found in DB.")
        return None

async def insert_player(data: dict) -> Optional[PlayerCreateData]:
    """
    Inserts a new player or updates an existing one.
    """
    player_name_lower = data['player_name'].lower()
    player_interface = DBInterface(Player)

    fetched_profile: Optional[PlayerCreateData] = await get_profile(player_name_lower)

    if fetched_profile is None:
        print('player_name_lower has to be incorrect, the profile from chess.com came back as None')
        return None

    # --- FIX: Convert Pydantic object to dictionary for the database ---
    fetched_profile_dict = fetched_profile.model_dump()

    try:
        print(f"Attempting to insert new player profile for: {player_name_lower}")
        # --- FIX: .create() already returns a dict ---
        created_player_dict = await player_interface.create(fetched_profile_dict)
        print(f'NEW player {player_name_lower} inserted')
        # Re-validate the dict from the DB into a Pydantic model
        return PlayerCreateData(**created_player_dict)
    
    except IntegrityError: # Catches unique constraint violations
        print(f"Player {player_name_lower} already exists. Attempting update.")
        try:
            # --- FIX: Use generic .update() with (primary_key, data) ---
            updated_player_dict = await player_interface.update(player_name_lower, fetched_profile_dict)
            
            if updated_player_dict:
                # --- FIX: .update() already returns a dict ---
                return PlayerCreateData(**updated_player_dict)
            else:
                print(f"Failed to update player {player_name_lower} (not found by update).")
                return None
                
        except Exception as update_e:
            print(f"Error updating player {player_name_lower} after failed insert: {update_e}")
            return None
            
    except Exception as e:
        print(f"An unexpected error occurred during player creation for {player_name_lower}: {e}")
        return None

# --- NEW FUNCTION ---
async def get_current_players_with_games_in_db() -> List[Dict[str, Any]]:
    """
    Fetches all players from the database that have a 'joined' timestamp
    other than 0. This signifies they are "real" players with profiles,
    not just "shell" players.

    Returns:
        List[Dict[str, Any]]: A list of player dictionaries.
    """
    # We use a raw SQL query because the generic DBInterface.read()
    # only supports simple equality checks.
    sql_query = """
        SELECT * FROM player
        WHERE joined != 0;
    """
    try:
        # open_async_request is imported from chessism_api.database.ask_db
        players = await open_async_request(sql_query, fetch_as_dict=True)
        return players if players else []
    except Exception as e:
        print(f"Error fetching players with joined != 0: {e}")
        return []