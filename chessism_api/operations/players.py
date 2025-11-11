# chessism_api/operations/players.py

from fastapi.encoders import jsonable_encoder
from typing import Optional, Union, Dict, Any, Tuple, List
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, update

# --- FIXED IMPORTS ---
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Player, PlayerStats # <-- NEW
from chessism_api.database.ask_db import open_async_request
from chessism_api.operations.models import PlayerCreateData, PlayerStatsCreateData # <-- NEW
from chessism_api.operations.chess_com_api import get_profile, get_player_stats # <-- NEW
# ---

async def read_player(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Reads a player's profile from the database by player_name.
    """
    player_db_interface = DBInterface(Player)
    player_name_lower = player_name.lower()
    
    # --- FIX: .read() returns a list ---
    player_list = await player_db_interface.read(player_name=player_name_lower)

    if player_list:
        return player_list[0] # Return the first (and only) player dict
    else:
        print(f"Player {player_name_lower} not found in DB.")
        return None

async def insert_player(data: dict) -> Optional[PlayerCreateData]:
    """
    Inserts or updates a player in the database after fetching their
    profile from Chess.com.
    """
    player_name_lower = data['player_name'].lower()
    player_interface = DBInterface(Player)

    fetched_profile: Optional[PlayerCreateData] = await get_profile(player_name_lower)

    if fetched_profile is None:
        print(f"Profile for {player_name_lower} came back as None from Chess.com.")
        return None

    # --- FIX: Convert Pydantic model to dict for DBInterface ---
    fetched_profile_dict = fetched_profile.model_dump()

    try:
        print(f"Attempting to insert new player profile for: {player_name_lower}")
        # .create() returns a dict
        created_player_dict = await player_interface.create(fetched_profile_dict)
        print(f'NEW player {player_name_lower} inserted')
        # --- FIX: Convert dict back to Pydantic model for return type ---
        return PlayerCreateData(**created_player_dict)
        
    except IntegrityError: 
        print(f"Player {player_name_lower} already exists. Updating.")
        try:
            # --- FIX: Use correct .update() method ---
            # .update() takes (primary_key, data_dict)
            updated_player_dict = await player_interface.update(player_name_lower, fetched_profile_dict)
            if updated_player_dict:
                 # --- FIX: Convert dict back to Pydantic model ---
                return PlayerCreateData(**updated_player_dict)
            else:
                print(f"Update failed for {player_name_lower}, player not found by update method.")
                return None
        except Exception as update_e:
            print(f"Error updating player {player_name_lower} after failed insert: {update_e}")
            return None
    except Exception as e:
        print(f"An unexpected error occurred during player creation for {player_name_lower}: {e}")
        return None


async def get_current_players_with_games_in_db() -> List[Dict[str, Any]]:
    """
    Returns every player which column 'joined' is not 0.
    """
    query = """
    SELECT * FROM player
    WHERE joined != 0;
    """
    players = await open_async_request(query, fetch_as_dict=True)
    return players


# --- NEW: Functions for PlayerStats ---

async def read_player_stats(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Reads a player's stats from the database.
    """
    stats_db_interface = DBInterface(PlayerStats)
    player_name_lower = player_name.lower()
    
    stats_list = await stats_db_interface.read(player_name=player_name_lower)

    if stats_list:
        return stats_list[0]
    else:
        print(f"Stats for {player_name_lower} not found in DB.")
        return None

def _parse_stats_category(raw_stats: Dict[str, Any], category: str) -> Dict[str, Any]:
    """Helper to safely parse one stats category (e.g., 'chess_rapid')."""
    data = raw_stats.get(category, {})
    
    # Handle cases where data is None
    if data is None:
        data = {}

    last = data.get('last', {}) or {}
    best = data.get('best', {}) or {}
    record = data.get('record', {}) or {}
    
    return {
        f"{category}_last_rating": last.get('rating'),
        f"{category}_best_rating": best.get('rating'),
        f"{category}_games": record.get('game_count'), # Custom mapping
        f"{category}_wins": record.get('win'),
        f"{category}_losses": record.get('loss'),
        f"{category}_draws": record.get('draw'),
    }

async def create_and_store_player_stats(player_name: str) -> Optional[PlayerStatsCreateData]:
    """
    Fetches stats from Chess.com, parses them, and performs an "upsert"
    (insert or update) into the PlayerStats table.
    """
    player_name_lower = player_name.lower()
    
    # 1. Ensure the player exists in the Player table first.
    player = await read_player(player_name_lower)
    if not player:
        # If player doesn't exist, create a shell record (or full profile)
        print(f"Player {player_name_lower} not in DB, creating before adding stats.")
        player_profile = await insert_player({"player_name": player_name_lower})
        if not player_profile:
            print(f"Could not create player {player_name_lower}, cannot store stats.")
            return None

    # 2. Fetch the raw stats from Chess.com API
    raw_stats = await get_player_stats(player_name_lower)
    if not raw_stats:
        print(f"Failed to fetch stats from Chess.com for {player_name_lower}.")
        return None

    # 3. Parse the raw stats into the Pydantic model format
    parsed_data = {"player_name": player_name_lower}
    
    parsed_data.update(_parse_stats_category(raw_stats, 'chess_rapid'))
    parsed_data.update(_parse_stats_category(raw_stats, 'chess_blitz'))
    parsed_data.update(_parse_stats_category(raw_stats, 'chess_bullet'))

    parsed_data['fide'] = raw_stats.get('fide')
    parsed_data['puzzle_rush_best_score'] = raw_stats.get('puzzle_rush', {}).get('best', {}).get('score')
    parsed_data['tactics_highest_rating'] = raw_stats.get('tactics', {}).get('highest', {}).get('rating')
    parsed_data['tactics_lowest_rating'] = raw_stats.get('tactics', {}).get('lowest', {}).get('rating')

    try:
        # Validate the parsed data
        stats_data = PlayerStatsCreateData(**parsed_data)
    except Exception as e:
        print(f"Error validating parsed stats for {player_name_lower}: {e}")
        return None

    # 4. Perform an "UPSERT"
    stats_interface = DBInterface(PlayerStats)
    stats_data_dict = stats_data.model_dump()

    try:
        # Try to create it
        created_stats = await stats_interface.create(stats_data_dict)
        print(f"Successfully inserted new stats for {player_name_lower}.")
        return PlayerStatsCreateData(**created_stats)
    except IntegrityError:
        # It already exists, so update it
        print(f"Stats for {player_name_lower} already exist. Updating.")
        try:
            updated_stats = await stats_interface.update(player_name_lower, stats_data_dict)
            if updated_stats:
                return PlayerStatsCreateData(**updated_stats)
            return None
        except Exception as e:
            print(f"Error updating stats for {player_name_lower}: {e}")
            return None
    except Exception as e:
        print(f"An unexpected error occurred during stats upsert: {e}")
        return None