#chessism_api/operations/players.py

import asyncio # <-- NEW
import time # <-- NEW
from fastapi.encoders import jsonable_encoder
from typing import Optional, Union, Dict, Any, Tuple, List
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, update

# --- FIXED IMPORTS ---
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Player, PlayerStats, to_dict # <-- NEW (PlayerStats, to_dict)
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
    Inserts a new player into the database or updates an existing one.
    Fetches the full profile from Chess.com.
    """
    player_name_lower = data['player_name'].lower()
    player_interface = DBInterface(Player)

    fetched_profile: Optional[PlayerCreateData] = await get_profile(player_name_lower)

    if fetched_profile is None:
        # This can happen if Chess.com API fails (e.g., timeout, 404)
        # We might still want to create a shell player if one doesn't exist
        # For now, we'll follow the logic of returning None if fetch fails
        print(f"Profile for {player_name_lower} came back as None from Chess.com.")
        
        # Check if a shell player exists, if not, create one.
        existing_player = await read_player(player_name_lower)
        if existing_player:
            return PlayerCreateData(**existing_player) # Return existing shell data

        # Create a shell player if fetch fails AND player doesn't exist
        print(f"Creating 'shell' player for {player_name_lower} as profile fetch failed.")
        shell_data = {"player_name": player_name_lower, "joined": 0}
        try:
            created_shell_dict = await player_interface.create(shell_data)
            return PlayerCreateData(**created_shell_dict)
        except IntegrityError:
             # Race condition: another process created it. Just read it.
             existing_player = await read_player(player_name_lower)
             return PlayerCreateData(**existing_player) if existing_player else None
        except Exception as e:
            print(f"Error creating shell player {player_name_lower}: {e}")
            return None


    # --- FIX: Convert Pydantic model to dict for DBInterface ---
    fetched_profile_dict = fetched_profile.model_dump()

    try:
        print(f"Attempting to insert new player profile for: {player_name_lower}")
        created_player_dict = await player_interface.create(fetched_profile_dict)
        print(f'NEW player {player_name_lower} inserted')
        # --- FIX: Convert dict back to Pydantic model for return type ---
        return PlayerCreateData(**created_player_dict)
        
    except IntegrityError: 
        # Player already exists (e.g., as a shell player). Update them.
        print(f"Player {player_name_lower} already exists. Updating.")
        try:
            # --- FIX: Use correct .update() method ---
            # We use player_name_lower as the primary key
            updated_player_dict = await player_interface.update(player_name_lower, fetched_profile_dict)
            if updated_player_dict:
                 # --- FIX: Convert dict back to Pydantic model ---
                return PlayerCreateData(**updated_player_dict)
            else:
                # This should not happen if IntegrityError was raised, but as a safeguard:
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
    Fetches all players that have a full profile (joined != 0).
    """
    query = """
    SELECT * FROM player
    WHERE joined != 0;
    """
    players = await open_async_request(query, fetch_as_dict=True)
    return players


# --- Functions for PlayerStats ---

async def read_player_stats(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Reads a player's stats from the database by player_name.
    """
    stats_db_interface = DBInterface(PlayerStats)
    player_name_lower = player_name.lower()
    
    stats_list = await stats_db_interface.read(player_name=player_name_lower)
    
    if stats_list:
        return stats_list[0] # Return the first (and only) stats dict
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
        # --- UPDATED: Check for percentile OR percentage_rank ---
        f"{category}_last_percentile": last.get('percentile') or last.get('percentage_rank')
    }

async def create_and_store_player_stats(player_name: str) -> Optional[PlayerStatsCreateData]:
    """
    Fetches fresh stats from Chess.com and performs an "upsert" (insert or update)
    into the database.
    """
    player_name_lower = player_name.lower()
    
    # 1. Ensure the player exists in the Player table first.
    # We must have a Player record to satisfy the foreign key constraint.
    player = await read_player(player_name_lower)
    if not player:
        # If player doesn't exist, create a shell record (or full profile)
        print(f"Player {player_name_lower} not found. Creating shell record before adding stats.")
        player_profile = await insert_player({"player_name": player_name_lower})
        if not player_profile:
            print(f"Failed to create player {player_name_lower}. Cannot add stats.")
            return None

    # 2. Fetch the raw stats from Chess.com API
    raw_stats = await get_player_stats(player_name_lower)
    if not raw_stats:
        print(f"Failed to fetch stats from Chess.com for {player_name_lower}.")
        return None
        
    # --- Debug Print ---
    print(f"--- RAW STATS FOR {player_name_lower} ---")
    import pprint
    pprint.pprint(raw_stats)
    print("---------------------------------")
    # ---
    
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
        # Try to create a new record
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

# --- NEW FUNCTION ---
async def update_stats_for_all_primary_players(api_delay: float = 1.0) -> Dict[str, List[str]]:
    """
    Fetches stats for all primary players (joined != 0).
    Includes a delay to be respectful to the Chess.com API.
    
    Returns:
        A dictionary categorizing successful and failed player updates.
    """
    print("Starting batch job: Update stats for all primary players...")
    primary_players = await get_current_players_with_games_in_db()
    
    if not primary_players:
        print("No primary players found in the database.")
        return {"success": [], "failed": []}
        
    print(f"Found {len(primary_players)} primary players to update.")
    
    success_list = []
    failed_list = []
    
    for i, player in enumerate(primary_players):
        player_name = player.get('player_name')
        if not player_name:
            continue
            
        print(f"Updating stats for: {player_name} ({i+1}/{len(primary_players)})...")
        try:
            stats = await create_and_store_player_stats(player_name)
            if stats:
                success_list.append(player_name)
            else:
                print(f"Failed to update stats for {player_name} (API returned None).")
                failed_list.append(player_name)
        except Exception as e:
            print(f"An exception occurred while updating stats for {player_name}: {repr(e)}")
            failed_list.append(player_name)
            
        # --- API Delay ---
        # Wait before the next request, even if this one failed
        await asyncio.sleep(api_delay)
        
    print("--- Batch Stats Update Complete ---")
    print(f"Successfully updated: {len(success_list)}")
    print(f"Failed to update: {len(failed_list)}")
    if failed_list:
        print(f"Failed players: {', '.join(failed_list)}")
        
    return {"success": success_list, "failed": failed_list}