# chessism_api/operations/check_player_in_db.py

import time
from typing import Set, List
import asyncio
from math import ceil

from sqlalchemy import Column, String, text, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base

# --- FIXED IMPORTS ---
from chessism_api.operations.models import PlayerCreateData
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.models import Player
# ---

# --- TEMPORARY TABLE MODEL ---
TempBase = declarative_base()

class TempPlayerName(TempBase):
    """
    SQLAlchemy model for a temporary table to hold player names for bulk lookups.
    """
    __tablename__ = "temp_player_names_check" # Use a distinct name
    # ON COMMIT DROP is good, but means everything must be in one transaction.
    __table_args__ = {'prefixes': ['TEMPORARY']}

    player_name_col = Column(String, primary_key=True)


# --- ASYNC FUNCTION TO GET ONLY NEW PLAYERS ---
async def get_only_players_not_in_db(player_names: Set[str]) -> Set[str]:
    """
    Identifies which player names from the input set do not yet exist in the Player table.
    Uses a temporary table to efficiently handle large numbers of player names.
    """
    player_interface = DBInterface(Player)
    if not player_names:
        print("No player names provided for lookup.")
        return set()

    player_names_list = list(player_names)
    INSERT_VALUES_BATCH_SIZE = 1000
    players_found_in_db = set()

    # --- FIX: Use .get_session() from the interface ---
    async with player_interface.get_session() as session:
        start_temp_table_ops = time.time()
        print(f"[{time.time()-start_temp_table_ops:.2f}s] Starting temp table operations for {len(player_names)} players...")

        try:
            # Step 1: Create the temporary table.
            # Use a unique table name to avoid conflicts if this runs concurrently.
            temp_table_name = "temp_player_names_check"
            await session.execute(text(f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table_name} (
                    player_name_col VARCHAR PRIMARY KEY
                ) ON COMMIT DROP;
            """))
            print(f"[{time.time()-start_temp_table_ops:.2f}s] Temporary table created.")

            # Step 2: Insert player names into the temporary table in batches.
            for i in range(0, len(player_names_list), INSERT_VALUES_BATCH_SIZE):
                batch = player_names_list[i : i + INSERT_VALUES_BATCH_SIZE]

                # --- SYNTAX ERROR FIX ---
                # Rewritten to avoid the complex f-string list comprehension
                # that was causing the SyntaxError.
                values_to_insert: List[str] = []
                for name in batch:
                    # Escape single quotes for SQL
                    safe_name = name.replace("'", "''")
                    values_to_insert.append(f"('{safe_name}')")
                
                values_clause = ", ".join(values_to_insert)
                # --- END FIX ---
                
                if values_clause: # Ensure batch wasn't empty
                    insert_sql = f"INSERT INTO {temp_table_name} (player_name_col) VALUES {values_clause} ON CONFLICT DO NOTHING;"
                    await session.execute(text(insert_sql))

            print(f"[{time.time()-start_temp_table_ops:.2f}s] All {len(player_names_list)} player names inserted into temp table.")

            # Step 3: Query the main player table by joining with the temporary table.
            start_join_query = time.time()
            print(f"[{time.time()-start_temp_table_ops:.2f}s] Performing JOIN query to find existing players...")
            
            # Use raw SQL for the join as the temp table is not in SQLAlchemy metadata
            join_sql = f"""
                SELECT p.player_name 
                FROM player AS p
                JOIN {temp_table_name} AS t ON p.player_name = t.player_name_col;
            """
            
            result = await session.execute(text(join_sql))
            players_found_in_db.update(result.scalars().all())
            print(f"[{time.time()-start_temp_table_ops:.2f}s] JOIN query completed in {time.time()-start_join_query:.2f}s.")
        
        except Exception as e:
            await session.rollback()
            print(f"Error during temp table player check: {e}")
            raise # Re-raise the exception
        
        # Session auto-commits here, and 'ON COMMIT DROP' cleans up the temp table.

    print(f"Total time for get_only_players_not_in_db: {time.time()-start_temp_table_ops:.2f} seconds")
    return player_names - players_found_in_db