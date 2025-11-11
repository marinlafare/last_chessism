# chessism_api/database/ask_db.py


import os
import requests
import tempfile
from itertools import chain
from typing import List, Dict, Any, Tuple, Set # <-- ADDED Tuple, Set
import asyncio 
from sqlalchemy.exc import ResourceClosedError

# --- FIXED IMPORTS ---
from constants import CONN_STRING
from sqlalchemy import text, select, update
from chessism_api.database.engine import async_engine, AsyncDBSession
from chessism_api.database.models import Fen, Game, AnalysisTimes
from chessism_api.database.db_interface import DBInterface
from chessism_api.database.engine import init_db
# ---

async def get_all_database_names():
    """
    Fetches the names of all databases accessible by the current connection.
    """
    # --- FIX: Removed redundant init_db() call ---
    if async_engine is None:
        raise RuntimeError("Database engine not initialized. Call init_db() at startup.")

    dialect_name = async_engine.dialect.name

    query = ""
    if "postgresql" in dialect_name:
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    elif "mysql" in dialect_name:
        query = "SHOW DATABASES;"
    elif "sqlite" in dialect_name:
        query = "PRAGMA database_list;"
    elif "mssql" in dialect_name: # SQL Server
        query = "SELECT name FROM sys.databases;"
    else:
        print(f"Warning: Database dialect '{dialect_name}' not explicitly handled for listing databases.")
        return []

    print(f"Querying databases for {dialect_name} using: {query}")
    try:
        results = await open_async_request(query, fetch_as_dict=True)
        if results:
            db_names = []
            for row in results:
                if "sqlite" in dialect_name:
                    db_names.append(row['name'])
                elif 'datname' in row:
                    db_names.append(row['datname'])
                elif 'name' in row:
                    db_names.append(row['name'])
                elif 'Database' in row:
                    db_names.append(row['Database'])
            return db_names
        return []
    except Exception as e:
        print(f"Error fetching database names: {e}")
        return []

async def open_async_request(sql_question: str,
                             params: dict = None,
                             fetch_as_dict: bool = False):
    """
    Executes an asynchronous SQL query, optionally with parameters, and fetches results.
    Uses AsyncDBSession for connection management.
    """
    async with AsyncDBSession() as session:
        try:
            if params:
                result = await session.execute(text(sql_question), params)
            else:
                result = await session.execute(text(sql_question))

            sql_upper = sql_question.strip().upper()
            if sql_upper.startswith("DROP TABLE") or \
               sql_upper.startswith("CREATE TABLE") or \
               sql_upper.startswith("ALTER TABLE") or \
               sql_upper.startswith("TRUNCATE TABLE"):
                
                print(f"DDL operation '{sql_question}' executed successfully (no rows returned).")
                # DDL often auto-commits, but we commit the session state
                await session.commit()
                return None

            if fetch_as_dict:
                rows = result.fetchall()
                return [row._mapping for row in rows]
            else:
                return result.fetchall()
                
        except ResourceClosedError as e:
            # This can happen for DML (INSERT, UPDATE) that doesn't return rows
            print(f"DML operation '{sql_question}' executed (no rows returned).")
            await session.commit() # Commit DML
            return None
        except Exception as e:
            await session.rollback() # Ensure rollback on error
            print(f"Error in open_async_request: {e}")
            raise

async def delete_all_leela_tables():
    """
    Deletes specified Leela-related tables asynchronously.
    """
    async with AsyncDBSession() as session:
        for table_name_to_delete in ['fen','game_fen_association']:
            print(f"Deleting table: {table_name_to_delete}...")
            try:
                await session.execute(text(f"DROP TABLE IF EXISTS \"{table_name_to_delete}\" CASCADE;"))
                print(f"Successfully deleted table: {table_name_to_delete}")
            except Exception as e:
                await session.rollback()
                print(f"An unexpected error occurred during deletion of {table_name_to_delete}: {e}")
        await session.commit()
    print("All specified Leela tables deletion attempt complete.")

async def get_players_with_names() -> List[Dict[str, Any]]:
    """
    Retrieves all player records where the 'name' column is not NULL,
    returning only their player_name.
    """
    sql_query = """
        SELECT
            player_name
        FROM
            player
        WHERE
            name IS NOT NULL;
    """
    result = await open_async_request(
        sql_query,
        fetch_as_dict=True
    )
    return result

async def reset_player_game_fens_done_to_false(player_name: str) -> int:
    """
    Resets the 'fens_done' column to False for all Game records associated with a specific player.
    """
    if not player_name:
        print("Player name cannot be empty. No games will be reset.")
        return 0

    async with AsyncDBSession() as session:
        try:
            stmt = (
                update(Game)
                .where(
                    (Game.white == player_name) | (Game.black == player_name)
                )
                .values(fens_done=False)
            )
            result = await session.execute(stmt)
            await session.commit()
            print(f"Successfully reset 'fens_done' to False for {result.rowcount} game(s) involving player '{player_name}'.")
            return result.rowcount

        except Exception as e:
            await session.rollback()
            print(f"An error occurred while resetting 'fens_done' status for player '{player_name}': {e}")
            raise

async def delete_analysis_times():
    """
    Deletes the 'analysis_times' table asynchronously.
    """
    async with AsyncDBSession() as session:
        print(f"Deleting table: analysis_times ...")
        try:
            await session.execute(text(f"DROP TABLE IF EXISTS analysis_times CASCADE;"))
            await session.commit()
            print(f"Successfully deleted table: analysis_times")
        except Exception as e:
            await session.rollback()
            print(f"An unexpected error occurred during deletion of analysis_times: {e}")

async def save_analysis_times(batch_data):
    print('________')
    analysis_times_interface = DBInterface(AnalysisTimes)
    await analysis_times_interface.create(batch_data)
    print('_________')

# --- NEW FUNCTION: get_games_already_in_db ---
async def get_games_already_in_db(game_links: Tuple[int, ...]) -> Set[int]:
    """
    Checks a tuple of game links against the DB and returns a set of links that already exist.
    Uses a temporary table for efficiency.
    """
    game_interface = DBInterface(Game)
    if not game_links:
        return set()

    links_list = list(game_links)
    INSERT_VALUES_BATCH_SIZE = 1000
    links_found_in_db = set()

    async with game_interface.get_session() as session:
        try:
            # Step 1: Create the temporary table.
            await session.execute(text("""
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_game_links (
                    link_col BIGINT PRIMARY KEY
                ) ON COMMIT DROP;
            """))

            # Step 2: Insert links into the temporary table in batches.
            for i in range(0, len(links_list), INSERT_VALUES_BATCH_SIZE):
                batch = links_list[i : i + INSERT_VALUES_BATCH_SIZE]
                # Create a clause like '(123), (456), (789)'
                values_clause = ", ".join([f"({link})" for link in batch])
                insert_sql = f"INSERT INTO temp_game_links (link_col) VALUES {values_clause} ON CONFLICT DO NOTHING;"
                await session.execute(text(insert_sql))

            # Step 3: Query the main game table by joining with the temporary table.
            join_sql = """
            SELECT g.link 
            FROM game AS g
            JOIN temp_game_links AS t ON g.link = t.link_col;
            """
            result = await session.execute(text(join_sql))
            links_found_in_db.update(result.scalars().all())
        
        except Exception as e:
            await session.rollback()
            print(f"Error during temp table game link check: {e}")
            raise # Re-raise the exception
    
    return links_found_in_db