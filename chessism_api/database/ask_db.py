# chessism_api/database/ask_db.py
import os
import asyncio
import time # <-- Added for internal timing in helpers
from typing import List, Dict, Any, Tuple, Set
from constants import CONN_STRING
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy import text, select, update, func
# --- NEW IMPORT ---
from sqlalchemy.ext.asyncio import AsyncSession

# --- FIXED IMPORTS ---
from chessism_api.database.engine import async_engine, AsyncDBSession, init_db
from chessism_api.database.models import Fen, Game, AnalysisTimes, PlayerStats
from chessism_api.database.db_interface import DBInterface
# ---

async def get_all_database_names():
    """
    Fetches the names of all databases accessible by the current connection.
    """
    if async_engine is None:
        print("Error: Database engine not initialized. Call init_db() first.")
        raise RuntimeError("Database engine not initialized. Call init_db() first.")

    dialect_name = async_engine.dialect.name
    query = ""
    
    if "postgresql" in dialect_name:
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    else:
        print(f"Warning: Database dialect '{dialect_name}' not explicitly handled.")
        return []

    print(f"Querying databases for {dialect_name} using: {query}")
    try:
        results = await open_async_request(query, fetch_as_dict=True)
        if results:
            db_names = []
            for row in results:
                if 'datname' in row:
                    db_names.append(row['datname'])
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
            if sql_upper.startswith("DROP") or \
               sql_upper.startswith("CREATE") or \
               sql_upper.startswith("ALTER") or \
               sql_upper.startswith("TRUNCATE"):
                
                print(f"DDL operation '{sql_question}' executed successfully.")
                await session.commit() # Explicitly commit DDL
                return None

            if fetch_as_dict:
                rows = result.fetchall()
                return [row._mapping for row in rows]
            else:
                return result.fetchall()
                
        except ResourceClosedError as e:
            sql_upper = sql_question.strip().upper()
            if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
                await session.commit() # Commit changes if it was a non-row-returning DML
                print(f"Non-row-returning statement executed: {sql_question}")
                return None
            print(f"Warning: Attempted to fetch rows from a non-row-returning statement: {sql_question}. Error: {e}")
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
                await session.commit() # Commit after each drop
                print(f"Successfully deleted table: {table_name_to_delete}")
            except Exception as e:
                await session.rollback()
                print(f"An unexpected error occurred during deletion of {table_name_to_delete}: {e}")
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
    Deletes the analysis_times table asynchronously.
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
    print("analysis_times table deletion attempt complete.")

async def save_analysis_times(batch_data):
    print('________')
    analysis_times_interface = DBInterface(AnalysisTimes)
    await analysis_times_interface.create(batch_data)
    print('_________')

async def get_games_already_in_db(links_to_check: Tuple[int, ...]) -> Set[int]:
    """
    Efficiently checks which game links already exist in the database
    using a temporary table for a large number of links.
    """
    if not links_to_check:
        return set()

    links_found_in_db = set()
    BATCH_SIZE = 1000
    
    async with AsyncDBSession() as session:
        start_time = time.time()
        try:
            # 1. Create a temporary table
            temp_table_name = "temp_game_links_check"
            await session.execute(text(f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table_name} (
                    link_col BIGINT PRIMARY KEY
                ) ON COMMIT DROP;
            """))

            # 2. Insert links into the temporary table in batches
            for i in range(0, len(links_to_check), BATCH_SIZE):
                batch = links_to_check[i : i + BATCH_SIZE]
                values_clause = ", ".join([f"({link})" for link in batch])
                
                if values_clause:
                    insert_sql = f"INSERT INTO {temp_table_name} (link_col) VALUES {values_clause} ON CONFLICT DO NOTHING;"
                    await session.execute(text(insert_sql))

            # 3. Join game table with the temporary table
            join_sql = f"""
                SELECT g.link 
                FROM game AS g
                JOIN {temp_table_name} AS t ON g.link = t.link_col;
            """
            result = await session.execute(text(join_sql))
            links_found_in_db.update(result.scalars().all())

            print(f"Checked {len(links_to_check)} links against DB in {time.time() - start_time:.2f}s. Found {len(links_found_in_db)} existing.")
        
        except Exception as e:
            await session.rollback()
            print(f"Error during temp table game check: {e}")
            raise
        
    return links_found_in_db

async def drop_player_stats_table():
    """
    Drops the 'player_stats' table to allow for schema recreation.
    """
    print("Attempting to drop 'player_stats' table...")
    try:
        await open_async_request("DROP TABLE IF EXISTS player_stats CASCADE;")
        print("Table 'player_stats' dropped successfully.")
    except Exception as e:
        print(f"Error dropping 'player_stats' table: {e}")

# --- NEW FUNCTION ---
async def get_top_fens(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Retrieves the top N FENs based on the highest count of n_games.
    """
    sql_query = """
        SELECT 
            fen,
            n_games,
            moves_counter,
            score
        FROM 
            fen
        ORDER BY 
            n_games DESC
        LIMIT :limit;
    """
    params = {"limit": limit}
    
    # Use open_async_request to execute the query
    result = await open_async_request(
        sql_query,
        params=params,
        fetch_as_dict=True
    )
    return result

# --- NEW FUNCTION ---
async def get_sum_n_games(threshold: int = 10) -> int:
    """
    Calculates the sum of all n_games in the Fen table where
    n_games is greater than the specified threshold.
    """
    async with AsyncDBSession() as session:
        try:
            stmt = (
                select(func.sum(Fen.n_games))
                .where(Fen.n_games > threshold)
            )
            result = await session.execute(stmt)
            total_sum = result.scalar()
            
            if total_sum is None:
                return 0
            # The sum might be a float or decimal, so cast to int.
            return int(total_sum)
            
        except Exception as e:
            print(f"Error calculating sum of n_games: {e}")
            return 0 # Return 0 on error

# --- NEW FUNCTION (Req 2 & 6) ---
async def get_fens_for_analysis(session: AsyncSession, limit: int) -> List[str]:
    """
    Fetches a batch of FENs that have not been analyzed (score IS NULL).
    It prioritizes by n_games (Req 2) and uses FOR UPDATE SKIP LOCKED
    to prevent race conditions between parallel workers (Solution 2).
    """
    try:
        # Use raw SQL for FOR UPDATE SKIP LOCKED
        sql_query = text("""
            SELECT fen
            FROM fen
            WHERE score IS NULL
            ORDER BY n_games DESC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        """)
        
        result = await session.execute(sql_query, {"limit": limit})
        fens = result.scalars().all()
        return fens
        
    except Exception as e:
        print(f"Error fetching FENs for analysis: {e}")
        # We must re-raise to trigger the rollback in the calling function
        raise

# --- NEW FUNCTION (Req 5) ---
async def get_player_fens_for_analysis(
    session: AsyncSession, 
    player_name: str, 
    limit: int
) -> List[str]:
    """
    Fetches a batch of FENs associated with a specific player
    that have not been analyzed (score IS NULL).
    Uses FOR UPDATE SKIP LOCKED to prevent race conditions.
    """
    try:
        # Use raw SQL for the JOIN and FOR UPDATE SKIP LOCKED
        sql_query = text("""
            SELECT f.fen
            FROM fen AS f
            JOIN game_fen_association AS gfa ON f.fen = gfa.fen_fen
            JOIN game AS g ON gfa.game_link = g.link
            WHERE 
                (g.white = :player_name OR g.black = :player_name)
                AND f.score IS NULL
            ORDER BY f.n_games DESC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        """)
        
        result = await session.execute(
            sql_query, 
            {"player_name": player_name, "limit": limit}
        )
        fens = result.scalars().all()
        return fens
        
    except Exception as e:
        print(f"Error fetching player FENs for analysis: {e}")
        # Re-raise to trigger rollback
        raise