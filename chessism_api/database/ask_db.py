# chessism_api/database/ask_db.py
import os
import asyncio
import time 
from typing import List, Dict, Any, Tuple, Set, Optional
from constants import CONN_STRING
from sqlalchemy.exc import ResourceClosedError
# --- MODIFIED: Import distinct ---
from sqlalchemy import text, select, update, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession

# --- FIXED IMPORTS ---
from chessism_api.database.engine import async_engine, AsyncDBSession, init_db
# --- THIS IS THE FIX: Import the CamelCase class name ---
from chessism_api.database.models import Fen, Game, AnalysisTimes, PlayerStats, GameFenAssociation
# ---

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

async def delete_analysis_tables():
    """
    Deletes specified analysis-related tables asynchronously.
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
    print("All specified analysis tables deletion attempt complete.")


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
    
    result = await open_async_request(
        sql_query,
        params=params,
        fetch_as_dict=True
    )
    return result

async def get_top_fens_unscored(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Retrieves the top N FENs based on the highest count of n_games,
    where the score has NOT been calculated yet.
    """
    sql_query = """
        SELECT 
            fen,
            n_games,
            score
        FROM 
            fen
        WHERE
            score IS NULL
        ORDER BY 
            n_games DESC
        LIMIT :limit;
    """
    params = {"limit": limit}
    
    result = await open_async_request(
        sql_query,
        params=params,
        fetch_as_dict=True
    )
    return result

async def get_sum_n_games(threshold: int = 10) -> Optional[int]:
    """
    Calculates the sum of all n_games where n_games > threshold.
    """
    async with AsyncDBSession() as session:
        try:
            stmt = (
                select(func.sum(Fen.n_games))
                .where(Fen.n_games > threshold)
            )
            result = await session.execute(stmt)
            total_sum = result.scalar()
            return total_sum
        except Exception as e:
            print(f"Error calculating sum of n_games: {e}")
            return None

async def get_fens_for_analysis(limit: int) -> Tuple[Optional[AsyncSession], Optional[List[str]]]:
    """
    Fetches the next batch of FENs that need analysis.
    Starts a new transaction and applies a row-level lock.
    """
    session = AsyncDBSession()
    try:
        await session.begin()
        
        stmt = (
            select(Fen.fen)
            .where(Fen.score.is_(None))
            .order_by(Fen.n_games.desc())
            .limit(limit)
            .with_for_update(skip_locked=True) 
        )
        
        result = await session.execute(stmt)
        fens = result.scalars().all()
        
        if not fens:
            await session.rollback()
            await session.close()
            return None, None
            
        return session, fens

    except Exception as e:
        print(f"Error in get_fens_for_analysis: {repr(e)}", flush=True)
        await session.rollback()
        await session.close()
        return None, None

async def get_player_fens_for_analysis(
    player_name: str,
    limit: int
) -> Tuple[Optional[AsyncSession], Optional[List[str]]]:
    """
    Fetches the next batch of DISTINCT FENs for a specific player.
    Starts a new transaction and applies a row-level lock.
    
    Returns the session (which holds the lock) and the list of FENs.
    """
    session = AsyncDBSession()
    try:
        await session.begin()
        
        # --- FIX: Use a Subquery with WHERE IN ---
        
        # 1. Create a subquery to find the TOP N *distinct* FENs for the player.
        # This subquery is NOT locked.
        player_fens_subquery = (
            # --- THIS IS THE FIX: Removed distinct() ---
            select(Fen.fen)
            .join(GameFenAssociation, Fen.fen == GameFenAssociation.fen_fen)
            .join(Game, GameFenAssociation.game_link == Game.link)
            .where(
                (Game.white == player_name) | (Game.black == player_name)
            )
            .where(Fen.score.is_(None))
            .group_by(Fen.fen, Fen.n_games) # <-- Use GROUP BY
            .order_by(Fen.n_games.desc()) # <-- This is now valid
            .limit(limit)
        ).scalar_subquery() # Makes it a subquery returning a list of scalars

        # 2. Now, select from the Fen table WHERE fen is in our subquery list,
        # and apply the lock HERE.
        stmt = (
            select(Fen.fen)
            .where(Fen.fen.in_(player_fens_subquery))
            .with_for_update(skip_locked=True)
        )
        
        result = await session.execute(stmt)
        fens = result.scalars().all()
        
        if not fens:
            await session.rollback()
            await session.close()
            return None, None
            
        return session, fens

    except Exception as e:
        print(f"Error in get_player_fens_for_analysis: {repr(e)}", flush=True)
        await session.rollback()
        await session.close()
        return None, None
# --- END CORRECTION ---

async def get_player_fen_score_counts(player_name: str) -> Dict[str, int]:
    """
    Counts FENs for a specific player based on their score status.
    """
    # --- FIX: Added DISTINCT to the counts ---
    sql_query = """
        SELECT
            COUNT(DISTINCT CASE WHEN f.score = 0 THEN f.fen END) as score_zero,
            COUNT(DISTINCT CASE WHEN f.score != 0 THEN f.fen END) as score_not_zero,
            COUNT(DISTINCT CASE WHEN f.score IS NULL THEN f.fen END) as score_null
        FROM fen f
        JOIN game_fen_association gfa ON f.fen = gfa.fen_fen
        JOIN game g ON gfa.game_link = g.link
        WHERE g.white = :player OR g.black = :player;
    """
    
    async with AsyncDBSession() as session:
        result = await session.execute(text(sql_query), {"player": player_name})
        row = result.mappings().first()
        return dict(row) if row else {"score_zero": 0, "score_not_zero": 0, "score_null": 0}


# --- NEW: STATISTICAL ANALYSIS QUERIES ---

async def get_player_performance_summary(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Calculates aggregate W/L/D stats for a player as White, Black, and Combined.
    """
    sql_query = """
        SELECT
            COUNT(*) as total_games,
            
            SUM(CASE WHEN white = :player THEN 1 ELSE 0 END) as white_games,
            SUM(CASE WHEN white = :player AND white_result = 1.0 THEN 1 ELSE 0 END) as white_wins,
            SUM(CASE WHEN white = :player AND white_result = 0.0 THEN 1 ELSE 0 END) as white_losses,
            SUM(CASE WHEN white = :player AND white_result = 0.5 THEN 1 ELSE 0 END) as white_draws,
            
            SUM(CASE WHEN black = :player THEN 1 ELSE 0 END) as black_games,
            SUM(CASE WHEN black = :player AND black_result = 1.0 THEN 1 ELSE 0 END) as black_wins,
            SUM(CASE WHEN black = :player AND black_result = 0.0 THEN 1 ELSE 0 END) as black_losses,
            SUM(CASE WHEN black = :player AND black_result = 0.5 THEN 1 ELSE 0 END) as black_draws
            
        FROM game
        WHERE white = :player OR black = :player;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(sql_query), {"player": player_name})
        row = result.mappings().first()
        return dict(row) if row else None

async def get_player_game_averages(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Calculates average opponent ELO and average game length for a player.
    """
    sql_query = """
        SELECT
            AVG(n_moves) as avg_game_length,
            AVG(CASE 
                WHEN white = :player THEN black_elo
                ELSE white_elo 
            END) as avg_opponent_rating
        FROM game
        WHERE white = :player OR black = :player;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(sql_query), {"player": player_name})
        row = result.mappings().first()
        return dict(row) if row else None

async def get_player_termination_stats(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Calculates a player's result counts grouped by termination type.
    """
    sql_query = """
        SELECT
            SUM(CASE WHEN (white = :player AND white_str_result = 'checkmated') OR (black = :player AND black_str_result = 'checkmated') THEN 1 ELSE 0 END) as checkmated,
            SUM(CASE WHEN (white = :player AND white_str_result = 'resigned') OR (black = :player AND black_str_result = 'resigned') THEN 1 ELSE 0 END) as resigned,
            SUM(CASE WHEN (white = :player AND white_str_result = 'timeout') OR (black = :player AND black_str_result = 'timeout') THEN 1 ELSE 0 END) as timeout,
            SUM(CASE WHEN (white = :player AND white_str_result = 'abandoned') OR (black = :player AND black_str_result = 'abandoned') THEN 1 ELSE 0 END) as abandoned
        FROM game
        WHERE white = :player OR black = :player;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(sql_query), {"player": player_name})
        row = result.mappings().first()
        return dict(row) if row else None

async def get_player_top_openings(player_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Gets the player's most played openings by ECO, with W/L/D stats.
    """
    sql_query = """
        SELECT
            eco,
            COUNT(*) as total_games,
            SUM(CASE 
                WHEN white = :player AND white_result = 1.0 THEN 1
                WHEN black = :player AND black_result = 1.0 THEN 1
                ELSE 0 
            END) as wins,
            SUM(CASE 
                WHEN white = :player AND white_result = 0.0 THEN 1
                WHEN black = :player AND black_result = 0.0 THEN 1
                ELSE 0 
            END) as losses,
            SUM(CASE 
                WHEN white = :player AND white_result = 0.5 THEN 1
                WHEN black = :player AND black_result = 0.5 THEN 1
                ELSE 0 
            END) as draws
        FROM game
        WHERE (white = :player OR black = :player) AND eco != 'no_eco'
        GROUP BY eco
        ORDER BY total_games DESC
        LIMIT :limit;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(sql_query), {"player": player_name, "limit": limit})
        rows = result.mappings().all()
        return [dict(row) for row in rows]

# --- TODO: Add Global Stats functions (similar to above, but without the WHERE clause) ---

# --- THIS IS THE FIX ---
# --- Re-adding the missing function for the FEN pipeline ---
async def _get_remaining_fens_count_committed() -> int:
    """
    Counts how many games still have fens_done = False using a NEW session.
    This reads the last COMMITTED state of the database.
    """
    async with AsyncDBSession() as session:
        stmt = select(func.count(Game.link)).where(Game.fens_done == False)
        result = await session.execute(stmt)
        count = result.scalar()
        return count or 0
