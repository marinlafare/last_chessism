# chessism_api/database/postgresql/ask_db.py
import time
import math
from typing import List, Dict, Any, Tuple, Set, Optional
from datetime import datetime, timedelta, timezone
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy import exists, text, select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.database.postgresql.engine import async_engine, AsyncDBSession
from chessism_api.database.postgresql.models import Fen, Game, GameFenAssociation

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
                return [dict(row._mapping) for row in rows]
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


async def sync_game_analytics_projection(game_links: Optional[Tuple[int, ...]] = None) -> Dict[str, int]:
    """
    Syncs additive analytical projection tables for either all games or a specific
    set of game links. Postgres remains the source of truth.
    """
    clean_links = tuple(sorted({int(link) for link in game_links or () if link is not None}))
    from chessism_api.database.postgresql.relational_storage import storage_for
    async with AsyncDBSession() as selection_session:
        if storage_for(selection_session):
            from chessism_api.operations.relational_projection import sync_projection
            await sync_projection(selection_session,clean_links)
            await selection_session.commit()
            return {'games':len(clean_links),'scope':'links' if clean_links else 'all'}
    source_join = ""
    source_from = ""
    source_where = ""
    move_source_join = ""

    async with AsyncDBSession() as session:
        try:
            if clean_links:
                await session.execute(text("""
                    CREATE TEMPORARY TABLE IF NOT EXISTS temp_projection_game_links (
                        link BIGINT PRIMARY KEY
                    ) ON COMMIT DROP;
                """))
                for start in range(0, len(clean_links), 1000):
                    chunk = clean_links[start:start + 1000]
                    values = ", ".join(f"({link})" for link in chunk)
                    if values:
                        await session.execute(text(f"""
                            INSERT INTO temp_projection_game_links (link)
                            VALUES {values}
                            ON CONFLICT DO NOTHING;
                        """))
                source_join = "JOIN temp_projection_game_links tpl ON tpl.link = g.link"
                source_from = "FROM temp_projection_game_links tpl"
                source_where = "tpl.link = g.link AND"
                move_source_join = "JOIN temp_projection_game_links tpl ON tpl.link = m.link"

            game_columns_update = f"""
                UPDATE game g
                SET
                    mode = CASE
                        WHEN g.time_control LIKE '%/%' THEN 'daily'
                        WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                            CASE
                                WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                                WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                                WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                                ELSE 'classical'
                            END
                        ELSE 'unknown'
                    END,
                    played_at = make_timestamp(
                        g.year::int,
                        g.month::int,
                        g.day::int,
                        g.hour::int,
                        g.minute::int,
                        g.second::double precision
                    ) AT TIME ZONE 'UTC',
                    avg_elo = (g.white_elo + g.black_elo) / 2.0
                {source_from}
                WHERE {source_where} (
                    g.mode IS NULL
                    OR g.played_at IS NULL
                    OR g.avg_elo IS NULL
                );
            """
            await session.execute(text(game_columns_update))

            game_player_insert = f"""
                INSERT INTO game_player (
                    link,
                    color,
                    player_name,
                    opponent_name,
                    result,
                    rating,
                    opponent_rating,
                    mode,
                    played_at,
                    eco,
                    n_moves,
                    time_elapsed,
                    avg_elo
                )
                SELECT
                    g.link,
                    'white',
                    g.white,
                    g.black,
                    g.white_result,
                    g.white_elo,
                    g.black_elo,
                    g.mode,
                    g.played_at,
                    g.eco,
                    g.n_moves,
                    g.time_elapsed,
                    g.avg_elo
                FROM game g
                {source_join}
                UNION ALL
                SELECT
                    g.link,
                    'black',
                    g.black,
                    g.white,
                    g.black_result,
                    g.black_elo,
                    g.white_elo,
                    g.mode,
                    g.played_at,
                    g.eco,
                    g.n_moves,
                    g.time_elapsed,
                    g.avg_elo
                FROM game g
                {source_join}
                ON CONFLICT (link, color) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    opponent_name = EXCLUDED.opponent_name,
                    result = EXCLUDED.result,
                    rating = EXCLUDED.rating,
                    opponent_rating = EXCLUDED.opponent_rating,
                    mode = EXCLUDED.mode,
                    played_at = EXCLUDED.played_at,
                    eco = EXCLUDED.eco,
                    n_moves = EXCLUDED.n_moves,
                    time_elapsed = EXCLUDED.time_elapsed,
                    avg_elo = EXCLUDED.avg_elo;
            """
            await session.execute(text(game_player_insert))

            opening_insert = f"""
                WITH requested AS (
                    SELECT n_moves, n_moves * 2 AS required_half_moves
                    FROM generate_series(3, 10) AS supported(n_moves)
                ),
                opening_moves AS (
                    SELECT
                        m.link,
                        r.n_moves,
                        m.n_move,
                        m.white_move,
                        m.black_move
                    FROM moves m
                    {move_source_join}
                    JOIN requested r ON m.n_move BETWEEN 1 AND r.n_moves
                ),
                complete_games AS (
                    SELECT link, n_moves
                    FROM opening_moves
                    GROUP BY link, n_moves
                    HAVING COUNT(DISTINCT n_move) = n_moves
                ),
                ply_moves AS (
                    SELECT link, n_moves, (n_move * 2 - 1) AS ply, white_move AS san
                    FROM opening_moves
                    UNION ALL
                    SELECT link, n_moves, (n_move * 2) AS ply, black_move AS san
                    FROM opening_moves
                ),
                openings AS (
                    SELECT
                        pm.link,
                        pm.n_moves,
                        string_agg(pm.san, ' ' ORDER BY pm.ply) AS opening
                    FROM ply_moves pm
                    JOIN complete_games cg
                      ON cg.link = pm.link
                     AND cg.n_moves = pm.n_moves
                    WHERE pm.san IS NOT NULL
                      AND pm.san <> ''
                      AND pm.san <> '--'
                    GROUP BY pm.link, pm.n_moves
                    HAVING COUNT(*) = (pm.n_moves * 2)
                )
                INSERT INTO game_opening (
                    link,
                    n_moves,
                    opening,
                    mode,
                    avg_elo,
                    played_at
                )
                SELECT
                    o.link,
                    o.n_moves,
                    o.opening,
                    g.mode,
                    g.avg_elo,
                    g.played_at
                FROM openings o
                JOIN game g ON g.link = o.link
                ON CONFLICT (link, n_moves) DO UPDATE SET
                    opening = EXCLUDED.opening,
                    mode = EXCLUDED.mode,
                    avg_elo = EXCLUDED.avg_elo,
                    played_at = EXCLUDED.played_at;
            """
            await session.execute(text(opening_insert))
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {
        "games": len(clean_links),
        "scope": "links" if clean_links else "all"
    }


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
            from chessism_api.database.postgresql.relational_storage import storage_for
            layout = storage_for(session)
            if layout:
                result = await session.execute(text(f"""UPDATE {layout.table('game')} SET fens_done=false
                    WHERE white_player_id=(SELECT id FROM {layout.table('player')} WHERE player_name=:player)
                       OR black_player_id=(SELECT id FROM {layout.table('player')} WHERE player_name=:player)"""),{'player':player_name})
            else:
                result = await session.execute(stmt)
            await session.commit()
            print(f"Successfully reset 'fens_done' to False for {result.rowcount} game(s) involving player '{player_name}'.")
            return result.rowcount

        except Exception as e:
            await session.rollback()
            print(f"An error occurred while resetting 'fens_done' status for player '{player_name}': {e}")
            raise


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

            # 3. Check both playable games and zero-move tombstones. A game
            # archived without moves must not be downloaded and discarded on
            # every future player update.
            join_sql = f"""
                SELECT g.link
                FROM game AS g
                JOIN {temp_table_name} AS t ON g.link = t.link_col
                UNION
                SELECT empty_game.game_id AS link
                FROM no_moves_games AS empty_game
                JOIN {temp_table_name} AS t ON empty_game.game_id = t.link_col;
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


async def get_player_hours_played(player_name: str) -> Dict[str, Any]:
    """
    Returns total played hours and played hours split by normalized time control mode.
    Uses game.time_elapsed (seconds).
    """
    from chessism_api.operations.player_analytics_summary import read_snapshot, played_hours
    async with AsyncDBSession() as session:
        saved = await read_snapshot(session, player_name, 'hours')
    if saved is not None:
        modes = saved['modes']
        return {'player_name': player_name,
                'total_hours': played_hours(sum(row['seconds'] for row in modes.values())),
                **{f'{mode}_games': modes.get(mode, {}).get('games', 0) for mode in ('bullet','blitz','rapid')},
                **{f'{mode}_hours': played_hours(modes.get(mode, {}).get('seconds', 0)) for mode in ('bullet','blitz','rapid')},
                'summary_revision': saved['summary_revision'], 'summary_stale': saved['summary_stale']}
    query = """
        SELECT
            ROUND((COALESCE(SUM(time_elapsed), 0) / 3600.0)::numeric, 2) AS total_hours,
            COUNT(*) FILTER (WHERE mode = 'bullet') AS bullet_games,
            ROUND((COALESCE(SUM(CASE WHEN mode = 'bullet' THEN time_elapsed ELSE 0 END), 0) / 3600.0)::numeric, 2) AS bullet_hours,
            COUNT(*) FILTER (WHERE mode = 'blitz') AS blitz_games,
            ROUND((COALESCE(SUM(CASE WHEN mode = 'blitz' THEN time_elapsed ELSE 0 END), 0) / 3600.0)::numeric, 2) AS blitz_hours,
            COUNT(*) FILTER (WHERE mode = 'rapid') AS rapid_games,
            ROUND((COALESCE(SUM(CASE WHEN mode = 'rapid' THEN time_elapsed ELSE 0 END), 0) / 3600.0)::numeric, 2) AS rapid_hours
        FROM game_player
        WHERE player_name = :player;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name})
        row = result.mappings().first() or {}

    return {
        "player_name": player_name,
        "total_hours": float(row.get("total_hours") or 0.0),
        "bullet_games": int(row.get("bullet_games") or 0),
        "bullet_hours": float(row.get("bullet_hours") or 0.0),
        "blitz_games": int(row.get("blitz_games") or 0),
        "blitz_hours": float(row.get("blitz_hours") or 0.0),
        "rapid_games": int(row.get("rapid_games") or 0),
        "rapid_hours": float(row.get("rapid_hours") or 0.0)
    }
