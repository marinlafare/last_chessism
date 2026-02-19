# chessism_api/database/ask_db.py
import os
import asyncio
import time
import math
from typing import List, Dict, Any, Tuple, Set, Optional
from datetime import datetime, timedelta
from constants import CONN_STRING
from sqlalchemy.exc import ResourceClosedError
# --- MODIFIED: Import distinct ---
from sqlalchemy import text, select, update, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession

# --- FIXED IMPORTS ---
from chessism_api.database.engine import async_engine, AsyncDBSession, init_db
# --- THIS IS THE FIX: Import the CamelCase class name ---
from chessism_api.database.models import Fen, Game, PlayerStats, GameFenAssociation
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

async def get_games_database_generalities() -> Dict[str, int]:
    """
    Returns core database generalities for the games dashboard:
    - total games in DB
    - players with joined set (main characters)
    - players with joined missing/zero (secondary characters)
    - total positions (fens)
    - scored fens (score != 0)
    """
    sql_query = """
        SELECT
            (SELECT COUNT(*) FROM game) AS n_games_in_db,
            (SELECT COUNT(*) FROM player WHERE joined IS NOT NULL AND joined <> 0) AS main_characters,
            (SELECT COUNT(*) FROM player WHERE joined IS NULL OR joined = 0) AS secondary_characters,
            (SELECT COUNT(*) FROM fen) AS n_positions,
            (SELECT COUNT(*) FROM fen WHERE score IS NOT NULL AND score <> 0) AS scored_fens;
    """
    rows = await open_async_request(sql_query, fetch_as_dict=True)
    if not rows:
        return {
            "n_games_in_db": 0,
            "main_characters": 0,
            "secondary_characters": 0,
            "n_positions": 0,
            "scored_fens": 0
        }

    row = rows[0]
    return {
        "n_games_in_db": int(row.get("n_games_in_db") or 0),
        "main_characters": int(row.get("main_characters") or 0),
        "secondary_characters": int(row.get("secondary_characters") or 0),
        "n_positions": int(row.get("n_positions") or 0),
        "scored_fens": int(row.get("scored_fens") or 0)
    }


async def get_time_control_mode_counts() -> Dict[str, int]:
    """
    Returns normalized game counts for bullet, blitz and rapid.
    """
    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """
    query = f"""
        SELECT mode, COUNT(*)::int AS total
        FROM (
            SELECT {mode_sql} AS mode
            FROM game g
        ) categorized
        WHERE mode IN ('bullet', 'blitz', 'rapid')
        GROUP BY mode;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        rows = result.mappings().all()

    counts = {"bullet": 0, "blitz": 0, "rapid": 0}
    for row in rows:
        mode = str(row.get("mode") or "")
        if mode in counts:
            counts[mode] = int(row.get("total") or 0)

    return counts


async def get_main_character_time_control_counts() -> Dict[str, int]:
    """
    Returns normalized game counts for bullet/blitz/rapid where at least one
    main character (joined != 0 and not NULL) is present.
    """
    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        WITH main_players AS (
            SELECT p.player_name
            FROM player p
            WHERE p.joined IS NOT NULL
              AND p.joined <> 0
        )
        SELECT
            mode,
            COUNT(*)::int AS total
        FROM (
            SELECT
                {mode_sql} AS mode,
                g.white,
                g.black
            FROM game g
        ) classified
        WHERE mode IN ('bullet', 'blitz', 'rapid')
          AND (
                white IN (SELECT player_name FROM main_players)
                OR black IN (SELECT player_name FROM main_players)
          )
        GROUP BY mode;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        rows = result.mappings().all()

    counts = {"bullet": 0, "blitz": 0, "rapid": 0}
    for row in rows:
        mode = str(row.get("mode") or "")
        if mode in counts:
            counts[mode] = int(row.get("total") or 0)

    return counts


async def get_top_main_characters_by_time_control(
    time_control: str,
    limit: int = 5
) -> Dict[str, Any]:
    """
    Returns top main characters for one normalized time control.
    Ranking is based on number of participations in that time control.
    """
    target_mode = (time_control or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"time_control": target_mode, "players": [], "limit": 0}

    # Keep a high cap so the bubble chart can include the full mode population.
    safe_limit = max(1, min(int(limit), 5000))

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        WITH filtered_games AS (
            SELECT
                g.white,
                g.black,
                g.white_elo,
                g.black_elo,
                g.white_result,
                g.black_result,
                make_timestamp(
                    g.year::int,
                    g.month::int,
                    g.day::int,
                    g.hour::int,
                    g.minute::int,
                    g.second::double precision
                ) AS played_at
            FROM game g
            WHERE {mode_sql} = :mode
        ),
        player_rows AS (
            SELECT
                fg.white AS player_name,
                fg.white_elo::int AS rating,
                fg.white_result AS result,
                'white'::text AS color,
                fg.played_at
            FROM filtered_games fg
            UNION ALL
            SELECT
                fg.black AS player_name,
                fg.black_elo::int AS rating,
                fg.black_result AS result,
                'black'::text AS color,
                fg.played_at
            FROM filtered_games fg
        ),
        main_player_rows AS (
            SELECT pr.*
            FROM player_rows pr
            JOIN player p ON p.player_name = pr.player_name
            WHERE p.joined IS NOT NULL
              AND p.joined <> 0
        ),
        aggregated AS (
            SELECT
                player_name,
                COUNT(*)::int AS n_games,
                ROUND(AVG(rating))::int AS avg_game_rating,
                SUM(CASE WHEN result = 1.0 THEN 1 ELSE 0 END)::int AS wins,
                SUM(CASE WHEN result = 0.5 THEN 1 ELSE 0 END)::int AS draws,
                SUM(CASE WHEN result = 0.0 THEN 1 ELSE 0 END)::int AS losses,
                SUM(CASE WHEN color = 'white' THEN 1 ELSE 0 END)::int AS as_white,
                SUM(CASE WHEN color = 'black' THEN 1 ELSE 0 END)::int AS as_black
            FROM main_player_rows
            GROUP BY player_name
        ),
        latest_per_player AS (
            SELECT DISTINCT ON (player_name)
                player_name,
                rating::int AS last_game_rating,
                played_at AS last_game_at
            FROM main_player_rows
            ORDER BY player_name, played_at DESC
        )
        SELECT
            a.player_name,
            a.n_games,
            COALESCE(lp.last_game_rating, a.avg_game_rating)::int AS rating,
            a.avg_game_rating::int AS avg_game_rating,
            lp.last_game_rating::int AS last_rating,
            a.wins,
            a.draws,
            a.losses,
            a.as_white,
            a.as_black,
            TO_CHAR(lp.last_game_at, 'YYYY-MON-DD') AS last_game_date,
            p.name AS full_name,
            p.avatar,
            p.url AS profile_url
        FROM aggregated a
        LEFT JOIN latest_per_player lp ON lp.player_name = a.player_name
        LEFT JOIN player p ON p.player_name = a.player_name
        ORDER BY a.n_games DESC, rating DESC, a.player_name ASC
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"mode": target_mode, "limit": safe_limit})
        rows = result.mappings().all()

    players = []
    for row in rows:
        wins = int(row.get("wins") or 0)
        draws = int(row.get("draws") or 0)
        losses = int(row.get("losses") or 0)
        total = wins + draws + losses
        score_rate = ((wins + 0.5 * draws) / total) if total > 0 else 0.0

        players.append({
            "player_name": str(row.get("player_name") or ""),
            "full_name": str(row.get("full_name") or ""),
            "avatar": str(row.get("avatar") or ""),
            "profile_url": str(row.get("profile_url") or ""),
            "rating": int(row.get("rating") or 0),
            "last_rating": int(row.get("last_rating") or 0),
            "avg_game_rating": int(row.get("avg_game_rating") or 0),
            "last_game_date": str(row.get("last_game_date") or ""),
            "n_games": int(row.get("n_games") or 0),
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "as_white": int(row.get("as_white") or 0),
            "as_black": int(row.get("as_black") or 0),
            "score_rate": round(score_rate, 4)
        })

    return {
        "time_control": target_mode,
        "limit": safe_limit,
        "players": players
    }


def _weighted_sse(
    prefix_w: List[float],
    prefix_wx: List[float],
    prefix_wx2: List[float],
    start_idx: int,
    end_idx: int
) -> float:
    """
    Weighted sum of squared errors for 1-based inclusive range [start_idx, end_idx].
    """
    total_w = prefix_w[end_idx] - prefix_w[start_idx - 1]
    if total_w <= 0:
        return 0.0
    total_wx = prefix_wx[end_idx] - prefix_wx[start_idx - 1]
    total_wx2 = prefix_wx2[end_idx] - prefix_wx2[start_idx - 1]
    return float(total_wx2 - ((total_wx * total_wx) / total_w))


def _weighted_jenks_breaks(values: List[int], weights: List[int], n_classes: int = 3) -> List[Tuple[int, int]]:
    """
    Weighted Jenks natural breaks for histogram data (unique sorted values + counts).
    Returns contiguous class ranges as [(min, max), ...] with n_classes entries.
    """
    if not values or not weights or len(values) != len(weights):
        return []

    n = len(values)
    k = max(1, int(n_classes))
    if n == 1:
        return [(values[0], values[0]) for _ in range(k)]

    if n < k:
        out = [(values[idx], values[idx]) for idx in range(n)]
        last = out[-1]
        while len(out) < k:
            out.append(last)
        return out

    prefix_w = [0.0] * (n + 1)
    prefix_wx = [0.0] * (n + 1)
    prefix_wx2 = [0.0] * (n + 1)

    for idx in range(1, n + 1):
        w = float(weights[idx - 1])
        v = float(values[idx - 1])
        prefix_w[idx] = prefix_w[idx - 1] + w
        prefix_wx[idx] = prefix_wx[idx - 1] + (w * v)
        prefix_wx2[idx] = prefix_wx2[idx - 1] + (w * v * v)

    inf = float("inf")
    dp = [[inf] * (n + 1) for _ in range(k + 1)]
    back = [[0] * (n + 1) for _ in range(k + 1)]

    for i in range(1, n + 1):
        dp[1][i] = _weighted_sse(prefix_w, prefix_wx, prefix_wx2, 1, i)

    for cls in range(2, k + 1):
        for i in range(cls, n + 1):
            best_cost = inf
            best_start = cls
            for start in range(cls, i + 1):
                cost = dp[cls - 1][start - 1] + _weighted_sse(prefix_w, prefix_wx, prefix_wx2, start, i)
                if cost < best_cost:
                    best_cost = cost
                    best_start = start
            dp[cls][i] = best_cost
            back[cls][i] = best_start

    ranges: List[Tuple[int, int]] = []
    end = n
    for cls in range(k, 0, -1):
        if cls == 1:
            start = 1
        else:
            start = back[cls][end]
            if start <= 0:
                start = cls
        ranges.append((values[start - 1], values[end - 1]))
        end = start - 1

    ranges.reverse()
    if len(ranges) < k:
        last = ranges[-1]
        while len(ranges) < k:
            ranges.append(last)
    return ranges


async def get_rating_time_control_chart(time_control: str) -> Dict[str, Any]:
    """
    Returns histogram-ready rating data for a normalized time control mode.
    x: sorted ratings, y: frequency per rating.
    """
    target_mode = (time_control or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"x": [], "y": [], "time_control": target_mode}

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        WITH filtered_games AS (
            SELECT
                g.white_elo,
                g.black_elo,
                {mode_sql} AS mode
            FROM game g
            WHERE g.white_elo IS NOT NULL
              AND g.black_elo IS NOT NULL
        ),
        rating_rows AS (
            SELECT white_elo::int AS rating, mode
            FROM filtered_games
            UNION ALL
            SELECT black_elo::int AS rating, mode
            FROM filtered_games
        )
        SELECT
            rating,
            COUNT(*)::int AS appearances
        FROM rating_rows
        WHERE mode = :mode
        GROUP BY rating
        ORDER BY rating ASC;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"mode": target_mode})
        rows = result.mappings().all()

    x_values = [int(row.get("rating") or 0) for row in rows]
    y_values = [int(row.get("appearances") or 0) for row in rows]
    jenks_ranges = _weighted_jenks_breaks(x_values, y_values, n_classes=3)

    bins = {
        "jenks_1": {"min_rating": 0, "max_rating": 0},
        "jenks_2": {"min_rating": 0, "max_rating": 0},
        "jenks_3": {"min_rating": 0, "max_rating": 0}
    }
    for idx, rating_range in enumerate(jenks_ranges[:3], start=1):
        bins[f"jenks_{idx}"] = {
            "min_rating": int(rating_range[0]),
            "max_rating": int(rating_range[1])
        }

    return {
        "x": x_values,
        "y": y_values,
        "time_control": target_mode,
        "bins": bins
    }


async def get_time_control_top_moves(
    mode: str,
    move_color: str = "white",
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    page: int = 1,
    page_size: int = 5,
    max_move: int = 10
) -> Dict[str, Any]:
    """
    Returns paginated most played moves by move number for a mode.
    The backend computes one top move per move number and paginates those rows.
    """
    target_mode = (mode or "").strip().lower()
    target_color = (move_color or "white").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "move_color": target_color,
            "page": 1,
            "page_size": 0,
            "total": 0,
            "total_pages": 0,
            "rows": []
        }
    if target_color not in {"white", "black"}:
        target_color = "white"
    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating

    safe_max_move = max(1, min(int(max_move), 30))
    safe_page_size = max(1, min(int(page_size), 10))
    safe_page = max(1, int(page))
    total_rows = safe_max_move
    total_pages = (total_rows + safe_page_size - 1) // safe_page_size if total_rows else 0
    if total_pages > 0 and safe_page > total_pages:
        safe_page = total_pages
    offset = (safe_page - 1) * safe_page_size

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        WITH filtered_moves AS (
            SELECT
                m.n_move,
                CASE
                    WHEN :move_color = 'black' THEN m.black_move
                    ELSE m.white_move
                END AS move
            FROM moves m
            JOIN game g ON g.link = m.link
            WHERE {mode_sql} = :mode
              AND m.n_move BETWEEN 1 AND :max_move
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        CASE
                            WHEN :move_color = 'black' THEN g.black_elo
                            ELSE g.white_elo
                        END
                    ) BETWEEN CAST(:min_rating AS INTEGER) AND CAST(:max_rating AS INTEGER)
              )
        ),
        all_moves AS (
            SELECT n_move AS move_number, move
            FROM filtered_moves
        ),
        clean_moves AS (
            SELECT move_number, move
            FROM all_moves
            WHERE move IS NOT NULL
              AND move <> ''
              AND move <> '--'
        ),
        ranked AS (
            SELECT
                move_number,
                move,
                COUNT(*)::int AS times_played,
                ROW_NUMBER() OVER (
                    PARTITION BY move_number
                    ORDER BY COUNT(*) DESC, move ASC
                ) AS rank_in_move
            FROM clean_moves
            GROUP BY move_number, move
        ),
        top_by_move AS (
            SELECT move_number, move, times_played
            FROM ranked
            WHERE rank_in_move = 1
        ),
        filled AS (
            SELECT
                gs AS move_number,
                COALESCE(t.move, '-') AS move,
                COALESCE(t.times_played, 0)::int AS times_played
            FROM generate_series(1, :max_move) AS gs
            LEFT JOIN top_by_move t ON t.move_number = gs
            ORDER BY gs
        )
        SELECT move_number, move, times_played
        FROM filled
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {
            "mode": target_mode,
            "move_color": target_color,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating,
            "max_move": safe_max_move,
            "offset": offset,
            "limit": safe_page_size
        })
        rows = result.mappings().all()

    output_rows = [
        {
            "move_number": int(row.get("move_number") or 0),
            "move": str(row.get("move") or "-"),
            "times_played": int(row.get("times_played") or 0)
        }
        for row in rows
    ]

    return {
        "mode": target_mode,
        "move_color": target_color,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total_rows,
        "total_pages": total_pages,
        "rows": output_rows
    }


async def get_time_control_top_openings(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    page: int = 1,
    page_size: int = 5,
    n_moves: int = 3
) -> Dict[str, Any]:
    """
    Returns paginated top openings for a normalized mode using move sequences.
    Opening sequence is built from exactly the first n_moves full moves in each game.
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "page": 1,
            "page_size": 0,
            "total": 0,
            "total_pages": 0,
            "rows": []
        }

    safe_page_size = max(1, min(int(page_size), 25))
    safe_page = max(1, int(page))
    safe_n_moves = max(3, min(int(n_moves), 10))
    required_half_moves = safe_n_moves * 2
    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    count_query = f"""
        WITH filtered_games AS (
            SELECT g.link
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        g.white_elo IS NOT NULL
                        AND g.black_elo IS NOT NULL
                        AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                    )
              )
        ),
        opening_moves AS (
            SELECT m.link, m.n_move, m.white_move, m.black_move
            FROM moves m
            JOIN filtered_games fg ON fg.link = m.link
            WHERE m.n_move BETWEEN 1 AND :n_moves
        ),
        complete_games AS (
            SELECT link
            FROM opening_moves
            GROUP BY link
            HAVING COUNT(DISTINCT n_move) = :n_moves
        ),
        ply_moves AS (
            SELECT link, (n_move * 2 - 1) AS ply, white_move AS san FROM opening_moves
            UNION ALL
            SELECT link, (n_move * 2) AS ply, black_move AS san FROM opening_moves
        ),
        clean_ply AS (
            SELECT link, ply, san
            FROM ply_moves
            WHERE san IS NOT NULL
              AND san <> ''
              AND san <> '--'
        ),
        openings AS (
            SELECT link, string_agg(san, ' ' ORDER BY ply) AS opening
            FROM clean_ply
            WHERE link IN (SELECT link FROM complete_games)
            GROUP BY link
            HAVING COUNT(*) = :required_half_moves
        ),
        aggregated AS (
            SELECT opening
            FROM openings
            WHERE opening IS NOT NULL
              AND opening <> ''
            GROUP BY opening
        )
        SELECT COUNT(*)::int AS total
        FROM aggregated;
    """

    data_query = f"""
        WITH filtered_games AS (
            SELECT g.link
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        g.white_elo IS NOT NULL
                        AND g.black_elo IS NOT NULL
                        AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                    )
              )
        ),
        opening_moves AS (
            SELECT m.link, m.n_move, m.white_move, m.black_move
            FROM moves m
            JOIN filtered_games fg ON fg.link = m.link
            WHERE m.n_move BETWEEN 1 AND :n_moves
        ),
        complete_games AS (
            SELECT link
            FROM opening_moves
            GROUP BY link
            HAVING COUNT(DISTINCT n_move) = :n_moves
        ),
        ply_moves AS (
            SELECT link, (n_move * 2 - 1) AS ply, white_move AS san FROM opening_moves
            UNION ALL
            SELECT link, (n_move * 2) AS ply, black_move AS san FROM opening_moves
        ),
        clean_ply AS (
            SELECT link, ply, san
            FROM ply_moves
            WHERE san IS NOT NULL
              AND san <> ''
              AND san <> '--'
        ),
        openings AS (
            SELECT link, string_agg(san, ' ' ORDER BY ply) AS opening
            FROM clean_ply
            WHERE link IN (SELECT link FROM complete_games)
            GROUP BY link
            HAVING COUNT(*) = :required_half_moves
        )
        SELECT
            opening,
            COUNT(*)::int AS times_played,
            ROUND(AVG((g.white_elo + g.black_elo) / 2.0))::int AS mean_rating_for_this_opening
        FROM openings o
        JOIN game g ON g.link = o.link
        WHERE opening IS NOT NULL
          AND opening <> ''
        GROUP BY opening
        ORDER BY times_played DESC, opening ASC
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query), {
            "mode": target_mode,
            "n_moves": safe_n_moves,
            "required_half_moves": required_half_moves,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating
        })
        count_row = count_result.mappings().first()
        total = int((count_row or {}).get("total") or 0)

        total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0
        if total_pages > 0 and safe_page > total_pages:
            safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size

        result = await session.execute(text(data_query), {
            "mode": target_mode,
            "n_moves": safe_n_moves,
            "required_half_moves": required_half_moves,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating,
            "offset": offset,
            "limit": safe_page_size
        })
        rows = result.mappings().all()

    total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0

    move_words = {
        1: "one",
        2: "two",
        3: "three",
        4: "four",
        5: "five",
        6: "six",
        7: "seven",
        8: "eight",
        9: "nine",
        10: "ten",
        11: "eleven",
        12: "twelve",
        13: "thirteen",
        14: "fourteen",
        15: "fifteen",
        16: "sixteen",
        17: "seventeen",
        18: "eighteen",
        19: "nineteen",
        20: "twenty"
    }

    openings_map: Dict[str, Dict[str, str]] = {}
    output_rows = []

    for idx, row in enumerate(rows, start=1):
        opening_text = str(row.get("opening") or "").strip()
        half_moves = [token for token in opening_text.split() if token]
        half_moves = half_moves[:required_half_moves]
        total_full_moves = safe_n_moves
        moves_payload: Dict[str, str] = {}
        mean_rating_for_this_opening = int(row.get("mean_rating_for_this_opening") or 0)

        for move_number in range(1, total_full_moves + 1):
            key_suffix = move_words.get(move_number, str(move_number))
            move_key = f"move_{key_suffix}"
            white_idx = (move_number - 1) * 2
            black_idx = white_idx + 1
            white_move = half_moves[white_idx] if white_idx < len(half_moves) else "--"
            black_move = half_moves[black_idx] if black_idx < len(half_moves) else "--"
            moves_payload[move_key] = f"{white_move},{black_move}"

        opening_key = f"most_common_opening_for_this_time_control_{idx}"
        n_games_for_this_opening = int(row.get("times_played") or 0)
        openings_map[opening_key] = {
            "mean_rating_for_this_opening": mean_rating_for_this_opening,
            "n_games_for_this_opening": n_games_for_this_opening,
            **moves_payload
        }
        output_rows.append({
            "top": idx,
            "key": opening_key,
            "moves": moves_payload,
            "half_moves": half_moves,
            "times_played": n_games_for_this_opening,
            "n_games_for_this_opening": n_games_for_this_opening,
            "mean_rating_for_this_opening": mean_rating_for_this_opening
        })

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "n_moves": safe_n_moves,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": total_pages,
        "openings": openings_map,
        "rows": output_rows
    }


async def get_time_control_result_color_matrix(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None
) -> Dict[str, Any]:
    """
    Returns global result matrix split by color (white/black) for a mode and rating range.
    Rating filter uses game average rating.
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "min_rating": min_rating,
            "max_rating": max_rating,
            "total_games": 0,
            "white": {"wins": 0, "draws": 0, "losses": 0, "total": 0, "score_rate": 0.0},
            "black": {"wins": 0, "draws": 0, "losses": 0, "total": 0, "score_rate": 0.0}
        }

    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        WITH filtered_games AS (
            SELECT
                g.white_result,
                g.black_result
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        g.white_elo IS NOT NULL
                        AND g.black_elo IS NOT NULL
                        AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                    )
              )
        )
        SELECT
            COUNT(*)::int AS total_games,
            SUM(CASE WHEN white_result = 1.0 THEN 1 ELSE 0 END)::int AS white_wins,
            SUM(CASE WHEN white_result = 0.5 THEN 1 ELSE 0 END)::int AS white_draws,
            SUM(CASE WHEN white_result = 0.0 THEN 1 ELSE 0 END)::int AS white_losses,
            SUM(CASE WHEN black_result = 1.0 THEN 1 ELSE 0 END)::int AS black_wins,
            SUM(CASE WHEN black_result = 0.5 THEN 1 ELSE 0 END)::int AS black_draws,
            SUM(CASE WHEN black_result = 0.0 THEN 1 ELSE 0 END)::int AS black_losses
        FROM filtered_games;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {
            "mode": target_mode,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating
        })
        row = result.mappings().first() or {}

    total_games = int(row.get("total_games") or 0)
    white_wins = int(row.get("white_wins") or 0)
    white_draws = int(row.get("white_draws") or 0)
    white_losses = int(row.get("white_losses") or 0)
    black_wins = int(row.get("black_wins") or 0)
    black_draws = int(row.get("black_draws") or 0)
    black_losses = int(row.get("black_losses") or 0)

    white_total = white_wins + white_draws + white_losses
    black_total = black_wins + black_draws + black_losses
    white_score_rate = round(((white_wins + 0.5 * white_draws) / white_total), 4) if white_total > 0 else 0.0
    black_score_rate = round(((black_wins + 0.5 * black_draws) / black_total), 4) if black_total > 0 else 0.0

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "total_games": total_games,
        "white": {
            "wins": white_wins,
            "draws": white_draws,
            "losses": white_losses,
            "total": white_total,
            "score_rate": white_score_rate
        },
        "black": {
            "wins": black_wins,
            "draws": black_draws,
            "losses": black_losses,
            "total": black_total,
            "score_rate": black_score_rate
        }
    }


async def get_time_control_game_length_analytics(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None
) -> Dict[str, Any]:
    """
    Returns game length analytics for a mode and rating range:
    - summary stats for n_moves and time_elapsed
    - histogram for moves and elapsed minutes
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "min_rating": min_rating,
            "max_rating": max_rating,
            "total_games": 0,
            "summary": {
                "avg_n_moves": None,
                "median_n_moves": None,
                "p90_n_moves": None,
                "avg_time_elapsed_sec": None,
                "median_time_elapsed_sec": None,
                "p90_time_elapsed_sec": None
            },
            "n_moves_hist": {"x": [], "y": []},
            "time_elapsed_hist": {"x": [], "y": [], "unit": "minutes"}
        }

    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    summary_query = f"""
        WITH filtered_games AS (
            SELECT g.n_moves, g.time_elapsed
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        g.white_elo IS NOT NULL
                        AND g.black_elo IS NOT NULL
                        AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                    )
              )
        )
        SELECT
            COUNT(*)::int AS total_games,
            ROUND(AVG(n_moves)::numeric, 2) AS avg_n_moves,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY n_moves) AS median_n_moves,
            percentile_cont(0.9) WITHIN GROUP (ORDER BY n_moves) AS p90_n_moves,
            ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed_sec,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY time_elapsed) AS median_time_elapsed_sec,
            percentile_cont(0.9) WITHIN GROUP (ORDER BY time_elapsed) AS p90_time_elapsed_sec
        FROM filtered_games;
    """

    moves_hist_query = f"""
        WITH filtered_games AS (
            SELECT g.n_moves
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        g.white_elo IS NOT NULL
                        AND g.black_elo IS NOT NULL
                        AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                    )
              )
        )
        SELECT
            (FLOOR(n_moves / 10.0) * 10)::int AS bucket_start,
            (FLOOR(n_moves / 10.0) * 10 + 9)::int AS bucket_end,
            COUNT(*)::int AS total
        FROM filtered_games
        GROUP BY bucket_start, bucket_end
        ORDER BY bucket_start;
    """

    elapsed_hist_query = f"""
        WITH filtered_games AS (
            SELECT g.time_elapsed
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR (
                        g.white_elo IS NOT NULL
                        AND g.black_elo IS NOT NULL
                        AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                    )
              )
        ),
        bucketed AS (
            SELECT
                CASE
                    WHEN (time_elapsed / 60.0) < 1 THEN '0-1'
                    WHEN (time_elapsed / 60.0) < 2 THEN '1-2'
                    WHEN (time_elapsed / 60.0) < 3 THEN '2-3'
                    WHEN (time_elapsed / 60.0) < 5 THEN '3-5'
                    WHEN (time_elapsed / 60.0) < 10 THEN '5-10'
                    WHEN (time_elapsed / 60.0) < 20 THEN '10-20'
                    WHEN (time_elapsed / 60.0) < 40 THEN '20-40'
                    ELSE '40+'
                END AS bucket_label,
                CASE
                    WHEN (time_elapsed / 60.0) < 1 THEN 1
                    WHEN (time_elapsed / 60.0) < 2 THEN 2
                    WHEN (time_elapsed / 60.0) < 3 THEN 3
                    WHEN (time_elapsed / 60.0) < 5 THEN 4
                    WHEN (time_elapsed / 60.0) < 10 THEN 5
                    WHEN (time_elapsed / 60.0) < 20 THEN 6
                    WHEN (time_elapsed / 60.0) < 40 THEN 7
                    ELSE 8
                END AS bucket_order
            FROM filtered_games
        )
        SELECT bucket_label, bucket_order, COUNT(*)::int AS total
        FROM bucketed
        GROUP BY bucket_label, bucket_order
        ORDER BY bucket_order;
    """

    def _to_float(val: Any) -> Optional[float]:
        return float(val) if val is not None else None

    async with AsyncDBSession() as session:
        summary_result = await session.execute(text(summary_query), {
            "mode": target_mode,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating
        })
        summary_row = summary_result.mappings().first() or {}

        moves_result = await session.execute(text(moves_hist_query), {
            "mode": target_mode,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating
        })
        moves_rows = moves_result.mappings().all()

        elapsed_result = await session.execute(text(elapsed_hist_query), {
            "mode": target_mode,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating
        })
        elapsed_rows = elapsed_result.mappings().all()

    moves_x = [f"{int(row['bucket_start'])}-{int(row['bucket_end'])}" for row in moves_rows]
    moves_y = [int(row.get("total") or 0) for row in moves_rows]
    elapsed_x = [str(row.get("bucket_label") or "") for row in elapsed_rows]
    elapsed_y = [int(row.get("total") or 0) for row in elapsed_rows]

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "total_games": int(summary_row.get("total_games") or 0),
        "summary": {
            "avg_n_moves": _to_float(summary_row.get("avg_n_moves")),
            "median_n_moves": _to_float(summary_row.get("median_n_moves")),
            "p90_n_moves": _to_float(summary_row.get("p90_n_moves")),
            "avg_time_elapsed_sec": _to_float(summary_row.get("avg_time_elapsed_sec")),
            "median_time_elapsed_sec": _to_float(summary_row.get("median_time_elapsed_sec")),
            "p90_time_elapsed_sec": _to_float(summary_row.get("p90_time_elapsed_sec"))
        },
        "n_moves_hist": {"x": moves_x, "y": moves_y},
        "time_elapsed_hist": {"x": elapsed_x, "y": elapsed_y, "unit": "minutes"}
    }


async def get_time_control_activity_trend(
    mode: str,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None
) -> Dict[str, Any]:
    """
    Returns activity heat data for a mode and rating range:
    - month of year (12 buckets)
    - day of week (7 buckets)
    - hour of day (24 buckets)
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "mode": target_mode,
            "min_rating": min_rating,
            "max_rating": max_rating,
            "month_heat": {"labels": [], "values": []},
            "weekday_heat": {"labels": [], "values": []},
            "hour_heat": {"labels": [], "values": []}
        }

    safe_min_rating = int(min_rating) if min_rating is not None else None
    safe_max_rating = int(max_rating) if max_rating is not None else None
    if safe_min_rating is not None and safe_max_rating is not None and safe_min_rating > safe_max_rating:
        safe_min_rating, safe_max_rating = safe_max_rating, safe_min_rating

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    month_query = f"""
        SELECT g.month::int AS month_idx, COUNT(*)::int AS total
        FROM game g
        WHERE {mode_sql} = :mode
          AND (
                CAST(:min_rating AS INTEGER) IS NULL
                OR CAST(:max_rating AS INTEGER) IS NULL
                OR (
                    g.white_elo IS NOT NULL
                    AND g.black_elo IS NOT NULL
                    AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                )
          )
        GROUP BY month_idx
        ORDER BY month_idx;
    """

    weekday_query = f"""
        SELECT
            EXTRACT(DOW FROM make_date(g.year, g.month, g.day))::int AS dow_idx,
            COUNT(*)::int AS total
        FROM game g
        WHERE {mode_sql} = :mode
          AND (
                CAST(:min_rating AS INTEGER) IS NULL
                OR CAST(:max_rating AS INTEGER) IS NULL
                OR (
                    g.white_elo IS NOT NULL
                    AND g.black_elo IS NOT NULL
                    AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                )
          )
        GROUP BY dow_idx
        ORDER BY dow_idx;
    """

    hour_query = f"""
        SELECT g.hour::int AS hour_idx, COUNT(*)::int AS total
        FROM game g
        WHERE {mode_sql} = :mode
          AND (
                CAST(:min_rating AS INTEGER) IS NULL
                OR CAST(:max_rating AS INTEGER) IS NULL
                OR (
                    g.white_elo IS NOT NULL
                    AND g.black_elo IS NOT NULL
                    AND ((g.white_elo + g.black_elo) / 2.0) BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
                )
          )
        GROUP BY hour_idx
        ORDER BY hour_idx;
    """

    async with AsyncDBSession() as session:
        query_params = {
            "mode": target_mode,
            "min_rating": safe_min_rating,
            "max_rating": safe_max_rating
        }
        month_result = await session.execute(text(month_query), query_params)
        month_rows = month_result.mappings().all()

        weekday_result = await session.execute(text(weekday_query), query_params)
        weekday_rows = weekday_result.mappings().all()

        hour_result = await session.execute(text(hour_query), query_params)
        hour_rows = hour_result.mappings().all()

    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_values = [0] * 12
    for row in month_rows:
        month_idx = int(row.get("month_idx") or 0)
        if 1 <= month_idx <= 12:
            month_values[month_idx - 1] = int(row.get("total") or 0)

    weekday_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    weekday_values = [0] * 7
    for row in weekday_rows:
        dow_idx = int(row.get("dow_idx") or 0)
        if 0 <= dow_idx <= 6:
            weekday_values[dow_idx] = int(row.get("total") or 0)

    hour_labels = [f"{hour:02d}" for hour in range(24)]
    hour_values = [0] * 24
    for row in hour_rows:
        hour_idx = int(row.get("hour_idx") or 0)
        if 0 <= hour_idx <= 23:
            hour_values[hour_idx] = int(row.get("total") or 0)

    return {
        "mode": target_mode,
        "min_rating": safe_min_rating,
        "max_rating": safe_max_rating,
        "month_heat": {"labels": month_labels, "values": month_values},
        "weekday_heat": {"labels": weekday_labels, "values": weekday_values},
        "hour_heat": {"labels": hour_labels, "values": hour_values}
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


async def get_player_games_page(player_name: str, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
    """
    Returns a paginated list of games for a player with date/time, color, and result.
    """
    safe_page = max(1, page)
    safe_page_size = max(1, min(100, page_size))
    offset = (safe_page - 1) * safe_page_size

    count_query = """
        SELECT COUNT(*) AS total
        FROM game
        WHERE white = :player OR black = :player;
    """

    games_query = """
        SELECT
            link,
            year,
            month,
            day,
            hour,
            minute,
            second,
            CASE
                WHEN white = :player THEN 'white'
                ELSE 'black'
            END AS color,
            CASE
                WHEN white = :player THEN white_result
                ELSE black_result
            END AS player_score
        FROM game
        WHERE white = :player OR black = :player
        ORDER BY year DESC, month DESC, day DESC, hour DESC, minute DESC, second DESC
        LIMIT :limit OFFSET :offset;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query), {"player": player_name})
        total = count_result.scalar() or 0

        rows_result = await session.execute(
            text(games_query),
            {"player": player_name, "limit": safe_page_size, "offset": offset}
        )
        rows = rows_result.mappings().all()

    games = []
    for row in rows:
        score = row.get("player_score")
        if score == 1.0:
            result_label = "win"
        elif score == 0.5:
            result_label = "draw"
        elif score == 0.0:
            result_label = "loss"
        else:
            result_label = "unknown"

        played_at = (
            f"{int(row['year']):04d}-{int(row['month']):02d}-{int(row['day']):02d} "
            f"{int(row['hour']):02d}:{int(row['minute']):02d}:{int(row['second']):02d}"
        )

        games.append({
            "link": row["link"],
            "played_at": played_at,
            "color": row["color"],
            "result": result_label
        })

    total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0

    return {
        "player_name": player_name,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": total_pages,
        "games": games
    }


async def get_player_game_summary(player_name: str) -> Dict[str, Any]:
    """
    Returns win/loss/draw counts, date range, and counts per time_control for a player.
    """
    summary_query = """
        SELECT
            SUM(CASE WHEN white = :player THEN white_result WHEN black = :player THEN black_result ELSE NULL END) as win_points,
            SUM(
                CASE
                    WHEN (white = :player AND white_result = 1.0) OR (black = :player AND black_result = 1.0) THEN 1
                    ELSE 0
                END
            ) as wins,
            SUM(
                CASE
                    WHEN (white = :player AND white_result = 0.0) OR (black = :player AND black_result = 0.0) THEN 1
                    ELSE 0
                END
            ) as losses,
            SUM(
                CASE
                    WHEN (white = :player AND white_result = 0.5) OR (black = :player AND black_result = 0.5) THEN 1
                    ELSE 0
                END
            ) as draws,
            MIN(make_timestamp(year, month, day, hour, minute, second)) as first_game,
            MAX(make_timestamp(year, month, day, hour, minute, second)) as last_game,
            COUNT(*) as total_games
        FROM game
        WHERE white = :player OR black = :player;
    """

    time_control_query = """
        SELECT time_control, COUNT(*) as total
        FROM game
        WHERE white = :player OR black = :player
        GROUP BY time_control
        ORDER BY total DESC;
    """

    async with AsyncDBSession() as session:
        summary_result = await session.execute(text(summary_query), {"player": player_name})
        summary_row = summary_result.mappings().first()

        tc_result = await session.execute(text(time_control_query), {"player": player_name})
        tc_rows = tc_result.mappings().all()

    def format_ts(ts):
        return ts.isoformat() if ts else None

    return {
        "player_name": player_name,
        "wins": summary_row["wins"] if summary_row else 0,
        "losses": summary_row["losses"] if summary_row else 0,
        "draws": summary_row["draws"] if summary_row else 0,
        "total_games": summary_row["total_games"] if summary_row else 0,
        "date_from": format_ts(summary_row["first_game"]) if summary_row else None,
        "date_to": format_ts(summary_row["last_game"]) if summary_row else None,
        "time_controls": [{"time_control": row["time_control"], "total": row["total"]} for row in tc_rows]
    }


async def get_player_mode_games(player_name: str, mode: str) -> Dict[str, Any]:
    """
    Returns all games for a player filtered by normalized mode (bullet/blitz/rapid),
    plus summary stats that can feed downstream containers.
    """
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {
            "player_name": player_name,
            "mode": target_mode,
            "total_games": 0,
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "score_rate": 0.0,
            "date_from": None,
            "date_to": None,
            "games": []
        }

    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        SELECT
            g.link,
            g.year,
            g.month,
            g.day,
            g.hour,
            g.minute,
            g.second,
            CASE
                WHEN g.white = :player THEN 'white'
                ELSE 'black'
            END AS color,
            CASE
                WHEN g.white = :player THEN g.white_result
                ELSE g.black_result
            END AS player_score
        FROM game g
        WHERE (g.white = :player OR g.black = :player)
          AND {mode_sql} = :mode
        ORDER BY g.year DESC, g.month DESC, g.day DESC, g.hour DESC, g.minute DESC, g.second DESC;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name, "mode": target_mode})
        rows = result.mappings().all()

    games: List[Dict[str, Any]] = []
    wins = losses = draws = 0

    for row in rows:
        score = row.get("player_score")
        if score == 1.0:
            result_label = "win"
            wins += 1
        elif score == 0.5:
            result_label = "draw"
            draws += 1
        elif score == 0.0:
            result_label = "loss"
            losses += 1
        else:
            result_label = "unknown"

        played_at = (
            f"{int(row['year']):04d}-{int(row['month']):02d}-{int(row['day']):02d} "
            f"{int(row['hour']):02d}:{int(row['minute']):02d}:{int(row['second']):02d}"
        )

        games.append({
            "link": row["link"],
            "played_at": played_at,
            "color": row["color"],
            "result": result_label
        })

    total_games = len(games)
    score_rate = round(((wins + 0.5 * draws) / total_games), 4) if total_games > 0 else 0.0
    date_to = games[0]["played_at"] if games else None
    date_from = games[-1]["played_at"] if games else None

    return {
        "player_name": player_name,
        "mode": target_mode,
        "total_games": total_games,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "score_rate": score_rate,
        "date_from": date_from,
        "date_to": date_to,
        "games": games
    }


async def get_player_hours_played(player_name: str) -> Dict[str, Any]:
    """
    Returns total played hours and played hours split by normalized time control mode.
    Uses game.time_elapsed (seconds).
    """
    mode_sql = """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """

    query = f"""
        WITH player_games AS (
            SELECT
                {mode_sql} AS mode,
                COALESCE(g.time_elapsed, 0) AS time_elapsed
            FROM game g
            WHERE g.white = :player OR g.black = :player
        )
        SELECT
            ROUND(COALESCE(SUM(time_elapsed), 0) / 3600.0, 2) AS total_hours,
            ROUND(COALESCE(SUM(CASE WHEN mode = 'bullet' THEN time_elapsed ELSE 0 END), 0) / 3600.0, 2) AS bullet_hours,
            ROUND(COALESCE(SUM(CASE WHEN mode = 'blitz' THEN time_elapsed ELSE 0 END), 0) / 3600.0, 2) AS blitz_hours,
            ROUND(COALESCE(SUM(CASE WHEN mode = 'rapid' THEN time_elapsed ELSE 0 END), 0) / 3600.0, 2) AS rapid_hours
        FROM player_games;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name})
        row = result.mappings().first() or {}

    return {
        "player_name": player_name,
        "total_hours": float(row.get("total_hours") or 0.0),
        "bullet_hours": float(row.get("bullet_hours") or 0.0),
        "blitz_hours": float(row.get("blitz_hours") or 0.0),
        "rapid_hours": float(row.get("rapid_hours") or 0.0)
    }


def _mode_case_sql() -> str:
    return """
        CASE
            WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control, '+', 1) ~ '^[0-9]+$' THEN
                CASE
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                    WHEN CAST(split_part(g.time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                    ELSE 'classical'
                END
            ELSE 'unknown'
        END
    """


def _player_result_to_score(result_filter: str) -> Optional[float]:
    normalized = (result_filter or "").strip().lower()
    if normalized == "win":
        return 1.0
    if normalized == "loss":
        return 0.0
    if normalized == "draw":
        return 0.5
    return None


def _to_float_safe(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def get_player_time_control_top_moves(
    player_name: str,
    mode: str,
    move_color: str = "white",
    page: int = 1,
    page_size: int = 5,
    max_move: int = 10
) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    target_color = (move_color or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "move_color": target_color, "rows": [], "total": 0, "page": 1, "page_size": 0, "total_pages": 0}
    if target_color not in {"white", "black"}:
        target_color = "white"

    safe_page_size = max(1, min(int(page_size), 10))
    safe_page = max(1, int(page))
    safe_max_move = max(1, min(int(max_move), 30))
    total_rows = safe_max_move
    total_pages = (total_rows + safe_page_size - 1) // safe_page_size if total_rows > 0 else 0
    if total_pages > 0 and safe_page > total_pages:
        safe_page = total_pages
    offset = (safe_page - 1) * safe_page_size
    mode_sql = _mode_case_sql()

    query = f"""
        WITH player_games AS (
            SELECT g.link
            FROM game g
            WHERE {mode_sql} = :mode
              AND (
                  (:move_color = 'white' AND g.white = :player)
                  OR (:move_color = 'black' AND g.black = :player)
              )
        ),
        filtered_moves AS (
            SELECT
                m.n_move,
                CASE
                    WHEN :move_color = 'black' THEN m.black_move
                    ELSE m.white_move
                END AS move
            FROM moves m
            JOIN player_games pg ON pg.link = m.link
            WHERE m.n_move BETWEEN 1 AND :max_move
        ),
        clean_moves AS (
            SELECT n_move AS move_number, move
            FROM filtered_moves
            WHERE move IS NOT NULL AND move <> '' AND move <> '--'
        ),
        ranked AS (
            SELECT
                move_number,
                move,
                COUNT(*)::int AS times_played,
                ROW_NUMBER() OVER (PARTITION BY move_number ORDER BY COUNT(*) DESC, move ASC) AS rank_in_move
            FROM clean_moves
            GROUP BY move_number, move
        ),
        top_by_move AS (
            SELECT move_number, move, times_played
            FROM ranked
            WHERE rank_in_move = 1
        ),
        filled AS (
            SELECT
                gs AS move_number,
                COALESCE(t.move, '-') AS move,
                COALESCE(t.times_played, 0)::int AS times_played
            FROM generate_series(1, :max_move) AS gs
            LEFT JOIN top_by_move t ON t.move_number = gs
            ORDER BY gs
        )
        SELECT move_number, move, times_played
        FROM filled
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {
            "mode": target_mode,
            "move_color": target_color,
            "player": player_name,
            "max_move": safe_max_move,
            "offset": offset,
            "limit": safe_page_size
        })
        rows = result.mappings().all()

    return {
        "mode": target_mode,
        "move_color": target_color,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total_rows,
        "total_pages": total_pages,
        "rows": [
            {
                "move_number": int(row.get("move_number") or 0),
                "move": str(row.get("move") or "-"),
                "times_played": int(row.get("times_played") or 0)
            }
            for row in rows
        ]
    }


async def get_player_time_control_top_openings(
    player_name: str,
    mode: str,
    result_filter: str,
    page: int = 1,
    page_size: int = 5,
    n_moves: int = 3
) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "rows": [], "total": 0, "page": 1, "page_size": 0, "total_pages": 0}

    target_result = (result_filter or "").strip().lower()
    target_score = _player_result_to_score(target_result)
    if target_score is None:
        target_result = "win"
        target_score = 1.0

    safe_page_size = max(1, min(int(page_size), 25))
    safe_page = max(1, int(page))
    safe_n_moves = max(3, min(int(n_moves), 10))
    required_half_moves = safe_n_moves * 2
    mode_sql = _mode_case_sql()

    count_query = f"""
        WITH player_games AS (
            SELECT
                g.link
            FROM game g
            WHERE {mode_sql} = :mode
              AND (g.white = :player OR g.black = :player)
              AND (
                    CASE
                        WHEN g.white = :player THEN g.white_result
                        ELSE g.black_result
                    END
                  ) = :target_score
        ),
        opening_moves AS (
            SELECT m.link, m.n_move, m.white_move, m.black_move
            FROM moves m
            JOIN player_games pg ON pg.link = m.link
            WHERE m.n_move BETWEEN 1 AND :n_moves
        ),
        complete_games AS (
            SELECT link
            FROM opening_moves
            GROUP BY link
            HAVING COUNT(DISTINCT n_move) = :n_moves
        ),
        ply_moves AS (
            SELECT link, (n_move * 2 - 1) AS ply, white_move AS san FROM opening_moves
            UNION ALL
            SELECT link, (n_move * 2) AS ply, black_move AS san FROM opening_moves
        ),
        clean_ply AS (
            SELECT link, ply, san
            FROM ply_moves
            WHERE san IS NOT NULL AND san <> '' AND san <> '--'
        ),
        openings AS (
            SELECT link, string_agg(san, ' ' ORDER BY ply) AS opening
            FROM clean_ply
            WHERE link IN (SELECT link FROM complete_games)
            GROUP BY link
            HAVING COUNT(*) = :required_half_moves
        ),
        aggregated AS (
            SELECT opening
            FROM openings
            WHERE opening IS NOT NULL AND opening <> ''
            GROUP BY opening
        )
        SELECT COUNT(*)::int AS total FROM aggregated;
    """

    data_query = f"""
        WITH player_games AS (
            SELECT
                g.link
            FROM game g
            WHERE {mode_sql} = :mode
              AND (g.white = :player OR g.black = :player)
              AND (
                    CASE
                        WHEN g.white = :player THEN g.white_result
                        ELSE g.black_result
                    END
                  ) = :target_score
        ),
        opening_moves AS (
            SELECT m.link, m.n_move, m.white_move, m.black_move
            FROM moves m
            JOIN player_games pg ON pg.link = m.link
            WHERE m.n_move BETWEEN 1 AND :n_moves
        ),
        complete_games AS (
            SELECT link
            FROM opening_moves
            GROUP BY link
            HAVING COUNT(DISTINCT n_move) = :n_moves
        ),
        ply_moves AS (
            SELECT link, (n_move * 2 - 1) AS ply, white_move AS san FROM opening_moves
            UNION ALL
            SELECT link, (n_move * 2) AS ply, black_move AS san FROM opening_moves
        ),
        clean_ply AS (
            SELECT link, ply, san
            FROM ply_moves
            WHERE san IS NOT NULL AND san <> '' AND san <> '--'
        ),
        openings AS (
            SELECT link, string_agg(san, ' ' ORDER BY ply) AS opening
            FROM clean_ply
            WHERE link IN (SELECT link FROM complete_games)
            GROUP BY link
            HAVING COUNT(*) = :required_half_moves
        )
        SELECT
            opening,
            COUNT(*)::int AS times_played
        FROM openings
        WHERE opening IS NOT NULL AND opening <> ''
        GROUP BY opening
        ORDER BY times_played DESC, opening ASC
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query), {
            "mode": target_mode,
            "player": player_name,
            "target_score": target_score,
            "n_moves": safe_n_moves,
            "required_half_moves": required_half_moves
        })
        count_row = count_result.mappings().first()
        total = int((count_row or {}).get("total") or 0)

        total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0
        if total_pages > 0 and safe_page > total_pages:
            safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size

        result = await session.execute(text(data_query), {
            "mode": target_mode,
            "player": player_name,
            "target_score": target_score,
            "n_moves": safe_n_moves,
            "required_half_moves": required_half_moves,
            "offset": offset,
            "limit": safe_page_size
        })
        rows = result.mappings().all()

    output_rows = []
    for idx, row in enumerate(rows, start=1):
        opening_text = str(row.get("opening") or "").strip()
        half_moves = [token for token in opening_text.split() if token][:required_half_moves]
        output_rows.append({
            "top": idx,
            "key": f"player_opening_{target_result}_{idx}",
            "opening": opening_text,
            "half_moves": half_moves,
            "times_played": int(row.get("times_played") or 0)
        })

    return {
        "mode": target_mode,
        "result_filter": target_result,
        "n_moves": safe_n_moves,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": (total + safe_page_size - 1) // safe_page_size if total > 0 else 0,
        "rows": output_rows
    }


async def get_player_time_control_results(player_name: str, mode: str) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "total_games": 0, "wins": 0, "losses": 0, "draws": 0, "score_rate": 0.0}
    return await get_player_mode_games(player_name=player_name, mode=target_mode)


async def get_player_time_control_lengths(player_name: str, mode: str) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "total_games": 0, "summary": {}, "n_moves_hist": {"x": [], "y": []}, "time_elapsed_hist": {"x": [], "y": [], "unit": "minutes"}}

    mode_sql = _mode_case_sql()
    summary_query = f"""
        WITH filtered_games AS (
            SELECT g.n_moves, g.time_elapsed
            FROM game g
            WHERE {mode_sql} = :mode
              AND (g.white = :player OR g.black = :player)
        )
        SELECT
            COUNT(*)::int AS total_games,
            ROUND(AVG(n_moves)::numeric, 2) AS avg_n_moves,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY n_moves) AS median_n_moves,
            percentile_cont(0.9) WITHIN GROUP (ORDER BY n_moves) AS p90_n_moves,
            ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed_sec
        FROM filtered_games;
    """
    moves_hist_query = f"""
        WITH filtered_games AS (
            SELECT g.n_moves
            FROM game g
            WHERE {mode_sql} = :mode
              AND (g.white = :player OR g.black = :player)
        )
        SELECT
            (FLOOR(n_moves / 10.0) * 10)::int AS bucket_start,
            (FLOOR(n_moves / 10.0) * 10 + 9)::int AS bucket_end,
            COUNT(*)::int AS total
        FROM filtered_games
        GROUP BY bucket_start, bucket_end
        ORDER BY bucket_start;
    """
    elapsed_hist_query = f"""
        WITH filtered_games AS (
            SELECT g.time_elapsed
            FROM game g
            WHERE {mode_sql} = :mode
              AND (g.white = :player OR g.black = :player)
        )
        SELECT
            (FLOOR((time_elapsed / 60.0) / 1.0) * 1)::int AS bucket_start_min,
            (FLOOR((time_elapsed / 60.0) / 1.0) * 1 + 1)::int AS bucket_end_min,
            COUNT(*)::int AS total
        FROM filtered_games
        GROUP BY bucket_start_min, bucket_end_min
        ORDER BY bucket_start_min;
    """
    async with AsyncDBSession() as session:
        summary_result = await session.execute(text(summary_query), {"mode": target_mode, "player": player_name})
        summary_row = summary_result.mappings().first() or {}
        moves_hist_result = await session.execute(text(moves_hist_query), {"mode": target_mode, "player": player_name})
        moves_hist_rows = moves_hist_result.mappings().all()
        elapsed_hist_result = await session.execute(text(elapsed_hist_query), {"mode": target_mode, "player": player_name})
        elapsed_hist_rows = elapsed_hist_result.mappings().all()

    return {
        "mode": target_mode,
        "total_games": int(summary_row.get("total_games") or 0),
        "summary": {
            "avg_n_moves": _to_float_safe(summary_row.get("avg_n_moves")),
            "median_n_moves": _to_float_safe(summary_row.get("median_n_moves")),
            "p90_n_moves": _to_float_safe(summary_row.get("p90_n_moves")),
            "avg_time_elapsed_sec": _to_float_safe(summary_row.get("avg_time_elapsed_sec"))
        },
        "n_moves_hist": {
            "x": [f"{int(r.get('bucket_start') or 0)}-{int(r.get('bucket_end') or 0)}" for r in moves_hist_rows],
            "y": [int(r.get("total") or 0) for r in moves_hist_rows]
        },
        "time_elapsed_hist": {
            "x": [f"{int(r.get('bucket_start_min') or 0)}-{int(r.get('bucket_end_min') or 0)}" for r in elapsed_hist_rows],
            "y": [int(r.get("total") or 0) for r in elapsed_hist_rows],
            "unit": "minutes"
        }
    }


async def get_player_time_control_activity_trend(player_name: str, mode: str) -> Dict[str, Any]:
    target_mode = (mode or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"mode": target_mode, "month_heat": {"labels": [], "values": []}, "weekday_heat": {"labels": [], "values": []}, "hour_heat": {"labels": [], "values": []}}
    mode_sql = _mode_case_sql()
    month_query = f"""
        SELECT month::int AS idx, COUNT(*)::int AS total
        FROM game g
        WHERE {mode_sql} = :mode
          AND (g.white = :player OR g.black = :player)
        GROUP BY month
        ORDER BY month;
    """
    weekday_query = f"""
        SELECT EXTRACT(DOW FROM make_timestamp(year, month, day, hour, minute, second))::int AS idx, COUNT(*)::int AS total
        FROM game g
        WHERE {mode_sql} = :mode
          AND (g.white = :player OR g.black = :player)
        GROUP BY idx
        ORDER BY idx;
    """
    hour_query = f"""
        SELECT hour::int AS idx, COUNT(*)::int AS total
        FROM game g
        WHERE {mode_sql} = :mode
          AND (g.white = :player OR g.black = :player)
        GROUP BY hour
        ORDER BY hour;
    """
    async with AsyncDBSession() as session:
        mres = await session.execute(text(month_query), {"mode": target_mode, "player": player_name})
        wres = await session.execute(text(weekday_query), {"mode": target_mode, "player": player_name})
        hres = await session.execute(text(hour_query), {"mode": target_mode, "player": player_name})
        months = mres.mappings().all()
        weekdays = wres.mappings().all()
        hours = hres.mappings().all()

    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    weekday_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    return {
        "mode": target_mode,
        "month_heat": {
            "labels": [month_names[max(0, min(11, int(r.get('idx') or 1) - 1))] for r in months],
            "values": [int(r.get("total") or 0) for r in months]
        },
        "weekday_heat": {
            "labels": [weekday_names[max(0, min(6, int(r.get('idx') or 0)))] for r in weekdays],
            "values": [int(r.get("total") or 0) for r in weekdays]
        },
        "hour_heat": {
            "labels": [f"{int(r.get('idx') or 0):02d}" for r in hours],
            "values": [int(r.get("total") or 0) for r in hours]
        }
    }


def _normalize_time_control_mode(time_control: Optional[str]) -> str:
    """
    Normalizes raw time_control strings into mode buckets.
    Examples: 60 and 60+1 -> bullet.
    """
    if not time_control:
        return "unknown"

    tc = str(time_control).strip()
    if not tc:
        return "unknown"

    if "/" in tc:
        return "daily"

    primary = tc.split("+", 1)[0]
    try:
        seconds = int(primary)
    except (TypeError, ValueError):
        return "unknown"

    # Chess.com-style buckets requested by user:
    # - bullet: < 3 minutes
    # - blitz:  < 10 minutes
    # - rapid:  10 to 30 minutes
    if seconds < 180:
        return "bullet"
    if seconds < 600:
        return "blitz"
    if seconds <= 1800:
        return "rapid"
    return "classical"


async def get_player_modes_stats(player_name: str) -> Dict[str, Dict[str, int]]:
    """
    Returns per-mode stats for a player keyed by normalized mode.
    """
    query = """
        SELECT
            white,
            black,
            white_elo,
            black_elo,
            time_control,
            year,
            month,
            day,
            hour,
            minute,
            second
        FROM game
        WHERE white = :player OR black = :player
        ORDER BY year ASC, month ASC, day ASC, hour ASC, minute ASC, second ASC;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name})
        rows = result.mappings().all()

    by_mode: Dict[str, Dict[str, int]] = {}
    for row in rows:
        mode = _normalize_time_control_mode(row.get("time_control"))
        as_white = row.get("white") == player_name
        rating = int(row.get("white_elo") if as_white else row.get("black_elo"))

        if mode not in by_mode:
            by_mode[mode] = {
                "n_games": 0,
                "as_white": 0,
                "as_black": 0,
                "oldest_rating": rating,
                "newest_rating": rating
            }

        by_mode[mode]["n_games"] += 1
        if as_white:
            by_mode[mode]["as_white"] += 1
        else:
            by_mode[mode]["as_black"] += 1
        by_mode[mode]["newest_rating"] = rating

    sorted_modes = sorted(by_mode.items(), key=lambda item: item[1]["n_games"], reverse=True)
    return {mode: stats for mode, stats in sorted_modes}


def _build_y_ticks(min_rating: int, max_rating: int, count: int = 5) -> List[int]:
    if min_rating == max_rating:
        center = min_rating
        return [center - 20, center - 10, center, center + 10, center + 20]

    step = (max_rating - min_rating) / max(1, count - 1)
    ticks = [round(min_rating + i * step) for i in range(count)]
    return sorted(set(ticks))


def _resolve_chart_cutoff(range_type: str, years: Optional[int], latest_dt: datetime) -> Tuple[Optional[datetime], str, Optional[int]]:
    rt = (range_type or "all").strip().lower()
    if rt == "six_months":
        return latest_dt - timedelta(days=183), "six_months", None
    if rt == "one_year":
        return latest_dt - timedelta(days=365), "one_year", None
    if rt == "years":
        safe_years = max(1, years or 1)
        return latest_dt - timedelta(days=365 * safe_years), "years", safe_years
    return None, "all", None


async def get_player_mode_chart(
    player_name: str,
    mode: str,
    range_type: str = "all",
    years: Optional[int] = None
) -> Dict[str, Any]:
    """
    Returns chart data for a single normalized mode (oldest to newest),
    including backend-computed color buckets from mean/std.
    """
    target_mode = mode.strip().lower()
    query = """
        SELECT
            white,
            black,
            white_elo,
            black_elo,
            time_control,
            year,
            month,
            day,
            hour,
            minute,
            second
        FROM game
        WHERE white = :player OR black = :player
        ORDER BY year ASC, month ASC, day ASC, hour ASC, minute ASC, second ASC;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name})
        rows = result.mappings().all()

    mode_points: List[Tuple[datetime, str, int]] = []
    for row in rows:
        row_mode = _normalize_time_control_mode(row.get("time_control"))
        if row_mode != target_mode:
            continue

        is_white = row.get("white") == player_name
        rating = int(row.get("white_elo") if is_white else row.get("black_elo"))
        dt = datetime(
            int(row["year"]),
            int(row["month"]),
            int(row["day"]),
            int(row["hour"]),
            int(row["minute"]),
            int(row["second"])
        )
        date_value = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
        mode_points.append((dt, date_value, rating))

    if not mode_points:
        return {
            "points": [],
            "chart_title": f"rating of {target_mode}",
            "mode": target_mode,
            "range": {"type": "all", "years": None},
            "stats": {"mean": None, "std": None, "lower": None, "upper": None, "count": 0},
            "y_axis": {"min": None, "max": None, "ticks": []}
        }

    latest_dt = mode_points[-1][0]
    cutoff, resolved_range, resolved_years = _resolve_chart_cutoff(range_type, years, latest_dt)

    filtered = [
        point for point in mode_points
        if cutoff is None or point[0] >= cutoff
    ]

    if not filtered:
        return {
            "points": [],
            "chart_title": f"rating of {target_mode}",
            "mode": target_mode,
            "range": {"type": resolved_range, "years": resolved_years},
            "stats": {"mean": None, "std": None, "lower": None, "upper": None, "count": 0},
            "y_axis": {"min": None, "max": None, "ticks": []}
        }

    y_values = [point[2] for point in filtered]

    mean_value = sum(y_values) / len(y_values)
    variance = sum((value - mean_value) ** 2 for value in y_values) / len(y_values)
    std_value = math.sqrt(variance)
    lower = mean_value - std_value
    upper = mean_value + std_value

    points = []
    for _, date_value, rating in filtered:
        if rating <= lower:
            bucket = "std_behind"
            color = "#f07167"
        elif rating >= upper:
            bucket = "std_ahead"
            color = "#3fd089"
        else:
            bucket = "mean_band"
            color = "#8be9fd"

        points.append({
            "x": date_value,
            "y": rating,
            "bucket": bucket,
            "color": color
        })

    y_min = min(y_values)
    y_max = max(y_values)
    y_ticks = _build_y_ticks(y_min, y_max, count=5)

    return {
        "points": points,
        "chart_title": f"rating of {target_mode}",
        "mode": target_mode,
        "range": {"type": resolved_range, "years": resolved_years},
        "stats": {
            "mean": round(mean_value, 2),
            "std": round(std_value, 2),
            "lower": round(lower, 2),
            "upper": round(upper, 2),
            "count": len(y_values)
        },
        "y_axis": {"min": y_min, "max": y_max, "ticks": y_ticks}
    }
