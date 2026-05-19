# chessism_api/database/ask_db.py
import os
import asyncio
import time
import math
import json
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


DATABASE_SUMMARY_COLUMNS = """
    n_games_in_db,
    main_characters,
    secondary_characters,
    n_positions,
    analyzed_fens,
    unscored_fens,
    scored_fens,
    nonzero_scored_fens,
    bullet_games,
    blitz_games,
    rapid_games
"""

DATABASE_SUMMARY_RETURNING_COLUMNS = """
    database_summary.n_games_in_db AS n_games_in_db,
    database_summary.main_characters AS main_characters,
    database_summary.secondary_characters AS secondary_characters,
    database_summary.n_positions AS n_positions,
    database_summary.analyzed_fens AS analyzed_fens,
    database_summary.unscored_fens AS unscored_fens,
    database_summary.scored_fens AS scored_fens,
    database_summary.nonzero_scored_fens AS nonzero_scored_fens,
    database_summary.bullet_games AS bullet_games,
    database_summary.blitz_games AS blitz_games,
    database_summary.rapid_games AS rapid_games
"""


def _database_summary_from_row(row: Any) -> Dict[str, int]:
    return {
        "n_games_in_db": int(row.get("n_games_in_db") or 0),
        "main_characters": int(row.get("main_characters") or 0),
        "secondary_characters": int(row.get("secondary_characters") or 0),
        "n_positions": int(row.get("n_positions") or 0),
        "analyzed_fens": int(row.get("analyzed_fens") or 0),
        "unscored_fens": int(row.get("unscored_fens") or 0),
        "scored_fens": int(row.get("scored_fens") or 0),
        "nonzero_scored_fens": int(row.get("nonzero_scored_fens") or 0),
        "bullet_games": int(row.get("bullet_games") or 0),
        "blitz_games": int(row.get("blitz_games") or 0),
        "rapid_games": int(row.get("rapid_games") or 0),
    }


async def _read_database_summary() -> Optional[Dict[str, int]]:
    query = f"""
        SELECT {DATABASE_SUMMARY_COLUMNS}
        FROM database_summary
        WHERE id = 1;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        row = result.mappings().first()

    if not row:
        return None
    return _database_summary_from_row(row)


async def refresh_database_summary() -> Dict[str, int]:
    """
    Rebuilds the full dashboard summary. This intentionally does the expensive
    FEN counts in a refresh path, not in page-render endpoints.
    """
    query = f"""
        INSERT INTO database_summary (
            id,
            n_games_in_db,
            main_characters,
            secondary_characters,
            n_positions,
            analyzed_fens,
            unscored_fens,
            scored_fens,
            nonzero_scored_fens,
            bullet_games,
            blitz_games,
            rapid_games,
            refreshed_at
        )
        SELECT
            1,
            (SELECT COUNT(*)::bigint FROM game),
            (SELECT COUNT(*)::bigint FROM player WHERE joined IS NOT NULL AND joined <> 0),
            (SELECT COUNT(*)::bigint FROM player WHERE joined IS NULL OR joined = 0),
            (SELECT COUNT(*)::bigint FROM fen),
            (SELECT COUNT(*)::bigint FROM fen WHERE score IS NOT NULL),
            (SELECT COUNT(*)::bigint FROM fen WHERE score IS NULL),
            (SELECT COUNT(*)::bigint FROM fen WHERE score IS NOT NULL AND score <> 0),
            (SELECT COUNT(*)::bigint FROM fen WHERE score IS NOT NULL AND score <> 0),
            (SELECT COUNT(*)::bigint FROM game WHERE mode = 'bullet'),
            (SELECT COUNT(*)::bigint FROM game WHERE mode = 'blitz'),
            (SELECT COUNT(*)::bigint FROM game WHERE mode = 'rapid'),
            CURRENT_TIMESTAMP
        ON CONFLICT (id) DO UPDATE SET
            n_games_in_db = EXCLUDED.n_games_in_db,
            main_characters = EXCLUDED.main_characters,
            secondary_characters = EXCLUDED.secondary_characters,
            n_positions = EXCLUDED.n_positions,
            analyzed_fens = EXCLUDED.analyzed_fens,
            unscored_fens = EXCLUDED.unscored_fens,
            scored_fens = EXCLUDED.scored_fens,
            nonzero_scored_fens = EXCLUDED.nonzero_scored_fens,
            bullet_games = EXCLUDED.bullet_games,
            blitz_games = EXCLUDED.blitz_games,
            rapid_games = EXCLUDED.rapid_games,
            refreshed_at = EXCLUDED.refreshed_at
        RETURNING {DATABASE_SUMMARY_COLUMNS};
    """
    async with AsyncDBSession() as session:
        try:
            result = await session.execute(text(query))
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return _database_summary_from_row(row or {})


async def refresh_database_summary_game_counts() -> Dict[str, int]:
    """
    Refreshes game/player/time-control counts without scanning the FEN table.
    """
    if await _read_database_summary() is None:
        return await refresh_database_summary()

    query = f"""
        UPDATE database_summary
        SET
            n_games_in_db = counts.n_games_in_db,
            main_characters = counts.main_characters,
            secondary_characters = counts.secondary_characters,
            bullet_games = counts.bullet_games,
            blitz_games = counts.blitz_games,
            rapid_games = counts.rapid_games,
            refreshed_at = CURRENT_TIMESTAMP
        FROM (
            SELECT
                (SELECT COUNT(*)::bigint FROM game) AS n_games_in_db,
                (SELECT COUNT(*)::bigint FROM player WHERE joined IS NOT NULL AND joined <> 0) AS main_characters,
                (SELECT COUNT(*)::bigint FROM player WHERE joined IS NULL OR joined = 0) AS secondary_characters,
                (SELECT COUNT(*)::bigint FROM game WHERE mode = 'bullet') AS bullet_games,
                (SELECT COUNT(*)::bigint FROM game WHERE mode = 'blitz') AS blitz_games,
                (SELECT COUNT(*)::bigint FROM game WHERE mode = 'rapid') AS rapid_games
        ) counts
        WHERE database_summary.id = 1
        RETURNING {DATABASE_SUMMARY_RETURNING_COLUMNS};
    """
    async with AsyncDBSession() as session:
        try:
            result = await session.execute(text(query))
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    if not row:
        return await refresh_database_summary()
    return _database_summary_from_row(row)


async def refresh_database_summary_fen_counts() -> Dict[str, int]:
    """
    Refreshes only FEN coverage counts. This scans the FEN table and should be
    called after bulk FEN generation, not during page rendering.
    """
    if await _read_database_summary() is None:
        return await refresh_database_summary()

    query = f"""
        UPDATE database_summary
        SET
            n_positions = counts.n_positions,
            analyzed_fens = counts.analyzed_fens,
            unscored_fens = counts.unscored_fens,
            scored_fens = counts.scored_fens,
            nonzero_scored_fens = counts.nonzero_scored_fens,
            refreshed_at = CURRENT_TIMESTAMP
        FROM (
            SELECT
                COUNT(*)::bigint AS n_positions,
                COUNT(*) FILTER (WHERE score IS NOT NULL)::bigint AS analyzed_fens,
                COUNT(*) FILTER (WHERE score IS NULL)::bigint AS unscored_fens,
                COUNT(*) FILTER (WHERE score IS NOT NULL AND score <> 0)::bigint AS scored_fens,
                COUNT(*) FILTER (WHERE score IS NOT NULL AND score <> 0)::bigint AS nonzero_scored_fens
            FROM fen
        ) counts
        WHERE database_summary.id = 1
        RETURNING {DATABASE_SUMMARY_RETURNING_COLUMNS};
    """
    async with AsyncDBSession() as session:
        try:
            result = await session.execute(text(query))
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    if not row:
        return await refresh_database_summary()
    return _database_summary_from_row(row)


async def increment_database_summary_fen_counts(
    analyzed_delta: int,
    nonzero_scored_delta: int
) -> Dict[str, int]:
    """
    Applies committed analysis progress to the summary row without rescanning FEN.
    """
    safe_analyzed_delta = max(0, int(analyzed_delta or 0))
    safe_nonzero_delta = max(0, int(nonzero_scored_delta or 0))
    if safe_analyzed_delta == 0 and safe_nonzero_delta == 0:
        return await ensure_database_summary()

    existing = await _read_database_summary()
    if existing is None:
        return await refresh_database_summary()

    query = f"""
        UPDATE database_summary
        SET
            analyzed_fens = analyzed_fens + :analyzed_delta,
            unscored_fens = GREATEST(unscored_fens - :analyzed_delta, 0),
            scored_fens = scored_fens + :nonzero_scored_delta,
            nonzero_scored_fens = nonzero_scored_fens + :nonzero_scored_delta,
            refreshed_at = CURRENT_TIMESTAMP
        WHERE id = 1
        RETURNING {DATABASE_SUMMARY_COLUMNS};
    """
    async with AsyncDBSession() as session:
        try:
            result = await session.execute(text(query), {
                "analyzed_delta": safe_analyzed_delta,
                "nonzero_scored_delta": safe_nonzero_delta
            })
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return _database_summary_from_row(row or existing)


async def ensure_database_summary() -> Dict[str, int]:
    """
    Returns the precomputed summary, building it only if the row does not exist.
    """
    summary = await _read_database_summary()
    if summary is not None:
        return summary
    return await refresh_database_summary()


SCORED_POSITION_SUMMARY_COLUMNS = """
    total_positions,
    analyzed_fens,
    scored_positions,
    nonzero_scored_fens,
    unscored_fens,
    equal_positions,
    small_positions,
    clear_positions,
    decisive_positions,
    mate_positions,
    equal_appearances,
    small_appearances,
    clear_appearances,
    decisive_appearances,
    mate_appearances,
    equal_abs_score_sum,
    small_abs_score_sum,
    clear_abs_score_sum,
    decisive_abs_score_sum,
    mate_abs_score_sum,
    white_better,
    black_better,
    balanced,
    score_sum,
    abs_score_sum,
    wdl_win_sum,
    wdl_draw_sum,
    wdl_loss_sum,
    wdl_positions
"""


SCORED_BUCKET_DEFINITIONS = [
    ("equal", "Equal", 1),
    ("small", "Small", 2),
    ("clear", "Clear", 3),
    ("decisive", "Decisive", 4),
    ("mate", "Mate", 5),
]


def _json_payload(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return value


def _scored_bucket_payload(row: Any, key: str, label: str, order: int) -> Dict[str, Any]:
    positions = int(row.get(f"{key}_positions") or 0)
    appearances = int(row.get(f"{key}_appearances") or 0)
    abs_score_sum = float(row.get(f"{key}_abs_score_sum") or 0.0)
    return {
        "key": key,
        "label": label,
        "order": order,
        "positions": positions,
        "appearances": appearances,
        "avg_abs_score": round(abs_score_sum / positions, 2) if positions else 0.0
    }


def _scored_position_summary_from_row(row: Any) -> Dict[str, Any]:
    analyzed_fens = int(row.get("analyzed_fens") or 0)
    wdl_positions = int(row.get("wdl_positions") or 0)
    score_sum = float(row.get("score_sum") or 0.0)
    abs_score_sum = float(row.get("abs_score_sum") or 0.0)

    return {
        "total_positions": int(row.get("total_positions") or 0),
        "analyzed_fens": analyzed_fens,
        "scored_positions": int(row.get("scored_positions") or 0),
        "nonzero_scored_fens": int(row.get("nonzero_scored_fens") or 0),
        "unscored_fens": int(row.get("unscored_fens") or 0),
        "buckets": [
            _scored_bucket_payload(row, key, label, order)
            for key, label, order in SCORED_BUCKET_DEFINITIONS
        ],
        "side_balance": {
            "white_better": int(row.get("white_better") or 0),
            "black_better": int(row.get("black_better") or 0),
            "balanced": int(row.get("balanced") or 0),
            "avg_score": round(score_sum / analyzed_fens, 2) if analyzed_fens else 0.0,
            "avg_abs_score": round(abs_score_sum / analyzed_fens, 2) if analyzed_fens else 0.0
        },
        "wdl": {
            "positions": wdl_positions,
            "avg_white_win": round(float(row.get("wdl_win_sum") or 0.0) / wdl_positions, 2) if wdl_positions else 0.0,
            "avg_draw": round(float(row.get("wdl_draw_sum") or 0.0) / wdl_positions, 2) if wdl_positions else 0.0,
            "avg_white_loss": round(float(row.get("wdl_loss_sum") or 0.0) / wdl_positions, 2) if wdl_positions else 0.0
        }
    }


async def _read_scored_position_summary() -> Optional[Dict[str, Any]]:
    query = f"""
        SELECT {SCORED_POSITION_SUMMARY_COLUMNS}
        FROM scored_position_summary
        WHERE id = 1;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        row = result.mappings().first()

    if not row:
        return None
    return _scored_position_summary_from_row(row)


async def refresh_scored_position_summary() -> Dict[str, Any]:
    """
    Rebuilds scored-position aggregates. This scans only scored FEN rows and
    should run in refresh paths, not page-render code.
    """
    summary = await ensure_database_summary()
    query = f"""
        WITH scored AS (
            SELECT
                COUNT(*) FILTER (WHERE ABS(score) < 50)::bigint AS equal_positions,
                COUNT(*) FILTER (WHERE ABS(score) >= 50 AND ABS(score) < 150)::bigint AS small_positions,
                COUNT(*) FILTER (WHERE ABS(score) >= 150 AND ABS(score) < 300)::bigint AS clear_positions,
                COUNT(*) FILTER (WHERE ABS(score) >= 300 AND ABS(score) < 9000)::bigint AS decisive_positions,
                COUNT(*) FILTER (WHERE ABS(score) >= 9000)::bigint AS mate_positions,
                COALESCE(SUM(n_games) FILTER (WHERE ABS(score) < 50), 0)::bigint AS equal_appearances,
                COALESCE(SUM(n_games) FILTER (WHERE ABS(score) >= 50 AND ABS(score) < 150), 0)::bigint AS small_appearances,
                COALESCE(SUM(n_games) FILTER (WHERE ABS(score) >= 150 AND ABS(score) < 300), 0)::bigint AS clear_appearances,
                COALESCE(SUM(n_games) FILTER (WHERE ABS(score) >= 300 AND ABS(score) < 9000), 0)::bigint AS decisive_appearances,
                COALESCE(SUM(n_games) FILTER (WHERE ABS(score) >= 9000), 0)::bigint AS mate_appearances,
                COALESCE(SUM(ABS(score)) FILTER (WHERE ABS(score) < 50), 0)::double precision AS equal_abs_score_sum,
                COALESCE(SUM(ABS(score)) FILTER (WHERE ABS(score) >= 50 AND ABS(score) < 150), 0)::double precision AS small_abs_score_sum,
                COALESCE(SUM(ABS(score)) FILTER (WHERE ABS(score) >= 150 AND ABS(score) < 300), 0)::double precision AS clear_abs_score_sum,
                COALESCE(SUM(ABS(score)) FILTER (WHERE ABS(score) >= 300 AND ABS(score) < 9000), 0)::double precision AS decisive_abs_score_sum,
                COALESCE(SUM(ABS(score)) FILTER (WHERE ABS(score) >= 9000), 0)::double precision AS mate_abs_score_sum,
                COUNT(*) FILTER (WHERE score > 50)::bigint AS white_better,
                COUNT(*) FILTER (WHERE score < -50)::bigint AS black_better,
                COUNT(*) FILTER (WHERE ABS(score) <= 50)::bigint AS balanced,
                COALESCE(SUM(score), 0)::double precision AS score_sum,
                COALESCE(SUM(ABS(score)), 0)::double precision AS abs_score_sum,
                COALESCE(SUM(wdl_win) FILTER (WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL), 0)::double precision AS wdl_win_sum,
                COALESCE(SUM(wdl_draw) FILTER (WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL), 0)::double precision AS wdl_draw_sum,
                COALESCE(SUM(wdl_loss) FILTER (WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL), 0)::double precision AS wdl_loss_sum,
                COUNT(*) FILTER (WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL)::bigint AS wdl_positions
            FROM fen
            WHERE score IS NOT NULL
        )
        INSERT INTO scored_position_summary (
            id,
            {SCORED_POSITION_SUMMARY_COLUMNS},
            refreshed_at
        )
        SELECT
            1,
            :total_positions,
            :analyzed_fens,
            :scored_positions,
            :nonzero_scored_fens,
            :unscored_fens,
            scored.equal_positions,
            scored.small_positions,
            scored.clear_positions,
            scored.decisive_positions,
            scored.mate_positions,
            scored.equal_appearances,
            scored.small_appearances,
            scored.clear_appearances,
            scored.decisive_appearances,
            scored.mate_appearances,
            scored.equal_abs_score_sum,
            scored.small_abs_score_sum,
            scored.clear_abs_score_sum,
            scored.decisive_abs_score_sum,
            scored.mate_abs_score_sum,
            scored.white_better,
            scored.black_better,
            scored.balanced,
            scored.score_sum,
            scored.abs_score_sum,
            scored.wdl_win_sum,
            scored.wdl_draw_sum,
            scored.wdl_loss_sum,
            scored.wdl_positions,
            CURRENT_TIMESTAMP
        FROM scored
        ON CONFLICT (id) DO UPDATE SET
            total_positions = EXCLUDED.total_positions,
            analyzed_fens = EXCLUDED.analyzed_fens,
            scored_positions = EXCLUDED.scored_positions,
            nonzero_scored_fens = EXCLUDED.nonzero_scored_fens,
            unscored_fens = EXCLUDED.unscored_fens,
            equal_positions = EXCLUDED.equal_positions,
            small_positions = EXCLUDED.small_positions,
            clear_positions = EXCLUDED.clear_positions,
            decisive_positions = EXCLUDED.decisive_positions,
            mate_positions = EXCLUDED.mate_positions,
            equal_appearances = EXCLUDED.equal_appearances,
            small_appearances = EXCLUDED.small_appearances,
            clear_appearances = EXCLUDED.clear_appearances,
            decisive_appearances = EXCLUDED.decisive_appearances,
            mate_appearances = EXCLUDED.mate_appearances,
            equal_abs_score_sum = EXCLUDED.equal_abs_score_sum,
            small_abs_score_sum = EXCLUDED.small_abs_score_sum,
            clear_abs_score_sum = EXCLUDED.clear_abs_score_sum,
            decisive_abs_score_sum = EXCLUDED.decisive_abs_score_sum,
            mate_abs_score_sum = EXCLUDED.mate_abs_score_sum,
            white_better = EXCLUDED.white_better,
            black_better = EXCLUDED.black_better,
            balanced = EXCLUDED.balanced,
            score_sum = EXCLUDED.score_sum,
            abs_score_sum = EXCLUDED.abs_score_sum,
            wdl_win_sum = EXCLUDED.wdl_win_sum,
            wdl_draw_sum = EXCLUDED.wdl_draw_sum,
            wdl_loss_sum = EXCLUDED.wdl_loss_sum,
            wdl_positions = EXCLUDED.wdl_positions,
            refreshed_at = EXCLUDED.refreshed_at
        RETURNING {SCORED_POSITION_SUMMARY_COLUMNS};
    """
    params = {
        "total_positions": int(summary.get("n_positions") or 0),
        "analyzed_fens": int(summary.get("analyzed_fens") or 0),
        "scored_positions": int(summary.get("analyzed_fens") or 0),
        "nonzero_scored_fens": int(summary.get("nonzero_scored_fens") or 0),
        "unscored_fens": int(summary.get("unscored_fens") or 0),
    }
    async with AsyncDBSession() as session:
        try:
            result = await session.execute(text(query), params)
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return _scored_position_summary_from_row(row or {})


async def increment_scored_position_summary_for_scored_fens(
    scored_fens: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Incrementally updates scored-position aggregates for FENs that transitioned
    from unscored to scored.
    """
    clean_rows = []
    seen_fens: Set[str] = set()
    for item in scored_fens or []:
        fen_value = str(item.get("fen") or "").strip()
        if not fen_value or fen_value in seen_fens:
            continue
        seen_fens.add(fen_value)
        try:
            score_value = float(item.get("score") or 0.0)
        except (TypeError, ValueError):
            score_value = 0.0

        clean_rows.append({
            "fen": fen_value,
            "score": score_value,
            "wdl_win": item.get("wdl_win"),
            "wdl_draw": item.get("wdl_draw"),
            "wdl_loss": item.get("wdl_loss"),
        })

    if not clean_rows:
        return {"fens": 0}

    if await _read_scored_position_summary() is None:
        await refresh_scored_position_summary()

    async with AsyncDBSession() as session:
        try:
            await session.execute(text("""
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_scored_position_summary_fens (
                    fen VARCHAR PRIMARY KEY,
                    score DOUBLE PRECISION NOT NULL,
                    wdl_win DOUBLE PRECISION NULL,
                    wdl_draw DOUBLE PRECISION NULL,
                    wdl_loss DOUBLE PRECISION NULL
                ) ON COMMIT DROP;
            """))
            for start in range(0, len(clean_rows), 1000):
                chunk = clean_rows[start:start + 1000]
                values = ", ".join(
                    f"(:fen_{idx}, :score_{idx}, :wdl_win_{idx}, :wdl_draw_{idx}, :wdl_loss_{idx})"
                    for idx in range(len(chunk))
                )
                params: Dict[str, Any] = {}
                for idx, row in enumerate(chunk):
                    params[f"fen_{idx}"] = row["fen"]
                    params[f"score_{idx}"] = row["score"]
                    params[f"wdl_win_{idx}"] = row["wdl_win"]
                    params[f"wdl_draw_{idx}"] = row["wdl_draw"]
                    params[f"wdl_loss_{idx}"] = row["wdl_loss"]
                await session.execute(text(f"""
                    INSERT INTO temp_scored_position_summary_fens (
                        fen,
                        score,
                        wdl_win,
                        wdl_draw,
                        wdl_loss
                    )
                    VALUES {values}
                    ON CONFLICT (fen) DO UPDATE SET
                        score = EXCLUDED.score,
                        wdl_win = EXCLUDED.wdl_win,
                        wdl_draw = EXCLUDED.wdl_draw,
                        wdl_loss = EXCLUDED.wdl_loss;
                """), params)

            update_query = f"""
                WITH delta AS (
                    SELECT
                        COUNT(*)::bigint AS analyzed_delta,
                        COUNT(*) FILTER (WHERE tsf.score <> 0)::bigint AS nonzero_delta,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) < 50)::bigint AS equal_positions,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 50 AND ABS(tsf.score) < 150)::bigint AS small_positions,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 150 AND ABS(tsf.score) < 300)::bigint AS clear_positions,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 300 AND ABS(tsf.score) < 9000)::bigint AS decisive_positions,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 9000)::bigint AS mate_positions,
                        COALESCE(SUM(f.n_games) FILTER (WHERE ABS(tsf.score) < 50), 0)::bigint AS equal_appearances,
                        COALESCE(SUM(f.n_games) FILTER (WHERE ABS(tsf.score) >= 50 AND ABS(tsf.score) < 150), 0)::bigint AS small_appearances,
                        COALESCE(SUM(f.n_games) FILTER (WHERE ABS(tsf.score) >= 150 AND ABS(tsf.score) < 300), 0)::bigint AS clear_appearances,
                        COALESCE(SUM(f.n_games) FILTER (WHERE ABS(tsf.score) >= 300 AND ABS(tsf.score) < 9000), 0)::bigint AS decisive_appearances,
                        COALESCE(SUM(f.n_games) FILTER (WHERE ABS(tsf.score) >= 9000), 0)::bigint AS mate_appearances,
                        COALESCE(SUM(ABS(tsf.score)) FILTER (WHERE ABS(tsf.score) < 50), 0)::double precision AS equal_abs_score_sum,
                        COALESCE(SUM(ABS(tsf.score)) FILTER (WHERE ABS(tsf.score) >= 50 AND ABS(tsf.score) < 150), 0)::double precision AS small_abs_score_sum,
                        COALESCE(SUM(ABS(tsf.score)) FILTER (WHERE ABS(tsf.score) >= 150 AND ABS(tsf.score) < 300), 0)::double precision AS clear_abs_score_sum,
                        COALESCE(SUM(ABS(tsf.score)) FILTER (WHERE ABS(tsf.score) >= 300 AND ABS(tsf.score) < 9000), 0)::double precision AS decisive_abs_score_sum,
                        COALESCE(SUM(ABS(tsf.score)) FILTER (WHERE ABS(tsf.score) >= 9000), 0)::double precision AS mate_abs_score_sum,
                        COUNT(*) FILTER (WHERE tsf.score > 50)::bigint AS white_better,
                        COUNT(*) FILTER (WHERE tsf.score < -50)::bigint AS black_better,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) <= 50)::bigint AS balanced,
                        COALESCE(SUM(tsf.score), 0)::double precision AS score_sum,
                        COALESCE(SUM(ABS(tsf.score)), 0)::double precision AS abs_score_sum,
                        COALESCE(SUM(tsf.wdl_win) FILTER (WHERE tsf.wdl_win IS NOT NULL AND tsf.wdl_draw IS NOT NULL AND tsf.wdl_loss IS NOT NULL), 0)::double precision AS wdl_win_sum,
                        COALESCE(SUM(tsf.wdl_draw) FILTER (WHERE tsf.wdl_win IS NOT NULL AND tsf.wdl_draw IS NOT NULL AND tsf.wdl_loss IS NOT NULL), 0)::double precision AS wdl_draw_sum,
                        COALESCE(SUM(tsf.wdl_loss) FILTER (WHERE tsf.wdl_win IS NOT NULL AND tsf.wdl_draw IS NOT NULL AND tsf.wdl_loss IS NOT NULL), 0)::double precision AS wdl_loss_sum,
                        COUNT(*) FILTER (WHERE tsf.wdl_win IS NOT NULL AND tsf.wdl_draw IS NOT NULL AND tsf.wdl_loss IS NOT NULL)::bigint AS wdl_positions
                    FROM temp_scored_position_summary_fens tsf
                    JOIN fen f ON f.fen = tsf.fen
                )
                UPDATE scored_position_summary sps
                SET
                    analyzed_fens = sps.analyzed_fens + delta.analyzed_delta,
                    scored_positions = sps.scored_positions + delta.analyzed_delta,
                    nonzero_scored_fens = sps.nonzero_scored_fens + delta.nonzero_delta,
                    unscored_fens = GREATEST(0, sps.unscored_fens - delta.analyzed_delta),
                    equal_positions = sps.equal_positions + delta.equal_positions,
                    small_positions = sps.small_positions + delta.small_positions,
                    clear_positions = sps.clear_positions + delta.clear_positions,
                    decisive_positions = sps.decisive_positions + delta.decisive_positions,
                    mate_positions = sps.mate_positions + delta.mate_positions,
                    equal_appearances = sps.equal_appearances + delta.equal_appearances,
                    small_appearances = sps.small_appearances + delta.small_appearances,
                    clear_appearances = sps.clear_appearances + delta.clear_appearances,
                    decisive_appearances = sps.decisive_appearances + delta.decisive_appearances,
                    mate_appearances = sps.mate_appearances + delta.mate_appearances,
                    equal_abs_score_sum = sps.equal_abs_score_sum + delta.equal_abs_score_sum,
                    small_abs_score_sum = sps.small_abs_score_sum + delta.small_abs_score_sum,
                    clear_abs_score_sum = sps.clear_abs_score_sum + delta.clear_abs_score_sum,
                    decisive_abs_score_sum = sps.decisive_abs_score_sum + delta.decisive_abs_score_sum,
                    mate_abs_score_sum = sps.mate_abs_score_sum + delta.mate_abs_score_sum,
                    white_better = sps.white_better + delta.white_better,
                    black_better = sps.black_better + delta.black_better,
                    balanced = sps.balanced + delta.balanced,
                    score_sum = sps.score_sum + delta.score_sum,
                    abs_score_sum = sps.abs_score_sum + delta.abs_score_sum,
                    wdl_win_sum = sps.wdl_win_sum + delta.wdl_win_sum,
                    wdl_draw_sum = sps.wdl_draw_sum + delta.wdl_draw_sum,
                    wdl_loss_sum = sps.wdl_loss_sum + delta.wdl_loss_sum,
                    wdl_positions = sps.wdl_positions + delta.wdl_positions,
                    refreshed_at = CURRENT_TIMESTAMP
                FROM delta
                WHERE sps.id = 1
                RETURNING {SCORED_POSITION_SUMMARY_COLUMNS};
            """
            result = await session.execute(text(update_query))
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {"fens": len(clean_rows)}


async def ensure_scored_position_summary() -> Dict[str, Any]:
    summary = await _read_scored_position_summary()
    if summary is not None:
        return summary
    return await refresh_scored_position_summary()


async def get_games_database_generalities() -> Dict[str, int]:
    """
    Returns core database generalities for the games dashboard:
    - total games in DB
    - players with joined set (main characters)
    - players with joined missing/zero (secondary characters)
    - total positions (fens)
    - scored fens (score != 0)
    """
    row = await ensure_database_summary()
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
    row = await ensure_database_summary()
    return {
        "bullet": int(row.get("bullet_games") or 0),
        "blitz": int(row.get("blitz_games") or 0),
        "rapid": int(row.get("rapid_games") or 0)
    }


async def get_main_character_time_control_counts() -> Dict[str, int]:
    """
    Returns normalized game counts for bullet/blitz/rapid where at least one
    main character (joined != 0 and not NULL) is present.
    """
    query = """
        SELECT
            gp.mode,
            COUNT(DISTINCT gp.link)::int AS total
        FROM game_player gp
        JOIN player p ON p.player_name = gp.player_name
        WHERE gp.mode IN ('bullet', 'blitz', 'rapid')
          AND p.joined IS NOT NULL
          AND p.joined <> 0
        GROUP BY gp.mode;
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


async def refresh_main_character_mode_summary() -> Dict[str, int]:
    """
    Rebuilds the small precomputed table used by main-character time-control filters.
    The expensive scan happens here instead of on every filter click.
    """
    delete_query = "DELETE FROM main_character_mode_summary;"
    insert_query = """
        INSERT INTO main_character_mode_summary (
            mode,
            player_name,
            n_games,
            rating,
            avg_game_rating,
            last_rating,
            wins,
            draws,
            losses,
            as_white,
            as_black,
            last_game_at,
            refreshed_at
        )
        WITH main_player_rows AS (
            SELECT
                gp.mode,
                gp.player_name,
                gp.rating,
                gp.result,
                gp.color,
                gp.played_at
            FROM game_player gp
            JOIN player p ON p.player_name = gp.player_name
            WHERE p.joined IS NOT NULL
              AND p.joined <> 0
              AND gp.mode IN ('bullet', 'blitz', 'rapid')
        ),
        aggregated AS (
            SELECT
                mode,
                player_name,
                COUNT(*)::int AS n_games,
                ROUND(AVG(rating))::int AS avg_game_rating,
                SUM(CASE WHEN result = 1.0 THEN 1 ELSE 0 END)::int AS wins,
                SUM(CASE WHEN result = 0.5 THEN 1 ELSE 0 END)::int AS draws,
                SUM(CASE WHEN result = 0.0 THEN 1 ELSE 0 END)::int AS losses,
                SUM(CASE WHEN color = 'white' THEN 1 ELSE 0 END)::int AS as_white,
                SUM(CASE WHEN color = 'black' THEN 1 ELSE 0 END)::int AS as_black
            FROM main_player_rows
            GROUP BY mode, player_name
        ),
        latest_per_player AS (
            SELECT DISTINCT ON (mode, player_name)
                mode,
                player_name,
                rating::int AS last_game_rating,
                played_at AS last_game_at
            FROM main_player_rows
            ORDER BY mode, player_name, played_at DESC
        )
        SELECT
            a.mode,
            a.player_name,
            a.n_games,
            lp.last_game_rating::int AS rating,
            a.avg_game_rating::int AS avg_game_rating,
            lp.last_game_rating::int AS last_rating,
            a.wins,
            a.draws,
            a.losses,
            a.as_white,
            a.as_black,
            lp.last_game_at,
            CURRENT_TIMESTAMP AS refreshed_at
        FROM aggregated a
        LEFT JOIN latest_per_player lp
          ON lp.mode = a.mode
         AND lp.player_name = a.player_name
        RETURNING mode;
    """

    async with AsyncDBSession() as session:
        try:
            await session.execute(text(delete_query))
            result = await session.execute(text(insert_query))
            rows = result.mappings().all()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    counts = {"bullet": 0, "blitz": 0, "rapid": 0}
    for row in rows:
        mode = str(row.get("mode") or "")
        if mode in counts:
            counts[mode] += 1
    counts["total"] = sum(counts.values())
    return counts


async def refresh_main_character_mode_summary_for_players(player_names: Set[str]) -> Dict[str, int]:
    """
    Refreshes main-character mode summaries only for the affected players.
    Non-main players are deleted from the summary and not reinserted.
    """
    clean_players = tuple(sorted({
        str(player_name).strip().lower()
        for player_name in player_names or set()
        if str(player_name or "").strip()
    }))
    counts = {"bullet": 0, "blitz": 0, "rapid": 0, "total": 0}
    if not clean_players:
        return counts

    async with AsyncDBSession() as session:
        try:
            await session.execute(text("""
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_main_summary_players (
                    player_name VARCHAR PRIMARY KEY
                ) ON COMMIT DROP;
            """))
            for start in range(0, len(clean_players), 1000):
                chunk = clean_players[start:start + 1000]
                values = ", ".join(f"(:player_{idx})" for idx in range(len(chunk)))
                params = {f"player_{idx}": player_name for idx, player_name in enumerate(chunk)}
                await session.execute(text(f"""
                    INSERT INTO temp_main_summary_players (player_name)
                    VALUES {values}
                    ON CONFLICT DO NOTHING;
                """), params)

            await session.execute(text("""
                DELETE FROM main_character_mode_summary summary
                USING temp_main_summary_players players
                WHERE summary.player_name = players.player_name;
            """))

            insert_query = """
                INSERT INTO main_character_mode_summary (
                    mode,
                    player_name,
                    n_games,
                    rating,
                    avg_game_rating,
                    last_rating,
                    wins,
                    draws,
                    losses,
                    as_white,
                    as_black,
                    last_game_at,
                    refreshed_at
                )
                WITH main_player_rows AS (
                    SELECT
                        gp.mode,
                        gp.player_name,
                        gp.rating,
                        gp.result,
                        gp.color,
                        gp.played_at
                    FROM game_player gp
                    JOIN temp_main_summary_players affected
                      ON affected.player_name = gp.player_name
                    JOIN player p ON p.player_name = gp.player_name
                    WHERE p.joined IS NOT NULL
                      AND p.joined <> 0
                      AND gp.mode IN ('bullet', 'blitz', 'rapid')
                ),
                aggregated AS (
                    SELECT
                        mode,
                        player_name,
                        COUNT(*)::int AS n_games,
                        ROUND(AVG(rating))::int AS avg_game_rating,
                        SUM(CASE WHEN result = 1.0 THEN 1 ELSE 0 END)::int AS wins,
                        SUM(CASE WHEN result = 0.5 THEN 1 ELSE 0 END)::int AS draws,
                        SUM(CASE WHEN result = 0.0 THEN 1 ELSE 0 END)::int AS losses,
                        SUM(CASE WHEN color = 'white' THEN 1 ELSE 0 END)::int AS as_white,
                        SUM(CASE WHEN color = 'black' THEN 1 ELSE 0 END)::int AS as_black
                    FROM main_player_rows
                    GROUP BY mode, player_name
                ),
                latest_per_player AS (
                    SELECT DISTINCT ON (mode, player_name)
                        mode,
                        player_name,
                        rating::int AS last_game_rating,
                        played_at AS last_game_at
                    FROM main_player_rows
                    ORDER BY mode, player_name, played_at DESC
                )
                SELECT
                    a.mode,
                    a.player_name,
                    a.n_games,
                    lp.last_game_rating::int AS rating,
                    a.avg_game_rating::int AS avg_game_rating,
                    lp.last_game_rating::int AS last_rating,
                    a.wins,
                    a.draws,
                    a.losses,
                    a.as_white,
                    a.as_black,
                    lp.last_game_at,
                    CURRENT_TIMESTAMP AS refreshed_at
                FROM aggregated a
                LEFT JOIN latest_per_player lp
                  ON lp.mode = a.mode
                 AND lp.player_name = a.player_name
                RETURNING mode;
            """
            result = await session.execute(text(insert_query))
            rows = result.mappings().all()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    for row in rows:
        mode = str(row.get("mode") or "")
        if mode in counts:
            counts[mode] += 1
    counts["total"] = sum(counts.values())
    return counts


async def ensure_main_character_mode_summary() -> Dict[str, Any]:
    """
    Builds the summary table when it is empty. Existing rows are trusted because
    ingestion refreshes the table after writing games.
    """
    query = """
        SELECT
            (SELECT COUNT(*) FROM main_character_mode_summary)::int AS summary_rows,
            (SELECT COUNT(*) FROM game)::int AS games,
            (
                SELECT COUNT(*)
                FROM player
                WHERE joined IS NOT NULL
                  AND joined <> 0
            )::int AS main_players;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        row = result.mappings().first()

    summary_rows = int(row.get("summary_rows") or 0) if row else 0
    games = int(row.get("games") or 0) if row else 0
    main_players = int(row.get("main_players") or 0) if row else 0

    if summary_rows == 0 and games > 0 and main_players > 0:
        return {
            "summary_rows": 0,
            "refreshed": True,
            "counts": await refresh_main_character_mode_summary()
        }

    return {
        "summary_rows": summary_rows,
        "games": games,
        "main_players": main_players,
        "refreshed": False
    }


async def sync_game_analytics_projection(game_links: Optional[Tuple[int, ...]] = None) -> Dict[str, int]:
    """
    Syncs additive analytical projection tables for either all games or a specific
    set of game links. Postgres remains the source of truth.
    """
    clean_links = tuple(sorted({int(link) for link in game_links or () if link is not None}))
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


async def get_top_main_characters_by_time_control(
    time_control: str,
    limit: int = 5
) -> Dict[str, Any]:
    """
    Returns main characters for one normalized time control from the precomputed summary.
    """
    target_mode = (time_control or "").strip().lower()
    if target_mode not in {"bullet", "blitz", "rapid"}:
        return {"time_control": target_mode, "players": [], "limit": 0}

    # Keep a high cap so the bubble chart can include the full mode population.
    safe_limit = max(1, min(int(limit), 5000))

    await ensure_main_character_mode_summary()

    query = """
        SELECT
            s.player_name,
            s.n_games,
            s.rating,
            s.avg_game_rating,
            s.last_rating,
            s.wins,
            s.draws,
            s.losses,
            s.as_white,
            s.as_black,
            TO_CHAR(s.last_game_at, 'YYYY-MON-DD') AS last_game_date,
            p.name AS full_name,
            p.avatar,
            p.url AS profile_url
        FROM main_character_mode_summary s
        LEFT JOIN player p ON p.player_name = s.player_name
        WHERE s.mode = :mode
        ORDER BY s.rating DESC NULLS LAST, s.n_games DESC, s.player_name ASC
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

    query = """
        SELECT
            rating,
            COUNT(*)::int AS appearances
        FROM game_player
        WHERE mode = :mode
          AND rating IS NOT NULL
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
            WHERE g.mode = :mode
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

    count_query = """
        WITH aggregated AS (
            SELECT opening
            FROM game_opening
            WHERE mode = :mode
              AND n_moves = :n_moves
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
              )
            GROUP BY opening
        )
        SELECT COUNT(*)::int AS total
        FROM aggregated;
    """

    data_query = """
        SELECT
            opening,
            COUNT(*)::int AS times_played,
            ROUND(AVG(avg_elo))::int AS mean_rating_for_this_opening
        FROM game_opening
        WHERE mode = :mode
          AND n_moves = :n_moves
          AND opening IS NOT NULL
          AND opening <> ''
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
              )
        GROUP BY opening
        ORDER BY times_played DESC, opening ASC
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query), {
            "mode": target_mode,
            "n_moves": safe_n_moves,
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

    query = """
        WITH filtered_games AS (
            SELECT
                g.white_result,
                g.black_result
            FROM game g
            WHERE g.mode = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
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

    summary_query = """
        WITH filtered_games AS (
            SELECT g.n_moves, g.time_elapsed
            FROM game g
            WHERE g.mode = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
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

    moves_hist_query = """
        WITH filtered_games AS (
            SELECT g.n_moves
            FROM game g
            WHERE g.mode = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
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

    elapsed_hist_query = """
        WITH filtered_games AS (
            SELECT g.time_elapsed
            FROM game g
            WHERE g.mode = :mode
              AND (
                    CAST(:min_rating AS INTEGER) IS NULL
                    OR CAST(:max_rating AS INTEGER) IS NULL
                    OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
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

    month_query = """
        SELECT g.month::int AS month_idx, COUNT(*)::int AS total
        FROM game g
        WHERE g.mode = :mode
          AND (
                CAST(:min_rating AS INTEGER) IS NULL
                OR CAST(:max_rating AS INTEGER) IS NULL
                OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
          )
        GROUP BY month_idx
        ORDER BY month_idx;
    """

    weekday_query = """
        SELECT
            EXTRACT(DOW FROM g.played_at)::int AS dow_idx,
            COUNT(*)::int AS total
        FROM game g
        WHERE g.mode = :mode
          AND (
                CAST(:min_rating AS INTEGER) IS NULL
                OR CAST(:max_rating AS INTEGER) IS NULL
                OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
          )
        GROUP BY dow_idx
        ORDER BY dow_idx;
    """

    hour_query = """
        SELECT g.hour::int AS hour_idx, COUNT(*)::int AS total
        FROM game g
        WHERE g.mode = :mode
          AND (
                CAST(:min_rating AS INTEGER) IS NULL
                OR CAST(:max_rating AS INTEGER) IS NULL
                OR g.avg_elo BETWEEN CAST(:min_rating AS NUMERIC) AND CAST(:max_rating AS NUMERIC)
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


async def get_fen_analysis_counts() -> Dict[str, int]:
    """
    Returns FEN analysis coverage counts.
    analyzed_fens counts every FEN with a stored score, including score 0.
    """
    row = await ensure_database_summary()
    return {
        "total_fens": int(row.get("n_positions") or 0),
        "analyzed_fens": int(row.get("analyzed_fens") or 0),
        "unscored_fens": int(row.get("unscored_fens") or 0),
        "nonzero_scored_fens": int(row.get("nonzero_scored_fens") or 0),
    }


async def get_scored_positions_overview() -> Dict[str, Any]:
    """
    Returns aggregate analysis for scored FENs from the precomputed projection.
    """
    return await ensure_scored_position_summary()


async def _build_scored_advantage_by_rating_payload() -> Dict[str, Any]:
    """
    Splits fully analyzed games into three avg_elo groups using weighted Jenks
    natural breaks, then counts scored position occurrences by advantage bucket
    inside each group.
    """
    labels = [
        ("bad", "Bad"),
        ("medium", "Medium"),
        ("great", "Great")
    ]
    bucket_template = [
        {"key": "equal", "label": "Equal", "order": 1, "positions": 0},
        {"key": "small", "label": "Small", "order": 2, "positions": 0},
        {"key": "clear", "label": "Clear", "order": 3, "positions": 0},
        {"key": "decisive", "label": "Decisive", "order": 4, "positions": 0},
        {"key": "mate", "label": "Mate", "order": 5, "positions": 0},
    ]
    rating_query = """
        SELECT
            ROUND(g.avg_elo::numeric)::int AS rating,
            COUNT(*)::int AS games
        FROM game_analysis_summary gas
        JOIN game g ON g.link = gas.link
        WHERE gas.is_fully_analyzed = true
          AND gas.total_positions > 0
          AND g.avg_elo > 0
        GROUP BY ROUND(g.avg_elo::numeric)::int
        ORDER BY rating ASC;
    """
    rating_points_query = """
        SELECT
            g.link,
            ROW_NUMBER() OVER (
                ORDER BY ROUND(g.avg_elo::numeric)::int ASC, g.link ASC
            )::int AS x_index,
            ROUND(g.avg_elo::numeric)::int AS rating,
            COUNT(*) OVER ()::int AS total_games
        FROM game_analysis_summary gas
        JOIN game g ON g.link = gas.link
        WHERE gas.is_fully_analyzed = true
          AND gas.total_positions > 0
          AND g.avg_elo > 0
        ORDER BY rating ASC, g.link ASC;
    """

    async with AsyncDBSession() as session:
        rating_result = await session.execute(text(rating_query))
        rating_rows = rating_result.mappings().all()
        rating_points_result = await session.execute(text(rating_points_query))
        rating_point_rows = rating_points_result.mappings().all()

        x_values = [int(row.get("rating") or 0) for row in rating_rows]
        y_values = [int(row.get("games") or 0) for row in rating_rows]
        jenks_ranges = _weighted_jenks_breaks(x_values, y_values, n_classes=3)

        groups = []
        for idx, (group_key, group_label) in enumerate(labels):
            if idx < len(jenks_ranges):
                min_rating, max_rating = jenks_ranges[idx]
            else:
                min_rating, max_rating = 0, 0
            groups.append({
                "key": group_key,
                "label": group_label,
                "min_rating": int(min_rating or 0),
                "max_rating": int(max_rating or 0),
                "games": 0,
                "positions": 0,
                "buckets": [dict(bucket) for bucket in bucket_template],
            })

        valid_groups = [group for group in groups if group["min_rating"] > 0 or group["max_rating"] > 0]
        if not valid_groups:
            return {
                "rating_basis": "avg_elo",
                "groups": groups,
                "ratings": []
            }

        values_sql = ", ".join(
            f"("
            f"CAST(:group_key_{idx} AS TEXT), "
            f"CAST(:group_label_{idx} AS TEXT), "
            f"CAST(:min_rating_{idx} AS INTEGER), "
            f"CAST(:max_rating_{idx} AS INTEGER), "
            f"CAST(:group_order_{idx} AS INTEGER)"
            f")"
            for idx, _ in enumerate(valid_groups)
        )
        params: Dict[str, Any] = {}
        for idx, group in enumerate(valid_groups):
            params[f"group_key_{idx}"] = group["key"]
            params[f"group_label_{idx}"] = group["label"]
            params[f"min_rating_{idx}"] = group["min_rating"]
            params[f"max_rating_{idx}"] = group["max_rating"]
            params[f"group_order_{idx}"] = idx + 1

        group_summary_query = f"""
            WITH rating_groups(group_key, group_label, min_rating, max_rating, group_order) AS (
                VALUES {values_sql}
            ),
            scoped_games AS (
                SELECT
                    rg.group_key,
                    gas.link,
                    gas.total_positions
                FROM rating_groups rg
                JOIN game g
                  ON ROUND(g.avg_elo::numeric)::int BETWEEN rg.min_rating AND rg.max_rating
                JOIN game_analysis_summary gas ON gas.link = g.link
                WHERE gas.is_fully_analyzed = true
                  AND gas.total_positions > 0
                  AND g.avg_elo > 0
            )
            SELECT
                group_key,
                COUNT(*)::bigint AS games,
                COALESCE(SUM(total_positions), 0)::bigint AS positions
            FROM scoped_games
            GROUP BY group_key;
        """
        bucket_query = f"""
            WITH rating_groups(group_key, group_label, min_rating, max_rating, group_order) AS (
                VALUES {values_sql}
            ),
            scoped_games AS (
                SELECT
                    rg.group_key,
                    gas.equal_positions,
                    gas.small_positions,
                    gas.clear_positions,
                    gas.decisive_positions,
                    gas.mate_positions
                FROM rating_groups rg
                JOIN game g
                  ON ROUND(g.avg_elo::numeric)::int BETWEEN rg.min_rating AND rg.max_rating
                JOIN game_analysis_summary gas ON gas.link = g.link
                WHERE gas.is_fully_analyzed = true
                  AND gas.total_positions > 0
                  AND g.avg_elo > 0
            )
            SELECT
                group_key,
                bucket_key,
                bucket_label,
                bucket_order,
                SUM(positions)::bigint AS positions
            FROM (
                SELECT group_key, 'equal' AS bucket_key, 'Equal' AS bucket_label, 1 AS bucket_order, equal_positions AS positions FROM scoped_games
                UNION ALL
                SELECT group_key, 'small' AS bucket_key, 'Small' AS bucket_label, 2 AS bucket_order, small_positions AS positions FROM scoped_games
                UNION ALL
                SELECT group_key, 'clear' AS bucket_key, 'Clear' AS bucket_label, 3 AS bucket_order, clear_positions AS positions FROM scoped_games
                UNION ALL
                SELECT group_key, 'decisive' AS bucket_key, 'Decisive' AS bucket_label, 4 AS bucket_order, decisive_positions AS positions FROM scoped_games
                UNION ALL
                SELECT group_key, 'mate' AS bucket_key, 'Mate' AS bucket_label, 5 AS bucket_order, mate_positions AS positions FROM scoped_games
            ) bucketed
            GROUP BY group_key, bucket_key, bucket_label, bucket_order
            ORDER BY group_key, bucket_order;
        """

        summary_result = await session.execute(text(group_summary_query), params)
        summary_rows = summary_result.mappings().all()
        bucket_result = await session.execute(text(bucket_query), params)
        bucket_rows = bucket_result.mappings().all()

    groups_by_key = {group["key"]: group for group in groups}
    for row in summary_rows:
        group = groups_by_key.get(str(row.get("group_key") or ""))
        if not group:
            continue
        group["games"] = int(row.get("games") or 0)
        group["positions"] = int(row.get("positions") or 0)

    for row in bucket_rows:
        group = groups_by_key.get(str(row.get("group_key") or ""))
        if not group:
            continue
        bucket_key = str(row.get("bucket_key") or "")
        for bucket in group["buckets"]:
            if bucket["key"] == bucket_key:
                bucket["positions"] = int(row.get("positions") or 0)
                break

    rating_points = []
    for row in rating_point_rows:
        link = int(row.get("link") or 0)
        x_index = int(row.get("x_index") or 0)
        rating = int(row.get("rating") or 0)
        group_key = ""
        for group in groups:
            if group["min_rating"] <= rating <= group["max_rating"]:
                group_key = group["key"]
                break
        if not group_key and groups:
            if rating < groups[0]["min_rating"]:
                group_key = groups[0]["key"]
            elif rating > groups[-1]["max_rating"]:
                group_key = groups[-1]["key"]
        rating_points.append({
            "link": link,
            "x_index": x_index,
            "rating": rating,
            "group": group_key
        })

    return {
        "rating_basis": "avg_elo",
        "groups": groups,
        "ratings": rating_points
    }


async def _read_scored_rating_summary() -> Optional[Dict[str, Any]]:
    query = """
        SELECT
            rating_basis,
            source_full_games,
            source_distinct_ratings,
            groups_payload,
            ratings_payload
        FROM scored_rating_summary
        WHERE id = 1;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        row = result.mappings().first()

    if not row:
        return None

    return {
        "rating_basis": str(row.get("rating_basis") or "avg_elo"),
        "groups": _json_payload(row.get("groups_payload"), []),
        "ratings": _json_payload(row.get("ratings_payload"), []),
    }


async def refresh_scored_rating_summary() -> Dict[str, Any]:
    """
    Rebuilds the rating-group chart projection. The weighted Jenks calculation
    is intentionally kept out of page-render requests.
    """
    payload = await _build_scored_advantage_by_rating_payload()
    groups = payload.get("groups") or []
    ratings = payload.get("ratings") or []
    distinct_ratings = len({int(point.get("rating") or 0) for point in ratings})

    query = """
        INSERT INTO scored_rating_summary (
            id,
            rating_basis,
            source_full_games,
            source_distinct_ratings,
            groups_payload,
            ratings_payload,
            refreshed_at
        )
        VALUES (
            1,
            :rating_basis,
            :source_full_games,
            :source_distinct_ratings,
            CAST(:groups_payload AS JSONB),
            CAST(:ratings_payload AS JSONB),
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (id) DO UPDATE SET
            rating_basis = EXCLUDED.rating_basis,
            source_full_games = EXCLUDED.source_full_games,
            source_distinct_ratings = EXCLUDED.source_distinct_ratings,
            groups_payload = EXCLUDED.groups_payload,
            ratings_payload = EXCLUDED.ratings_payload,
            refreshed_at = EXCLUDED.refreshed_at
        RETURNING rating_basis, groups_payload, ratings_payload;
    """
    params = {
        "rating_basis": str(payload.get("rating_basis") or "avg_elo"),
        "source_full_games": len(ratings),
        "source_distinct_ratings": distinct_ratings,
        "groups_payload": json.dumps(groups),
        "ratings_payload": json.dumps(ratings),
    }
    async with AsyncDBSession() as session:
        try:
            result = await session.execute(text(query), params)
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {
        "rating_basis": str(row.get("rating_basis") or "avg_elo") if row else params["rating_basis"],
        "groups": _json_payload(row.get("groups_payload"), groups) if row else groups,
        "ratings": _json_payload(row.get("ratings_payload"), ratings) if row else ratings,
    }


async def get_scored_advantage_by_rating() -> Dict[str, Any]:
    summary = await _read_scored_rating_summary()
    if summary is not None:
        return summary
    return await refresh_scored_rating_summary()


async def get_scored_positions_page(
    sort: str = "impact",
    min_abs_score: int = 0,
    page: int = 1,
    page_size: int = 20
) -> Dict[str, Any]:
    """
    Returns paginated scored FEN rows for the scored positions page.
    """
    target_sort = (sort or "impact").strip().lower()
    order_sql_by_sort = {
        "frequency": "n_games DESC, ABS(score) DESC, fen ASC",
        "impact": "(n_games * ABS(score)) DESC, n_games DESC, fen ASC",
        "evaluation": "ABS(score) DESC, n_games DESC, fen ASC"
    }
    order_sql = order_sql_by_sort.get(target_sort, order_sql_by_sort["impact"])
    if target_sort not in order_sql_by_sort:
        target_sort = "impact"

    safe_min_abs_score = max(0, min(int(min_abs_score or 0), 10000))
    safe_page_size = max(1, min(int(page_size or 20), 50))
    safe_page = max(1, int(page or 1))

    where_sql = """
        score IS NOT NULL
        AND ABS(score) >= :min_abs_score
    """
    count_query = f"""
        SELECT COUNT(*)::bigint AS total
        FROM fen
        WHERE {where_sql};
    """
    data_query = f"""
        SELECT
            fen,
            n_games,
            moves_counter,
            next_moves,
            score,
            wdl_win,
            wdl_draw,
            wdl_loss,
            split_part(fen, ' ', 2) AS side_to_move,
            (n_games * ABS(score)) AS impact_score
        FROM fen
        WHERE {where_sql}
        ORDER BY {order_sql}
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query), {
            "min_abs_score": safe_min_abs_score
        })
        count_row = count_result.mappings().first() or {}
        total = int(count_row.get("total") or 0)
        total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0
        if total_pages > 0 and safe_page > total_pages:
            safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size

        data_result = await session.execute(text(data_query), {
            "min_abs_score": safe_min_abs_score,
            "offset": offset,
            "limit": safe_page_size
        })
        rows = data_result.mappings().all()

    return {
        "sort": target_sort,
        "min_abs_score": safe_min_abs_score,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": total_pages,
        "rows": [
            {
                "fen": str(row.get("fen") or ""),
                "n_games": int(row.get("n_games") or 0),
                "moves_counter": str(row.get("moves_counter") or ""),
                "next_moves": str(row.get("next_moves") or ""),
                "score": float(row.get("score") or 0.0),
                "wdl_win": float(row.get("wdl_win") or 0.0) if row.get("wdl_win") is not None else None,
                "wdl_draw": float(row.get("wdl_draw") or 0.0) if row.get("wdl_draw") is not None else None,
                "wdl_loss": float(row.get("wdl_loss") or 0.0) if row.get("wdl_loss") is not None else None,
                "side_to_move": str(row.get("side_to_move") or ""),
                "impact_score": float(row.get("impact_score") or 0.0)
            }
            for row in rows
        ]
    }


async def refresh_game_analysis_summary(game_links: Optional[Tuple[int, ...]] = None) -> Dict[str, int]:
    """
    Rebuilds per-game scored-position coverage. When game_links are provided,
    only those games are refreshed.
    """
    clean_links = tuple(sorted({int(link) for link in game_links or () if link is not None}))
    source_join = ""
    source_filter = ""

    async with AsyncDBSession() as session:
        try:
            if clean_links:
                await session.execute(text("""
                    CREATE TEMPORARY TABLE IF NOT EXISTS temp_game_analysis_links (
                        link BIGINT PRIMARY KEY
                    ) ON COMMIT DROP;
                """))
                for start in range(0, len(clean_links), 1000):
                    chunk = clean_links[start:start + 1000]
                    values = ", ".join(f"({link})" for link in chunk)
                    if values:
                        await session.execute(text(f"""
                            INSERT INTO temp_game_analysis_links (link)
                            VALUES {values}
                            ON CONFLICT DO NOTHING;
                        """))
                source_join = "JOIN temp_game_analysis_links tgal ON tgal.link = gfa.game_link"
                source_filter = "WHERE link IN (SELECT link FROM temp_game_analysis_links)"

            delete_missing_query = f"""
                DELETE FROM game_analysis_summary
                {source_filter}
                  {"AND" if source_filter else "WHERE"} link NOT IN (
                    SELECT DISTINCT gfa.game_link
                    FROM game_fen_association gfa
                    {source_join}
                );
            """
            refresh_query = f"""
                INSERT INTO game_analysis_summary (
                    link,
                    total_positions,
                    analyzed_positions,
                    unscored_positions,
                    is_fully_analyzed,
                    score_sum,
                    abs_score_sum,
                    avg_score,
                    avg_abs_score,
                    max_abs_score,
                    equal_positions,
                    small_positions,
                    clear_positions,
                    decisive_positions,
                    mate_positions,
                    refreshed_at
                )
                SELECT
                    gfa.game_link AS link,
                    COUNT(*)::int AS total_positions,
                    COUNT(f.score)::int AS analyzed_positions,
                    (COUNT(*) - COUNT(f.score))::int AS unscored_positions,
                    (COUNT(*) > 0 AND COUNT(*) = COUNT(f.score)) AS is_fully_analyzed,
                    COALESCE(SUM(f.score) FILTER (WHERE f.score IS NOT NULL), 0)::double precision AS score_sum,
                    COALESCE(SUM(ABS(f.score)) FILTER (WHERE f.score IS NOT NULL), 0)::double precision AS abs_score_sum,
                    (AVG(f.score) FILTER (WHERE f.score IS NOT NULL))::double precision AS avg_score,
                    (AVG(ABS(f.score)) FILTER (WHERE f.score IS NOT NULL))::double precision AS avg_abs_score,
                    (MAX(ABS(f.score)) FILTER (WHERE f.score IS NOT NULL))::double precision AS max_abs_score,
                    COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) < 50)::int AS equal_positions,
                    COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 50 AND ABS(f.score) < 150)::int AS small_positions,
                    COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 150 AND ABS(f.score) < 300)::int AS clear_positions,
                    COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 300 AND ABS(f.score) < 9000)::int AS decisive_positions,
                    COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 9000)::int AS mate_positions,
                    CURRENT_TIMESTAMP AS refreshed_at
                FROM game_fen_association gfa
                JOIN fen f ON f.fen = gfa.fen_fen
                {source_join}
                GROUP BY gfa.game_link
                ON CONFLICT (link) DO UPDATE SET
                    total_positions = EXCLUDED.total_positions,
                    analyzed_positions = EXCLUDED.analyzed_positions,
                    unscored_positions = EXCLUDED.unscored_positions,
                    is_fully_analyzed = EXCLUDED.is_fully_analyzed,
                    score_sum = EXCLUDED.score_sum,
                    abs_score_sum = EXCLUDED.abs_score_sum,
                    avg_score = EXCLUDED.avg_score,
                    avg_abs_score = EXCLUDED.avg_abs_score,
                    max_abs_score = EXCLUDED.max_abs_score,
                    equal_positions = EXCLUDED.equal_positions,
                    small_positions = EXCLUDED.small_positions,
                    clear_positions = EXCLUDED.clear_positions,
                    decisive_positions = EXCLUDED.decisive_positions,
                    mate_positions = EXCLUDED.mate_positions,
                    refreshed_at = EXCLUDED.refreshed_at
                RETURNING link;
            """
            await session.execute(text(delete_missing_query))
            result = await session.execute(text(refresh_query))
            rows = result.mappings().all()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {
        "games": len(rows),
        "scope": "links" if clean_links else "all"
    }


async def increment_game_analysis_summary_for_scored_fens(
    scored_fens: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Updates per-game coverage for a batch of newly scored FENs.
    The caller should pass only FENs that transitioned from unscored to scored.
    """
    clean_rows = []
    seen_fens: Set[str] = set()
    for item in scored_fens or []:
        fen_value = str(item.get("fen") or "").strip()
        if not fen_value or fen_value in seen_fens:
            continue
        seen_fens.add(fen_value)
        try:
            score_value = float(item.get("score") or 0.0)
        except (TypeError, ValueError):
            score_value = 0.0
        clean_rows.append({"fen": fen_value, "score": score_value})

    if not clean_rows:
        return {"games": 0, "fens": 0}

    async with AsyncDBSession() as session:
        try:
            await session.execute(text("""
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_scored_fens (
                    fen VARCHAR PRIMARY KEY,
                    score DOUBLE PRECISION NOT NULL
                ) ON COMMIT DROP;
            """))
            for start in range(0, len(clean_rows), 1000):
                chunk = clean_rows[start:start + 1000]
                values = ", ".join(
                    f"(:fen_{idx}, :score_{idx})"
                    for idx in range(len(chunk))
                )
                params = {}
                for idx, row in enumerate(chunk):
                    params[f"fen_{idx}"] = row["fen"]
                    params[f"score_{idx}"] = row["score"]
                await session.execute(text(f"""
                    INSERT INTO temp_scored_fens (fen, score)
                    VALUES {values}
                    ON CONFLICT (fen) DO UPDATE SET score = EXCLUDED.score;
                """), params)

            update_query = """
                WITH affected AS (
                    SELECT
                        gfa.game_link AS link,
                        COUNT(*)::int AS newly_analyzed,
                        SUM(tsf.score)::double precision AS score_delta,
                        SUM(ABS(tsf.score))::double precision AS abs_score_delta,
                        MAX(ABS(tsf.score))::double precision AS max_abs_delta,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) < 50)::int AS equal_delta,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 50 AND ABS(tsf.score) < 150)::int AS small_delta,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 150 AND ABS(tsf.score) < 300)::int AS clear_delta,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 300 AND ABS(tsf.score) < 9000)::int AS decisive_delta,
                        COUNT(*) FILTER (WHERE ABS(tsf.score) >= 9000)::int AS mate_delta
                    FROM game_fen_association gfa
                    JOIN temp_scored_fens tsf ON tsf.fen = gfa.fen_fen
                    GROUP BY gfa.game_link
                )
                UPDATE game_analysis_summary gas
                SET
                    analyzed_positions = LEAST(gas.total_positions, gas.analyzed_positions + affected.newly_analyzed),
                    unscored_positions = GREATEST(0, gas.unscored_positions - affected.newly_analyzed),
                    is_fully_analyzed = gas.total_positions > 0
                        AND GREATEST(0, gas.unscored_positions - affected.newly_analyzed) = 0,
                    score_sum = gas.score_sum + affected.score_delta,
                    abs_score_sum = gas.abs_score_sum + affected.abs_score_delta,
                    avg_score = CASE
                        WHEN LEAST(gas.total_positions, gas.analyzed_positions + affected.newly_analyzed) > 0
                            THEN (gas.score_sum + affected.score_delta)
                                / LEAST(gas.total_positions, gas.analyzed_positions + affected.newly_analyzed)
                        ELSE NULL
                    END,
                    avg_abs_score = CASE
                        WHEN LEAST(gas.total_positions, gas.analyzed_positions + affected.newly_analyzed) > 0
                            THEN (gas.abs_score_sum + affected.abs_score_delta)
                                / LEAST(gas.total_positions, gas.analyzed_positions + affected.newly_analyzed)
                        ELSE NULL
                    END,
                    max_abs_score = GREATEST(COALESCE(gas.max_abs_score, 0), COALESCE(affected.max_abs_delta, 0)),
                    equal_positions = gas.equal_positions + affected.equal_delta,
                    small_positions = gas.small_positions + affected.small_delta,
                    clear_positions = gas.clear_positions + affected.clear_delta,
                    decisive_positions = gas.decisive_positions + affected.decisive_delta,
                    mate_positions = gas.mate_positions + affected.mate_delta,
                    refreshed_at = CURRENT_TIMESTAMP
                FROM affected
                WHERE gas.link = affected.link
                RETURNING gas.link, gas.is_fully_analyzed;
            """
            result = await session.execute(text(update_query))
            rows = result.mappings().all()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {
        "games": len(rows),
        "fully_analyzed_games": sum(1 for row in rows if bool(row.get("is_fully_analyzed"))),
        "fens": len(clean_rows)
    }


async def get_scored_game_analysis_overview() -> Dict[str, int]:
    query = """
        SELECT
            COUNT(*)::bigint AS games_with_positions,
            COUNT(*) FILTER (WHERE is_fully_analyzed)::bigint AS fully_analyzed_games,
            COUNT(*) FILTER (WHERE NOT is_fully_analyzed AND total_positions > 0)::bigint AS incomplete_games,
            COALESCE(SUM(total_positions), 0)::bigint AS total_game_positions,
            COALESCE(SUM(analyzed_positions), 0)::bigint AS analyzed_game_positions,
            COALESCE(SUM(unscored_positions), 0)::bigint AS unscored_game_positions
        FROM game_analysis_summary;
    """
    async with AsyncDBSession() as session:
        result = await session.execute(text(query))
        row = result.mappings().first() or {}

    return {
        "games_with_positions": int(row.get("games_with_positions") or 0),
        "fully_analyzed_games": int(row.get("fully_analyzed_games") or 0),
        "incomplete_games": int(row.get("incomplete_games") or 0),
        "total_game_positions": int(row.get("total_game_positions") or 0),
        "analyzed_game_positions": int(row.get("analyzed_game_positions") or 0),
        "unscored_game_positions": int(row.get("unscored_game_positions") or 0)
    }


async def get_scored_game_analysis_page(
    status: str = "fully",
    page: int = 1,
    page_size: int = 20
) -> Dict[str, Any]:
    target_status = (status or "fully").strip().lower()
    if target_status not in {"fully", "incomplete", "all"}:
        target_status = "fully"
    safe_page_size = max(1, min(int(page_size or 20), 50))
    safe_page = max(1, int(page or 1))

    where_sql = "gas.total_positions > 0"
    if target_status == "fully":
        where_sql += " AND gas.is_fully_analyzed = true"
        order_sql = "gas.total_positions DESC, gas.max_abs_score DESC NULLS LAST, gas.link DESC"
    elif target_status == "incomplete":
        where_sql += " AND gas.is_fully_analyzed = false"
        order_sql = "gas.unscored_positions DESC, gas.total_positions DESC, gas.link DESC"
    else:
        order_sql = "gas.is_fully_analyzed DESC, gas.total_positions DESC, gas.link DESC"

    count_query = f"""
        SELECT COUNT(*)::bigint AS total
        FROM game_analysis_summary gas
        WHERE {where_sql};
    """
    data_query = f"""
        SELECT
            gas.link,
            gas.total_positions,
            gas.analyzed_positions,
            gas.unscored_positions,
            gas.is_fully_analyzed,
            gas.avg_score,
            gas.avg_abs_score,
            gas.max_abs_score,
            g.white,
            g.black,
            g.mode,
            g.played_at,
            g.white_result,
            g.black_result
        FROM game_analysis_summary gas
        JOIN game g ON g.link = gas.link
        WHERE {where_sql}
        ORDER BY {order_sql}
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query))
        count_row = count_result.mappings().first() or {}
        total = int(count_row.get("total") or 0)
        total_pages = (total + safe_page_size - 1) // safe_page_size if total > 0 else 0
        if total_pages > 0 and safe_page > total_pages:
            safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size
        data_result = await session.execute(text(data_query), {
            "offset": offset,
            "limit": safe_page_size
        })
        rows = data_result.mappings().all()

    return {
        "status": target_status,
        "page": safe_page,
        "page_size": safe_page_size,
        "total": total,
        "total_pages": total_pages,
        "rows": [
            {
                "link": int(row.get("link") or 0),
                "total_positions": int(row.get("total_positions") or 0),
                "analyzed_positions": int(row.get("analyzed_positions") or 0),
                "unscored_positions": int(row.get("unscored_positions") or 0),
                "is_fully_analyzed": bool(row.get("is_fully_analyzed")),
                "avg_score": float(row.get("avg_score") or 0.0) if row.get("avg_score") is not None else None,
                "avg_abs_score": float(row.get("avg_abs_score") or 0.0) if row.get("avg_abs_score") is not None else None,
                "max_abs_score": float(row.get("max_abs_score") or 0.0) if row.get("max_abs_score") is not None else None,
                "white": str(row.get("white") or ""),
                "black": str(row.get("black") or ""),
                "mode": str(row.get("mode") or ""),
                "played_at": row.get("played_at").isoformat() if row.get("played_at") else "",
                "white_result": float(row.get("white_result") or 0.0),
                "black_result": float(row.get("black_result") or 0.0)
            }
            for row in rows
        ]
    }


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
    Counts per-game position coverage for a specific player.
    """
    sql_query = """
        SELECT
            COALESCE(SUM(gas.total_positions), 0)::bigint AS total_positions,
            COALESCE(SUM(gas.analyzed_positions), 0)::bigint AS analyzed_positions,
            COALESCE(SUM(gas.unscored_positions), 0)::bigint AS unscored_positions
        FROM game_player gp
        LEFT JOIN game_analysis_summary gas ON gas.link = gp.link
        WHERE gp.player_name = :player;
    """
    
    async with AsyncDBSession() as session:
        result = await session.execute(text(sql_query), {"player": player_name})
        row = result.mappings().first()

    return {
        "player_name": player_name,
        "total_positions": int((row or {}).get("total_positions") or 0),
        "analyzed_positions": int((row or {}).get("analyzed_positions") or 0),
        "unscored_positions": int((row or {}).get("unscored_positions") or 0),
    }


# --- NEW: STATISTICAL ANALYSIS QUERIES ---

async def get_player_performance_summary(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Calculates aggregate W/L/D stats for a player as White, Black, and Combined.
    """
    sql_query = """
        SELECT
            COUNT(*) AS total_games,
            SUM(CASE WHEN color = 'white' THEN 1 ELSE 0 END) AS white_games,
            SUM(CASE WHEN color = 'white' AND result = 1.0 THEN 1 ELSE 0 END) AS white_wins,
            SUM(CASE WHEN color = 'white' AND result = 0.0 THEN 1 ELSE 0 END) AS white_losses,
            SUM(CASE WHEN color = 'white' AND result = 0.5 THEN 1 ELSE 0 END) AS white_draws,
            SUM(CASE WHEN color = 'black' THEN 1 ELSE 0 END) AS black_games,
            SUM(CASE WHEN color = 'black' AND result = 1.0 THEN 1 ELSE 0 END) AS black_wins,
            SUM(CASE WHEN color = 'black' AND result = 0.0 THEN 1 ELSE 0 END) AS black_losses,
            SUM(CASE WHEN color = 'black' AND result = 0.5 THEN 1 ELSE 0 END) AS black_draws
        FROM game_player
        WHERE player_name = :player;
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
            AVG(opponent_rating) as avg_opponent_rating
        FROM game_player
        WHERE player_name = :player;
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
            SUM(CASE WHEN (gp.color = 'white' AND g.white_str_result = 'checkmated') OR (gp.color = 'black' AND g.black_str_result = 'checkmated') THEN 1 ELSE 0 END) as checkmated,
            SUM(CASE WHEN (gp.color = 'white' AND g.white_str_result = 'resigned') OR (gp.color = 'black' AND g.black_str_result = 'resigned') THEN 1 ELSE 0 END) as resigned,
            SUM(CASE WHEN (gp.color = 'white' AND g.white_str_result = 'timeout') OR (gp.color = 'black' AND g.black_str_result = 'timeout') THEN 1 ELSE 0 END) as timeout,
            SUM(CASE WHEN (gp.color = 'white' AND g.white_str_result = 'abandoned') OR (gp.color = 'black' AND g.black_str_result = 'abandoned') THEN 1 ELSE 0 END) as abandoned
        FROM game_player gp
        JOIN game g ON g.link = gp.link
        WHERE gp.player_name = :player;
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
            SUM(CASE WHEN result = 1.0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 0.0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result = 0.5 THEN 1 ELSE 0 END) as draws
        FROM game_player
        WHERE player_name = :player
          AND eco != 'no_eco'
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
        FROM game_player
        WHERE player_name = :player;
    """

    games_query = """
        SELECT
            link,
            played_at,
            color,
            result AS player_score
        FROM game_player
        WHERE player_name = :player
        ORDER BY played_at DESC, link DESC
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

        played_at_value = row.get("played_at")
        played_at = played_at_value.strftime("%Y-%m-%d %H:%M:%S") if played_at_value else ""

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
            SUM(result) AS win_points,
            SUM(CASE WHEN result = 1.0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN result = 0.0 THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN result = 0.5 THEN 1 ELSE 0 END) AS draws,
            MIN(played_at) AS first_game,
            MAX(played_at) AS last_game,
            COUNT(*) as total_games
        FROM game_player
        WHERE player_name = :player;
    """

    time_control_query = """
        SELECT mode AS time_control, COUNT(*) as total
        FROM game_player
        WHERE player_name = :player
        GROUP BY mode
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

    query = """
        SELECT
            link,
            played_at,
            color,
            result AS player_score
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
        ORDER BY played_at DESC, link DESC;
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

        played_at_value = row.get("played_at")
        played_at = played_at_value.strftime("%Y-%m-%d %H:%M:%S") if played_at_value else ""

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
    query = """
        SELECT
            ROUND((COALESCE(SUM(time_elapsed), 0) / 3600.0)::numeric, 2) AS total_hours,
            ROUND((COALESCE(SUM(CASE WHEN mode = 'bullet' THEN time_elapsed ELSE 0 END), 0) / 3600.0)::numeric, 2) AS bullet_hours,
            ROUND((COALESCE(SUM(CASE WHEN mode = 'blitz' THEN time_elapsed ELSE 0 END), 0) / 3600.0)::numeric, 2) AS blitz_hours,
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
        "bullet_hours": float(row.get("bullet_hours") or 0.0),
        "blitz_hours": float(row.get("blitz_hours") or 0.0),
        "rapid_hours": float(row.get("rapid_hours") or 0.0)
    }


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
    query = """
        WITH player_games AS (
            SELECT link
            FROM game_player
            WHERE player_name = :player
              AND mode = :mode
              AND color = :move_color
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

    count_query = """
        WITH player_openings AS (
            SELECT go.opening
            FROM game_opening go
            JOIN game_player gp ON gp.link = go.link
            WHERE gp.player_name = :player
              AND gp.mode = :mode
              AND gp.result = :target_score
              AND go.n_moves = :n_moves
              AND go.opening IS NOT NULL
              AND go.opening <> ''
        ),
        aggregated AS (
            SELECT opening
            FROM player_openings
            GROUP BY opening
        )
        SELECT COUNT(*)::int AS total FROM aggregated;
    """

    data_query = """
        SELECT
            go.opening,
            COUNT(*)::int AS times_played
        FROM game_opening go
        JOIN game_player gp ON gp.link = go.link
        WHERE gp.player_name = :player
          AND gp.mode = :mode
          AND gp.result = :target_score
          AND go.n_moves = :n_moves
          AND go.opening IS NOT NULL
          AND go.opening <> ''
        GROUP BY go.opening
        ORDER BY times_played DESC, opening ASC
        OFFSET :offset
        LIMIT :limit;
    """

    async with AsyncDBSession() as session:
        count_result = await session.execute(text(count_query), {
            "mode": target_mode,
            "player": player_name,
            "target_score": target_score,
            "n_moves": safe_n_moves
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

    summary_query = """
        SELECT
            COUNT(*)::int AS total_games,
            ROUND(AVG(n_moves)::numeric, 2) AS avg_n_moves,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY n_moves) AS median_n_moves,
            percentile_cont(0.9) WITHIN GROUP (ORDER BY n_moves) AS p90_n_moves,
            ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed_sec
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode;
    """
    moves_hist_query = """
        SELECT
            (FLOOR(n_moves / 10.0) * 10)::int AS bucket_start,
            (FLOOR(n_moves / 10.0) * 10 + 9)::int AS bucket_end,
            COUNT(*)::int AS total
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
        GROUP BY bucket_start, bucket_end
        ORDER BY bucket_start;
    """
    elapsed_hist_query = """
        SELECT
            (FLOOR((time_elapsed / 60.0) / 1.0) * 1)::int AS bucket_start_min,
            (FLOOR((time_elapsed / 60.0) / 1.0) * 1 + 1)::int AS bucket_end_min,
            COUNT(*)::int AS total
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
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
    month_query = """
        SELECT EXTRACT(MONTH FROM played_at)::int AS idx, COUNT(*)::int AS total
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
        GROUP BY idx
        ORDER BY idx;
    """
    weekday_query = """
        SELECT EXTRACT(DOW FROM played_at)::int AS idx, COUNT(*)::int AS total
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
        GROUP BY idx
        ORDER BY idx;
    """
    hour_query = """
        SELECT EXTRACT(HOUR FROM played_at)::int AS idx, COUNT(*)::int AS total
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
        GROUP BY idx
        ORDER BY idx;
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


async def get_player_modes_stats(player_name: str) -> Dict[str, Dict[str, int]]:
    """
    Returns per-mode stats for a player keyed by normalized mode.
    """
    query = """
        SELECT
            mode,
            color,
            rating,
            played_at
        FROM game_player
        WHERE player_name = :player
        ORDER BY played_at ASC, link ASC;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name})
        rows = result.mappings().all()

    by_mode: Dict[str, Dict[str, int]] = {}
    for row in rows:
        mode = str(row.get("mode") or "unknown")
        as_white = row.get("color") == "white"
        rating = int(row.get("rating") or 0)

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
    target_mode = (mode or "").strip().lower()
    query = """
        SELECT
            rating,
            played_at
        FROM game_player
        WHERE player_name = :player
          AND mode = :mode
        ORDER BY played_at ASC, link ASC;
    """

    async with AsyncDBSession() as session:
        result = await session.execute(text(query), {"player": player_name, "mode": target_mode})
        rows = result.mappings().all()

    mode_points: List[Tuple[datetime, str, int]] = []
    for row in rows:
        dt = row.get("played_at")
        if not dt:
            continue

        rating = int(row.get("rating") or 0)
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
