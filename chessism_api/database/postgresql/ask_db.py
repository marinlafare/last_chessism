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
from chessism_api.scored_positions import summary_payload as scored_summary_payload

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


async def refresh_database_summary(*, session_factory=None) -> Dict[str, int]:
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
    async with (session_factory or AsyncDBSession)() as session:
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
                game_counts.n_games_in_db,
                player_counts.main_characters,
                player_counts.secondary_characters,
                game_counts.bullet_games,
                game_counts.blitz_games,
                game_counts.rapid_games
            FROM (
                SELECT
                    COUNT(*)::bigint AS n_games_in_db,
                    COUNT(*) FILTER (WHERE mode = 'bullet')::bigint AS bullet_games,
                    COUNT(*) FILTER (WHERE mode = 'blitz')::bigint AS blitz_games,
                    COUNT(*) FILTER (WHERE mode = 'rapid')::bigint AS rapid_games
                FROM game
            ) AS game_counts
            CROSS JOIN (
                SELECT
                    COUNT(*) FILTER (
                        WHERE joined IS NOT NULL AND joined <> 0
                    )::bigint AS main_characters,
                    COUNT(*) FILTER (
                        WHERE joined IS NULL OR joined = 0
                    )::bigint AS secondary_characters
                FROM player
            ) AS player_counts
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


FEN_INGESTION_APPEARANCE_FIELDS = (
    "equal_appearances",
    "small_appearances",
    "clear_appearances",
    "decisive_appearances",
    "mate_appearances",
)


async def get_fen_ingestion_summary(fen_values: List[str]) -> Dict[str, int]:
    """Read summary inputs only for FENs touched by one ingestion pass."""
    unique_fens = list(dict.fromkeys(str(fen) for fen in fen_values if fen))
    summary = {
        "positions": 0,
        **{field: 0 for field in FEN_INGESTION_APPEARANCE_FIELDS},
    }
    if not unique_fens:
        return summary

    query = text("""
        SELECT
            COUNT(*)::bigint AS positions,
            COALESCE(SUM(n_games) FILTER (
                WHERE score IS NOT NULL AND ABS(score) < 50
            ), 0)::bigint AS equal_appearances,
            COALESCE(SUM(n_games) FILTER (
                WHERE score IS NOT NULL AND ABS(score) >= 50 AND ABS(score) < 150
            ), 0)::bigint AS small_appearances,
            COALESCE(SUM(n_games) FILTER (
                WHERE score IS NOT NULL AND ABS(score) >= 150 AND ABS(score) < 300
            ), 0)::bigint AS clear_appearances,
            COALESCE(SUM(n_games) FILTER (
                WHERE score IS NOT NULL AND ABS(score) >= 300 AND ABS(score) < 9000
            ), 0)::bigint AS decisive_appearances,
            COALESCE(SUM(n_games) FILTER (
                WHERE score IS NOT NULL AND ABS(score) >= 9000
            ), 0)::bigint AS mate_appearances
        FROM fen
        WHERE fen = ANY(CAST(:fen_values AS VARCHAR[]));
    """)
    async with AsyncDBSession() as session:
        for start in range(0, len(unique_fens), 5_000):
            result = await session.execute(
                query,
                {"fen_values": unique_fens[start:start + 5_000]},
            )
            row = result.mappings().first() or {}
            summary["positions"] += int(row.get("positions") or 0)
            for field in FEN_INGESTION_APPEARANCE_FIELDS:
                summary[field] += int(row.get(field) or 0)

    return summary


def fen_ingestion_summary_delta(
    before: Dict[str, int],
    after: Dict[str, int],
) -> Dict[str, int]:
    """Calculate idempotent cache deltas from affected-FEN snapshots."""
    return {
        "new_positions": max(
            0,
            int(after.get("positions") or 0) - int(before.get("positions") or 0),
        ),
        **{
            field: int(after.get(field) or 0) - int(before.get(field) or 0)
            for field in FEN_INGESTION_APPEARANCE_FIELDS
        },
    }


async def increment_fen_ingestion_summaries(
    before: Dict[str, int],
    after: Dict[str, int],
) -> Dict[str, int]:
    """Apply one ingestion pass without rescanning the complete FEN table."""
    delta = fen_ingestion_summary_delta(before, after)

    database_summary = await _read_database_summary()
    scored_summary = await _read_scored_position_summary()
    if database_summary is None or scored_summary is None:
        # First-run recovery is intentionally exhaustive; normal ingestion uses
        # the O(affected FENs) update below.
        await refresh_database_summary_fen_counts()
        await refresh_scored_position_summary()
        return {**delta, "reconciled": 1}

    if not any(delta.values()):
        return {**delta, "reconciled": 0}

    params = {
        "new_positions": delta["new_positions"],
        **{
            f"{field}_delta": delta[field]
            for field in FEN_INGESTION_APPEARANCE_FIELDS
        },
    }
    async with AsyncDBSession() as session:
        try:
            await session.execute(text("""
                UPDATE database_summary
                SET
                    n_positions = n_positions + :new_positions,
                    unscored_fens = unscored_fens + :new_positions,
                    refreshed_at = CURRENT_TIMESTAMP
                WHERE id = 1;
            """), params)
            await session.execute(text("""
                UPDATE scored_position_summary
                SET
                    total_positions = total_positions + :new_positions,
                    unscored_fens = unscored_fens + :new_positions,
                    equal_appearances = GREATEST(
                        0, equal_appearances + :equal_appearances_delta
                    ),
                    small_appearances = GREATEST(
                        0, small_appearances + :small_appearances_delta
                    ),
                    clear_appearances = GREATEST(
                        0, clear_appearances + :clear_appearances_delta
                    ),
                    decisive_appearances = GREATEST(
                        0, decisive_appearances + :decisive_appearances_delta
                    ),
                    mate_appearances = GREATEST(
                        0, mate_appearances + :mate_appearances_delta
                    ),
                    refreshed_at = CURRENT_TIMESTAMP
                WHERE id = 1;
            """), params)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {**delta, "reconciled": 0}


async def ensure_database_summary() -> Dict[str, int]:
    """
    Returns the precomputed summary, building it only if the row does not exist.
    """
    summary = await _read_database_summary()
    if summary is not None:
        return summary
    return await refresh_database_summary()


def _fen_pipeline_summary_from_row(row: Any) -> Dict[str, Any]:
    return {
        "parsed_games": int(row.get("parsed_games") or 0),
        "analyzable_games": int(row.get("analyzable_games") or 0),
        "excluded_games": int(row.get("excluded_games") or 0),
        "fen_extracted_games": int(row.get("fen_extracted_games") or 0),
        "tablebase_marked_games": int(row.get("tablebase_marked_games") or 0),
        "refreshed_at": row.get("refreshed_at").isoformat()
        if row.get("refreshed_at") else None,
    }


async def refresh_fen_pipeline_summary(
    *,
    mark_tablebase_current: bool = False,
    session_factory=None,
) -> Dict[str, Any]:
    """Refresh stage totals once after pipeline writes, never during UI polling."""
    tablebase_insert = "counts.fen_extracted_games" if mark_tablebase_current else "0"
    tablebase_update = (
        "EXCLUDED.fen_extracted_games"
        if mark_tablebase_current
        else "LEAST(fen_pipeline_summary.tablebase_marked_games, EXCLUDED.fen_extracted_games)"
    )
    query = f"""
        INSERT INTO fen_pipeline_summary (
            id,
            parsed_games,
            analyzable_games,
            excluded_games,
            fen_extracted_games,
            tablebase_marked_games,
            refreshed_at
        )
        SELECT
            1,
            counts.parsed_games,
            counts.analyzable_games,
            counts.excluded_games,
            counts.fen_extracted_games,
            {tablebase_insert},
            CURRENT_TIMESTAMP
        FROM (
            SELECT
                (SELECT COUNT(*)::bigint FROM game) AS parsed_games,
                (
                    SELECT COUNT(*)::bigint
                    FROM game
                    WHERE rules = 'chess'
                ) AS analyzable_games,
                (
                    SELECT COUNT(*)::bigint
                    FROM game
                    WHERE rules <> 'chess'
                ) AS excluded_games,
                (
                    SELECT COUNT(*)::bigint
                    FROM game_analysis_summary summary
                    JOIN game game_row ON game_row.link = summary.link
                    WHERE summary.total_positions > 0
                      AND game_row.rules = 'chess'
                ) AS fen_extracted_games
        ) counts
        ON CONFLICT (id) DO UPDATE SET
            parsed_games = EXCLUDED.parsed_games,
            analyzable_games = EXCLUDED.analyzable_games,
            excluded_games = EXCLUDED.excluded_games,
            fen_extracted_games = EXCLUDED.fen_extracted_games,
            tablebase_marked_games = {tablebase_update},
            refreshed_at = CURRENT_TIMESTAMP
        RETURNING parsed_games, analyzable_games, excluded_games,
                  fen_extracted_games, tablebase_marked_games, refreshed_at;
    """
    async with (session_factory or AsyncDBSession)() as session:
        try:
            result = await session.execute(text(query))
            row = result.mappings().first() or {}
            await session.commit()
        except Exception:
            await session.rollback()
            raise
    return _fen_pipeline_summary_from_row(row)


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
    return scored_summary_payload(row)


async def refresh_scored_position_summary(*, session_factory=None, database_summary=None) -> Dict[str, Any]:
    """
    Rebuilds scored-position aggregates. This scans only scored FEN rows and
    should run in refresh paths, not page-render code.
    """
    if session_factory is not None and database_summary is None:
        raise ValueError('An explicit session factory requires its own database summary')
    summary = database_summary if database_summary is not None else await ensure_database_summary()
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
    async with (session_factory or AsyncDBSession)() as session:
        try:
            result = await session.execute(text(query), params)
            row = result.mappings().first()
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return scored_summary_payload(row or {})


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
                RETURNING sps.*;
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
