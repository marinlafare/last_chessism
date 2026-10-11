"""Shared appearance histograms. HTTP reads never aggregate the FEN table.

Rebuilds run on the analysis queue, at most once per five minutes while this
view is used and the database summary has changed. The displayed timestamp
identifies the snapshot; concurrent ingestion is picked up on a later refresh.
"""
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession

LOG = logging.getLogger(__name__)
REFRESH_SECONDS = 300
JOB_ID = "fen-appearance-summary-v1"
LOCK_ID = 731_946_218


def needs_refresh(meta, source_updated_at, now=None):
    now = now or datetime.now(timezone.utc)
    # Back off failures and debounce repeated requests during ingestion/import.
    previous = meta.get("attempted_at") or meta.get("refreshed_at")
    if previous and (now - previous).total_seconds() < REFRESH_SECONDS:
        return False
    return (meta.get("refreshed_at") is None
            or meta.get("last_error") is not None
            or meta.get("source_updated_at") != source_updated_at)


async def get_fen_appearances(redis):
    # One statement so bin replacement and its timestamp are always consistent.
    async with AsyncDBSession() as session:
        rows = (await session.execute(text("""
            SELECT m.refreshed_at, m.source_updated_at, m.attempted_at, m.last_error,
                   d.refreshed_at AS current_source_at,
                   b.appearances, b.total_fens, b.unscored_fens
            FROM (SELECT 1 AS id) singleton
            LEFT JOIN fen_appearance_summary m ON m.id = singleton.id
            LEFT JOIN database_summary d ON d.id = singleton.id
            LEFT JOIN fen_appearance_bin b ON TRUE
            ORDER BY b.appearances
        """))).mappings().all()
    meta = dict(rows[0])
    refresh_error = meta.get("last_error")
    if needs_refresh(meta, meta.get("current_source_at")):
        try:
            await redis.enqueue_job("run_pending_repetition_refresh",
                                    _job_id=JOB_ID, _queue_name="analysis_queue")
        except Exception:
            LOG.exception("Could not enqueue FEN appearance refresh")
            refresh_error = "Summary refresh unavailable; try again later."
    refreshed_at = meta.get("refreshed_at")
    from chessism_api.operations.common_positions import get_common_positions
    commonness = await get_common_positions(redis, meta.get("current_source_at"), total_repeated=sum(
        int(row["total_fens"]) for row in rows if row["appearances"] is not None and row["appearances"] > 1))
    return {
        "definition": "all_fens",
        "refreshed_at": refreshed_at.isoformat() if refreshed_at else None,
        "stale": refreshed_at is None or meta.get("source_updated_at") != meta.get("current_source_at"),
        "refresh_error": refresh_error,
        "commonness": commonness,
        "rows": [{"appearances": int(row["appearances"]),
                  "total_fens": int(row["total_fens"]), "unscored_fens": int(row["unscored_fens"])}
                 for row in rows if row["appearances"] is not None],
    }


async def get_pending_repetitions(redis):
    """Keep the existing endpoint compatible, backed by the same snapshot."""
    payload = await get_fen_appearances(redis)
    return {**payload, "definition": "score_is_null", "rows": [
        {"repetitions": row["appearances"], "fen_count": row["unscored_fens"]}
        for row in payload["rows"] if row["unscored_fens"] > 0
    ]}


async def run_pending_repetition_refresh(ctx):
    async with AsyncDBSession() as session:
        await session.execute(text("""
            INSERT INTO fen_appearance_summary (id, attempted_at)
            VALUES (1, clock_timestamp()) ON CONFLICT (id)
            DO UPDATE SET attempted_at = EXCLUDED.attempted_at
        """))
        await session.commit()
    try:
        async with AsyncDBSession() as session:
            # A consistent snapshot, and atomic publication of bins + metadata.
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await session.execute(text("SET LOCAL statement_timeout = '180s'"))
            await session.execute(text("SET LOCAL lock_timeout = '5s'"))
            await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 2"))
            locked = (await session.execute(text(
                "SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_ID})).scalar()
            if not locked:
                return {"status": "already_refreshing"}
            source = (await session.execute(text(
                "SELECT refreshed_at FROM database_summary WHERE id = 1"))).scalar()
            await session.execute(text("DELETE FROM fen_appearance_bin"))
            await session.execute(text("""
                INSERT INTO fen_appearance_bin (appearances, total_fens, unscored_fens)
                SELECT n_games, COUNT(*)::bigint,
                       COUNT(*) FILTER (WHERE score IS NULL)::bigint
                FROM fen GROUP BY n_games
            """))
            await session.execute(text("""
                UPDATE fen_appearance_summary
                SET source_updated_at = :source, refreshed_at = clock_timestamp(), last_error = NULL
                WHERE id = 1
            """), {"source": source})
            await session.commit()
    except Exception:
        LOG.exception("FEN appearance refresh failed; retaining previous snapshot")
        async with AsyncDBSession() as session:
            await session.execute(text("""
                UPDATE fen_appearance_summary
                SET last_error = 'Summary refresh failed; the previous snapshot is retained.'
                WHERE id = 1
            """))
            await session.commit()
        raise
    return {"status": "complete"}
