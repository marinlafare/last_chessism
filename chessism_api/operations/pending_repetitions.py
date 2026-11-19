"""Shared appearance histograms. HTTP reads never aggregate the FEN table.

Rebuilds run on the analysis queue, at most once per five minutes while this
view is used and the database summary has changed. The displayed timestamp
identifies the snapshot; concurrent ingestion is picked up on a later refresh.
"""
import logging
from datetime import datetime, timezone


from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import fen_appearances as persistence
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction

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


async def get_fen_appearances(redis, *, session_factory=None):
    # One statement so bin replacement and its timestamp are always consistent.
    async with read_transaction(session_factory or AsyncDBSession) as session:
        rows = await persistence.read_bins(session)
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
        int(row["total_fens"]) for row in rows if row["appearances"] is not None and row["appearances"] > 1), session_factory=session_factory)
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


async def get_pending_repetitions(redis, *, session_factory=None):
    """Keep the existing endpoint compatible, backed by the same snapshot."""
    payload = await get_fen_appearances(redis, session_factory=session_factory)
    return {**payload, "definition": "score_is_null", "rows": [
        {"repetitions": row["appearances"], "fen_count": row["unscored_fens"]}
        for row in payload["rows"] if row["unscored_fens"] > 0
    ]}


async def run_pending_repetition_refresh(ctx):
    factory = ctx.get('session_factory') or AsyncDBSession
    async with write_transaction(factory) as session:
        attempt = await persistence.mark_attempt(session, 'appearance')
    try:
        async with write_transaction(factory) as session:
            await persistence.execute(session, 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            await persistence.read_limits(session, 180)
            if not await persistence.try_lock(session, LOCK_ID):
                return {"status": "already_refreshing"}
            source = await persistence.source_stamp(session)
            await persistence.replace_bins(session)
            await persistence.publish_stamp(session, 'appearance', source)
    except Exception:
        LOG.exception("FEN appearance refresh failed; retaining previous snapshot")
        try:
            async with write_transaction(factory) as session:
                await persistence.mark_failure(session, 'appearance', attempt,
                    'Summary refresh failed; the previous snapshot is retained.')
        except Exception:
            LOG.exception('Could not record histogram failure; preserving the refresh error')
        raise
    return {"status": "complete"}
