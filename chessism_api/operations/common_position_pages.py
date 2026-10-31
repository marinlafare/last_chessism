"""On-demand, ten-position pages; no deep OFFSET scans or unbounded SQL cache.

Later pages expire from Redis after an hour. Their cursor carries the source
snapshot plus the last count/FEN, so ties paginate deterministically. Changed
source data requires starting again instead of silently mixing rankings.
"""
import asyncio
import base64
import hashlib
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.common_positions import describe_positions, move_label

LOG = logging.getLogger(__name__)
CACHE_SECONDS = 3600


def page_cursor(last, snapshot):
    raw = json.dumps({"v": 1, "fen": last["fen"], "appearances": last["appearances"],
                      "rank": last["rank"], "snapshot": snapshot}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def parse_cursor(cursor):
    try:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 768:
            raise ValueError()
        data = json.loads(base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True))
        if not isinstance(data, dict) or data.get("v") != 1:
            raise ValueError()
        if not isinstance(data.get("fen"), str) or not 1 <= len(data["fen"]) <= 120 or len(data["fen"].split()) != 4:
            raise ValueError()
        if type(data.get("appearances")) is not int or not 1 < data["appearances"] <= 2**53 - 1:
            raise ValueError()
        if type(data.get("rank")) is not int or not 10 <= data["rank"] <= 2**53 - 11 or data["rank"] % 10:
            raise ValueError()
        stamp = data.get("snapshot")
        if stamp is not None and (not isinstance(stamp, str) or datetime.fromisoformat(stamp).tzinfo is None):
            raise ValueError()
        return data
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise ValueError("Invalid common-position cursor.") from None


def cache_key(cursor):
    return "fen-commonness-page-v1:" + hashlib.sha256(cursor.encode()).hexdigest()


def changed_payload():
    return {"status": "changed", "detail": "Positions changed. Return to the first ten for the updated ranking."}


async def current_snapshot(session):
    stamp = (await session.execute(text("SELECT refreshed_at FROM database_summary WHERE id = 1"))).scalar()
    return stamp.isoformat() if stamp else None


async def get_common_position_page(redis, cursor, retry=False):
    after = parse_cursor(cursor)
    async with AsyncDBSession() as session:
        if await current_snapshot(session) != after["snapshot"]:
            return changed_payload()
    key = cache_key(cursor)
    cached = await redis.get(key)
    if cached:
        payload = json.loads(cached)
        if not (retry and payload.get("status") == "error"):
            return payload
        await redis.delete(key)
    await redis.enqueue_job("run_commonness_page_refresh", cursor,
                            _job_id=key, _queue_name="analysis_queue")
    return {"status": "pending", "start_rank": after["rank"] + 1, "rows": []}


async def build_page(cursor, redis=None):
    after = parse_cursor(cursor)
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text("SET LOCAL statement_timeout = '90s'"))
        await session.execute(text("SET LOCAL lock_timeout = '5s'"))
        await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 2"))
        await session.execute(text("SET LOCAL jit = off"))
        # Common and rare FENs need different plans even after prepared queries
        # have been reused; do not let the generic estimate serialize hot FENs.
        await session.execute(text("SET LOCAL plan_cache_mode = force_custom_plan"))
        if await current_snapshot(session) != after["snapshot"]:
            return changed_payload()
        rows = (await session.execute(text("""
            SELECT fen, fen_id, n_games AS appearances FROM fen
            WHERE n_games > 1 AND n_games <= :appearances
              AND (n_games < :appearances OR (n_games = :appearances AND fen > :fen))
            ORDER BY n_games DESC, fen LIMIT 11
        """), {"appearances": after["appearances"], "fen": after["fen"]})).mappings().all()
        described = await describe_positions(session, rows[:10], after["rank"] + 1,
                                              redis=redis, snapshot=after["snapshot"])
    output = [{"rank": row["rank"], "fen": row["fen"], "appearances": row["appearances"],
               "label": move_label(row["n_move"], row["move_color"], row["san"]),
               "label_occurrences": row["label_occurrences"], "example_line": row["example_line"]}
              for row in described]
    return {"status": "complete", "snapshot": after["snapshot"], "start_rank": after["rank"] + 1,
            "refreshed_at": datetime.now(timezone.utc).isoformat(), "stale": False, "refresh_error": None,
            "rows": output, "next_cursor": page_cursor(output[-1], after["snapshot"]) if len(rows) > 10 else None}


async def run_commonness_page_refresh(ctx, cursor):
    parse_cursor(cursor)
    key = cache_key(cursor)
    try:
        payload = await asyncio.wait_for(build_page(cursor, redis=ctx["redis"]), timeout=540)
        await ctx["redis"].set(key, json.dumps(payload), ex=CACHE_SECONDS)
    except Exception:
        LOG.exception("Common-position page refresh failed")
        await ctx["redis"].set(key, json.dumps({"status": "error", "detail": "Could not prepare these positions. Please retry."}), ex=60)
        raise
    return {"status": payload["status"]}
