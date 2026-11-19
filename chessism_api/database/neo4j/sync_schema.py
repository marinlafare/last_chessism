"""Explicit, additive PostgreSQL outbox installation; no startup DDL.

Statement triggers capture committed changes from every writer, including older
running cloud/ingestion workers. Only IDs are queued, never copied FEN payloads.
"""
from sqlalchemy import text

LOCK = 731946299
VERSION = "chess_graph_v1"


async def available(session):
    return bool(await session.scalar(text("SELECT to_regclass('graph_sync.control') IS NOT NULL")))


async def status(session):
    if not await available(session):
        return {"installed": False, "enabled": False, "phase": "not_installed"}
    control = dict((await session.execute(text('SELECT * FROM graph_sync.control WHERE id'))).mappings().one())
    pending = dict((await session.execute(text('''SELECT count(*) AS pending_events,
        COALESCE(sum(cardinality(ids)-offset_items),0) AS pending_items,
        count(*) FILTER(WHERE error IS NOT NULL) AS failed_events,
        min(created_at) AS oldest_pending_at FROM graph_sync.event WHERE completed_at IS NULL'''))).mappings().one())
    runs = [dict(row) for row in (await session.execute(text('''SELECT b.id,b.player_name,
        cardinality(b.game_ids) AS total_games,b.created_at,b.verification,
        COALESCE(sum(CASE WHEN e.completed_at IS NOT NULL THEN cardinality(e.ids) ELSE e.offset_items END),0) AS processed_games
        FROM graph_sync.backfill b LEFT JOIN graph_sync.event e ON e.batch_id=b.id
        GROUP BY b.id ORDER BY b.created_at DESC LIMIT 5'''))).mappings()]
    return {"installed": True, **control, **pending, "backfills": runs,
            "count_scope": "Imported graph only; not whole-system PostgreSQL totals"}
