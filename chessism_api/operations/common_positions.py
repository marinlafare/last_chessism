"""Ten cached FEN identities, modal arrival labels and verified example lines.

Only the background worker joins game occurrences to moves. Readers use the
small cache and retain the previous complete snapshot during refresh/failure.
"""
import asyncio
import logging

import chess
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.common_position_descriptions import description_keys, load_descriptions, save_description
from chessism_api.operations.pending_repetitions import needs_refresh

LOG = logging.getLogger(__name__)
JOB_ID = "fen-commonness-summary-v1"
LOCK_ID = 731_946_231

# LATERAL LIMIT avoids duplicate legacy move rows multiplying appearances.
MODAL_MOVE_SQL = """
    SELECT a.n_move, a.move_color,
           CASE WHEN a.move_color = 'white' THEN m.white_move ELSE m.black_move END AS san,
           COUNT(*)::bigint AS label_occurrences, MIN(a.game_link) AS example_game_link
    FROM game_fen_association a
    JOIN LATERAL (
        SELECT white_move, black_move FROM moves
        WHERE link = a.game_link AND n_move = a.n_move ORDER BY id LIMIT 1
    ) m ON TRUE
    WHERE a.fen_id = (SELECT fen_id FROM fen WHERE fen = :fen)
      AND a.n_move > 0 AND a.move_color IN ('white', 'black')
      AND NULLIF(BTRIM(CASE WHEN a.move_color = 'white' THEN m.white_move ELSE m.black_move END), '') IS NOT NULL
      AND CASE WHEN a.move_color = 'white' THEN m.white_move ELSE m.black_move END <> '--'
    GROUP BY a.n_move, a.move_color, san
    ORDER BY label_occurrences DESC, a.n_move, a.move_color, san
    LIMIT 1
"""


def compact_modal_sql(layout):
    # Use the numeric identity and native boolean, not the compatibility view.
    # Otherwise the planner badly underestimates common FENs and serializes
    # tens of thousands of LATERAL lookups. Compact moves have a unique PK.
    return f"""
        SELECT a.n_move, CASE WHEN a.is_white THEN 'white' ELSE 'black' END AS move_color,
               CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END AS san,
               COUNT(*)::bigint AS label_occurrences, MIN(a.game_link) AS example_game_link
        FROM {layout.table('game_fen_association')} a
        JOIN {layout.table('moves')} m ON m.link = a.game_link AND m.n_move = a.n_move
        WHERE a.fen_id = :fen_id AND a.n_move > 0 AND a.is_white IS NOT NULL
          AND NULLIF(BTRIM(CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END), '') IS NOT NULL
          AND CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END <> '--'
        GROUP BY a.n_move, a.is_white, san
        ORDER BY label_occurrences DESC, a.n_move, move_color, san LIMIT 1
    """


def move_label(n_move, color, san):
    if not n_move or color not in ("white", "black") or not san:
        return "Position"
    return f"{n_move}{'.' if color == 'white' else '...'} {san}"


def verified_example_line(rows, n_move, color, target_fen):
    """Never display a sequence that doesn't actually reach the cached board."""
    if not n_move or n_move > 100 or color not in ("white", "black"):
        return None
    board, tokens = chess.Board(), []
    try:
        for expected, row in enumerate(rows, 1):
            if int(row["n_move"]) != expected:
                return None
            for side, key in (("white", "white_move"), ("black", "black_move")):
                if expected > n_move or (expected == n_move and color == "white" and side == "black"):
                    return None
                san = row[key]
                move = board.parse_san(san)
                canonical_san = board.san(move)
                tokens.append(f"{expected}. {canonical_san}" if side == "white" else canonical_san)
                board.push(move)
                if expected == n_move and side == color:
                    return " ".join(tokens) if " ".join(board.fen().split()[:4]) == target_fen else None
    except (ValueError, TypeError, KeyError):
        return None
    return None


async def get_common_positions(redis, source_updated_at, total_repeated=None):
    async with AsyncDBSession() as session:
        rows = (await session.execute(text("""
            SELECT m.refreshed_at, m.source_updated_at, m.attempted_at, m.last_error,
                   p.rank, p.fen, p.appearances, p.n_move, p.move_color, p.san,
                   p.label_occurrences, p.example_line
            FROM (SELECT 1 AS id) singleton
            LEFT JOIN fen_commonness_summary m ON m.id = singleton.id
            LEFT JOIN fen_commonness_position p ON TRUE ORDER BY p.rank
        """))).mappings().all()
    meta = dict(rows[0])
    error = meta.get("last_error")
    if needs_refresh(meta, source_updated_at):
        try:
            await redis.enqueue_job("run_commonness_refresh", _job_id=JOB_ID, _queue_name="analysis_queue")
        except Exception:
            LOG.exception("Could not enqueue common-position refresh")
            error = "Position labels unavailable; try again later."
    stamp = meta.get("refreshed_at")
    payload = {"refreshed_at": stamp.isoformat() if stamp else None,
            "stale": stamp is None or meta.get("source_updated_at") != source_updated_at,
            "refresh_error": error, "rows": [
                {"rank": row["rank"], "fen": row["fen"], "appearances": row["appearances"],
                 "label": move_label(row["n_move"], row["move_color"], row["san"]),
                 "label_occurrences": row["label_occurrences"], "example_line": row["example_line"]}
                for row in rows if row["rank"] is not None]}
    from chessism_api.operations.common_position_pages import page_cursor
    snapshot = meta.get("source_updated_at")
    payload.update(snapshot=snapshot.isoformat() if snapshot else None, start_rank=1,
                   status="complete" if stamp else "pending")
    has_more = total_repeated > 10 if total_repeated is not None else len(payload["rows"]) == 10
    payload["next_cursor"] = page_cursor(payload["rows"][-1], payload["snapshot"]) if has_more and payload["rows"] else None
    return payload


async def describe_positions(session, top, start_rank=1, *, redis=None, snapshot=None):
    layout = storage_for(session)
    compact = layout is not None and layout.compact
    # Compact moves are unique on (link, n_move); their surrogate id no longer exists.
    modal_sql = compact_modal_sql(layout) if compact else MODAL_MOVE_SQL
    example_sql = """
        SELECT n_move, white_move, black_move FROM moves
        WHERE link = :link AND n_move <= :n_move ORDER BY n_move LIMIT 100
    """ if compact else """
        SELECT DISTINCT ON (n_move) n_move, white_move, black_move FROM moves
        WHERE link = :link AND n_move <= :n_move ORDER BY n_move, id LIMIT 100
    """
    keys = description_keys(top, snapshot, layout)
    cached = await load_descriptions(redis, keys)
    positions = []
    for offset, position in enumerate(top):
        rank = start_rank + offset
        row = {"rank": rank, "fen": position["fen"], "appearances": position["appearances"],
               "n_move": None, "move_color": None, "san": None, "label_occurrences": 0,
               "example_game_link": None, "example_line": None}
        if cached[offset] is not None:
            row.update(cached[offset])
            positions.append(row)
            continue
        params = {"fen_id": position["fen_id"]} if compact else {"fen": position["fen"]}
        modal = (await session.execute(text(modal_sql), params)).mappings().first()
        if modal:
            row.update(dict(modal))
            if row["n_move"] <= 100:
                moves = (await session.execute(text(example_sql),
                    {"link": row["example_game_link"], "n_move": row["n_move"]})).mappings().all()
                row["example_line"] = verified_example_line(moves, row["n_move"], row["move_color"], row["fen"])
        await save_description(redis, keys[offset], row)
        positions.append(row)
    return positions


async def rebuild_positions(session, redis=None):
    source = (await session.execute(text("SELECT refreshed_at FROM database_summary WHERE id = 1"))).scalar()
    top = (await session.execute(text("""
        SELECT fen, fen_id, n_games AS appearances FROM fen
        WHERE n_games > 1 ORDER BY n_games DESC, fen LIMIT 10
    """))).mappings().all()
    positions = await describe_positions(session, top, redis=redis, snapshot=source.isoformat() if source else None)
    # Publish identities, labels and freshness atomically; never expose a partial ten.
    await session.execute(text("DELETE FROM fen_commonness_position"))
    if positions:
        await session.execute(text("""
            INSERT INTO fen_commonness_position
              (rank, fen, appearances, n_move, move_color, san, label_occurrences, example_game_link, example_line)
            VALUES (:rank, :fen, :appearances, :n_move, :move_color, :san, :label_occurrences, :example_game_link, :example_line)
        """), positions)
    await session.execute(text("""
        UPDATE fen_commonness_summary SET source_updated_at = :source,
        refreshed_at = clock_timestamp(), last_error = NULL WHERE id = 1
    """), {"source": source})


async def refresh_snapshot(redis=None):
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text("SET LOCAL statement_timeout = '90s'"))
        await session.execute(text("SET LOCAL lock_timeout = '5s'"))
        await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 2"))
        await session.execute(text("SET LOCAL jit = off"))
        await session.execute(text("SET LOCAL plan_cache_mode = force_custom_plan"))
        if not (await session.execute(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_ID})).scalar():
            return {"status": "already_refreshing"}
        await rebuild_positions(session, redis=redis)
        await session.commit()
    return {"status": "complete"}


async def run_commonness_refresh(ctx):
    async with AsyncDBSession() as session:
        await session.execute(text("""
            INSERT INTO fen_commonness_summary (id, attempted_at) VALUES (1, clock_timestamp())
            ON CONFLICT (id) DO UPDATE SET attempted_at = EXCLUDED.attempted_at
        """))
        await session.commit()
    try:
        return await asyncio.wait_for(refresh_snapshot(ctx.get("redis")), timeout=540)
    except Exception:
        LOG.exception("Common-position refresh failed; retaining previous snapshot")
        async with AsyncDBSession() as session:
            await session.execute(text("""
                UPDATE fen_commonness_summary
                SET last_error = 'Position refresh failed; the previous snapshot is retained.' WHERE id = 1
            """))
            await session.commit()
        raise
