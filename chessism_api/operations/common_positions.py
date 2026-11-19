"""Ten cached FEN identities, modal arrival labels and verified example lines.

Only the background worker joins game occurrences to moves. Readers use the
small cache and retain the previous complete snapshot during refresh/failure.
"""
import asyncio
import logging

import chess

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import fen_appearances as persistence
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup
from chessism_api.operations.common_position_descriptions import description_keys, load_descriptions, save_description
from chessism_api.operations.pending_repetitions import needs_refresh

LOG = logging.getLogger(__name__)
JOB_ID = "fen-commonness-summary-v1"
LOCK_ID = 731_946_231


async def await_refresh(operation, *, timeout=540):
    """Keep the worker alive until its timed/cancelled database work has closed."""
    task = asyncio.create_task(operation)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    finally:
        if not task.done():
            task.cancel()
            await finish_analysis_cleanup(asyncio.gather(task, return_exceptions=True))


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


async def get_common_positions(redis, source_updated_at, total_repeated=None, *, session_factory=None):
    async with read_transaction(session_factory or AsyncDBSession) as session:
        rows = await persistence.read_common(session)
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
    keys = description_keys(top, snapshot)
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
        modal = await persistence.modal_move(session, position['fen_id'])
        if modal:
            row.update(dict(modal))
            if row["n_move"] <= 100:
                moves = await persistence.example_moves(session, row['example_game_link'], row['n_move'])
                row["example_line"] = verified_example_line(moves, row["n_move"], row["move_color"], row["fen"])
        await save_description(redis, keys[offset], row)
        positions.append(row)
    return positions


async def rebuild_positions(session, redis=None):
    source = await persistence.source_stamp(session)
    top = await persistence.top_positions(session)
    positions = await describe_positions(session, top, redis=redis, snapshot=source.isoformat() if source else None)
    await persistence.replace_common(session, positions)
    await persistence.publish_stamp(session, 'commonness', source)


async def refresh_snapshot(redis=None, *, session_factory=None):
    async with write_transaction(session_factory or AsyncDBSession) as session:
        await persistence.execute(session, 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        await persistence.read_limits(session)
        if not await persistence.try_lock(session, LOCK_ID):
            return {"status": "already_refreshing"}
        await rebuild_positions(session, redis=redis)
    return {"status": "complete"}


async def run_commonness_refresh(ctx):
    factory = ctx.get('session_factory') or AsyncDBSession
    async with write_transaction(factory) as session:
        attempt = await persistence.mark_attempt(session, 'commonness')
    try:
        return await await_refresh(refresh_snapshot(ctx.get("redis"), session_factory=factory))
    except Exception:
        LOG.exception("Common-position refresh failed; retaining previous snapshot")
        try:
            async with write_transaction(factory) as session:
                await persistence.mark_failure(session, 'commonness', attempt,
                    'Position refresh failed; the previous snapshot is retained.')
        except Exception:
            LOG.exception('Could not record common-position failure; preserving the refresh error')
        raise
