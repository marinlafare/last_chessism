"""Build game ordering once; lease bounded pages with atomic queue consumption.

Only Batch game selections opt in. Local selection and existing cloud inputs,
contracts, reservations, imports and recovery remain unchanged. Queue rows are
IDs, not FEN strings or results. No DDL or cloud calls occur in this module.
"""
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json

from sqlalchemy import delete, text
from sqlalchemy.dialects.postgresql import insert

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudGameFenQueue
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.fen_claims import unclaimed_locked_fens

MAX_PAGE_SIZE = 50_000


def selection_identity(game_links):
    links = [int(link) for link in game_links if link is not None]
    raw = json.dumps(['ordered-game-fens-v1', links], separators=(',', ':')).encode()
    return links, hashlib.sha256(raw).hexdigest()


def candidate_sql(layout):
    association = layout.table('game_fen_association') if layout else 'game_fen_association'
    fen = layout.table('fen') if layout else 'fen'
    white = 'gfa.is_white' if layout and layout.compact else "gfa.move_color = 'white'"
    # Preserve the previous ordering exactly, including the global minimum move
    # priority for shared positions and the FEN-text tie-break, not fen_id order.
    return f"""WITH selected_games AS MATERIALIZED (
        SELECT link, priority FROM unnest(CAST(:links AS bigint[]))
        WITH ORDINALITY AS selected(link, priority)
    ), candidates AS MATERIALIZED (
        SELECT gfa.fen_id, MIN(selected.priority) AS game_priority,
            MIN(gfa.n_move * 2 + CASE WHEN {white} THEN 0 ELSE 1 END) AS move_priority
        FROM selected_games selected JOIN {association} gfa ON gfa.game_link = selected.link
        GROUP BY gfa.fen_id
    ) INSERT INTO cloud_game_fen_candidate(job_id, ordinal, fen_id)
        SELECT :job_id, row_number() OVER (ORDER BY c.game_priority, c.move_priority, f.fen), c.fen_id
        FROM candidates c JOIN {fen} f ON f.fen_id = c.fen_id WHERE f.score IS NULL"""


def page_sql(layout):
    fen = layout.table('fen') if layout else 'fen'
    # LIMIT belongs inside the materialized queue page, before joining FENs.
    # A lockable lateral lookup cannot turn a 50k page into a corpus-wide join.
    # Locked rows remain queued for a subsequent call rather than being lost
    # behind a persisted cursor. Scored/claimed rows are rechecked after locks.
    return f"""WITH page AS MATERIALIZED (
        SELECT ordinal, fen_id FROM cloud_game_fen_candidate
        WHERE job_id = :job_id AND ordinal > :after ORDER BY ordinal LIMIT :limit
    ) SELECT page.ordinal, locked.fen FROM page
        LEFT JOIN LATERAL (
            SELECT f.fen FROM {fen} f WHERE f.fen_id = page.fen_id
            FOR UPDATE OF f SKIP LOCKED
        ) locked ON true ORDER BY page.ordinal"""


async def ensure_queue(job_id, game_links, *, timings=None):
    links, identity = selection_identity(game_links)
    measure = timings.measure if timings else lambda _: nullcontext()
    # Commit only the small metadata row first. Its job FK must not hold a
    # key-share lock on the job throughout the expensive initial build.
    async with AsyncDBSession() as session, session.begin():
        await session.execute(insert(CloudGameFenQueue).values(
            job_id=job_id, selection_sha256=identity, candidate_count=0,
        ).on_conflict_do_nothing(index_elements=['job_id']))
    async with AsyncDBSession() as session, session.begin():
        queue = await session.get(CloudGameFenQueue, job_id, with_for_update=True)
        if queue.selection_sha256 != identity:
            raise ValueError('Game selection changed; refusing to reuse a different FEN queue')
        if queue.built_at is None:
            with measure('prepare_queue_build'):
                result = await session.execute(text(candidate_sql(storage_for(session))), {'job_id': job_id, 'links': links})
                queue.candidate_count = result.rowcount
                queue.built_at = datetime.now(timezone.utc)
                # Explicit stats after a bulk build prevent an empty-table plan
                # on the first page. Never analyze the full FEN/association tables.
                await session.execute(text('ANALYZE cloud_game_fen_candidate'))
        return identity


async def get_game_queue_fens(job_id, game_links, limit, *, timings=None):
    """Return an OPEN transaction: caller commits queue removals with claims.

    Validation failure, disconnect or rollback restores candidate rows. The
    metadata row serializes same-job reservers. FEN locks plus a fresh snapshot
    still exclude other cloud claims and concurrently completed local analysis.
    """
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError('Game queue reservations must contain 1..50,000 FENs')
    identity = await ensure_queue(job_id, game_links, timings=timings)
    session = AsyncDBSession()
    measure = timings.measure if timings else lambda _: nullcontext()
    try:
        await session.begin()
        queue = await session.get(CloudGameFenQueue, job_id, with_for_update=True)
        if queue is None or queue.built_at is None or queue.selection_sha256 != identity:
            raise ValueError('FEN queue changed before reservation')
        statement, after, fens = text(page_sql(storage_for(session))), 0, []
        while len(fens) < limit:
            with measure('prepare_select'):
                rows = (await session.execute(statement, {
                    'job_id': job_id, 'after': after, 'limit': min(MAX_PAGE_SIZE, limit - len(fens)),
                })).mappings().all()
            if not rows:
                break
            after = rows[-1]['ordinal']
            locked = [row['fen'] for row in rows if row['fen'] is not None]
            with measure('prepare_recheck'):
                eligible = await unclaimed_locked_fens(session, locked)
            fens.extend(eligible)
            # Rows skipped because their FEN is locked are NOT consumed. Rows
            # now scored/claimed are ineligible at this reservation snapshot.
            consumed = [row['ordinal'] for row in rows if row['fen'] is not None]
            if consumed:
                with measure('prepare_queue_consume'):
                    await session.execute(text('''DELETE FROM cloud_game_fen_candidate
                        WHERE job_id = :job_id AND ordinal = ANY(CAST(:ordinals AS bigint[]))'''),
                        {'job_id': job_id, 'ordinals': consumed})
        if not fens:
            await session.rollback()
            await session.close()
            return None, None
        return session, fens
    except BaseException:
        await session.close()  # Also rolls back on cancellation, not just errors.
        raise


async def discard_queue(session, job_id):
    """Caller owns transaction; cascade removes only this job's scratch IDs."""
    await session.execute(delete(CloudGameFenQueue).where(CloudGameFenQueue.job_id == job_id))
