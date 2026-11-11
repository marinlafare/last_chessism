"""Build game ordering once; lease bounded pages with atomic queue consumption.

Batch and Cloud Run game selections opt in. Local selection and existing cloud inputs,
contracts, reservations, imports and recovery remain unchanged. Queue rows are
IDs, not FEN strings or results. No DDL or cloud calls occur in this module.
"""
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json

from sqlalchemy import delete, text, select
from sqlalchemy.dialects.postgresql import insert

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.models import CloudGameFenQueue, CloudGameDraft, CloudGameDraftCache
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.database.postgresql.analysis_selection import GAME_SET_CANDIDATES_SQL, release_analysis_session
from chessism_api.database.postgresql.fen_claims import unclaimed_locked_fens

MAX_PAGE_SIZE = 50_000


def selection_identity(game_links):
    links = [int(link) for link in game_links if link is not None]
    raw = json.dumps(['ordered-game-fens-v1', links], separators=(',', ':')).encode()
    return links, hashlib.sha256(raw).hexdigest()


def candidate_sql():
    return GAME_SET_CANDIDATES_SQL + """INSERT INTO public.cloud_game_fen_candidate(job_id, ordinal, fen_id)
        SELECT :job_id, row_number() OVER (ORDER BY c.game_priority, c.move_priority, f.fen), c.fen_id
        FROM candidate_fens c JOIN public.fen f ON f.fen_id=c.fen_id WHERE f.score IS NULL"""


def page_sql():
    # Bound the queue page before looking up FENs. Locked rows remain queued;
    # scored/claimed rows are rechecked after acquiring their locks.
    return """WITH page AS MATERIALIZED (
        SELECT ordinal, fen_id FROM public.cloud_game_fen_candidate
        WHERE job_id = :job_id AND ordinal > :after ORDER BY ordinal LIMIT :limit
    ) SELECT page.ordinal, locked.fen FROM page
        LEFT JOIN LATERAL (
            SELECT f.fen FROM public.fen f WHERE f.fen_id = page.fen_id
            FOR UPDATE OF f SKIP LOCKED
        ) locked ON true ORDER BY page.ordinal"""


async def ensure_queue(job_id, game_links, *, timings=None):
    links, identity = selection_identity(game_links)
    measure = timings.measure if timings else lambda _: nullcontext()
    # Commit only the small metadata row first. Its job FK must not hold a
    # key-share lock on the job throughout the expensive initial build.
    async with write_transaction(AsyncDBSession) as session:
        await session.execute(insert(CloudGameFenQueue).values(
            job_id=job_id, selection_sha256=identity, candidate_count=0,
        ).on_conflict_do_nothing(index_elements=['job_id']))
    async with write_transaction(AsyncDBSession) as session:
        queue = await session.get(CloudGameFenQueue, job_id, with_for_update=True)
        if queue.selection_sha256 != identity:
            raise ValueError('Game selection changed; refusing to reuse a different FEN queue')
        if queue.built_at is None:
            with measure('prepare_queue_build'):
                draft = (await session.scalars(select(CloudGameDraft).where(
                    CloudGameDraft.submitted_job_id == job_id))).first()
                cache = await session.get(CloudGameDraftCache, draft.id) if draft else None
                if cache and cache.revision == draft.revision:
                    from . import game_drafts, draft_cache
                    if await game_drafts.game_links(session, draft.id) != links:
                        raise ValueError('Submitted game selection changed')
                    # Combined selections contain the entire authorized union;
                    # no ranking is needed. Lock/recheck every FEN when leased.
                    result = await session.execute(text('''INSERT INTO public.cloud_game_fen_candidate(job_id,ordinal,fen_id)
                        SELECT :job_id,row_number() OVER (ORDER BY fen_id),fen_id
                        FROM public.cloud_game_draft_fen WHERE draft_id=:draft_id'''),
                        {'job_id':job_id,'draft_id':draft.id})
                    await draft_cache.discard(session, draft.id)
                else:
                    result = await session.execute(text(candidate_sql()), {'job_id': job_id, 'game_links': links})
                queue.candidate_count = result.rowcount
                queue.built_at = datetime.now(timezone.utc)
                # Explicit stats after a bulk build prevent an empty-table plan
                # on the first page. Never analyze the full FEN/association tables.
                await session.execute(text('ANALYZE public.cloud_game_fen_candidate'))
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
        statement, after, fens = text(page_sql()), 0, []
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
                    await session.execute(text('''DELETE FROM public.cloud_game_fen_candidate
                        WHERE job_id = :job_id AND ordinal = ANY(CAST(:ordinals AS bigint[]))'''),
                        {'job_id': job_id, 'ordinals': consumed})
    except BaseException:
        try:
            await release_analysis_session(session)
        except BaseException as error:
            print(f"Failed to release game queue selection session: {error!r}", flush=True)
        raise
    if not fens:
        await release_analysis_session(session)
        return None, None
    return session, fens


async def discard_queue(session, job_id):
    """Caller owns transaction; cascade removes only this job's scratch IDs."""
    await session.execute(delete(CloudGameFenQueue).where(CloudGameFenQueue.job_id == job_id))
    from .draft_cache import discard
    for ident in (await session.scalars(select(CloudGameDraft.id).where(CloudGameDraft.submitted_job_id == job_id))).all():
        await discard(session, ident)
