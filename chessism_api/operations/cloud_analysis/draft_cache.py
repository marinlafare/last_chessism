"""Incremental draft-only FEN membership. Caller must hold the draft row lock.

Only the edited entry reads game_fen_association. Shared FENs retain their
snapshot eligibility until explicit refresh; submission independently rechecks
the live game union. No cloud calls, reservations or engine-result writes.
"""
from datetime import datetime, timezone
from sqlalchemy import delete, func, select, text
from chessism_api.database.models import (
    CloudGameDraftCache, CloudGameDraftEntry, CloudGameDraftGame,
)


async def discard(session, draft_id):
    # Cascades delete only derived builder membership, never saved steps/games.
    await session.execute(delete(CloudGameDraftCache).where(CloudGameDraftCache.draft_id == draft_id))


async def add_entry(session, cache, entry_id):
    delta = (await session.execute(text('''
        WITH saved AS (
            INSERT INTO cloud_game_draft_entry_fen(entry_id, fen_id, draft_id)
            SELECT CAST(:entry_id AS varchar(32)), gfa.fen_id, CAST(:draft_id AS varchar(32))
            FROM cloud_game_draft_game g
            JOIN game_fen_association gfa ON gfa.game_link = g.game_link
            WHERE g.entry_id = :entry_id
            GROUP BY gfa.fen_id
            RETURNING fen_id
        ), merged AS (
            INSERT INTO cloud_game_draft_fen(draft_id, fen_id, n_entries, is_missing, is_claimed)
            SELECT CAST(:draft_id AS varchar(32)), f.fen_id, 1, f.score IS NULL,
                   f.score IS NULL AND c.fen IS NOT NULL
            FROM saved p JOIN fen f ON f.fen_id = p.fen_id
            LEFT JOIN cloud_fen_claim c ON c.fen = f.fen
            ON CONFLICT (draft_id, fen_id) DO UPDATE
                SET n_entries = cloud_game_draft_fen.n_entries + 1
            RETURNING n_entries, is_missing, is_claimed
        )
        SELECT count(*) FILTER (WHERE n_entries = 1 AND is_missing) AS missing,
               count(*) FILTER (WHERE n_entries = 1 AND is_claimed) AS claimed
        FROM merged
    '''), {'entry_id': entry_id, 'draft_id': cache.draft_id})).mappings().one()
    cache.missing += int(delta['missing'])
    cache.claimed += int(delta['claimed'])


async def remove_entry(session, cache, entry_id):
    delta = (await session.execute(text('''
        WITH affected AS MATERIALIZED (
            SELECT u.fen_id, u.n_entries
            FROM cloud_game_draft_fen u
            JOIN cloud_game_draft_entry_fen e ON e.fen_id = u.fen_id
            WHERE u.draft_id = :draft_id AND e.entry_id = :entry_id
        ), removed AS (
            DELETE FROM cloud_game_draft_fen u USING affected a
            WHERE u.draft_id = :draft_id AND u.fen_id = a.fen_id AND a.n_entries = 1
            RETURNING u.is_missing, u.is_claimed
        ), decremented AS (
            UPDATE cloud_game_draft_fen u SET n_entries = u.n_entries - 1
            FROM affected a
            WHERE u.draft_id = :draft_id AND u.fen_id = a.fen_id AND a.n_entries > 1
        )
        SELECT count(*) FILTER (WHERE is_missing) AS missing,
               count(*) FILTER (WHERE is_claimed) AS claimed
        FROM removed
    '''), {'entry_id': entry_id, 'draft_id': cache.draft_id})).mappings().one()
    await session.execute(text('DELETE FROM cloud_game_draft_entry_fen WHERE entry_id = :entry_id'),
                          {'entry_id': entry_id})
    cache.missing -= int(delta['missing'])
    cache.claimed -= int(delta['claimed'])


async def ensure(session, draft, *, rebuild=False):
    cache = await session.get(CloudGameDraftCache, draft.id)
    if cache is not None and cache.revision == draft.revision and not rebuild:
        return cache
    if cache is not None:
        await discard(session, draft.id)
    cache = CloudGameDraftCache(draft_id=draft.id, revision=draft.revision, missing=0, claimed=0)
    session.add(cache)
    await session.flush()
    entries = await session.scalars(select(CloudGameDraftEntry.id)
        .where(CloudGameDraftEntry.draft_id == draft.id).order_by(CloudGameDraftEntry.ordinal))
    for entry_id in entries:
        await add_entry(session, cache, entry_id)
    return cache


async def publish(session, draft, cache):
    # This small metadata count reads only saved game IDs, never old FEN sets.
    games = await session.scalar(select(func.count(func.distinct(CloudGameDraftGame.game_link)))
        .join(CloudGameDraftEntry, CloudGameDraftEntry.id == CloudGameDraftGame.entry_id)
        .where(CloudGameDraftEntry.draft_id == draft.id))
    cache.revision = draft.revision
    draft.preview_revision = draft.revision
    draft.preview_missing_fens, draft.preview_claimed_fens = cache.missing, cache.claimed
    draft.preview_fens, draft.preview_games = cache.missing - cache.claimed, games
    draft.previewed_at = datetime.now(timezone.utc)
    await session.flush()
