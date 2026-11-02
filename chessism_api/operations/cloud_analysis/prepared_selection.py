"""Prepare a bounded, indexed FEN selection once; Add consumes its native chunks.

Only missing FEN IDs and reservation flags are retained. No FEN strings leave
PostgreSQL, no cloud resources are touched, and launch still rechecks live games.
"""
import hashlib
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from sqlalchemy import text
from chessism_api.database.postgresql.models import CloudGamePreparedSelection

GAME_BLOCK = 512
FEN_BLOCK = 16384
TTL_SECONDS = 3600


def games_digest(links):
    return hashlib.sha256(','.join(map(str, links)).encode('ascii')).hexdigest()


async def build(session, plan_id, links):
    await session.execute(text("SET LOCAL statement_timeout='60s'"))
    await session.execute(text("SET LOCAL lock_timeout='3s'"))
    await session.execute(text("SET LOCAL work_mem='64MB'"))
    await session.execute(text('SET LOCAL jit=off'))
    # Expired unused plans are disposable; bounded cleanup never touches drafts.
    await session.execute(text('''DELETE FROM cloud_game_prepared_selection WHERE id IN (
        SELECT id FROM cloud_game_prepared_selection WHERE expires_at < clock_timestamp()
        ORDER BY expires_at LIMIT 10 FOR UPDATE SKIP LOCKED)'''))
    plan = CloudGamePreparedSelection(id=plan_id, games_sha256=games_digest(links), missing=0, claimed=0,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=TTL_SECONDS))
    session.add(plan)
    await session.flush()
    await session.execute(text('CREATE TEMP TABLE chessism_selected_fens(fen_id bigint PRIMARY KEY) ON COMMIT DROP'))
    for offset in range(0, len(links), GAME_BLOCK):
        # OFFSET 0 intentionally preserves the parameterized indexed probe. A
        # large ANY array otherwise chooses full association/FEN table scans.
        await session.execute(text('''INSERT INTO pg_temp.chessism_selected_fens(fen_id)
            SELECT DISTINCT a.fen_id FROM unnest(CAST(:links AS bigint[])) g(game_link)
            CROSS JOIN LATERAL (SELECT fen_id FROM game_fen_association
                WHERE game_link=g.game_link OFFSET 0) a
            ON CONFLICT DO NOTHING'''), {'links':links[offset:offset+GAME_BLOCK]})
    after = 0; ordinal = 0
    while True:
        row = (await session.execute(text('''WITH selected AS MATERIALIZED (
                SELECT fen_id FROM pg_temp.chessism_selected_fens WHERE fen_id>:after
                ORDER BY fen_id LIMIT :limit
            ), missing AS MATERIALIZED (
                SELECT p.fen_id,c.fen IS NOT NULL AS claimed FROM selected p
                CROSS JOIN LATERAL (SELECT fen FROM fen WHERE fen_id=p.fen_id AND score IS NULL OFFSET 0) f
                LEFT JOIN cloud_fen_claim c ON c.fen=f.fen
            ), saved AS (
                INSERT INTO cloud_game_prepared_chunk(plan_id,ordinal,fen_ids,claimed)
                SELECT :id,:ordinal,array_agg(fen_id ORDER BY fen_id),array_agg(claimed ORDER BY fen_id)
                FROM missing HAVING count(*)>0
            ) SELECT (SELECT max(fen_id) FROM selected) AS last,
                count(*) AS missing,count(*) FILTER (WHERE claimed) AS claimed FROM missing'''),
            {'id':plan_id,'ordinal':ordinal,'after':after,'limit':FEN_BLOCK})).mappings().one()
        if row['last'] is None:
            break
        plan.missing += row['missing']; plan.claimed += row['claimed']
        after = row['last']; ordinal += 1
    # The retention window starts when calculation succeeds, not at its start.
    plan.expires_at = datetime.now(timezone.utc) + timedelta(seconds=TTL_SECONDS)
    await session.flush()
    return {'fens_to_analyze':plan.missing,'claimed_fens':plan.claimed,'prepared_fens_version':1}


async def merge(session, cache, entry_id, plan_id, links):
    plan = (await session.execute(text('''SELECT games_sha256 FROM cloud_game_prepared_selection
        WHERE id=:id AND expires_at>clock_timestamp() FOR UPDATE'''), {'id':plan_id})).mappings().first()
    if not plan or plan['games_sha256'] != games_digest(links):
        raise HTTPException(409, 'Calculated FEN selection expired or changed; calculate again')
    delta = (await session.execute(text('''WITH prepared AS MATERIALIZED (
            SELECT p.fen_id,p.claimed FROM cloud_game_prepared_chunk c
            CROSS JOIN LATERAL unnest(c.fen_ids,c.claimed) p(fen_id,claimed) WHERE c.plan_id=:plan
        ), saved AS (
            INSERT INTO cloud_game_draft_entry_fen(entry_id,fen_id,draft_id)
            SELECT :entry,fen_id,:draft FROM prepared
        ), merged AS (
            INSERT INTO cloud_game_draft_fen(draft_id,fen_id,n_entries,is_missing,is_claimed)
            SELECT :draft,fen_id,1,true,claimed FROM prepared
            ON CONFLICT (draft_id,fen_id) DO UPDATE SET n_entries=cloud_game_draft_fen.n_entries+1
            RETURNING n_entries,is_missing,is_claimed
        ) SELECT count(*) FILTER (WHERE n_entries=1 AND is_missing) AS missing,
            count(*) FILTER (WHERE n_entries=1 AND is_claimed) AS claimed FROM merged'''),
        {'plan':plan_id,'entry':entry_id,'draft':cache.draft_id})).mappings().one()
    cache.missing += delta['missing']; cache.claimed += delta['claimed']
    # Delete only this consumed temporary plan; failed saves roll this back too.
    await session.execute(text('DELETE FROM cloud_game_prepared_selection WHERE id=:id'), {'id':plan_id})
