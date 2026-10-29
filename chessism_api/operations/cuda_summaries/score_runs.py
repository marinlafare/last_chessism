"""Versioned score scopes for manual runs; no calculation in request handlers.

Games are read once across owners. Changed UTC days are staged until every
input is processed; a player revision must still match before publication.
"""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from .schema import CONTROL, RUN, ITEM, PENDING

SCOPE = 'public.cuda_score_scope'
DAY = 'public.cuda_score_day'
OWNER = 'public.cuda_score_owner'
PART = 'public.cuda_score_part'
CAPTURE = 'public.cuda_score_pending_capture'
VERSION = 'shared_scores_v1'


async def install(session):
    await session.execute(text(f'ALTER TABLE {CONTROL} ADD COLUMN IF NOT EXISTS scores boolean NOT NULL DEFAULT false'))
    await session.execute(text(f'ALTER TABLE {ITEM} ADD COLUMN IF NOT EXISTS blunders boolean NOT NULL DEFAULT true'))
    for table, fields in (
        (SCOPE, '''player_name text, revision bigint NOT NULL,full_rebuild boolean NOT NULL,
            planned boolean NOT NULL DEFAULT false,status text NOT NULL DEFAULT 'pending'
            CHECK(status IN ('pending','published','superseded')),PRIMARY KEY(run_id,player_name)'''),
        (DAY, 'player_name text,day date,PRIMARY KEY(run_id,player_name,day)'),
        (OWNER, '''game_link bigint,player_name text,day date,is_white boolean NOT NULL,
            PRIMARY KEY(run_id,game_link,player_name)'''),
        (PART, 'player_name text,day date,batch bigint,payload jsonb NOT NULL,PRIMARY KEY(run_id,player_name,day,batch)'),
        (CAPTURE, 'game_link bigint,revision bigint NOT NULL,PRIMARY KEY(run_id,game_link)'),
    ):
        await session.execute(text(f'CREATE TABLE IF NOT EXISTS {table}(run_id uuid REFERENCES {RUN}(id),{fields})'))
    await session.execute(text(f'CREATE INDEX IF NOT EXISTS cuda_score_owner_player ON {OWNER}(run_id,player_name,day)'))


async def enabled(session):
    return bool(await session.scalar(text(f'SELECT scores FROM {CONTROL} WHERE id')))


async def capture(session, run_id):
    t = lambda n: summary_table(storage_for(session), n)
    await session.execute(text(f'''INSERT INTO {SCOPE}(run_id,player_name,revision,full_rebuild)
        SELECT :id,player_name,revision,full_rebuild FROM {t('state')}
        WHERE revision<>published_revision'''), {'id': run_id})
    layout = storage_for(session)
    await session.execute(text(f'''INSERT INTO {SCOPE}(run_id,player_name,revision,full_rebuild)
        SELECT DISTINCT CAST(:id AS uuid),p.player_name,s.revision,false FROM {ITEM} i
        JOIN {layout.table('game_player')} gp ON gp.game_link=i.game_link
        JOIN {layout.table('player')} p ON p.id=gp.player_id JOIN {t('state')} s USING(player_name)
        WHERE i.run_id=:id AND p.joined>0 AND p.deleted_at IS NULL ON CONFLICT DO NOTHING'''),{'id':run_id})
    await session.execute(text(f'''INSERT INTO {DAY}(run_id,player_name,day)
        SELECT :id,d.player_name,d.day FROM {t('dirty')} d
        JOIN {SCOPE} s ON s.run_id=:id AND s.player_name=d.player_name'''), {'id': run_id})
    await session.execute(text(f'''INSERT INTO {CAPTURE}(run_id,game_link,revision)
        SELECT :id,game_link,revision FROM {PENDING} WHERE metric='score_distribution' '''), {'id': run_id})
    return await session.scalar(text(f'''SELECT (SELECT count(*) FROM {SCOPE} WHERE run_id=:id)
        +(SELECT count(*) FROM {CAPTURE} WHERE run_id=:id)'''), {'id': run_id})


async def plan(session, run):
    """Bounded metadata planning, never FEN scans, one captured owner per tick."""
    layout = storage_for(session); t = lambda n: summary_table(layout, n)
    scope = (await session.execute(text(f'''SELECT * FROM {SCOPE} WHERE run_id=:id
        AND NOT planned ORDER BY player_name LIMIT 1 FOR UPDATE'''), {'id': run['id']})).mappings().first()
    if not scope:
        return False
    p = {'id': run['id'], 'player': scope['player_name'], 'revision': scope['revision']}
    current = await session.scalar(text(f'SELECT revision FROM {t("state")} WHERE player_name=:player'), p)
    if current != scope['revision']:
        await session.execute(text(f"UPDATE {SCOPE} SET planned=true,status='superseded' WHERE run_id=:id AND player_name=:player"), p)
        return True
    day = "COALESCE((g.played_at AT TIME ZONE 'UTC')::date,DATE '-infinity')"
    if scope['full_rebuild']:
        await session.execute(text(f'''INSERT INTO {DAY}(run_id,player_name,day)
            SELECT :id,:player,day FROM (
                SELECT {day} AS day FROM {layout.table('game_player')} gp
                JOIN {layout.table('player')} p ON p.id=gp.player_id
                JOIN {layout.table('game')} g ON g.link=gp.game_link WHERE p.player_name=:player
                UNION SELECT day FROM {t('partition')} WHERE player_name=:player
            ) days ON CONFLICT DO NOTHING'''), p)
    # Empty/deleted owners still need an empty snapshot/removal, even with no games.
    await session.execute(text(f'''INSERT INTO {OWNER}(run_id,game_link,player_name,day,is_white)
        SELECT :id,g.link,p.player_name,{day},gp.is_white
        FROM {layout.table('game_player')} gp JOIN {layout.table('player')} p ON p.id=gp.player_id
        JOIN {layout.table('game')} g ON g.link=gp.game_link
        JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
        JOIN {DAY} d ON d.run_id=:id AND d.player_name=p.player_name AND d.day={day}
        WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL
        AND m.name IN ('bullet','blitz','rapid')
        AND ((gp.is_white AND g.white_player_id=p.id) OR (NOT gp.is_white AND g.black_player_id=p.id))
        ON CONFLICT DO NOTHING'''), p)
    await session.execute(text(f'''INSERT INTO {ITEM}(run_id,game_link,revision,blunders)
        SELECT :id,game_link,0,false FROM {OWNER} WHERE run_id=:id AND player_name=:player
        ON CONFLICT DO NOTHING'''), p)
    await session.execute(text(f'UPDATE {SCOPE} SET planned=true WHERE run_id=:id AND player_name=:player'), p)
    await session.execute(text(f'''UPDATE {RUN} SET total_games=(SELECT count(*) FROM {ITEM} WHERE run_id=:id),
        status='running',phase='Preparing shared game inputs',started_at=COALESCE(started_at,clock_timestamp()),
        updated_at=clock_timestamp() WHERE id=:id'''), p)
    return True


async def next_batch(session, run, limit):
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {SCOPE} WHERE run_id=:id AND NOT planned)'), {'id': run['id']}):
        return {**dict(run), 'planning': True}
    items = await pending_items(session, run, limit)
    await session.execute(text(f'''UPDATE {RUN} SET status='running',phase=:phase,
        started_at=COALESCE(started_at,clock_timestamp()),updated_at=clock_timestamp() WHERE id=:id'''),
        {'id': run['id'], 'phase': 'Loading shared inputs' if items else 'Publishing score distributions'})
    return {**dict(run), 'items': items, 'finalizing': not items}


async def pending_items(session, run, limit=32768, *, after=None):
    """Read-only keyset paging of this captured run, never newly queued work."""
    cursor = 'AND game_link>:after' if after is not None else ''
    return [dict(r) for r in (await session.execute(text(f'''SELECT game_link,revision,blunders FROM {ITEM}
        WHERE run_id=:id AND status='pending' {cursor} ORDER BY game_link LIMIT :limit'''),
        {'id': run['id'], 'limit': limit, 'after': after})).mappings()]


async def stage(session, run, items, parts, timings):
    """Commit this batch's canonical results, staged counts and cursor together."""
    from ..canonical_blunders.schema import table
    t = lambda n: table(storage_for(session), n)
    p = {'id': run['id'], 'links': [i['game_link'] for i in items], 'model': run['model_id']}
    if parts:
        from .bulk import temp_rows, json_rows
        batch = min(p['links'])
        values = await json_rows(lambda: [(player, day, json.dumps(payload)) for (player, day), payload in parts.items()])
        await temp_rows(session, 'cuda_parts_upload', 'player_name text,day text,payload jsonb',
                        ('player_name','day','payload'), values)
        await session.execute(text(f'''INSERT INTO {PART}(run_id,player_name,day,batch,payload)
            SELECT :id,player_name,day::date,:batch,payload FROM pg_temp.cuda_parts_upload
            ON CONFLICT(run_id,player_name,day,batch) DO UPDATE SET payload=EXCLUDED.payload'''),
            {'id': run['id'], 'batch': batch})
    await session.execute(text(f'''DELETE FROM {PENDING} p USING {ITEM} i
        WHERE p.metric='blunders' AND i.run_id=:id AND i.blunders
        AND i.game_link=ANY(CAST(:links AS bigint[])) AND p.game_link=i.game_link AND p.revision=i.revision
        AND EXISTS(SELECT 1 FROM {t('game')} g WHERE g.game_link=i.game_link AND g.model_id=:model AND g.revision=i.revision)
        AND NOT EXISTS(SELECT 1 FROM {t('dirty')} d WHERE d.game_link=i.game_link)'''), p)
    await session.execute(text(f'''UPDATE {ITEM} i SET status=CASE WHEN i.blunders AND EXISTS(
        SELECT 1 FROM {PENDING} p WHERE p.metric='blunders' AND p.game_link=i.game_link)
        THEN 'superseded' ELSE 'published' END
        WHERE run_id=:id AND game_link=ANY(CAST(:links AS bigint[])) AND status='pending' '''), p)
    old = await session.scalar(text(f'SELECT timings FROM {RUN} WHERE id=:id FOR UPDATE'), p)
    totals = {**old, 'input_contract': VERSION, 'score_algorithm': 'score_distribution_v2', 'backend': 'cuda_cupy'}
    if 'accuracy' in run['metrics']:
        from .accuracy_compute import VERSION as accuracy_version
        totals['accuracy_algorithm'] = accuracy_version
    for key, value in timings.items():
        totals[key] = totals.get(key, 0) + value
    await session.execute(text(f'''UPDATE {RUN} SET processed_games=(SELECT count(*) FROM {ITEM} WHERE run_id=:id AND status='published'),
        superseded_games=(SELECT count(*) FROM {ITEM} WHERE run_id=:id AND status='superseded'),
        timings=CAST(:timings AS jsonb),attempts=0,error=NULL,retry_after=NULL,updated_at=clock_timestamp() WHERE id=:id'''),
        {**p, 'timings': json.dumps(totals)})


async def complete(session, run):
    p = {'id': run['id']}
    if 'accuracy' in run['metrics']:
        from . import accuracy_store
        await accuracy_store.complete(session, run)
    # Conservative acknowledgement when any owner changed: retain pending games
    # rather than accidentally clearing a shared game's other player's work.
    await session.execute(text(f'''DELETE FROM {PENDING} p USING {CAPTURE} c
        WHERE c.run_id=:id AND p.metric='score_distribution' AND p.game_link=c.game_link AND p.revision=c.revision
        AND NOT EXISTS(SELECT 1 FROM {SCOPE} WHERE run_id=:id AND status<>'published')'''), p)
    skipped = await session.scalar(text(f"SELECT count(*) FROM {SCOPE} WHERE run_id=:id AND status='superseded'"), p)
    if 'salience' in run['metrics']:
        from .salience_store import SCOPE as salience_scope
        skipped += await session.scalar(text(f"SELECT count(*) FROM {salience_scope} WHERE run_id=:id AND status='superseded'"), p)
    await session.execute(text(f'''UPDATE {RUN} SET status='complete',phase=:phase,finished_at=clock_timestamp(),
        updated_at=clock_timestamp(),attempts=0,error=NULL,retry_after=NULL,
        timings=timings || jsonb_build_object('superseded_score_players',CAST(:skipped AS bigint)) WHERE id=:id'''),
        {**p, 'skipped': skipped, 'phase': 'Complete — changed players remain pending' if skipped else 'Complete'})
    # Keep compact run/scope audit; discard reproducible staging and game manifests.
    for table in (PART, OWNER, DAY, CAPTURE):
        await session.execute(text(f'DELETE FROM {table} WHERE run_id=:id'), p)
