"""Captured manual work sets, exact-revision acknowledgements and durable history."""
import json
import uuid
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..canonical_blunders.schema import table as canonical_table
from ..canonical_blunders.source import MAX_GAMES
from .schema import CONTROL, PENDING, RUN, ITEM, enabled

ACTIVE = "('queued','running','retrying')"
AVAILABLE = ['blunders']
DEFERRED = ['accuracy', 'score_distribution']


async def progress(session, run_id):
    return dict((await session.execute(text(f'SELECT * FROM {RUN} WHERE id=:id'), {'id': run_id})).mappings().one())


async def request_run(session, account_id):
    if not await enabled(session):
        raise ValueError('Manual CUDA summaries are not enabled yet.')
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    existing = await session.scalar(text(f'SELECT id FROM {RUN} WHERE status IN {ACTIVE}'))
    if existing:
        return await progress(session, existing)
    layout = storage_for(session)
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {layout.table("fen_summary_refresh")})')):
        raise ValueError('Game preparation is still pending. Wait until coverage preparation finishes.')
    model = await session.scalar(text(f'SELECT target_model FROM {canonical_table(layout,"control")} WHERE id'))
    if not model:
        raise ValueError('No canonical CUDA model is selected.')
    from . import score_runs
    scores = await score_runs.enabled(session)
    from . import accuracy_store
    accuracy = scores and await accuracy_store.enabled(session)
    run_id = uuid.uuid4()
    await session.execute(text(f'''INSERT INTO {RUN}(id,requested_by,model_id,status)
        VALUES(:id,:account,:model,'queued')'''), {'id': run_id, 'account': str(account_id), 'model': model})
    await session.execute(text(f'''INSERT INTO {ITEM}(run_id,game_link,revision)
        SELECT :id,p.game_link,p.revision FROM {PENDING} p
        JOIN {canonical_table(layout,'dirty')} d ON d.game_link=p.game_link AND d.revision=p.revision
        WHERE p.metric='blunders' ORDER BY p.game_link'''), {'id': run_id})
    total = await session.scalar(text(f'SELECT count(*) FROM {ITEM} WHERE run_id=:id'), {'id': run_id})
    score_work = await score_runs.capture(session, run_id) if scores else 0
    if accuracy:
        await accuracy_store.capture(session, run_id)
        score_work += await session.scalar(text(f'SELECT count(*) FROM {score_runs.SCOPE} WHERE run_id=:id'), {'id': run_id})
        score_work += await session.scalar(text(f'SELECT count(*) FROM {accuracy_store.CAPTURE} WHERE run_id=:id'), {'id': run_id})
    from . import metadata_store
    metadata_work = await metadata_store.capture(session, run_id) if scores else 0
    from . import salience_store
    salience_work = await salience_store.capture(session, run_id) if scores else 0
    if not total and not score_work and not metadata_work and not salience_work:
        raise ValueError('No pending work for the currently available CUDA calculations.')
    if scores:
        await session.execute(text(f"UPDATE {RUN} SET metrics=ARRAY['blunders','score_distribution'] WHERE id=:id"), {'id': run_id})
    if accuracy:
        await session.execute(text(f"UPDATE {RUN} SET metrics=ARRAY['blunders','score_distribution','accuracy'] WHERE id=:id"), {'id': run_id})
    if metadata_work:
        await session.execute(text(f"UPDATE {RUN} SET metrics=metrics || ARRAY['behavioral','unexpected_endings'] WHERE id=:id"), {'id':run_id})
    if salience_work:
        await session.execute(text(f"UPDATE {RUN} SET metrics=metrics || ARRAY['salience'] WHERE id=:id"), {'id':run_id})
    await session.execute(text(f'UPDATE {RUN} SET total_games=:total WHERE id=:id'), {'id': run_id, 'total': total})
    return await progress(session, run_id)


async def retry_run(session, run_id):
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {RUN} WHERE status IN {ACTIVE})')):
        raise ValueError('Another CUDA summary run is already active.')
    row = (await session.execute(text(f'''UPDATE {RUN} SET status='queued',phase='queued',attempts=0,
        error=NULL,retry_after=NULL,finished_at=NULL,updated_at=clock_timestamp()
        WHERE id=:id AND status='failed' RETURNING id'''), {'id': run_id})).scalar_one_or_none()
    if row is None:
        raise ValueError('Only a failed CUDA summary run can be retried.')
    return await progress(session, run_id)


async def next_batch(session):
    layout = storage_for(session)
    run = (await session.execute(text(f'''SELECT * FROM {RUN} WHERE status IN {ACTIVE}
        AND (retry_after IS NULL OR retry_after<=clock_timestamp()) ORDER BY created_at LIMIT 1 FOR UPDATE'''))).mappings().first()
    if not run:
        return None
    model = await session.scalar(text(f'SELECT target_model FROM {canonical_table(layout,"control")} WHERE id'))
    if model != run['model_id']:
        await fail(session, run['id'], 'Canonical model changed. Start a new run for the new model.', permanent=True)
        return None
    if 'score_distribution' in run['metrics']:
        from .score_runs import next_batch as shared_batch
        return await shared_batch(session, run, MAX_GAMES)
    # Newly arrived revisions are never consumed by an earlier manual request.
    await session.execute(text(f'''UPDATE {ITEM} i SET status='superseded'
        WHERE i.run_id=:id AND i.status='pending' AND (
            NOT EXISTS(SELECT 1 FROM {canonical_table(layout,'dirty')} d
                WHERE d.game_link=i.game_link AND d.revision=i.revision)
            OR EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')} f WHERE f.game_link=i.game_link))'''), {'id': run['id']})
    items = [dict(r) for r in (await session.execute(text(f'''SELECT game_link,revision FROM {ITEM}
        WHERE run_id=:id AND status='pending' ORDER BY game_link LIMIT :limit'''),
        {'id': run['id'], 'limit': MAX_GAMES})).mappings()]
    await update_counts(session, run['id'])
    if not items:
        return None
    await session.execute(text(f'''UPDATE {RUN} SET status='running',phase='Loading shared inputs',
        started_at=COALESCE(started_at,clock_timestamp()),updated_at=clock_timestamp() WHERE id=:id'''), {'id': run['id']})
    return {**dict(run), 'items': items}


async def phase(session, run_id, name):
    await session.execute(text(f'UPDATE {RUN} SET phase=:phase,updated_at=clock_timestamp() WHERE id=:id'),
                          {'id': run_id, 'phase': name})


async def update_counts(session, run_id):
    await session.execute(text(f'''UPDATE {RUN} r SET processed_games=c.published,
        superseded_games=c.superseded,status=CASE WHEN c.pending=0 THEN 'complete' ELSE r.status END,
        phase=CASE WHEN c.pending=0 THEN 'Complete' ELSE r.phase END,
        finished_at=CASE WHEN c.pending=0 THEN clock_timestamp() ELSE NULL END,updated_at=clock_timestamp()
        FROM (SELECT count(*) FILTER(WHERE status='published') AS published,
              count(*) FILTER(WHERE status='superseded') AS superseded,
              count(*) FILTER(WHERE status='pending') AS pending FROM {ITEM} WHERE run_id=:id) c
        WHERE r.id=:id'''), {'id': run_id})


async def finish_batch(session, run, items, timings):
    layout = storage_for(session)
    links = [item['game_link'] for item in items]
    # Same transaction as canonical publication, retaining its exact-revision locks.
    await session.execute(text(f'''UPDATE {ITEM} i SET status=CASE WHEN
        EXISTS(SELECT 1 FROM {canonical_table(layout,'game')} g WHERE g.model_id=:model
            AND g.game_link=i.game_link AND g.revision=i.revision)
        AND NOT EXISTS(SELECT 1 FROM {canonical_table(layout,'dirty')} d WHERE d.game_link=i.game_link)
        THEN 'published' ELSE 'superseded' END
        WHERE run_id=:id AND status='pending' AND game_link=ANY(CAST(:links AS bigint[]))'''),
        {'id': run['id'], 'model': run['model_id'], 'links': links})
    await session.execute(text(f'''DELETE FROM {PENDING} p USING {ITEM} i WHERE p.metric='blunders'
        AND i.run_id=:id AND i.status='published' AND p.game_link=i.game_link AND p.revision=i.revision'''), {'id': run['id']})
    old = await session.scalar(text(f'SELECT timings FROM {RUN} WHERE id=:id FOR UPDATE'), {'id': run['id']})
    totals = {**old}
    for key, value in timings.items():
        totals[key] = totals.get(key, 0) + value
    await session.execute(text(f'''UPDATE {RUN} SET timings=CAST(:timings AS jsonb),attempts=0,error=NULL,
        retry_after=NULL WHERE id=:id'''), {'id': run['id'], 'timings': json.dumps(totals)})
    await update_counts(session, run['id'])
    return await progress(session, run['id'])


async def fail(session, run_id, error, *, permanent=False):
    await session.execute(text(f'''UPDATE {RUN} SET attempts=attempts+1,
        status=CASE WHEN :permanent OR attempts>=2 THEN 'failed' ELSE 'retrying' END,
        phase=CASE WHEN :permanent OR attempts>=2 THEN 'Failed — retry required' ELSE 'Retrying' END,
        finished_at=CASE WHEN :permanent OR attempts>=2 THEN clock_timestamp() END,
        error=:error,retry_after=clock_timestamp()+interval '60 seconds',updated_at=clock_timestamp()
        WHERE id=:id'''), {'id': run_id, 'error': error[:1000], 'permanent': permanent})


async def status(session, *, player=None):
    if not await enabled(session):
        return {'manual': False, 'available_metrics': AVAILABLE, 'deferred_metrics': DEFERRED}
    layout = storage_for(session)
    params = {'player': player}
    from . import score_runs
    scores = await score_runs.enabled(session)
    available = [*AVAILABLE, 'score_distribution'] if scores else AVAILABLE
    deferred = ['accuracy'] if scores else DEFERRED
    from . import accuracy_store
    accuracy = scores and await accuracy_store.enabled(session)
    if accuracy:
        available = [*available, 'accuracy']; deferred = []
    from . import metadata_store
    metadata = await metadata_store.pending(session, player)
    if await metadata_store.enabled(session):
        available = [*available, *metadata_store.METRICS]
    from . import salience_store
    salience = await salience_store.enabled(session)
    salience_players = (await session.execute(text(f'SELECT player_name FROM ({salience_store.needed(layout)}) q'))).scalars().all() if salience else []
    if salience:
        available = [*available, 'salience']
    owner_filter = f'''AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp
        JOIN {layout.table('player')} p ON p.id=gp.player_id
        WHERE gp.game_link=q.game_link AND p.player_name=:player)''' if player else ''
    counts = dict((await session.execute(text(f'''SELECT metric,count(*) FROM {PENDING} q
        WHERE true {owner_filter} GROUP BY metric'''), params)).all())
    if player:
        preparing = await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')} q
            WHERE true {owner_filter})'''), params)
        from chessism_api.database.stockfish_summary_schema import summary_table
        score_stale = scores and await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {summary_table(layout,'state')}
            WHERE player_name=:player AND revision<>published_revision)'''), params)
        accuracy_stale = accuracy and await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM ({accuracy_store.needs_work(layout)}) q WHERE player_name=:player)'), params)
        metadata_metrics = [name for name,key in zip(metadata_store.METRICS,('behavior','endings')) if any(r[key] for r in metadata)]
        return {'manual': True, 'pending_metrics': [m for m in ['blunders',*DEFERRED]
                if counts.get(m) or preparing or (m == 'score_distribution' and score_stale) or (m == 'accuracy' and accuracy_stale)]+metadata_metrics+(['salience'] if player in salience_players else []),
                'pending_games': counts, 'deferred_metrics': deferred}
    players = await session.scalar(text(f'''SELECT count(DISTINCT gp.player_id) FROM {PENDING} q
        JOIN {layout.table('game_player')} gp ON gp.game_link=q.game_link
        JOIN {layout.table('player')} p ON p.id=gp.player_id WHERE p.joined>0 AND p.deleted_at IS NULL'''))
    preparing = await session.scalar(text(f'SELECT count(*) FROM {layout.table("fen_summary_refresh")}'))
    history = [dict(r) for r in (await session.execute(text(f'SELECT * FROM {RUN} ORDER BY created_at DESC LIMIT 20'))).mappings()]
    active = next((r for r in history if r['status'] in ('queued','running','retrying')), None)
    from chessism_api.database.stockfish_summary_schema import summary_table
    score_players = await session.scalar(text(f'SELECT count(*) FROM {summary_table(layout,"state")} WHERE revision<>published_revision')) if scores else 0
    accuracy_players = await session.scalar(text(f'SELECT count(*) FROM ({accuracy_store.needs_work(layout)}) q')) if accuracy else 0
    if scores:
        accuracy_union = f'UNION SELECT player_name FROM ({accuracy_store.needs_work(layout)}) a' if accuracy else ''
        metadata_union = f'UNION SELECT player_name FROM ({metadata_store.needed(layout)}) md' if await metadata_store.enabled(session) else ''
        salience_union = f'UNION SELECT player_name FROM ({salience_store.needed(layout)}) sa' if salience else ''
        players = await session.scalar(text(f'''SELECT count(*) FROM (
            SELECT p.player_name FROM {PENDING} q JOIN {layout.table('game_player')} gp ON gp.game_link=q.game_link
            JOIN {layout.table('player')} p ON p.id=gp.player_id WHERE p.joined>0 AND p.deleted_at IS NULL
            UNION SELECT player_name FROM {summary_table(layout,'state')} WHERE revision<>published_revision
            {accuracy_union}
            {metadata_union}
            {salience_union}
        ) pending_players'''))
    return {'manual': True, 'available_metrics': available, 'deferred_metrics': deferred,
            'pending_games': counts, 'pending_players': players, 'pending_score_players': score_players,
            'pending_accuracy_players': accuracy_players,
            'pending_salience_players': len(salience_players),
            'pending_behavioral_players': sum(r['behavior'] for r in metadata),
            'pending_unexpected_endings_players': sum(r['endings'] for r in metadata),
            'preparing_games': preparing,
            'active': active, 'latest': history[0] if history else None,
            'history': [r for r in history if not active or r['id'] != active['id']]}
