"""Index-bounded snapshots and impact checks, under READ ONLY transactions."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from .artifacts import VERSION, MAX_FENS, MAX_REFERENCES, MAX_CONTINUATIONS, seal, verify, snapshot_digest

MAX_WINDOW = 100_000


async def prepare(session):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='10s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text("SET LOCAL max_parallel_workers_per_gather=0"))
    await session.execute(text("SET LOCAL jit=off"))


async def require_idle(session, *, read_only=True):
    if read_only and await session.scalar(text("SELECT current_setting('transaction_read_only')")) != 'on':
        raise ValueError('Repair planning requires a read-only transaction')
    for table, predicate in (
        ('cloud_analysis_job', "status NOT IN('complete','cancelled','limit_reached','failed')"),
        ('ingestion_pipeline_run', "status IN('queued','running')"),
        ('algorithm_run', "status IN('queued','running','cancelling')"),
    ):
        if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {table} WHERE {predicate})')):
            raise ValueError(f'Wait for active {table} work before the repair pilot')


async def snapshot(session, fen_id):
    layout = storage_for(session)
    if not layout or not layout.compact:
        raise ValueError('Repair planning requires compact relational storage')
    row = (await session.execute(text(f'''SELECT to_jsonb(f) AS before,f.xmin::text AS row_revision
        FROM {layout.table('fen')} f WHERE fen_id=:id'''), {'id': fen_id})).mappings().one_or_none()
    if row is None:
        return None
    item = dict(row)
    if await session.scalar(text('SELECT EXISTS(SELECT 1 FROM public.cloud_fen_claim WHERE fen=:fen)'),
                            {'fen': item['before']['fen']}):
        return None
    # LIMIT applies before sorting/deduplication: even stale n_games cannot
    # make one popular opening expand into a whole-cohort scan.
    refs = [dict(r) for r in (await session.execute(text(f'''SELECT game_link,n_move,is_white
        FROM {layout.table('game_fen_association')} WHERE fen_id=:id LIMIT :bound'''),
        {'id': fen_id, 'bound': MAX_REFERENCES + 1})).mappings()]
    if not 1 <= len(refs) <= MAX_REFERENCES:
        return None
    item['references'] = sorted(refs, key=lambda r: (r['game_link'], r['n_move'], r['is_white']))
    item['continuations'] = [dict(r) for r in (await session.execute(text(f'''SELECT rank,move,score
        FROM {layout.table('fen_continuation')} WHERE fen_id=:id ORDER BY rank LIMIT :bound'''),
        {'id': fen_id, 'bound': MAX_CONTINUATIONS + 1})).mappings()]
    if len(item['continuations']) > MAX_CONTINUATIONS:
        return None
    links = sorted({r['game_link'] for r in refs})
    item['players'] = [dict(r) for r in (await session.execute(text(f'''SELECT DISTINCT p.player_name,
        p.joined,p.deleted_at::text AS deleted_at FROM {layout.table('game_player')} gp
        JOIN {layout.table('player')} p ON p.id=gp.player_id
        WHERE gp.game_link=ANY(CAST(:links AS bigint[])) ORDER BY p.player_name'''), {'links': links})).mappings()]
    item['snapshot_sha256'] = snapshot_digest(item)
    return item


async def preview(session, *, after_id=0, window=MAX_WINDOW, limit=8, mates_only=False, cp_only=False):
    for value, lower, upper in ((after_id, 0, 2**63-1-MAX_WINDOW), (window, 1, MAX_WINDOW),
                                (limit, 1, MAX_FENS)):
        if type(value) is not int or not lower <= value <= upper:
            raise ValueError('Invalid bounded repair selection')
    if mates_only and cp_only:
        raise ValueError('Choose one numeric score class')
    await require_idle(session)
    layout = storage_for(session)
    if not layout or not layout.compact:
        raise ValueError('Repair planning requires compact relational storage')
    ids = (await session.execute(text(f'''SELECT fen_id FROM {layout.table('fen')}
        WHERE fen_id>:after AND fen_id<=:end AND score IS NOT NULL
          AND analysis_source IS NULL AND analyzed_at IS NULL
          AND tablebase_wdl IS NULL AND tablebase_dtz IS NULL
          AND n_games BETWEEN 1 AND :refs {'AND abs(score)>=9000' if mates_only else ''}
          {'AND abs(score)<9000' if cp_only else ''}
        ORDER BY fen_id LIMIT :limit'''), {'after': after_id, 'end': after_id + window,
        'refs': MAX_REFERENCES, 'limit': MAX_FENS * 4})).scalars().all()
    items, skipped = [], []
    for fen_id in ids:
        item = await snapshot(session, fen_id)
        if item is None:
            skipped.append(fen_id)
        else:
            items.append(item)
        if len(items) == limit:
            break
    if not items:
        raise ValueError('No safe candidates in this bounded ID window; choose another explicit window')
    return await finish(session, items, skipped, {'after_id': after_id, 'window': window,
                        'mates_only': mates_only, 'cp_only': cp_only})


async def preview_ids(session, ids):
    if (not 1 <= len(ids) <= MAX_FENS or len(set(ids)) != len(ids)
            or any(type(i) is not int or not 0 < i < 2**63 for i in ids)):
        raise ValueError('Choose 1 to 16 unique explicit FEN IDs')
    await require_idle(session)
    items = []
    for fen_id in sorted(ids):
        item = await snapshot(session, fen_id)
        if item is None:
            raise ValueError(f'Unsafe, claimed or absent FEN {fen_id}')
        items.append(item)
    return await finish(session, items, [], {'explicit_ids': sorted(ids)})


async def finish(session, items, skipped, selection):
    result = seal({'version': VERSION, 'kind': 'plan', 'scores_changed': 0,
        'created_at': str(await session.scalar(text('SELECT clock_timestamp()'))),
        'selection': {**selection, 'max_references_per_fen': MAX_REFERENCES},
        'skipped_ids': skipped, 'items': items,
        'affected_games': sorted({r['game_link'] for item in items for r in item['references']}),
        'eligible_players': sorted({p['player_name'] for item in items for p in item['players']
                                   if p['joined'] and p['joined'] > 0 and not p['deleted_at']}),
        'publication': 'explicit_publication_requires_review'})
    return verify(result, 'plan')


async def recheck(session, plan):
    verify(plan, 'plan')
    await require_idle(session)
    changed = []
    for item in plan['items']:
        current = await snapshot(session, item['before']['fen_id'])
        if current is None or current['snapshot_sha256'] != item['snapshot_sha256']:
            changed.append(item['before']['fen_id'])
    return {'kind': 'recheck', 'version': VERSION, 'plan_sha256': plan['sha256'],
            'checked_at': str(await session.scalar(text('SELECT clock_timestamp()'))),
            'changed_ids': changed, 'unchanged': not changed, 'scores_changed': 0}
