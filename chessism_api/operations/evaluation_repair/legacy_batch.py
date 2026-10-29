"""Transactional legacy repair with immutable originals and normal triggers.

No trigger disabling, fabricated dates, new-scored increments or worker restarts.
The CLI owns backup-lane exclusion and invalidation before the first commit.
"""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from . import aggregates, rating_delta, legacy_policy as policy
from .artifacts import digest
from .plan import require_idle


async def configure(session, *, readonly=False):
    if readonly:
        await session.execute(text('SET TRANSACTION READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='120s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text('SET LOCAL jit=off'))
    layout = storage_for(session)
    if not layout or not layout.compact:
        raise ValueError('Legacy repair requires compact normalized storage')
    return layout


async def discover(session):
    layout = await configure(session, readonly=True)
    await require_idle(session)
    rows = (await session.execute(text(f'''SELECT fen_id,{policy.PIECES} AS pieces,
        abs(score)>=9000 AS mate FROM {layout.table('fen')} WHERE {policy.MISSING}
        ORDER BY fen_id'''))).all()
    groups = {'tablebase': [], 'assumed': [], 'held_mate': []}
    for fen_id, pieces, mate in rows:
        if not 2 <= pieces <= 32:
            raise ValueError(f'Invalid piece count on FEN {fen_id}')
        groups['tablebase' if pieces <= 5 else 'held_mate' if mate else 'assumed'].append(fen_id)
    return {'policy': policy.POLICY, 'created_at': str(await session.scalar(text('SELECT clock_timestamp()'))),
            'ids': groups, 'counts': {key: len(ids) for key, ids in groups.items()}}


async def load(session, ids):
    layout = await configure(session, readonly=True)
    if not 1 <= len(ids) <= policy.MAX_BATCH or len(set(ids)) != len(ids):
        raise ValueError('Invalid repair batch')
    return list((await session.execute(text(f'''SELECT to_jsonb(f) FROM {layout.table('fen')} f
        WHERE fen_id=ANY(CAST(:ids AS bigint[])) ORDER BY fen_id'''), {'ids': ids})).scalars())


async def publish(session, candidates, *, run_id, actor, acknowledgement):
    policy.validate(candidates)
    if acknowledgement != policy.ACK or not actor or len(actor) > 200:
        raise ValueError('Explicit legacy repair approval is required')
    ids = [c['before']['fen_id'] for c in candidates]
    key = digest({'policy': policy.POLICY, 'run': run_id, 'ids': ids})
    layout = await configure(session)
    receipt = layout.table('evaluation_repair_receipt')
    previous = await session.scalar(text(f'SELECT payload FROM {receipt} WHERE id=:id'), {'id': key})
    if previous is not None:
        return {'status': 'already_published', 'receipt_id': key, 'changed': 0}
    if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('chessism:evaluation-repair:v1',0))")):
        raise ValueError('Another evaluation repair is running')
    # Hold admission/association locks only for this bounded write transaction.
    for name in sorted(['public.' + name for name in ('cloud_analysis_job', 'cloud_fen_claim',
                        'ingestion_pipeline_run', 'algorithm_run')] +
                       [layout.table(name) for name in ('game_fen_association', 'game_player', 'game', 'player')]):
        await session.execute(text(f'LOCK TABLE {name} IN SHARE MODE NOWAIT'))
    await require_idle(session, read_only=False)
    if await session.scalar(text("SELECT current_setting('session_replication_role')")) != 'origin':
        raise ValueError('Normal trigger execution is required')
    for relation, name in ((receipt, 'evaluation_repair_immutable'), (layout.table('fen'), 'fen_summary_refresh')):
        if not await session.scalar(text('''SELECT EXISTS(SELECT 1 FROM pg_trigger
            WHERE tgrelid=to_regclass(:relation) AND tgname=:name AND tgenabled IN ('O','A'))'''),
            {'relation': relation, 'name': name}):
            raise ValueError(f'Required trigger missing: {name}')
    current = list((await session.execute(text(f'''SELECT to_jsonb(f) FROM {layout.table('fen')} f
        WHERE fen_id=ANY(CAST(:ids AS bigint[])) ORDER BY fen_id FOR UPDATE NOWAIT'''), {'ids': ids})).scalars())
    if current != [c['before'] for c in candidates]:
        raise ValueError('Stale repair inputs; nothing was committed')
    if await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM public.cloud_fen_claim c
        JOIN {layout.table('fen')} f ON f.fen=c.fen WHERE f.fen_id=ANY(CAST(:ids AS bigint[])))'''), {'ids': ids}):
        raise ValueError('Cloud-claimed positions cannot be repaired')
    small_ids = [c['before']['fen_id'] for c in candidates if c['replacement']['analysis_source'] == 'tablebase']
    continuations = list((await session.execute(text(f'''SELECT to_jsonb(c) FROM {layout.table('fen_continuation')} c
        WHERE fen_id=ANY(CAST(:ids AS bigint[])) ORDER BY fen_id,rank'''), {'ids': small_ids})).scalars())
    # Summary counters and rating categories change atomically with the scores.
    global_delta = await aggregates.apply(session, candidates)
    rating = await rating_delta.apply(session, layout, candidates)
    replacements = [c['replacement'] for c in candidates]
    result = await session.execute(text(f'''UPDATE {layout.table('fen')} f SET
        score=r.score,piece_count=r.piece_count,next_moves=r.next_moves,
        wdl_win=r.wdl_win,wdl_draw=r.wdl_draw,wdl_loss=r.wdl_loss,
        analysis_source=r.analysis_source,tablebase_wdl=r.tablebase_wdl,
        tablebase_dtz=r.tablebase_dtz,analyzed_at=r.analyzed_at
        FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(fen_id bigint,score integer,piece_count smallint,
            next_moves text,wdl_win smallint,wdl_draw smallint,wdl_loss smallint,
            analysis_source text,tablebase_wdl smallint,tablebase_dtz integer,analyzed_at timestamptz)
        WHERE f.fen_id=r.fen_id'''), {'rows': json.dumps(replacements, allow_nan=False)})
    if result.rowcount != len(candidates):
        raise ValueError('Incomplete repair write')
    if small_ids:
        await session.execute(text(f'DELETE FROM {layout.table("fen_continuation")} WHERE fen_id=ANY(CAST(:ids AS bigint[]))'), {'ids': small_ids})
    if await session.scalar(text(f'''SELECT EXISTS(
        SELECT 1 FROM {layout.table('game_fen_association')} a
        WHERE a.fen_id=ANY(CAST(:ids AS bigint[])) AND NOT EXISTS(
            SELECT 1 FROM {layout.table('fen_summary_refresh')} q WHERE q.game_link=a.game_link))'''), {'ids': ids}):
        raise ValueError('Some affected games were not durably queued')
    payload = {'policy': policy.POLICY, 'run_id': run_id, 'actor': actor, 'acknowledgement': acknowledgement,
               'processed_ids': ids,
               'before': current, 'after': replacements, 'removed_continuations': continuations,
               'global_delta': global_delta, 'rating_delta': rating,
               'source_is_assumed': policy.SOURCE, 'historical_timestamps_fabricated': False}
    await session.execute(text(f'INSERT INTO {receipt}(id,actor,payload) VALUES(:id,:actor,CAST(:payload AS jsonb))'),
        {'id': key, 'actor': actor, 'payload': json.dumps(payload, allow_nan=False)})
    return {'status': 'published', 'receipt_id': key, 'changed': len(candidates),
            'tablebase': len(small_ids), 'assumed': len(candidates)-len(small_ids)}
