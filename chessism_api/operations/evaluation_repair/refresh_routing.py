"""One-time, receipt-backed routing of the exclusive legacy repair backlog.

This is NOT a generic inference of queue reasons. The legacy repair ran under
idle-work/admission guards, tablebases first, source-only batches last. Require
that ordering and no other recorded evaluation/ingestion activity in that
interval. Preserve older requests, later writers, failed requests, and all
requests first queued during the score-changing phase. No FENs are written.
"""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from .. import fen_summary_recovery as recovery
from .legacy_policy import POLICY, SOURCE
from .plan import require_idle
from .artifacts import digest


async def route(session, run_id, *, apply=False):
    layout = storage_for(session)
    if not layout or not layout.compact or not run_id:
        raise ValueError('Explicit legacy repair identity and compact storage required')
    await session.execute(text("SET LOCAL statement_timeout='120s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text("SET LOCAL work_mem='32MB'"))
    await session.execute(text('SET LOCAL jit=off'))
    if apply:
        if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('chessism:evaluation-repair:v1',0))")):
            raise ValueError('An evaluation repair is still running')
        # Keep evaluation/job/ownership writes outside this short migration.
        for name in sorted(['public.' + name for name in ('cloud_analysis_job',
                            'ingestion_pipeline_run', 'algorithm_run', 'analysis_time')] +
                           [layout.table(n) for n in ('fen', 'game', 'game_player', 'game_fen_association')]):
            await session.execute(text(f'LOCK TABLE {name} IN SHARE MODE NOWAIT'))
    await require_idle(session, read_only=False)
    receipt = layout.table('evaluation_repair_receipt')
    receipt_id = digest({'operation': 'source_only_refresh_routing_v1', 'run_id': run_id})
    prior = await session.scalar(text(f'SELECT payload FROM {receipt} WHERE id=:id'),
                                 {'id': receipt_id})
    if prior is not None:
        return {key: value for key, value in {**prior, 'already_applied': True}.items()
                if key != 'queue_revisions'}
    await session.execute(text('''CREATE TEMP TABLE provenance_receipt_routes(
        transaction_id text PRIMARY KEY, committed_at timestamptz, source_only boolean)
        ON COMMIT DROP'''))
    await session.execute(text(f'''INSERT INTO provenance_receipt_routes
        SELECT r.xmin::text,r.committed_at,
          jsonb_array_length(payload->'before')>0 AND payload->'after'=(
            SELECT jsonb_agg(value || jsonb_build_object('analysis_source',CAST(:source AS text)))
            FROM jsonb_array_elements(payload->'before'))
        FROM {receipt} r WHERE payload->>'run_id'=:run AND payload->>'policy'=:policy'''),
        {'run': run_id, 'policy': POLICY, 'source': SOURCE})
    bounds = (await session.execute(text('''SELECT count(*) AS receipts,
        min(committed_at) FILTER(WHERE source_only) AS first_source,
        max(committed_at) AS finished,
        max(committed_at) FILTER(WHERE NOT source_only) AS full_until
        FROM provenance_receipt_routes'''))).mappings().one()
    if not bounds['receipts'] or not bounds['first_source'] or not bounds['full_until']:
        raise ValueError('Expected complete, separate tablebase and attribution phases')
    if bounds['first_source'] <= bounds['full_until']:
        raise ValueError('Repair phases overlap; cannot infer old queue reasons')
    params = {'start': bounds['full_until'], 'end': bounds['finished']}
    # These histories also catch jobs that completed between the original
    # per-batch idle checks. Ambiguous runs must retain the full refresh path.
    for name, predicate in (
        ('ingestion_pipeline_run', 'started_at<=:end AND (finished_at IS NULL OR finished_at>=:start)'),
        ('cloud_analysis_job', 'created_at<=:end AND updated_at>=:start'),
        ('algorithm_run', 'COALESCE(started_at,created_at)<=:end AND (finished_at IS NULL OR finished_at>=:start)'),
        ('analysis_time', 'created_at BETWEEN :start AND :end'),
    ):
        if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM public.{name} WHERE {predicate})'), params):
            raise ValueError(f'Overlapping {name} history; keep conservative refreshes')
    # xmin is evidence only for this immediate migration, never a durable ID.
    # Frozen/rewritten tuples or later writers simply fail to match and remain.
    queue = recovery.table(layout)
    eligible = f'''FROM {queue} q JOIN provenance_receipt_routes r
        ON q.xmin::text=r.transaction_id AND r.source_only
        WHERE q.queued_at>:start AND q.queued_at<=:end
          AND q.error IS NULL AND q.attempts=0'''
    result = {**dict(bounds), 'run_id': run_id, 'scores_changed': 0,
              'eligible': await session.scalar(text('SELECT count(*) ' + eligible), params),
              'applied': False}
    if apply:
        # Preserve queue rows/revisions: only their scheduling reason changes.
        await session.execute(text('''CREATE TEMP TABLE provenance_refresh_routes(
            game_link bigint PRIMARY KEY,revision bigint) ON COMMIT DROP'''))
        await session.execute(text('INSERT INTO provenance_refresh_routes SELECT q.game_link,q.revision ' + eligible), params)
        await recovery.install(session, layout)
        changed = await session.execute(text(f'''UPDATE {queue} q SET refresh_engine=false
            FROM provenance_refresh_routes r WHERE q.game_link=r.game_link AND q.revision=r.revision'''))
        result.update(applied=True, routed=changed.rowcount)
        # Reuse immutable repair receipts for an audit, including exact revisions.
        await session.execute(text(f'''INSERT INTO {receipt}(id,actor,payload)
            SELECT :id,'provenance-refresh-routing',CAST(:summary AS jsonb) ||
                jsonb_build_object('queue_revisions',COALESCE(jsonb_agg(jsonb_build_array(game_link,revision)),'[]'))
            FROM provenance_refresh_routes'''), {'id': receipt_id,
                'summary': json.dumps(result, default=str)})
    return result
