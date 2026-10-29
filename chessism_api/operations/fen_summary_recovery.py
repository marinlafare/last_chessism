"""Durable game-summary refresh requests committed with local FEN changes.

Triggers only queue affected identities. Bounded background recovery rebuilds
absolute coverage/engine summaries and acknowledges the exact captured revision.
Cloud-reserved FENs retain their existing durable cloud projection/retry workflow.
"""
import json
import logging
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import identifier, storage_for

LOG = logging.getLogger(__name__)
BATCH_GAMES = 500
SOURCE_ONLY_BATCH_GAMES = 32768
PIPELINE_VERSION = 'evaluation_refresh_v4'


def table(layout):
    return layout.table('fen_summary_refresh')


async def upgrade(session):
    """Explicit, idle-only trigger upgrade; no backfill, inference, or snapshots."""
    from .canonical_blunders import schema
    layout = storage_for(session)
    if not layout or not await schema.installed(session, layout):
        raise ValueError('Install canonical summaries before upgrading the input pipeline')
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    control = (await session.execute(text(f'SELECT * FROM {schema.table(layout,"control")} WHERE id FOR UPDATE'))).mappings().one()
    if control['active_model'] != control['target_model']:
        raise ValueError('Wait for the active model rebuild before upgrading the input pipeline')
    for relation in (schema.table(layout, 'dirty'), table(layout)):
        if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation})')):
            raise ValueError('Wait for pending summary refreshes before upgrading the input pipeline')
    for relation, predicate in (
        ('cloud_analysis_job', "status NOT IN ('complete','cancelled','limit_reached','failed')"),
        ('algorithm_run', "status IN ('queued','running','cancelling')"),
        ('ingestion_pipeline_run', "status IN ('queued','running')"),
    ):
        if await session.scalar(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': relation}):
            if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation} WHERE {predicate})')):
                raise ValueError(f'Wait for active {relation} work before upgrading the input pipeline')
    await install(session, layout)
    return {'input_pipeline': PIPELINE_VERSION, 'model_id': control['active_model'],
            'inference_changed': False, 'scores_changed': 0, 'games_queued': 0}


async def install(connection, layout):
    queue = table(layout)
    await connection.execute(text(f'''CREATE TABLE IF NOT EXISTS {queue} (
        game_link bigint PRIMARY KEY, revision bigserial NOT NULL,
        queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        retry_after timestamptz NOT NULL DEFAULT clock_timestamp(),
        attempts integer NOT NULL DEFAULT 0, error text)'''))
    # Old requests have no change reason: retain the conservative full refresh.
    await connection.execute(text(f'''ALTER TABLE {queue} ADD COLUMN IF NOT EXISTS
        refresh_engine boolean NOT NULL DEFAULT true'''))
    await connection.execute(text(f'''CREATE INDEX IF NOT EXISTS fen_summary_refresh_ready
        ON {queue}(retry_after,queued_at,game_link)'''))
    await connection.execute(text(f'''CREATE INDEX IF NOT EXISTS fen_summary_refresh_source_only
        ON {queue}(game_link) WHERE NOT refresh_engine'''))
    # A claim exists when the cloud writer saves a batch, and is released in the
    # same transaction. Its persisted job already guarantees projection recovery.
    claims = await connection.scalar(text("SELECT to_regclass('public.cloud_fen_claim') IS NOT NULL"))
    skip_cloud = 'AND NOT EXISTS(SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=n.fen)' if claims else ''
    function = f'{identifier(layout.core)}.queue_fen_summary_refresh'
    # Ingestion's conflict-locking INSERT fires an UPDATE trigger with zero
    # transition rows, followed by metadata/counter updates with thousands.
    # A cached old/new JOIN plan learned from the empty statements degenerates
    # into repeated transition scans. Compare only evaluation fields as sets;
    # never touch the large association table when no evaluation changed.
    # Keep the changed IDs statement-local (not a session flag or temp table),
    # so nesting, pooled connections and rollback cannot suppress real changes.
    await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        DECLARE changed_ids bigint[]; engine_ids bigint[];
        BEGIN
        SELECT array_agg(fen_id) INTO changed_ids FROM (
            SELECT fen_id,score,analysis_source FROM new_fens
            EXCEPT
            SELECT fen_id,score,analysis_source FROM old_fens
        ) changed;
        IF changed_ids IS NULL THEN RETURN NULL; END IF;
        -- Legacy CPU projections use the score and whether it is tablebase,
        -- not the Stockfish source label. Compare sets, avoiding a quadratic
        -- transition-table join after empty/no-op ingestion statements.
        SELECT array_agg(fen_id) INTO engine_ids FROM (
            SELECT fen_id,score,COALESCE(analysis_source='tablebase',false) FROM new_fens
            EXCEPT
            SELECT fen_id,score,COALESCE(analysis_source='tablebase',false) FROM old_fens
        ) changed;
        INSERT INTO {queue} AS pending(game_link,refresh_engine)
            SELECT a.game_link,bool_or(n.fen_id=ANY(COALESCE(engine_ids,ARRAY[]::bigint[])))
            FROM unnest(changed_ids) changed(fen_id)
            JOIN {layout.table('fen')} n USING(fen_id)
            JOIN {layout.table('game_fen_association')} a ON a.fen_id=n.fen_id
            WHERE true
              {skip_cloud}
            GROUP BY a.game_link ORDER BY a.game_link
        ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
            refresh_engine=pending.refresh_engine OR EXCLUDED.refresh_engine,
            retry_after=clock_timestamp(), attempts=0, error=NULL;
        RETURN NULL; END $$'''))
    await connection.execute(text(f'DROP TRIGGER IF EXISTS fen_summary_refresh ON {layout.table("fen")}'))
    await connection.execute(text(f'''CREATE TRIGGER fen_summary_refresh AFTER UPDATE ON {layout.table('fen')}
        REFERENCING OLD TABLE AS old_fens NEW TABLE AS new_fens
        FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
    # New games may reuse only already-scored positions. No FEN UPDATE happens
    # in that case, so association/ownership commits must also persist recovery.
    # One row per affected game coalesces repeated ingestion batches and retries.
    for relation in ('game_fen_association', 'game_player'):
        for event in ('INSERT', 'UPDATE', 'DELETE'):
            aliases = ('old_rows', 'new_rows') if event == 'UPDATE' else (
                'new_rows' if event == 'INSERT' else 'old_rows',)
            selected = ' UNION '.join(f'SELECT game_link FROM {alias}' for alias in aliases)
            name = f'fen_summary_{relation}_{event.lower()}'
            function = f'{identifier(layout.core)}.{name}'
            await connection.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO {queue}(game_link) SELECT DISTINCT game_link FROM ({selected}) changed
                    WHERE game_link IS NOT NULL ORDER BY game_link
                ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
                    refresh_engine=true,retry_after=clock_timestamp(),attempts=0,error=NULL;
                RETURN NULL; END $$'''))
            await connection.execute(text(f'DROP TRIGGER IF EXISTS {name} ON {layout.table(relation)}'))
            refs = ' '.join('OLD TABLE AS old_rows' if alias == 'old_rows' else 'NEW TABLE AS new_rows'
                            for alias in aliases)
            await connection.execute(text(f'''CREATE TRIGGER {name} AFTER {event} ON {layout.table(relation)}
                REFERENCING {refs} FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))


async def recover_batch(*, factory=None, refresh=None):
    factory = factory or AsyncDBSession
    if refresh is None:
        from chessism_api.database.ask_db import refresh_game_analysis_summary
        refresh = refresh_game_analysis_summary
    # Source-only updates do not touch coverage, accuracy, efficiency or legacy
    # CPU blunders. Transfer their exact revisions to CUDA in one transaction.
    # A concurrent real evaluation/association change either wins before the
    # row lock (and remains full), or creates a fresh request after this commit.
    async with factory() as session, session.begin():
        await session.execute(text("SET LOCAL lock_timeout='2s'"))
        await session.execute(text("SET LOCAL statement_timeout='10s'"))
        layout = storage_for(session)
        from .canonical_blunders.schema import table as canonical_table
        source_only = [dict(r) for r in (await session.execute(text(f'''
            SELECT game_link,revision FROM {table(layout)}
            WHERE NOT refresh_engine AND retry_after<=clock_timestamp()
            ORDER BY game_link LIMIT :limit FOR UPDATE SKIP LOCKED'''),
            {'limit': SOURCE_ONLY_BATCH_GAMES})).mappings()]
        if source_only:
            await session.execute(text(f'''INSERT INTO {canonical_table(layout,'dirty')}(game_link)
                SELECT unnest(CAST(:links AS bigint[])) ORDER BY 1
                ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision'''),
                {'links': [r['game_link'] for r in source_only]})
            await session.execute(text(f'''DELETE FROM {table(layout)} q USING
                jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(game_link bigint,revision bigint)
                WHERE q.game_link=r.game_link AND q.revision=r.revision AND NOT q.refresh_engine'''),
                {'rows': json.dumps(source_only)})
    async with factory() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        layout = storage_for(session)
        rows = [dict(row) for row in (await session.execute(text(f'''
            SELECT game_link,revision FROM {table(layout)} WHERE refresh_engine AND retry_after<=clock_timestamp()
            ORDER BY queued_at,game_link LIMIT :limit'''), {'limit': BATCH_GAMES})).mappings()]
    error = None
    if rows:
        try:
            await refresh(tuple(row['game_link'] for row in rows), strict=True)
        except Exception as failure:
            error = f'{type(failure).__name__}: {failure}'[:1000]
            LOG.exception('Game summary refresh retained for retry; FEN scores are safe.')
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL lock_timeout='2s'"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            captured = 'jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(game_link bigint,revision bigint)'
            if error:
                await session.execute(text(f'''UPDATE {table(layout)} q SET attempts=q.attempts+1,
                    error=:error,retry_after=clock_timestamp()+interval '60 seconds'
                    FROM {captured} WHERE q.game_link=r.game_link AND q.revision=r.revision'''),
                    {'rows': json.dumps(rows), 'error': error})
            else:
                await session.execute(text(f'''DELETE FROM {table(layout)} q USING {captured}
                    WHERE q.game_link=r.game_link AND q.revision=r.revision'''), {'rows': json.dumps(rows)})
    async with factory() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        pending = await session.scalar(text(f'SELECT count(*) FROM {table(layout)}'))
    return {'processed': (len(rows) if error is None else 0) + len(source_only),
            'source_only': len(source_only), 'engine_refreshed': len(rows) if error is None else 0,
            'pending': pending, 'error': error}
