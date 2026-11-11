"""Durable game-summary refresh requests committed with local FEN changes.

Triggers only queue affected identities. Bounded background recovery rebuilds
absolute coverage/engine summaries and acknowledges the exact captured revision.
Cloud-reserved FENs retain their existing durable cloud projection/retry workflow.
"""
import logging
from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import summary_refresh
from chessism_api.database.postgresql.transactions import driver_connection, write_transaction
from chessism_api.database.postgresql.relational_storage import identifier, storage_for

LOG = logging.getLogger(__name__)
BATCH_GAMES = 500
PIPELINE_VERSION = 'evaluation_refresh_v4'


# Legacy installation/upgrade entry points remain for the A3/A4 caller conversion.
# Canonical initialization and recover_batch never invoke them.
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
    """Refresh absolute projections, then acknowledge exactly the captured revisions.

    No database transaction is held during the refresh callback. If the process
    stops after a projection commit, the saved request safely repeats that work.
    """
    factory = factory or AsyncDBSession
    if refresh is None:
        from functools import partial
        from chessism_api.operations.game_analysis_summary import refresh_game_analysis_summary
        refresh = partial(refresh_game_analysis_summary, factory=factory)
    async with write_transaction(factory) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        rows = await summary_refresh.pending_batch(await driver_connection(session), BATCH_GAMES)
    error = None
    if rows:
        try:
            await refresh(tuple(row['game_link'] for row in rows))
        except Exception as failure:
            error = f'{type(failure).__name__}: {failure}'[:1000]
            LOG.exception('Game summary refresh retained for retry; FEN scores are safe.')
        async with write_transaction(factory) as session:
            await summary_refresh.finish_batch(await driver_connection(session), rows, error)
    async with write_transaction(factory) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        pending = await summary_refresh.pending_count(await driver_connection(session))
    processed = len(rows) if error is None else 0
    # Canonical scored sources are known; historical provenance-only relabeling
    # cannot enter this queue. Keep truthful existing report counters.
    return {'processed': processed, 'source_only': 0, 'engine_refreshed': processed,
            'pending': pending, 'error': error}
