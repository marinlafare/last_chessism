"""Additive manual-run state. Installation never starts calculations."""
from sqlalchemy import text
from ..canonical_blunders.schema import table as canonical_table

CONTROL = 'public.cuda_summary_control'
PENDING = 'public.cuda_summary_pending'
RUN = 'public.cuda_summary_run'
ITEM = 'public.cuda_summary_item'


async def enabled(session):
    if not await session.scalar(text('SELECT to_regclass(:table) IS NOT NULL'), {'table': CONTROL}):
        return False
    return bool(await session.scalar(text(f'SELECT manual FROM {CONTROL} WHERE id')))


async def install(session, layout):
    if not layout:
        raise ValueError('Manual CUDA summaries require normalized storage.')
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {CONTROL} (
        id boolean PRIMARY KEY DEFAULT true CHECK(id), manual boolean NOT NULL DEFAULT false)'''))
    await session.execute(text(f'INSERT INTO {CONTROL}(id) VALUES(true) ON CONFLICT DO NOTHING'))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {PENDING} (
        metric text NOT NULL CHECK(metric IN ('blunders','accuracy','score_distribution')),
        game_link bigint NOT NULL,revision bigint NOT NULL,queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY(metric,game_link))'''))
    await session.execute(text(f'CREATE INDEX IF NOT EXISTS cuda_summary_pending_game ON {PENDING}(game_link)'))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {RUN} (
        id uuid PRIMARY KEY, requested_by text NOT NULL,model_id text NOT NULL,
        metrics text[] NOT NULL DEFAULT ARRAY['blunders'],
        status text NOT NULL CHECK(status IN ('queued','running','retrying','complete','failed')),
        phase text NOT NULL DEFAULT 'queued',created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        started_at timestamptz,finished_at timestamptz,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        total_games bigint NOT NULL DEFAULT 0,processed_games bigint NOT NULL DEFAULT 0,
        superseded_games bigint NOT NULL DEFAULT 0,attempts int NOT NULL DEFAULT 0,
        retry_after timestamptz,error text,timings jsonb NOT NULL DEFAULT '{{}}')'''))
    await session.execute(text(f'''CREATE UNIQUE INDEX IF NOT EXISTS cuda_summary_one_active
        ON {RUN}((true)) WHERE status IN ('queued','running','retrying')'''))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {ITEM} (
        run_id uuid NOT NULL REFERENCES {RUN}(id),game_link bigint NOT NULL,revision bigint NOT NULL,
        status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','published','superseded')),
        PRIMARY KEY(run_id,game_link))'''))
    await session.execute(text(f'''CREATE INDEX IF NOT EXISTS cuda_summary_item_pending
        ON {ITEM}(run_id,game_link) WHERE status='pending' '''))
    # Reuse canonical invalidation, which already covers evaluation, ownership,
    # coverage, rating, association and deletion changes. Statement triggers
    # coalesce games rather than adding one task per FEN or ingestion batch.
    await session.execute(text(f'''CREATE OR REPLACE FUNCTION public.queue_cuda_summaries()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF NOT (SELECT manual FROM {CONTROL} WHERE id) THEN RETURN NULL; END IF;
        INSERT INTO {PENDING}(metric,game_link,revision)
            SELECT m.metric,r.game_link,r.revision FROM new_rows r CROSS JOIN
                unnest(ARRAY['blunders','accuracy','score_distribution']) m(metric)
            ORDER BY m.metric,r.game_link
        ON CONFLICT(metric,game_link) DO UPDATE SET revision=EXCLUDED.revision;
        RETURN NULL; END $$'''))
    for event in ('INSERT', 'UPDATE'):
        name = 'queue_cuda_summaries_' + event.lower()
        await session.execute(text(f'DROP TRIGGER IF EXISTS {name} ON {canonical_table(layout,"dirty")}'))
        await session.execute(text(f'''CREATE TRIGGER {name} AFTER {event} ON {canonical_table(layout,'dirty')}
            REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.queue_cuda_summaries()'''))
    from .score_runs import install as install_scores
    await install_scores(session)
    from .accuracy_store import install as install_accuracy
    await install_accuracy(session)
    from .metadata_store import install as install_metadata
    await install_metadata(session)
    from .salience_store import install as install_salience
    await install_salience(session, layout)


async def activate(session, layout):
    """Call only after upgraded producers/consumers are deployed and idle."""
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    for relation, predicate in (
        ('cloud_analysis_job', "status NOT IN ('complete','cancelled','limit_reached','failed')"),
        ('algorithm_run', "status IN ('queued','running','cancelling')"),
        ('ingestion_pipeline_run', "status IN ('queued','running')"),
    ):
        if await session.scalar(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': relation}):
            if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation} WHERE {predicate})')):
                raise ValueError(f'Wait for active {relation} work before activating manual summaries.')
    await session.execute(text(f'UPDATE {CONTROL} SET manual=true WHERE id'))
    await session.execute(text(f'''INSERT INTO {PENDING}(metric,game_link,revision)
        SELECT m.metric,d.game_link,d.revision FROM {canonical_table(layout,'dirty')} d
        CROSS JOIN unnest(ARRAY['blunders','accuracy','score_distribution']) m(metric)
        ON CONFLICT(metric,game_link) DO UPDATE SET revision=EXCLUDED.revision'''))
