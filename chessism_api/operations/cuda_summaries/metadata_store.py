"""Captured incremental metadata scopes and additive, opt-in CUDA ownership."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.player_analytics_schema import analytics_table
from chessism_api.database.stockfish_summary_schema import summary_table
from .schema import CONTROL,RUN
from .behavior_compute import VERSION as BEHAVIOR_VERSION
from .ending_compute import VERSION as ENDING_VERSION

STATE='public.cuda_metadata_state'
SCOPE='public.cuda_metadata_scope'
DAY='public.cuda_metadata_day'
PART='public.cuda_metadata_part'
ENDING_PART='public.cuda_ending_partition'
METRICS=('behavioral','unexpected_endings')


async def enabled(session):
    if not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'),{'t':CONTROL}):return False
    return bool(await session.scalar(text(f"SELECT COALESCE((to_jsonb(c)->>'metadata')::boolean,false) AND manual FROM {CONTROL} c WHERE id")))


async def install(session):
    await session.execute(text(f'ALTER TABLE {CONTROL} ADD COLUMN IF NOT EXISTS metadata boolean NOT NULL DEFAULT false'))
    for relation,fields in (
        (STATE,'''player_name text PRIMARY KEY,behavior_revision bigint NOT NULL,score_revision bigint NOT NULL,
            behavior_version text NOT NULL,ending_version text NOT NULL,payload jsonb NOT NULL,
            generated_at timestamptz NOT NULL DEFAULT clock_timestamp()'''),
        (SCOPE,f'''run_id uuid REFERENCES {RUN}(id),player_name text,behavior_revision bigint NOT NULL,
            score_revision bigint NOT NULL,behavior boolean NOT NULL,endings boolean NOT NULL,
            full_behavior boolean NOT NULL,full_endings boolean NOT NULL,profile jsonb NOT NULL,
            status text NOT NULL DEFAULT 'pending',result jsonb,PRIMARY KEY(run_id,player_name)'''),
        (DAY,f'''run_id uuid REFERENCES {RUN}(id),player_name text,day date,
            behavior boolean NOT NULL DEFAULT false,endings boolean NOT NULL DEFAULT false,
            PRIMARY KEY(run_id,player_name,day)'''),
        (PART,f'''run_id uuid REFERENCES {RUN}(id),batch bigint,player_name text,day date,
            behavior jsonb,endings jsonb,PRIMARY KEY(run_id,batch,player_name,day)'''),
        (ENDING_PART,'player_name text,day date,payload jsonb NOT NULL,PRIMARY KEY(player_name,day)'),
        ('public.cuda_metadata_baseline',f'run_id uuid REFERENCES {RUN}(id),player_name text,section text,payload jsonb NOT NULL,PRIMARY KEY(run_id,player_name,section)'),
    ):await session.execute(text(f'CREATE TABLE IF NOT EXISTS {relation}({fields})'))


def needed(layout):
    return f'''SELECT a.player_name,a.revision AS behavior_revision,s.revision AS score_revision,
        (a.revision<>a.published_revision OR c.behavior_version IS DISTINCT FROM '{BEHAVIOR_VERSION}') AS behavior,
        (c.score_revision IS DISTINCT FROM s.revision OR c.ending_version IS DISTINCT FROM '{ENDING_VERSION}') AS endings,
        (a.full_rebuild OR c.behavior_version IS DISTINCT FROM '{BEHAVIOR_VERSION}') AS full_behavior,
        (s.full_rebuild OR c.ending_version IS DISTINCT FROM '{ENDING_VERSION}') AS full_endings,
        jsonb_build_object('player_name',p.player_name,'joined',p.joined,'deleted_at',p.deleted_at,
            'country',p.country,'location',p.location,'timezone',p.timezone,'timezone_source',p.timezone_source) AS profile
        FROM {analytics_table(layout,'state')} a JOIN {summary_table(layout,'state')} s USING(player_name)
        JOIN {layout.table('player')} p USING(player_name) LEFT JOIN {STATE} c USING(player_name)
        WHERE ((p.joined>0 AND p.deleted_at IS NULL) OR c.player_name IS NOT NULL)
          AND (a.revision<>a.published_revision OR c.behavior_version IS DISTINCT FROM '{BEHAVIOR_VERSION}'
            OR c.score_revision IS DISTINCT FROM s.revision OR c.ending_version IS DISTINCT FROM '{ENDING_VERSION}')'''


async def capture(session,run_id):
    if not await enabled(session):return 0
    layout=storage_for(session);params={'id':run_id}
    await session.execute(text(f'''INSERT INTO {SCOPE}(run_id,player_name,behavior_revision,score_revision,
        behavior,endings,full_behavior,full_endings,profile) SELECT :id,q.* FROM ({needed(layout)}) q'''),params)
    for flag,table in (('behavior',analytics_table(layout,'dirty')),('endings',summary_table(layout,'dirty'))):
        await session.execute(text(f'''INSERT INTO {DAY}(run_id,player_name,day,{flag})
            SELECT :id,d.player_name,d.day,true FROM {table} d JOIN {SCOPE} s
            ON s.run_id=:id AND s.player_name=d.player_name WHERE s.{flag}
            ON CONFLICT(run_id,player_name,day) DO UPDATE SET {flag}=true'''),params)
    return await session.scalar(text(f'SELECT count(*) FROM {SCOPE} WHERE run_id=:id'),params)


async def pending(session,player=None):
    if not await enabled(session):return []
    query=needed(storage_for(session))
    return [dict(r) for r in (await session.execute(text(f'''SELECT player_name,behavior,endings FROM ({query}) q
        WHERE (CAST(:player AS text) IS NULL OR player_name=:player)'''),{'player':player})).mappings()]


async def activate(session):
    # Lock admission for producers and the old metadata writer at the handoff.
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    for relation,predicate in ((RUN,"status IN ('queued','running','retrying')"),
        ('algorithm_run',"status IN ('queued','running','cancelling')"),
        ('cloud_analysis_job',"status NOT IN ('complete','cancelled','failed','limit_reached')"),
        ('ingestion_pipeline_run',"status IN ('queued','running')")):
        await session.execute(text(f'LOCK TABLE {relation} IN SHARE MODE NOWAIT'))
        if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation} WHERE {predicate})')):
            raise ValueError('Wait for active producers and CUDA jobs before migrating metadata.')
    await session.execute(text(f'LOCK TABLE {analytics_table(storage_for(session),"state")} IN SHARE MODE NOWAIT'))
    await session.execute(text(f'UPDATE {CONTROL} SET metadata=true WHERE id AND manual AND scores AND accuracy'))
