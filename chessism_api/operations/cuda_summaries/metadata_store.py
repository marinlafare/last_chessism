"""Captured incremental metadata scopes and additive, opt-in CUDA ownership."""
from sqlalchemy import text
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
    return bool(await session.scalar(text(f"SELECT metadata AND manual FROM {CONTROL} c WHERE id")))




def needed():
    return f'''SELECT p.id AS player_id,p.player_name,a.revision AS behavior_revision,s.revision AS score_revision,
        (a.revision<>a.published_revision OR c.behavior_version IS DISTINCT FROM '{BEHAVIOR_VERSION}') AS behavior,
        (c.score_revision IS DISTINCT FROM s.revision OR c.ending_version IS DISTINCT FROM '{ENDING_VERSION}') AS endings,
        (a.full_rebuild OR c.behavior_version IS DISTINCT FROM '{BEHAVIOR_VERSION}') AS full_behavior,
        (s.full_rebuild OR c.ending_version IS DISTINCT FROM '{ENDING_VERSION}') AS full_endings,
        jsonb_build_object('player_name',p.player_name,'joined',p.joined,'deleted_at',p.deleted_at,
            'country',p.country,'location',p.location,'timezone',p.timezone,'timezone_source',p.timezone_source) AS profile
        FROM public.player_analytics_state a JOIN public.player p ON p.id=a.player_id
        JOIN public.player_stockfish_state s USING(player_name) LEFT JOIN {STATE} c USING(player_name)
        WHERE ((p.joined>0 AND p.deleted_at IS NULL) OR c.player_name IS NOT NULL)
          AND (a.revision<>a.published_revision OR c.behavior_version IS DISTINCT FROM '{BEHAVIOR_VERSION}'
            OR c.score_revision IS DISTINCT FROM s.revision OR c.ending_version IS DISTINCT FROM '{ENDING_VERSION}')'''


async def capture(session,run_id):
    if not await enabled(session):return 0
    params={'id':run_id}
    await session.execute(text(f'''INSERT INTO {SCOPE}(run_id,player_id,player_name,behavior_revision,score_revision,
        behavior,endings,full_behavior,full_endings,profile) SELECT :id,q.* FROM ({needed()}) q'''),params)
    for flag,table in (('behavior','public.player_analytics_dirty'),('endings','public.player_stockfish_dirty')):
        owner = 's.player_id=d.player_id' if flag=='behavior' else 's.player_name=d.player_name'
        await session.execute(text(f'''INSERT INTO {DAY}(run_id,player_name,day,{flag})
            SELECT :id,s.player_name,d.day,true FROM {table} d JOIN {SCOPE} s
            ON s.run_id=:id AND {owner} WHERE s.{flag}
            ON CONFLICT(run_id,player_name,day) DO UPDATE SET {flag}=true'''),params)
    return await session.scalar(text(f'SELECT count(*) FROM {SCOPE} WHERE run_id=:id'),params)


async def pending(session,player=None):
    if not await enabled(session):return []
    query=needed()
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
    await session.execute(text(f'LOCK TABLE public.player_analytics_state IN SHARE MODE NOWAIT'))
    await session.execute(text(f'UPDATE {CONTROL} SET metadata=true WHERE id AND manual AND scores AND accuracy'))
