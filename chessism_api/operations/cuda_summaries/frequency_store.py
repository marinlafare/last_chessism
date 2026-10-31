"""One published table; reuse existing source revisions and run JSON checkpoints."""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.player_analytics_schema import analytics_table
from .schema import CONTROL, RUN
from .salience_store import STATE
from .frequency_compute import VERSION

TABLE = 'public.player_fen_frequency'
METRIC = 'position_frequency'


async def enabled(session):
    if not storage_for(session) or not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'), {'t': TABLE}):
        return False
    return bool(await session.scalar(text(f"SELECT manual AND scores AND COALESCE((to_jsonb(c)->>'frequencies')::boolean,false) FROM {CONTROL} c WHERE id")))


async def install(session, layout):
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    if not await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'), {'t': STATE}):
        raise ValueError('Install manual salience revision tracking before position frequencies.')
    await session.execute(text(f'ALTER TABLE {CONTROL} ADD COLUMN IF NOT EXISTS frequencies boolean NOT NULL DEFAULT false'))
    for definition in ('frequency_revision bigint', 'frequency_behavior_revision bigint', 'frequency_version text',
                       'frequency_generation uuid', 'frequency_generated_at timestamptz'):
        await session.execute(text(f'ALTER TABLE {STATE} ADD COLUMN IF NOT EXISTS {definition}'))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
        player_id bigint REFERENCES {layout.table('player')}(id) ON DELETE CASCADE,
        is_white boolean NOT NULL,mode_id integer REFERENCES {layout.table('game_mode')}(id),
        fen_id bigint REFERENCES {layout.table('fen')}(fen_id) ON DELETE CASCADE,appearances bigint NOT NULL CHECK(appearances>0),
        PRIMARY KEY(player_id,is_white,mode_id,fen_id))'''))
    await session.execute(text(f'CREATE INDEX IF NOT EXISTS player_fen_frequency_rank ON {TABLE}(player_id,appearances DESC,fen_id,is_white) INCLUDE(mode_id)'))


def needed(layout):
    return f'''SELECT p.id AS player_id,p.player_name,COALESCE(s.revision,0) AS revision,
        COALESCE(a.revision,0) AS behavior_revision FROM {layout.table('player')} p
        LEFT JOIN {STATE} s ON s.player_id=p.id
        LEFT JOIN {analytics_table(layout,'state')} a USING(player_name)
        WHERE p.joined>0 AND p.deleted_at IS NULL AND (
            s.frequency_version IS DISTINCT FROM '{VERSION}'
            OR s.frequency_revision IS DISTINCT FROM COALESCE(s.revision,0)
            OR s.frequency_behavior_revision IS DISTINCT FROM COALESCE(a.revision,0))'''


async def pending(session):
    if not await enabled(session): return []
    return [dict(r) for r in (await session.execute(text(needed(storage_for(session))))).mappings()]


async def capture(session, run_id):
    players = await pending(session)
    if players:
        await session.execute(text(f"UPDATE {RUN} SET timings=jsonb_set(timings,'{{position_frequency}}',CAST(:state AS jsonb)) WHERE id=:id"),
            {'id':run_id, 'state':json.dumps(dict(version=VERSION,players=players,chunks=[],outputs=[],cursor=[0,0],
                rows=0,prepared=False,shards=0,shard_total=128,published=0))})
    return len(players)


async def activate(session):
    from .runs import ACTIVE
    await session.execute(text(f'SELECT id FROM {CONTROL} WHERE id FOR UPDATE'))
    if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {RUN} WHERE status IN {ACTIVE})')):
        raise ValueError('Wait for the active CUDA run before enabling position frequencies.')
    await session.execute(text(f'UPDATE {CONTROL} SET frequencies=true WHERE id AND manual AND scores AND salience'))
    if not await enabled(session):
        raise ValueError('Manual scores and salience revision tracking must be enabled first.')
