"""One published table; reuse existing source revisions and run JSON checkpoints."""
import json
from sqlalchemy import text
from .schema import CONTROL, RUN
from .salience_store import STATE
from .frequency_compute import VERSION

TABLE = 'public.player_fen_frequency'
METRIC = 'position_frequency'


async def enabled(session):
    return bool(await session.scalar(text(f"SELECT manual AND scores AND frequencies FROM {CONTROL} c WHERE id")))




def needed():
    return f'''SELECT p.id AS player_id,p.player_name,COALESCE(s.revision,0) AS revision,
        COALESCE(a.revision,0) AS behavior_revision FROM public.player p
        LEFT JOIN {STATE} s ON s.player_id=p.id
        LEFT JOIN public.player_analytics_state a ON a.player_id=p.id
        WHERE p.joined>0 AND p.deleted_at IS NULL AND (
            s.frequency_version IS DISTINCT FROM '{VERSION}'
            OR s.frequency_revision IS DISTINCT FROM COALESCE(s.revision,0)
            OR s.frequency_behavior_revision IS DISTINCT FROM COALESCE(a.revision,0))'''


async def pending(session):
    if not await enabled(session): return []
    return [dict(r) for r in (await session.execute(text(needed()))).mappings()]


async def capture(session, run_id):
    players = await pending(session)
    if players:
        await session.execute(text(f"UPDATE {RUN} SET timings=jsonb_set(timings,'{{position_frequency}}',CAST(:state AS jsonb)) WHERE id=:id"),
            {'id':run_id, 'state':json.dumps(dict(version=VERSION,players=players,chunks=[],outputs=[],cursor=[0,0,0,0],
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
