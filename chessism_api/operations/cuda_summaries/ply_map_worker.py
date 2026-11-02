"""Manual graph stage: checkpoint input, bulk GPU counting, atomic publication."""
import json
import time
from sqlalchemy import text
from chessism_api.database.neo4j import ply_map_store as store, ply_map_compute as compute
from . import runs, batch_budget, ply_map_process
from .bulk import temp_rows


async def save(session, run, state, phase):
    await session.execute(text(f'''UPDATE {runs.RUN} SET timings=jsonb_set(timings,'{{ply_map}}',CAST(:state AS jsonb)),
        phase=:phase,updated_at=clock_timestamp(),attempts=0,error=NULL,retry_after=NULL WHERE id=:id'''),
        {'id':run['id'],'state':json.dumps(state),'phase':phase})


async def tick(factory, run, xp):
    if store.METRIC not in run['metrics']: return False
    async with factory() as session:
        state = (await runs.progress(session,run['id']))['timings'][store.METRIC]
    if state['complete']: return False
    if state['version'] != compute.VERSION:
        raise ValueError('Ply-map contract changed. Start a new manual summary run.')
    started = time.monotonic()
    async with factory() as session,session.begin():
        if not await store.current(session,state['revision']):
            await session.execute(text('DELETE FROM graph_map.input WHERE run_id=:id'),{'id':run['id']})
            await save(session,run,{**state,'complete':True,'superseded':True},
                       'Ply map changed during preparation — previous snapshot retained')
            return False
    if not state['prepared']:
        from chessism_api.database.neo4j.ply_map_source import read
        rows = await read(state['cursor'])
        async with factory() as session,session.begin():
            if rows:
                await temp_rows(session,'ply_map_input','run_id uuid,game_id bigint,payload jsonb',
                    ('run_id','game_id','payload'),((run['id'],g['id'],json.dumps(g)) for g in rows))
                await session.execute(text('INSERT INTO graph_map.input SELECT * FROM pg_temp.ply_map_input ON CONFLICT DO NOTHING'))
                state={**state,'cursor':rows[-1]['id'],'captured_games':state['captured_games']+len(rows)}
            else:
                state={**state,'prepared':True}
            state['input_seconds']=state.get('input_seconds',0)+time.monotonic()-started
            await save(session,run,state,f'Preparing collapsed ply map — {state["captured_games"]:,} games captured')
        return True
    budget=batch_budget.plan(*xp.cuda.runtime.memGetInfo())
    # Explicit bounded capacity, never silently drop games or fall back to CPU.
    estimate=state['captured_games']*(compute.MAX_PLY+2)*1024
    if estimate > min(budget.host_bytes,budget.device_bytes):
        raise ValueError('Captured ply map exceeds this batch memory budget. Keep the previous snapshot; partitioned map preparation is required.')
    async with factory() as session:
        games=(await session.execute(text('SELECT payload FROM graph_map.input WHERE run_id=:id ORDER BY game_id'),{'id':run['id']})).scalars().all()
    if len(games)!=state['captured_games']:
        raise ValueError('Ply-map input checkpoint is incomplete.')
    async def progress(event):
        stage=event['stage']
        phase=(f'Compressing ply map paths — {event["games"]:,} / {event["total_games"]:,} games'
               if stage=='compressing' else
               f'Grouping ply map memberships on RTX 4060 — {event["input_pairs"]:,} pairs'
               if stage=='grouping' else 'Preparing saved ply map layout')
        async with factory() as session,session.begin():
            await runs.phase(session,run['id'],phase)

    await progress(dict(stage='compressing',games=0,total_games=len(games)))
    result,metrics=await ply_map_process.execute(games,budget.device_bytes,progress)
    state={**state,**metrics,'build_seconds':time.monotonic()-started,'nodes':len(result['nodes']),
           'connections':len(result['connections']),'complete':True}
    async with factory() as session,session.begin():
        await runs.phase(session,run['id'],'Publishing collapsed ply map')
    publication_started=time.monotonic()
    async with factory() as session,session.begin():
        await session.execute(text("SET LOCAL lock_timeout='2s'"))
        accepted=await store.publish(session,run['id'],state['revision'],result)
        if not accepted:
            await session.execute(text('DELETE FROM graph_map.input WHERE run_id=:id'),{'id':run['id']})
        await save(session,run,{**state,'publication_seconds':time.monotonic()-publication_started,'superseded':not accepted},
                   'Collapsed ply map published' if accepted else 'Ply map changed — previous snapshot retained')
    return False
