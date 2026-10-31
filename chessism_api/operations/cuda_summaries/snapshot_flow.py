"""Short, independently checkpointed input preparation; no CUDA allocations."""
import asyncio
import json
import time
from sqlalchemy import text
from chessism_api.database.relational_storage import driver_connection
from ..canonical_blunders import source
from ..matrix_constructor.artifact_files import file_operation
from . import score_runs, shared_source, input_snapshot, runs


async def read_chunk(factory, run, after):
    session = factory(); driver = None
    try:
        await source.prepare(session)
        items = await score_runs.pending_items(session, run, after=after)
        if not items:
            return None
        driver = await driver_connection(session)
        # Extraction checkpoints are small; several compose one large GPU batch.
        return await shared_source.load(session, {**run, 'items':items, 'batch_budget':None})
    except BaseException:
        # COPY cancellation can leave asyncpg waiting for protocol completion.
        # Terminate only this borrowed read-only connection before pool cleanup.
        if driver is not None:
            driver.terminate()
        raise
    finally:
        closing = asyncio.create_task(session.close())
        try:
            await asyncio.shield(closing)
        except asyncio.CancelledError:
            await closing
            raise


async def prepare(factory, run):
    state = dict(run['timings'].get('input_snapshot') or {
        'version':input_snapshot.VERSION,'chunks':[],'games':0,'positions':0,'bytes':0,'computed':0,'ready':False,'after':None})
    if state['version'] != input_snapshot.VERSION:
        raise ValueError('Unsupported input checkpoint version.')
    if state['ready']:
        return state
    started = time.monotonic()
    for _ in range(3):
        before = time.monotonic()
        data = await read_chunk(factory, run, state['after'])
        if data is None:
            state['ready'] = True
            seconds = 0.; positions = 0
        else:
            chunk = await file_operation(input_snapshot.save, run['id'], data)
            state['chunks'] = [*state['chunks'],chunk]
            for key in ('games','positions','bytes'): state[key] += chunk[key]
            state['after'] = chunk['last']; positions = chunk['positions']
            seconds = time.monotonic()-before
            del data
        phase = (f"CUDA inputs ready — {state['positions']:,} positions" if state['ready'] else
                 f"Preparing CUDA inputs — {state['games']:,} / {run['total_games']:,} games · {state['positions']:,} positions")
        async with factory() as session, session.begin():
            await session.execute(text(f'''UPDATE {runs.RUN} SET phase=:phase,updated_at=clock_timestamp(),
                attempts=0,error=NULL,retry_after=NULL,timings=timings || jsonb_build_object(
                'input_snapshot',CAST(:state AS jsonb),'pipeline','snapshot_v2',
                'source_seconds',COALESCE((timings->>'source_seconds')::float8,0)+CAST(:seconds AS float8),
                'source_positions',COALESCE((timings->>'source_positions')::bigint,0)+CAST(:positions AS bigint)) WHERE id=:id'''),
                {'id':run['id'],'phase':phase,'state':json.dumps(state),'seconds':seconds,'positions':positions})
        if state['ready'] or time.monotonic()-started >= 30:
            break
    return state
