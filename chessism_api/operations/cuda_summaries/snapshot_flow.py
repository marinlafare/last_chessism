"""Short, independently checkpointed input preparation; no CUDA allocations."""
import asyncio
import json
import logging
import time
from sqlalchemy import text
from ..canonical_blunders import source
from ..matrix_constructor.artifact_files import file_operation
from . import score_runs, shared_source, input_snapshot, runs

LOG = logging.getLogger(__name__)


async def _finish_session(session, *, invalidate=False):
    closing = asyncio.create_task(session.invalidate() if invalidate else session.close())
    try:
        await asyncio.shield(closing)
    except asyncio.CancelledError:
        await closing
        raise


async def read_chunk(factory, run, after):
    session = factory()
    try:
        await source.prepare(session)
        items = await score_runs.pending_items(session, run, after=after)
        # Extraction checkpoints are small; several compose one large GPU batch.
        result = await shared_source.load(session, {**run, 'items':items, 'batch_budget':None}) if items else None
    except BaseException as error:
        # Let SQLAlchemy discard its connection AND transaction together. Raw
        # driver.terminate() followed by close() attempts rollback on a dead
        # connection, masking the original COPY timeout/cancellation.
        try:
            await _finish_session(session, invalidate=True)
        except asyncio.CancelledError:
            pass  # Cleanup finished despite another cancellation; keep the original error.
        except BaseException:
            LOG.exception('CUDA reader cleanup failed while handling %s; preserving the original error.', type(error).__name__)
        raise
    else:
        await _finish_session(session)
        return result


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
