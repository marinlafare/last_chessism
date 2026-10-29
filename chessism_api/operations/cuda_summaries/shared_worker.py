"""Prepare all inputs, calculate large CUDA batches, bulk-publish checkpoints."""
import asyncio
import json
import time
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from ..canonical_blunders import publication
from ..canonical_blunders.schema import table
from ..matrix_constructor.artifact_files import file_operation
from ..research_algorithms.cuda_runtime import cuda_backend
from . import runs, score_runs, shared_source, score_compute, score_publication
from . import accuracy_compute, accuracy_store, batch_budget, blunder_compute
from . import snapshot_flow, input_snapshot, device_inputs


async def may_continue(session, run):
    return bool(await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {runs.RUN} r
        WHERE r.id=:id AND r.status IN ('queued','running') AND r.model_id=:model
        AND (SELECT target_model FROM {table(storage_for(session),'control')} WHERE id)=r.model_id
        AND NOT EXISTS(SELECT 1 FROM cloud_analysis_job
            WHERE status NOT IN ('complete','cancelled','limit_reached','failed')))'''),
        {'id':run['id'],'model':run['model_id']}))


async def calculate(xp, data, accuracy):
    config, items, games, owners, raw, eligible, unsupported, transitions = data
    device = None
    try:
        device = await file_operation(device_inputs.upload,xp,raw,games)
        records, info = await file_operation(blunder_compute.infer,xp,transitions,eligible,config,device=device)
        records.extend(unsupported)
        parts = await file_operation(score_compute.calculate,xp,raw,games,owners,device=device)
        accuracies = await file_operation(accuracy_compute.calculate,xp,raw,games,device=device) if accuracy else []
        info['position_upload_bytes'] = device.bytes
        return records,info,parts,accuracies
    except Exception as error:
        if isinstance(error,getattr(getattr(xp.cuda,'memory',None),'OutOfMemoryError',())): return None
        raise
    finally:
        del device


async def process_batch(factory, run, xp, hardware, budget, state):
    selected = input_snapshot.choose(state['chunks'][state['computed']:],budget)
    retries = 0; read_seconds = 0.
    while True:
        started = time.monotonic()
        data = await file_operation(input_snapshot.load,run['id'],selected)
        read_seconds += time.monotonic()-started
        async with factory() as session,session.begin():
            if not await may_continue(session,run): return {'status':'building','remaining':1}
            await runs.phase(session,run['id'],f'Calculating on RTX 4060 — {len(data[4]):,} prepared positions')
        started = time.monotonic()
        result = await asyncio.wait_for(calculate(xp,data,'accuracy' in run['metrics']),timeout=240)
        if result is not None: break
        del data
        xp.get_default_memory_pool().free_all_blocks(); retries += 1
        if retries > 3 or len(selected)==1:
            raise RuntimeError('CUDA memory retries exhausted; retained input checkpoints, no CPU fallback.')
        selected = selected[:max(1,len(selected)//2)]
    records,info,parts,accuracy_records = result
    stages = {'local_input_seconds':read_seconds,'cuda_seconds':time.monotonic()-started,
              'batches':1,'cuda_memory_retries':retries,'cached_positions':len(data[4])}
    info.update(hardware=hardware['name'],input_reader=input_snapshot.VERSION,stage_timings=stages)
    last = {**budget.describe(),'positions':len(data[4]),'games':len(data[1]),'input_chunks':len(selected),
            'position_upload_bytes':info['position_upload_bytes'],
            'pool_reserved_bytes':getattr(xp.get_default_memory_pool(),'total_bytes',lambda:0)()}
    async with factory() as session,session.begin():
        await runs.phase(session,run['id'],f'Publishing CUDA results — {len(data[1]):,} games')
    started = time.monotonic()
    async with factory() as session,session.begin():
        if not await may_continue(session,run): return {'status':'building','remaining':1}
        blunder_items = [i for i in data[1] if i['blunders']]
        if blunder_items:
            await publication.publish(session,run['model_id'],blunder_items,records,info,refresh_snapshots=False)
        if 'accuracy' in run['metrics']: await accuracy_store.stage(session,run,accuracy_records)
        await score_runs.stage(session,run,data[1],parts,stages)
        state = {**state,'computed':state['computed']+len(selected)}
        stages['publication_seconds'] = time.monotonic()-started
        await session.execute(text(f'''UPDATE {runs.RUN} SET timings=timings || jsonb_build_object(
            'publication_seconds',COALESCE((timings->>'publication_seconds')::float8,0)+CAST(:seconds AS float8),
            'input_snapshot',CAST(:state AS jsonb),'last_batch',CAST(:last AS jsonb)) WHERE id=:id'''),
            {'id':run['id'],'seconds':stages['publication_seconds'],'state':json.dumps(state),'last':json.dumps(last)})
    print('Shared CUDA checkpoint: '+json.dumps({**last,**stages}),flush=True)
    return {'status':'building','remaining':1,'run_id':str(run['id'])}


async def tick(factory, run):
    if run.get('planning'):
        async with factory() as session,session.begin():
            for _ in range(8):
                if not await score_runs.plan(session,run): break
        return {'status':'building','remaining':1,'run_id':str(run['id'])}
    state = None
    if not run.get('finalizing'):
        state = await snapshot_flow.prepare(factory,run)
        if not run['timings'].get('input_snapshot',{}).get('ready'):
            return {'status':'building','remaining':1,'run_id':str(run['id'])}
    xp = None
    try:
        xp,hardware = await file_operation(cuda_backend,0)
        if hardware['name'] != 'NVIDIA GeForce RTX 4060': raise RuntimeError('CUDA summaries require the RTX 4060.')
        budget = batch_budget.plan(*xp.cuda.runtime.memGetInfo())
        xp.get_default_memory_pool().set_limit(size=budget.device_bytes)
        if run.get('finalizing'):
            started = time.monotonic()
            for _ in range(8):
                result = await score_publication.finalize(factory,run,xp)
                if result['status']=='ready': break
            async with factory() as session,session.begin():
                await session.execute(text(f'''UPDATE {runs.RUN} SET timings=jsonb_set(timings,'{{finalization_seconds}}',
                    to_jsonb(COALESCE((timings->>'finalization_seconds')::float8,0)+CAST(:seconds AS float8))) WHERE id=:id'''),
                    {'id':run['id'],'seconds':time.monotonic()-started})
            return {**result,'run_id':str(run['id'])}
        return await process_batch(factory,run,xp,hardware,budget,state)
    finally:
        if xp is not None:
            xp.get_default_memory_pool().free_all_blocks()
            xp.get_default_memory_pool().set_limit(size=256*1024**2)
