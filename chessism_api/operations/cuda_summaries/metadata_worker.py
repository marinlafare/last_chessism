"""Restartable preparation, large mixed-player CUDA batches, atomic publication."""
import json
import time
from sqlalchemy import text
from ..matrix_constructor.artifact_files import file_operation
from . import metadata_store as store,metadata_source as source,metadata_files as files
from . import metadata_publication as publication,behavior_compute as behavior,ending_compute as endings
from . import runs,batch_budget


async def save_state(s,run,state,phase):
    await s.execute(text(f'''UPDATE {runs.RUN} SET timings=jsonb_set(timings,'{{metadata}}',CAST(:state AS jsonb)),
        phase=:phase,updated_at=clock_timestamp(),attempts=0,error=NULL,retry_after=NULL WHERE id=:id'''),
        {'id':run['id'],'state':json.dumps(state),'phase':phase})


async def tick(factory,run,xp):
    if 'behavioral' not in run['metrics']:return False
    async with factory() as s:
        current=await runs.progress(s,run['id'])
        state=current['timings'].get('metadata',{'chunks':[],'computed':0,'cursor':0,'ready':False,'reduced':False,'rows':0})
        profiles=(await s.execute(text(f'SELECT profile FROM {store.SCOPE} WHERE run_id=:id ORDER BY player_name'),{'id':run['id']})).scalars().all()
    if not state['ready']:
        started=time.monotonic()
        async with factory() as s:
            rows=await source.read(s,run['id'],state['cursor'])
        if rows:
            descriptor=await file_operation(files.save,run['id'],rows,profiles)
            state={**state,'chunks':[*state['chunks'],descriptor],'cursor':descriptor['last'],'rows':state['rows']+len(rows)}
        else:state={**state,'ready':True}
        state['preparation_seconds']=state.get('preparation_seconds',0)+time.monotonic()-started
        async with factory() as s,s.begin():await save_state(s,run,state,f'Preparing behavioral and ending inputs — {state["rows"]:,} player-games')
        return True
    if state['computed']<len(state['chunks']):
        budget=batch_budget.plan(*xp.cuda.runtime.memGetInfo());selected=files.choose(state['chunks'][state['computed']:],budget)
        async with factory() as s,s.begin():
            await runs.phase(s,run['id'],f'Calculating metadata on RTX 4060 — {sum(c["rows"] for c in selected):,} prepared player-games')
        attempts=0;read_seconds=0.;compute_seconds=0.
        while True:
            started=time.monotonic()
            raw,extra,mask,dictionaries,profiles=await file_operation(files.load,run['id'],selected)
            read_seconds+=time.monotonic()-started;started=time.monotonic()
            try:
                b=await file_operation(behavior.partitions,xp,raw[mask[:,0]],dictionaries,profiles)
                e=await file_operation(endings.partitions,xp,raw[mask[:,1]],extra[mask[:,1]],dictionaries,profiles)
                compute_seconds+=time.monotonic()-started
                break
            except Exception as error:
                if not isinstance(error,getattr(getattr(xp.cuda,'memory',None),'OutOfMemoryError',())):raise
                attempts+=1
                if attempts>3 or len(selected)==1:raise RuntimeError('Metadata CUDA memory retries exhausted; checkpoints retained.') from error
                del raw,extra,mask
                xp.get_default_memory_pool().free_all_blocks();selected=selected[:max(1,len(selected)//2)]
        async with factory() as s,s.begin():
            started=time.monotonic()
            await publication.stage(s,run,state['computed'],b,e)
            state={**state,'computed':state['computed']+len(selected),'batches':state.get('batches',0)+1,
                'local_input_seconds':state.get('local_input_seconds',0)+read_seconds,
                'publication_seconds':state.get('publication_seconds',0)+time.monotonic()-started,
                'cuda_seconds':state.get('cuda_seconds',0)+compute_seconds,'last_batch_rows':len(raw)}
            await save_state(s,run,state,f'Calculated behavioral and ending summaries — {len(raw):,} player-games on RTX 4060')
        print('Metadata CUDA checkpoint: '+json.dumps({k:state[k] for k in ('rows','computed','batches','cuda_seconds','last_batch_rows')}),flush=True)
        return True
    if not state['reduced']:
        started=time.monotonic();await publication.reduce(factory,run,xp)
        state={**state,'reduced':True,'reduction_seconds':time.monotonic()-started}
        async with factory() as s,s.begin():await save_state(s,run,state,'Publishing behavioral and ending summaries')
        return True
    if await publication.publish(factory,run):return True
    async with factory() as s,s.begin():
        skipped=await s.scalar(text(f"SELECT count(*) FROM {store.SCOPE} WHERE run_id=:id AND status='superseded'"),{'id':run['id']})
        state={**state,'superseded_players':skipped,'complete':True}
        await save_state(s,run,state,'Metadata summaries complete' if not skipped else 'Changed metadata remains pending')
        for relation in (store.PART,store.DAY):await s.execute(text(f'DELETE FROM {relation} WHERE run_id=:id'),{'id':run['id']})
    return False
