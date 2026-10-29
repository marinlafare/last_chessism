"""Restartable manual CUDA salience stage; never invokes legacy CPU salience."""
import json
import time
from sqlalchemy import text
from ..matrix_constructor.artifact_files import file_operation
from . import salience_store as store,salience_source as source,salience_files as files
from . import salience_compute as compute,salience_publication as publication,runs,batch_budget
from .salience_daily import VERSION as DAILY_VERSION


async def save(session,run,state,phase):
    await session.execute(text(f'''UPDATE {runs.RUN} SET timings=jsonb_set(timings,'{{salience}}',CAST(:state AS jsonb)),
        phase=:phase,updated_at=clock_timestamp(),attempts=0,error=NULL,retry_after=NULL WHERE id=:id'''),
        {'id':run['id'],'state':json.dumps(state),'phase':phase})


async def tick(factory,run,xp):
    if 'salience' not in run['metrics']:return False
    async with factory() as session:
        current = await runs.progress(session,run['id'])
    state = current['timings'].get('salience',{'version':compute.VERSION,'daily_version':DAILY_VERSION,'chunks':[],'cursor':0,
        'rows':0,'games':0,'prepared':False,'shards':0,'shard_total':compute.SHARDS,'reduced_cursor':0,'reduced':False})
    if state.get('complete'):return False
    if state['version'] != compute.VERSION:raise ValueError('Captured salience formula changed; start a new run.')
    if state.get('daily_version') != DAILY_VERSION:raise ValueError('Captured daily salience formula changed; start a new run.')
    started = time.monotonic()
    if not state['prepared']:
        async with factory() as session: result = await source.read(session,run['id'],state['cursor'])
        if result is None:state={**state,'prepared':True}
        else:
            raw,cursor,games = result
            chunks = await file_operation(files.save,run['id'],raw,cursor)
            state={**state,'chunks':[*state['chunks'],*chunks],'cursor':cursor,
                'rows':state['rows']+len(raw),'games':state['games']+games}
        state['input_seconds']=state.get('input_seconds',0)+time.monotonic()-started
        async with factory() as session,session.begin():
            await save(session,run,state,f'Preparing salience inputs — {state["games"]:,} games · {state["rows"]:,} early positions')
        return True
    xp.get_default_memory_pool().free_all_blocks()
    budget=batch_budget.plan(*xp.cuda.runtime.memGetInfo())
    if state['shards'] < compute.SHARDS:
        shards=files.choose(state['chunks'],state['shards'],budget);attempts=0
        while True:
            raw=await file_operation(files.load,run['id'],state['chunks'],shards)
            async with factory() as session,session.begin():
                await runs.phase(session,run['id'],f'Calculating salience on RTX 4060 — {len(raw):,} positions across players')
            before=time.monotonic()
            try:
                parts,frequencies=await file_operation(compute.calculate,xp,raw)
                break
            except Exception as error:
                if not isinstance(error,getattr(getattr(xp.cuda,'memory',None),'OutOfMemoryError',())):raise
                attempts+=1
                if attempts>3 or len(shards)==1:raise RuntimeError('Salience CUDA allocation failed; no CPU fallback.') from error
                del raw
                xp.get_default_memory_pool().free_all_blocks();shards=shards[:max(1,len(shards)//2)]
        state={**state,'shards':shards[-1]+1,'batches':state.get('batches',0)+1,
            'cuda_seconds':state.get('cuda_seconds',0)+time.monotonic()-before,'last_batch_positions':len(raw)}
        before=time.monotonic()
        async with factory() as session,session.begin():
            await publication.stage(session,run,shards[0],parts,frequencies)
            state['save_seconds']=state.get('save_seconds',0)+time.monotonic()-before
            await save(session,run,state,f'Salience CUDA groups — {state["shards"]} / {compute.SHARDS}')
        return True
    if not state['reduced']:
        # Whole games stay together. At most 30 partial rows per game.
        limit=max(1,min(budget.device_bytes//(30*160),budget.host_bytes//(30*256)))
        async with factory() as session:raw=await publication.read_parts(session,run,state['reduced_cursor'],limit)
        if len(raw):
            result=await file_operation(compute.reduce_games,xp,raw)
            state={**state,'reduced_cursor':int(result[-1,0]),'reduced_games':state.get('reduced_games',0)+len(result)}
        else:state={**state,'reduced':True}
        async with factory() as session,session.begin():
            if len(raw):await publication.stage_games(session,run,result)
            state['reduction_seconds']=state.get('reduction_seconds',0)+time.monotonic()-started
            await save(session,run,state,f'Reducing game salience on RTX 4060 — {state.get("reduced_games",0):,} games')
        return True
    if await publication.publish(factory,run,xp):
        async with factory() as session,session.begin():
            state['publication_seconds']=state.get('publication_seconds',0)+time.monotonic()-started
            state['published_players']=state.get('published_players',0)+1
            await save(session,run,state,'Publishing salience and refreshing hero daily summaries')
        return True
    async with factory() as session,session.begin():
        skipped=await session.scalar(text(f"SELECT count(*) FROM {store.SCOPE} WHERE run_id=:id AND status='superseded'"),{'id':run['id']})
        state={**state,'complete':True,'superseded_players':skipped}
        await save(session,run,state,'Salience complete' if not skipped else 'Changed salience inputs remain pending')
        for table in (store.PART,store.FREQUENCY,store.GAME):
            await session.execute(text(f'DELETE FROM {table} WHERE run_id=:id'),{'id':run['id']})
    return False
