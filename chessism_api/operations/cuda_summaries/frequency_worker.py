"""Manual, restartable full-position frequency stage; no CPU count fallback."""
import json
import time
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.player_analytics_schema import analytics_table
from ..matrix_constructor.artifact_files import file_operation
from . import frequency_store as store, frequency_source as source, frequency_files as files
from . import frequency_compute as compute, runs, batch_budget
from .bulk import temp_rows


async def save(session, run, state, phase):
    await session.execute(text(f'''UPDATE {runs.RUN} SET timings=jsonb_set(timings,'{{position_frequency}}',CAST(:state AS jsonb)),
        phase=:phase,updated_at=clock_timestamp(),attempts=0,error=NULL,retry_after=NULL WHERE id=:id'''),
        {'id':run['id'],'state':json.dumps(state),'phase':phase})


async def publish(session, run, state, owner, rows):
    layout = storage_for(session); t = layout.table
    p = {'owner':owner['player_id'],'player':owner['player_name']}
    await temp_rows(session,'frequency_upload','player_id bigint,is_white boolean,mode_id integer,fen_id bigint,appearances bigint',
        ('player_id','is_white','mode_id','fen_id','appearances'),rows)
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    behavior = await session.scalar(text(f'SELECT revision FROM {analytics_table(layout,"state")} WHERE player_name=:player FOR UPDATE'),p)
    await session.execute(text(f'SELECT player_id FROM {t("player_salience_summary")} WHERE player_id=:owner FOR UPDATE'),p)
    revision = await session.scalar(text(f'SELECT revision FROM {store.STATE} WHERE player_id=:owner FOR UPDATE'),p)
    valid = await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {t('player')}
        WHERE id=:owner AND joined>0 AND deleted_at IS NULL) AND NOT EXISTS(
            SELECT 1 FROM {t('game_player')} gp JOIN {t('game')} g ON g.link=gp.game_link
            WHERE gp.player_id=:owner AND NOT g.fens_done)'''),p)
    if not valid or (revision or 0) != owner['revision'] or (behavior or 0) != owner['behavior_revision']:
        return False
    await session.execute(text(f'DELETE FROM {store.TABLE} WHERE player_id=:owner'),p)
    await session.execute(text(f'INSERT INTO {store.TABLE} SELECT * FROM pg_temp.frequency_upload'))
    await session.execute(text(f'''INSERT INTO {store.STATE}(player_id,revision,frequency_revision,
        frequency_behavior_revision,frequency_version,frequency_generation,frequency_generated_at)
        VALUES(:owner,:revision,:revision,:behavior,:version,:generation,clock_timestamp())
        ON CONFLICT(player_id) DO UPDATE SET frequency_revision=EXCLUDED.frequency_revision,
        frequency_behavior_revision=EXCLUDED.frequency_behavior_revision,frequency_version=EXCLUDED.frequency_version,
        frequency_generation=EXCLUDED.frequency_generation,frequency_generated_at=EXCLUDED.frequency_generated_at'''),
        {**p,'revision':owner['revision'],'behavior':owner['behavior_revision'],'version':compute.VERSION,'generation':run['id']})
    return True


async def tick(factory, run, xp):
    if store.METRIC not in run['metrics']: return False
    async with factory() as session:
        state = (await runs.progress(session,run['id']))['timings'][store.METRIC]
    if state.get('complete'): return False
    if state['version'] != compute.VERSION: raise ValueError('Frequency contract changed; start a new manual run.')
    started = time.monotonic()
    if not state['prepared']:
        async with factory() as session:
            result = await source.read(session,[p['player_id'] for p in state['players']],state['cursor'])
        if result is None: state={**state,'prepared':True}
        else:
            raw,cursor = result
            chunks = await file_operation(files.save,run['id'],raw,cursor)
            state={**state,'chunks':[*state['chunks'],*chunks],'cursor':cursor,'rows':state['rows']+len(raw)}
        state['input_seconds']=state.get('input_seconds',0)+time.monotonic()-started
        async with factory() as session,session.begin():
            await save(session,run,state,f'Preparing position frequencies — {state["rows"]:,} occurrences')
        return True
    if state['shards'] < compute.SHARDS:
        xp.get_default_memory_pool().free_all_blocks()
        budget=batch_budget.plan(*xp.cuda.runtime.memGetInfo())
        shards=files.choose(state['chunks'],state['shards'],budget)
        while True:
            raw=await file_operation(files.load,run['id'],state['chunks'],shards)
            async with factory() as session,session.begin():
                await runs.phase(session,run['id'],f'Counting repeated positions on RTX 4060 — {len(raw):,} occurrences')
            before=time.monotonic()
            try:
                result=await file_operation(compute.calculate,xp,raw)
                break
            except Exception as error:
                if not isinstance(error,getattr(getattr(xp.cuda,'memory',None),'OutOfMemoryError',())): raise
                if len(shards)==1: raise RuntimeError('Frequency CUDA allocation failed; no CPU fallback.') from error
                del raw
                xp.get_default_memory_pool().free_all_blocks(); shards=shards[:max(1,len(shards)//2)]
        duration=time.monotonic()-before
        output=await file_operation(files.write,run['id'],result,f'frequency-output-{shards[0]}.npz',shards[0])
        state={**state,'shards':shards[-1]+1,'outputs':[*state['outputs'],output],
            'batches':state.get('batches',0)+1,'cuda_seconds':state.get('cuda_seconds',0)+duration,
            'stored_rows':state.get('stored_rows',0)+len(result),'last_batch_positions':len(raw)}
        async with factory() as session,session.begin():
            await save(session,run,state,f'Position frequencies calculated — {state["shards"]} / {compute.SHARDS} groups')
        return True
    if state['published'] < len(state['players']):
        owner=state['players'][state['published']]
        rows=await file_operation(lambda:list(files.player_rows(run['id'],state['outputs'],owner['player_id'])))
        async with factory() as session,session.begin():
            accepted=await publish(session,run,state,owner,rows)
            state={**state,'published':state['published']+1,'superseded':state.get('superseded',0)+(not accepted),
                'save_seconds':state.get('save_seconds',0)+time.monotonic()-started}
            await save(session,run,state,f'Publishing position frequencies — {state["published"]} / {len(state["players"])} players')
        return True
    async with factory() as session,session.begin():
        await save(session,run,{**state,'complete':True},'Position frequencies complete')
    return False
