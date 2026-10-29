"""Serialized with research in the existing single-job RTX 4060 worker."""
import asyncio
import logging
import time
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from ..research_algorithms.cuda_runtime import cuda_backend
from ..research_algorithms.blunder_full_compute import memory_plan
from ..fen_summary_recovery import recover_batch
from . import source,compute,publication
from .binary_source import VERSION as INPUT_READER
from .schema import table,installed
from ..cuda_summaries.schema import enabled as manual_summaries
from ..cuda_summaries import runs

LOG=logging.getLogger(__name__)


async def canonical_tick(ctx):
    started=time.monotonic(); layout=None; request=None
    async with AsyncDBSession() as session:
        layout=storage_for(session)
        if not layout or not await installed(session,layout): return {'status':'not_installed'}
        t=lambda name:table(layout,name)
        control=(await session.execute(text(f'SELECT * FROM {t("control")}'))).mappings().one()
        manual = await manual_summaries(session)
        if not manual and control['error'] and (await session.scalar(text('SELECT now()'))-control['updated_at']).total_seconds()<60:
            return {'status':'retry_backoff'}
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            if manual:
                await session.execute(text(f'''UPDATE {runs.RUN} SET phase='Waiting for active cloud analysis',
                    updated_at=clock_timestamp() WHERE status IN {runs.ACTIVE}'''))
                await session.commit()
            return {'status':'waiting_cloud'}
    xp=None
    try:
        stage_started=time.monotonic()
        recovery=await recover_batch(factory=AsyncDBSession)
        stages={'input_refresh_seconds':time.monotonic()-stage_started}
        if manual:
            async with AsyncDBSession() as session, session.begin():
                request = await runs.next_batch(session)
            if request is None:
                return {'status': 'building' if recovery['processed'] and recovery['pending'] else 'awaiting_user',
                        'remaining': recovery['pending'], 'input_recovery': recovery}
            if 'score_distribution' in request['metrics']:
                from ..cuda_summaries.shared_worker import tick as shared_tick
                return await shared_tick(AsyncDBSession, request)
        if not control['target_model']:
            return {'status':'building' if recovery['processed'] and recovery['pending'] else 'not_selected',
                'remaining':recovery['pending'],'input_recovery':recovery}
        stage_started=time.monotonic()
        async with AsyncDBSession() as session:
            await source.prepare(session)
            source_timings={}
            data=await source.load(session,position_reader=INPUT_READER,timings=source_timings,
                                   selected_items=request['items'] if request else None)
        stages['source_seconds']=time.monotonic()-stage_started
        if data is None: return {'status':'not_selected'}
        model_id,config,items,games,rows=data
        if request and model_id != request['model_id']:
            raise ValueError('Canonical model changed after the run was captured.')
        if items:
            if request:
                async with AsyncDBSession() as session, session.begin():
                    await runs.phase(session, request['id'], 'Calculating blunders on RTX 4060')
            xp,hardware=await file_operation(cuda_backend,0)
            if hardware['name']!=config['device']: raise RuntimeError('Canonical model requires the RTX 4060.')
            plan=memory_plan(*xp.cuda.runtime.memGetInfo()); xp.get_default_memory_pool().set_limit(size=plan['pool_limit_bytes'])
            eligible,unsupported=games
            stage_started=time.monotonic()
            records,info=await file_operation(compute.infer,xp,rows,eligible,config)
            stages['cuda_seconds']=time.monotonic()-stage_started
            records.extend(unsupported); info.update(hardware=hardware['name'],seconds=time.monotonic()-started)
        else:
            records=[]; info={'moves':0,'games':0,'seconds':time.monotonic()-started}
            if recovery['pending']:
                return {'status':'building' if recovery['processed'] else 'waiting_input_refresh',
                    'remaining':recovery['pending'],'input_recovery':recovery,'input_reader':INPUT_READER}
            # No repeated whole-cohort publication once the target is already ready.
            if control['active_model']==model_id and control['status']=='ready':
                return {'status':'idle','input_reader':INPUT_READER}
        info.update(input_reader=INPUT_READER,source_timings=source_timings,input_recovery=recovery,stage_timings=stages)
        if request:
            async with AsyncDBSession() as session, session.begin():
                await runs.phase(session, request['id'], 'Publishing summaries')
        stage_started=time.monotonic()
        async with AsyncDBSession() as session,session.begin():
            result=await publication.publish(session,model_id,items,records,info)
            if request:
                stages['publication_seconds'] = time.monotonic()-stage_started
                saved = await runs.finish_batch(session,request,items,stages)
                remaining = saved['total_games']-saved['processed_games']-saved['superseded_games']
                result.update(status='building' if remaining else 'ready',remaining=remaining,run_id=str(request['id']))
        stages['publication_seconds']=time.monotonic()-stage_started
        result.update(seconds=time.monotonic()-started,stage_timings=stages,moves=info.get('moves',0))
        result['input_reader']=INPUT_READER
        print('Canonical CUDA blunders: '+str(result),flush=True)
        return result
    except asyncio.CancelledError:
        if request:
            async def mark_interrupted():
                async with AsyncDBSession() as session,session.begin():
                    await runs.fail(session,request['id'],'CUDA worker interrupted or timed out; saved input/result checkpoints retained.')
            task = asyncio.create_task(mark_interrupted())
            try: await asyncio.shield(task)
            except asyncio.CancelledError: await task
        raise
    except Exception as error:
        LOG.exception('Canonical GPU refresh failed; previous published results retained.')
        async with AsyncDBSession() as session,session.begin():
            await session.execute(text(f'''UPDATE {table(layout,'control')} SET status='retrying',
                error=:error,updated_at=now() WHERE id'''),{'error':(type(error).__name__+': '+str(error))[:1000]})
            if request:
                await runs.fail(session,request['id'],type(error).__name__+': '+str(error))
        return {'status':'retrying'}
    finally:
        if xp is not None:
            xp.get_default_memory_pool().free_all_blocks(); xp.get_default_memory_pool().set_limit(size=256*1024**2)
