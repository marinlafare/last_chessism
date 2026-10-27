"""Serialized with research in the existing single-job RTX 4060 worker."""
import logging
import time
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from ..research_algorithms.cuda_runtime import cuda_backend
from ..research_algorithms.blunder_full_compute import memory_plan
from . import source,compute,publication
from .binary_source import VERSION as INPUT_READER
from .schema import table,installed

LOG=logging.getLogger(__name__)


async def canonical_tick(ctx):
    started=time.monotonic(); layout=None
    async with AsyncDBSession() as session:
        layout=storage_for(session)
        if not layout or not await installed(session,layout): return {'status':'not_installed'}
        t=lambda name:table(layout,name)
        control=(await session.execute(text(f'SELECT * FROM {t("control")}'))).mappings().one()
        if not control['target_model']: return {'status':'not_selected'}
        if control['error'] and (await session.scalar(text('SELECT now()'))-control['updated_at']).total_seconds()<60:
            return {'status':'retry_backoff'}
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            return {'status':'waiting_cloud'}
    xp=None
    try:
        async with AsyncDBSession() as session:
            await source.prepare(session)
            source_timings={}
            data=await source.load(session,position_reader=INPUT_READER,timings=source_timings)
        if data is None: return {'status':'not_selected'}
        model_id,config,items,games,rows=data
        if items:
            xp,hardware=await file_operation(cuda_backend,0)
            if hardware['name']!=config['device']: raise RuntimeError('Canonical model requires the RTX 4060.')
            plan=memory_plan(*xp.cuda.runtime.memGetInfo()); xp.get_default_memory_pool().set_limit(size=plan['pool_limit_bytes'])
            eligible,unsupported=games
            records,info=await file_operation(compute.infer,xp,rows,eligible,config)
            records.extend(unsupported); info.update(hardware=hardware['name'],seconds=time.monotonic()-started)
        else:
            records=[]; info={'moves':0,'games':0,'seconds':time.monotonic()-started}
            # No repeated whole-cohort publication once the target is already ready.
            if control['active_model']==model_id and control['status']=='ready':
                return {'status':'idle','input_reader':INPUT_READER}
        info.update(input_reader=INPUT_READER,source_timings=source_timings)
        async with AsyncDBSession() as session,session.begin():
            result=await publication.publish(session,model_id,items,records,info)
        result['input_reader']=INPUT_READER
        print('Canonical CUDA blunders: '+str(result),flush=True)
        return result
    except Exception as error:
        LOG.exception('Canonical GPU refresh failed; previous published results retained.')
        async with AsyncDBSession() as session,session.begin():
            await session.execute(text(f'''UPDATE {table(layout,'control')} SET status='retrying',
                error=:error,updated_at=now() WHERE id'''),{'error':(type(error).__name__+': '+str(error))[:1000]})
        return {'status':'retrying'}
    finally:
        if xp is not None:
            xp.get_default_memory_pool().free_all_blocks(); xp.get_default_memory_pool().set_limit(size=256*1024**2)
