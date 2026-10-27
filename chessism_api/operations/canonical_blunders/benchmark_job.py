"""Sequential performance baseline; immutable formula, isolated publication.

Not a new model fit, quality gate or activation. Timed stages run sequentially;
CUDA events are nested within wall timings and must never be added twice.
"""
import asyncio
from copy import deepcopy
import hashlib
from pathlib import Path
import time
from collections import Counter
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from ..research_algorithms.cuda_runtime import cuda_backend
from ..research_algorithms.blunder_full_compute import memory_plan
from ..research_algorithms.storage import workspace,ensure_capacity
from . import compute,benchmark_compute as measured,benchmark_source as source
from .benchmark_store import Scratch,OPERATION
from .schema import table

GAMES=100000
MAX_ROWS=32000000


def configuration(model_id,model,*,reader_comparison=False):
    if model['kernel_sha256']!=compute.implementation_hash():raise ValueError('Canonical implementation changed.')
    result={'operation':OPERATION,'implementation_version':1,'backend':'cuda_cupy',
        'model_id':model_id,'model':deepcopy(model),'games':GAMES,'max_rows':MAX_ROWS,
        'columns':['game','ply','before','after','before_kind','after_kind']}
    if reader_comparison:result.update(implementation_version=2,reader_comparison='legacy_vs_binary_v1')
    return result


async def current_configuration():
    async with AsyncDBSession() as s:
        await source.source.prepare(s)
        control=await source.guard(s)
        from chessism_api.database.relational_storage import storage_for
        model=await s.scalar(text(f'SELECT config FROM {table(storage_for(s),"model")} WHERE id=:id'),
            {'id':control['active_model']})
        return configuration(control['active_model'],model,reader_comparison=True)


def add_histograms(histograms,records):
    for row in records:
        for side in ('white','black'):
            owner=row.get(side+'_owner'); count=row.get(side+'_count')
            if owner in histograms and count is not None:
                dest=histograms[owner][row['mode']]
                dest['counts'][min(6,count)]+=1; dest['games_count']+=1


def batch(xp,rows,data,model):
    # Validate the instrumentation against the untouched canonical function.
    start=time.perf_counter()
    reference,_=compute.infer(xp,rows,data['games'],model)
    reference_seconds=time.perf_counter()-start
    xp.get_default_memory_pool().free_all_blocks()
    start=time.perf_counter()
    actual,timings=measured.infer(xp,rows,data['games'],model)
    timings['instrumented_inference_seconds']=time.perf_counter()-start
    start=time.perf_counter()
    numerical=measured.compare(actual,reference,digest=True)
    published=measured.compare(actual,data['expected'])
    timings['comparison_seconds']=time.perf_counter()-start
    timings['reference_inference_seconds']=reference_seconds
    if numerical or published:
        raise ValueError(f'Exact agreement failed: {len(numerical)} reference / {len(published)} published games. Live summaries unchanged.')
    return actual,timings


async def execute(config,reporter,*,factory=None):
    if config!=configuration(config['model_id'],config['model'],reader_comparison='reader_comparison' in config):
        raise ValueError('Modified benchmark instructions.')
    started=time.perf_counter(); totals=Counter(); xp=None; scratch=None
    ensure_capacity(config)
    try:
        await reporter.progress('checking_gpu',detail='Checking RTX 4060; no CPU fallback and no live summary changes.')
        xp,hardware=await file_operation(cuda_backend,0)
        if hardware['name']!=config['model']['device']:raise RuntimeError('Benchmark requires the canonical RTX 4060.')
        plan=memory_plan(*xp.cuda.runtime.memGetInfo()); xp.get_default_memory_pool().set_limit(size=plan['pool_limit_bytes'])
        with workspace(reporter.run_id) as folder:
            info,timings,items,players,layout=await source.freeze(config,reporter,folder,factory=factory)
            totals.update(timings)
            await reporter.check()
            await reporter.progress('preparing_scratch',0,info['games'],'Preparing private WAL-logged tables; live publication is not used.')
            scratch=Scratch(reporter.run_id,layout,factory=factory)
            start=time.perf_counter(); await scratch.create(config['model_id'],items,players)
            totals['scratch_setup_seconds']=time.perf_counter()-start
            del items
            histograms={p:{m:{'counts':[0]*7,'games_count':0} for m in ('bullet','blitz','rapid')} for p in players}
            batches=[]; processed=0
            # Warm up on one bounded batch; exclude this extra computation.
            rows,data=await file_operation(source.read,folder,0)
            await reporter.progress('warming_gpu',0,info['games'],'Warming CUDA kernels separately from measured stages.')
            start=time.perf_counter()
            warm=await file_operation(compute.infer,xp,rows,data['games'],config['model'])
            del warm,rows,data
            xp.get_default_memory_pool().free_all_blocks()
            totals['warmup_seconds']=time.perf_counter()-start
            for index in range(info['batches']):
                await reporter.check()
                # Yield to a newly started cloud workflow rather than competing.
                async with (factory or AsyncDBSession)() as session:
                    await source.source.prepare(session)
                    current=await source.guard(session)
                    if current['active_model']!=config['model_id']:raise ValueError('Canonical model changed during benchmark.')
                await reporter.progress('measuring_pipeline',processed,info['games'],
                    'Timing CUDA, host preparation and isolated database commits; checking every move flag.')
                start=time.perf_counter(); rows,data=await file_operation(source.read,folder,index)
                totals['replay_read_seconds']+=time.perf_counter()-start
                records,timing=await file_operation(batch,xp,rows,data,config['model'])
                add_histograms(histograms,data['expected'])
                result,write=await scratch.write(config['model_id'],data['items'],records)
                if result['written']!=len(records):raise ValueError('Scratch publication rejected output.')
                totals.update(timing); totals.update(write)
                processed+=len(records)
                batches.append({'games':len(records),'rows':len(rows),'timings':{**timing,**write}})
                del records,rows,data
                xp.get_default_memory_pool().free_all_blocks()
            if processed!=info['games']:raise ValueError('Incomplete benchmark.')
            await reporter.progress('verifying_summaries',processed,info['games'],'Comparing isolated player/mode histograms with frozen published counts.')
            actual=await scratch.snapshots(config['model_id'])
            if actual!=histograms:raise ValueError('Scratch histogram agreement failed; live summaries unchanged.')
            await reporter.progress('cleaning_scratch',processed,info['games'],'Removing only this run’s five private tables and temporary input files.')
            start=time.perf_counter(); await scratch.cleanup(); totals['cleanup_seconds']=time.perf_counter()-start
        totals['run_wall_seconds']=time.perf_counter()-started
        # Sum non-overlapping baseline stages; exclude freeze/replay I/O,
        # sampling, warmup, comparisons, setup and cleanup. Not end-to-end time.
        baseline=('source_queries_seconds','source_pack_seconds','instrumented_inference_seconds',
            'publication_prepare_sql_seconds','publication_commit_seconds')
        totals['sequential_stage_sum_seconds']=sum(totals[k] for k in baseline)
        return {'format':'chessism_canonical_blunder_timing_v1','status':'passed','production_changed':False,
            'backend':'cuda_cupy','hardware':hardware,'memory_plan':plan,'model_id':config['model_id'],
            'kernel_sha256':config['model']['kernel_sha256'],
            'instrumentation_sha256':hashlib.sha256(Path(measured.__file__).read_bytes()).hexdigest(),
            'source':info,'input_rows':info['rows'],'included_rows':info['rows'],
            'checks':{'canonical_flags_counts':'passed','published_flags_counts':'passed','player_histograms':'passed',
                **({'reader_inputs':'passed'} if 'reader_comparison' in config else {})},
            'scratch_removed':not scratch.created,'timings':dict(totals),'batches':batches,
            'notes':['Performance sample, not a calibration or quality test.',
                'Wall times include host launch/wait overhead. CUDA-event intervals are nested; do not add them to wall totals.',
                'Private logged tables use the canonical writer, but contain only this sample, not all live game history.',
                'Sequential stage sum excludes validation, warmup, sampling and temporary file overhead; it is not end-to-end runtime.',
                'Frozen temporary inputs are removed; input digest identifies this snapshot. A later changed-source run is not an identical-input comparison.',
                *(['This comparison reads every batch with both readers. Compare their paired read/pack timings, not the longer full validation runtime.']
                  if 'reader_comparison' in config else [])]}
    finally:
        if scratch and scratch.created:await asyncio.shield(scratch.cleanup())
        if xp is not None:
            xp.get_default_memory_pool().free_all_blocks(); xp.get_default_memory_pool().set_limit(size=256*1024**2)
