"""Frozen mixed-player baseline inputs from one read-only source snapshot."""
import hashlib
import json
import time
from collections import Counter
from pathlib import Path
import numpy as np
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from . import source
from .schema import table
from .benchmark_compute import FIELDS


def spool(folder,index,rows,games,items,expected):
    np.save(folder/f'{index}.npy',rows,allow_pickle=False)
    (folder/f'{index}.json').write_text(json.dumps({'games':games,'items':items,'expected':expected},default=str,allow_nan=False))


def read(folder,index):
    data=json.loads((folder/f'{index}.json').read_text())
    return np.load(folder/f'{index}.npy',allow_pickle=False),data


async def guard(session):
    if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
        raise ValueError('An active cloud job takes priority; benchmark refused.')
    layout=storage_for(session)
    row=(await session.execute(text(f'SELECT * FROM {table(layout,"control")}'))).mappings().one()
    if row['status']!='ready' or not row['active_model'] or row['active_model']!=row['target_model']:
        raise ValueError('Wait for canonical summary publication before benchmarking.')
    if await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {table(layout,"dirty")}) OR
        EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')})''')):
        raise ValueError('Canonical updates are pending; benchmark refused.')
    return row


async def freeze(config,reporter,folder,*,factory=None):
    factory=factory or AsyncDBSession; timings={}; counts=Counter(); owners=set(); all_items=[]
    rows_count=0; count=0; batches=0; digest=hashlib.sha256(); comparisons=[]
    async with factory() as session:
        await source.prepare(session)
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        control=await guard(session); layout=storage_for(session)
        taken_at=str(await session.scalar(text('SELECT transaction_timestamp()')))
        if control['active_model']!=config['model_id']:raise ValueError('Selected canonical model changed.')
        start=time.perf_counter()
        # Deterministic interleaving across mode/owner strata, then unique games.
        # This is performance sampling, not a population or calibration estimate.
        selected=[dict(r) for r in (await session.execute(text(f'''WITH candidates AS (
            SELECT game_link,revision,mode,COALESCE(white_owner,black_owner) AS owner,
                ROW_NUMBER() OVER(PARTITION BY mode,COALESCE(white_owner,black_owner)
                    ORDER BY md5(game_link::text||:seed),game_link) AS n
            FROM {table(layout,'game')} WHERE model_id=:model AND status='assessed')
            SELECT game_link,revision FROM candidates ORDER BY n,mode,owner,game_link LIMIT :limit'''),
            {'model':config['model_id'],'limit':config['games'],'seed':':canonical-timing-v1'})).mappings()]
        timings['selection_seconds']=time.perf_counter()-start
        if len(selected)!=config['games']:raise ValueError('Insufficient assessed games for the exact requested benchmark; no silent smaller sample.')
        players=(await session.execute(text(f'SELECT player_name FROM {layout.table("player")} WHERE joined>0 AND deleted_at IS NULL ORDER BY player_name'))).scalars().all()
        while count<len(selected):
            await reporter.check()
            if config.get('reader_comparison'):
                await reporter.progress('comparing_readers',count,len(selected),
                    'Comparing existing and binary database readers on identical inputs; CUDA follows. Live summaries are unchanged.')
            else:
                await reporter.progress('reading_and_preparing',count,len(selected),'Freezing read-only inputs; published summaries are unchanged.')
            batch_items=selected[count:count+source.MAX_GAMES]
            if config.get('reader_comparison'):
                data,pair=await compare_readers(session,batch_items,batches,reporter)
                comparisons.append(pair)
                for key,value in pair['binary_v1'].items():timings[key]=timings.get(key,0.)+value
                for key,value in pair['legacy'].items():timings['reference_'+key]=timings.get('reference_'+key,0.)+value
            else:
                data=await source.load(session,selected_items=batch_items,timings=timings,check=reporter.check)
            model,model_config,items,(games,unsupported),rows=data
            if model!=config['model_id'] or model_config!=config['model'] or unsupported or not items:
                raise ValueError('Canonical source eligibility/model changed; no partial sample accepted.')
            if rows_count+len(rows)>config['max_rows']:raise ValueError('Benchmark input exceeds its reserved row budget; no truncation allowed.')
            start=time.perf_counter()
            expected=[dict(r) for r in (await session.execute(text(f'''SELECT game_link,{','.join(FIELDS)}
                FROM {table(layout,'game')} WHERE model_id=:model AND game_link=ANY(CAST(:links AS bigint[]))'''),
                {'model':model,'links':[r['game_link'] for r in items]})).mappings()]
            timings['reference_read_seconds']=timings.get('reference_read_seconds',0.)+time.perf_counter()-start
            start=time.perf_counter()
            await file_operation(spool,folder,batches,rows,games,items,expected)
            timings['spool_seconds']=timings.get('spool_seconds',0.)+time.perf_counter()-start
            digest.update(rows.tobytes()); digest.update(json.dumps(items,sort_keys=True).encode())
            digest.update(json.dumps(games,sort_keys=True,default=str).encode())
            rows_count+=len(rows); count+=len(items); batches+=1; all_items.extend(items)
            counts.update(g['mode'] for g in games)
            owners.update(g[k] for g in games for k in ('white_owner','black_owner') if g[k])
    info={'games':count,'rows':rows_count,'batches':batches,'by_mode':dict(counts),'taken_at':taken_at,
        'represented_players':len(owners),'eligible_players':len(players),'input_sha256':digest.hexdigest()}
    if comparisons:
        old=sum(timings.get('reference_'+k,0) for k in ('source_queries_seconds','source_pack_seconds'))
        new=sum(timings.get(k,0) for k in ('source_queries_seconds','source_pack_seconds'))
        from . import binary_source
        info['reader_comparison']={'status':'passed','same_snapshot':True,'order':'alternating AB/BA by batch',
            'legacy_seconds':old,'binary_seconds':new,'speedup':old/new if new else None,
            'input_byte_agreement':True,'batches':comparisons,
            'implementation_sha256':hashlib.sha256(Path(binary_source.__file__).read_bytes()).hexdigest()}
    return info,timings,all_items,players,layout


async def compare_readers(session,items,index,reporter):
    order=('legacy','binary_v1') if index%2==0 else ('binary_v1','legacy')
    results={}; pair={'order':list(order)}
    for reader in order:
        await reporter.check(); pair[reader]={}
        results[reader]=await source.load(session,selected_items=items,timings=pair[reader],
            check=reporter.check,position_reader=reader)
    old,new=results['legacy'],results['binary_v1']
    # Same selected games, metadata, revisions, order and every numerical byte.
    if old[:4]!=new[:4] or old[4].tobytes()!=new[4].tobytes():
        raise ValueError('Reader input disagreement; live calculation remains unchanged.')
    pair.update(games=len(new[2]),rows=len(new[4]))
    return new,pair
