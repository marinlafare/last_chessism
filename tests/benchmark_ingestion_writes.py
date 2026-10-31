"""Synthetic private-PostgreSQL comparison; never attaches to a live database."""
import argparse
import asyncio
import importlib.util
import json
from time import perf_counter
from . import check_ingestion_bulk_postgres as base
from chessism_api.operations.ingestion_pipeline.fen_bulk import persist_positions


def key(i):return f'benchmark-{i:09d}-'+'p'*48


async def benchmark(conn,factory,baseline):
    await conn.copy_records_to_table('fen',columns=['fen','piece_count','moves_counter','n_games','score'],
        records=[(key(i),6,'#0_1',0,25 if i%7==0 else None) for i in range(150000)])
    await conn.execute('ANALYZE fen')
    fens=[{'fen':key(i*7 if i<8000 else 200000+i),'piece_count':6,'moves_counter':'#0_1'} for i in range(12000)]
    links=[{'game_link':1+i//100,'n_move':1+(i%100)//2,'move_color':'white' if i%2==0 else 'black',
            'fen_fen':fens[i%len(fens)]['fen']} for i in range(40000)]
    reference=None
    for name,writer in [('old',baseline),('new',persist_positions),('new',persist_positions),('old',baseline)]:
        async with factory() as session:
            await session.begin();start=perf_counter();timings={}
            result=await writer(session,fens,links,**({'timings':timings} if name=='new' else {}))
            elapsed=perf_counter()-start
            from sqlalchemy import text
            snapshot=[tuple(r) for r in (await session.execute(text('SELECT fen,n_games FROM fen WHERE n_games>0 ORDER BY fen'))).all()]
            counters=[tuple(r) for r in (await session.execute(text('SELECT total_positions,unscored_fens,equal_appearances FROM scored_position_summary'))).all()]
            actual=(result,snapshot,counters)
            if reference is None:reference=actual
            assert actual==reference,'Writer changed identities, counts or summary deltas'
            await session.rollback()
            print(json.dumps({'writer':name,'seconds':round(elapsed,3),'fens':len(fens),'links':len(links),
                'timings':{k:round(v,3) for k,v in timings.items()}}),flush=True)
    print('PASS old/new writer equality; timings exclude seed, comparison and rollback; synthetic, not a production speedup',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--baseline',required=True)
    args=parser.parse_args();spec=importlib.util.spec_from_file_location('before_ingestion',args.baseline)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    async def exercise(conn,factory):await benchmark(conn,factory,module.persist_positions)
    base.exercise=exercise
    asyncio.run(base.main())
