"""Read-only ABBA comparison of old/new input loading for one small player.

Run explicitly in an application environment. Refuses active cloud work, stale
summaries and players above the original 100,000-row limit. Never runs an engine,
writes a summary or changes the database. Results are printed, not published.
"""
import argparse
import asyncio
import json
import tempfile
import time

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.matrix_constructor.queries import matrix_sql
from chessism_api.operations.player_score_distribution import MODES, prepare
from chessism_api.operations.player_stockfish_summary import read_saved
from chessism_api.operations.research_algorithms.score_validation_job import configuration, input_sql, spool_batch, summary_groups
from chessism_api.operations.research_algorithms.score_stream import DTYPE
from chessism_api.operations.research_algorithms.summary_validation import BATCH_ROWS, KINDS, MAX_ROWS


async def benchmark(player):
    config = configuration(player)
    player = config['player']
    async with AsyncDBSession() as session:
        await prepare(session)
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            raise ValueError('Active cloud workflow; benchmark refused.')
        if not await session.scalar(text('SELECT 1 FROM player WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'),{'player':player}):
            raise ValueError('Complete player required.')
        saved=await read_saved(session,player)
        if saved['summary_pending'] or saved['stale']: raise ValueError('Current summary required.')
        total=sum(sum(g['counts']) for g in summary_groups(player,saved['by_mode']).values())
        if not 1 <= total <= MAX_ROWS: raise ValueError('Benchmark is bounded to 100,000 rows.')
        _,old,params=matrix_sql(config['matrix'])
        params['max_rows']=MAX_ROWS+1
        results=[]
        expected=None
        for method in ('legacy','streamed','streamed','legacy'):
            started=time.perf_counter()
            if method=='legacy':
                rows=[dict(r) for r in (await session.execute(text(old),params)).mappings()]
                elapsed=time.perf_counter()-started
                assert len(rows)==total
                data=np.fromiter(((MODES.index(r['mode']),r['player_score'],KINDS[r['score_kind']],
                    -1 if r['mate_moves'] is None else r['mate_moves']) for r in rows),dtype=DTYPE,count=len(rows))
            else:
                with tempfile.TemporaryFile() as spool:
                    cursor=await session.stream(text(input_sql(session)),{'player':player,'modes':list(MODES),'row_limit':MAX_ROWS+1},execution_options={'yield_per':BATCH_ROWS})
                    count=0
                    try:
                        async for records in cursor.partitions(BATCH_ROWS):
                            count+=len(records)
                            if count>MAX_ROWS: raise ValueError('Input grew beyond benchmark bound.')
                            spool_batch(spool,records)
                    finally: await cursor.close()
                    spool.flush()
                    elapsed=time.perf_counter()-started
                    assert count==total
                    spool.seek(0)
                    data=np.frombuffer(spool.read(),dtype=DTYPE)
            # Outside measured loading: compare complete multisets, preserving
            # occurrence multiplicity regardless of the differing stream order.
            ordered=np.sort(data)
            if expected is None: expected=ordered
            else: np.testing.assert_array_equal(ordered,expected)
            results.append({'method':method,'seconds':elapsed,'rows':len(data)})
        return {'player':player,'summary_snapshot':saved['snapshot'],'order':'ABBA',
            'input_multiset':'exact_match','runs':results,
            'notes':'Same read-only snapshot. Warm-cache/order-sensitive extraction timings, not production GPU speedup.'}


async def main(player):
    from constants import CONN_STRING
    from chessism_api.database.runtime import bind_runtime_engine
    engine=await bind_runtime_engine(CONN_STRING)
    try: print(json.dumps(await asyncio.wait_for(benchmark(player),timeout=120)))
    finally: await engine.dispose()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('player')
    asyncio.run(main(parser.parse_args().player))
