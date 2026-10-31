"""Canonical binary-reader activation, synthetic PostgreSQL only.

The device boundary is mocked here; exact GPU agreement is covered separately.
Real source extraction, queue revisions, publication and error handling run.
"""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
import numpy as np
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations.canonical_blunders import worker,binary_source

original_check=base.check


async def check(factory,connection,layout):
    assert worker.INPUT_READER=='binary_v1'
    # Run all existing source-race, rollback, eligibility and publication cases
    # through the reader selected by the real automatic worker.
    await original_check(factory,connection,layout,position_reader=worker.INPUT_READER)
    async def snapshot():
        return await connection.fetchval('''SELECT jsonb_agg(to_jsonb(s) ORDER BY s.player_name)
            FROM candidate.canonical_blunder_snapshot s''')
    device=SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(2**33,2**33))),
        get_default_memory_pool=lambda:SimpleNamespace(free_all_blocks=lambda:None,set_limit=lambda **kw:None))
    async def device_boundary(function,*args,**kwargs):
        if function is worker.cuda_backend:return device,{'name':'NVIDIA GeForce RTX 4060'}
        assert function is worker.compute.infer and args[0] is device
        return function(np,*args[1:],require_cuda=False)
    with patch.object(worker,'AsyncDBSession',factory),patch.object(worker,'file_operation',side_effect=device_boundary):
        before=await snapshot()
        assert await worker.canonical_tick({})=={'status':'idle','input_reader':'binary_v1'}
        assert await snapshot()==before
        # An actual source change queues a one-game refresh, not a full rebuild.
        await connection.execute('UPDATE candidate.game SET white_elo=900 WHERE link=1')
        result=await worker.canonical_tick({})
        assert result['written']==1 and result['remaining']==0 and result['input_reader']=='binary_v1'
        progress=json.loads(await connection.fetchval('SELECT progress FROM candidate.canonical_blunder_control'))
        assert progress['input_reader']=='binary_v1' and progress['source_timings']['source_queries_seconds']>0
        before=await snapshot()
        await connection.execute('UPDATE candidate.game SET black_elo=black_elo WHERE link=2')
        # No silent legacy/CPU fallback if the new reader detects invalid input.
        with patch.object(binary_source,'load',AsyncMock(side_effect=ValueError('Synthetic binary input failure'))) as loader, \
                patch.object(worker.LOG,'exception'):
            assert await worker.canonical_tick({})=={'status':'retrying'}
            loader.assert_awaited_once()
        assert await snapshot()==before
        assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')==1
        assert await connection.fetchval('SELECT active_model=target_model FROM candidate.canonical_blunder_control')
        assert await worker.canonical_tick({})=={'status':'retry_backoff'}
        await connection.execute("UPDATE candidate.canonical_blunder_control SET updated_at=now()-interval '61 seconds'")
        result=await worker.canonical_tick({})
        assert result['written']==1 and result['status']=='ready'
        assert await connection.fetchval('SELECT error IS NULL FROM candidate.canonical_blunder_control')
        assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')==0
    print('PASS automatic worker: binary source, incremental refresh, reader timing metadata, idle preserves summaries, failure retains queue/results, no fallback, successful retry')


if __name__=='__main__':
    base.check=check
    asyncio.run(base.main())
