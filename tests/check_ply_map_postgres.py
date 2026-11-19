"""Synthetic disposable PostgreSQL only; require an explicit isolated URL."""
import asyncio
import os
import uuid
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from chessism_api.database.postgresql.transactions import write_transaction, read_transaction
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from chessism_api.database.neo4j import ply_map_store as store,ply_map_compute as compute,ply_map_reader as reader
from chessism_api.operations.cuda_summaries import ply_map_worker as worker
from tests.test_ply_map import fixture


async def exercise(factory):
    """Exercise the original map scenarios on an initialized canonical database."""
    async with write_transaction(factory) as s:
        await s.execute(text("UPDATE graph_sync.control SET enabled=true"))
        assert (await store.status(s))['pending']
        assert (await reader.snapshot(s))[1] is None
        assert await s.scalar(text('SELECT count(*) FROM cuda_summary_run'))==0
    async def request():
        run=dict(id=uuid.uuid4(),metrics=['ply_map'])
        async with write_transaction(factory) as s:
            await s.execute(text("UPDATE cuda_summary_run SET status='complete'"))
            await s.execute(text("INSERT INTO cuda_summary_run(id,requested_by,model_id,metrics,status) VALUES(:id,'test','synthetic',ARRAY['ply_map'],'running')"),run)
            assert await store.capture(s,run['id'])==1
        return run
    async def source(after):return [g for g in fixture() if g['id']>after][:2]
    async def operation(games,budget,progress):
        await progress(dict(stage='compressing',games=len(games),total_games=len(games)))
        return compute.build(games,np),dict(implementation=compute.IMPLEMENTATION)
    xp=SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(8*2**30,8*2**30))))
    async def finish(run):
        for _ in range(10):
            if not await worker.tick(factory,run,xp):return
        raise AssertionError('Stage did not finish')
    with patch('chessism_api.database.neo4j.ply_map_source.read',source),patch.object(worker.ply_map_process,'execute',operation):
        run=await request();await finish(run)
        async with read_transaction(factory) as s:
            state=await store.status(s)
            assert not state['pending'] and state['games']==3
            saved=state['active_snapshot']
            view=await reader.view(s,saved,0,6,0,[1,2])
            root=next(n for n in view['nodes'] if n['key']=='root')
            assert root['selected_games']==3 and root['games']==3
            assert len(await reader.players(s,saved,'player1'))==1
            details=await reader.detail(s,saved,'t:p:3:20:ending',0,[])
            assert details['total']==2 and all(g['start_ply']==4 for g in details['rows'])
            direct=next(e for e in view['connections'] if e['kind']=='direct' and e['reason']=='shared')
            assert (await reader.detail(s,saved,direct['key'],0,[]))['rows'][0]['end_ply']==1
        async with write_transaction(factory) as s:
            await s.execute(text("INSERT INTO graph_sync.event(kind,ids) VALUES('score',ARRAY[1])"))
            assert not (await store.status(s))['pending']
            await s.execute(text("INSERT INTO graph_sync.event(kind,ids,completed_at) VALUES('game',ARRAY[1],clock_timestamp())"))
            assert (await store.status(s))['pending']
        run=await request();await worker.tick(factory,run,xp)
        async with write_transaction(factory) as s:
            await s.execute(text("INSERT INTO graph_sync.event(kind,ids) VALUES('player',ARRAY[1])"))
        await finish(run)
        async with write_transaction(factory) as s:
            state=await store.status(s)
            assert state['active_snapshot']==saved and state['pending'] and state['syncing']
            assert await s.scalar(text('SELECT count(*) FROM graph_map.input'))==0
            assert await store.capture(s,uuid.uuid4())==0
            await s.execute(text('UPDATE graph_sync.event SET completed_at=clock_timestamp()'))
        run=await request();await finish(run)
        async with read_transaction(factory) as s:
            state=await store.status(s)
            assert state['active_snapshot']==run['id'] and not state['pending']
            assert (await reader.snapshot(s,saved))[1]['games']==3
    print('PASS: canonical initializer, manual-only capture, checkpointed input, publication, player union, expansion, score-only stability, concurrent topology invalidation, previous snapshot retention and recovery')
