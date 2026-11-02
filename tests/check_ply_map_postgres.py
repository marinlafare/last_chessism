"""Synthetic disposable PostgreSQL only; require an explicit isolated URL."""
import asyncio
import os
import uuid
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from chessism_api.database.neo4j import ply_map_store as store,ply_map_compute as compute,ply_map_reader as reader
from chessism_api.operations.cuda_summaries import ply_map_worker as worker
from tests.test_ply_map import fixture


async def main():
    url=os.environ['PLY_MAP_TEST_DATABASE_URL']
    if 'ply-map-test-db' not in url:raise RuntimeError('Use the dedicated disposable database, not a live URL.')
    engine=create_async_engine(url);factory=async_sessionmaker(engine)
    async with engine.begin() as c:
        await c.execute(text('CREATE SCHEMA graph_sync'))
        await c.execute(text('CREATE TABLE graph_sync.event(id integer GENERATED ALWAYS AS IDENTITY,kind text,completed_at timestamptz)'))
        await c.execute(text('''CREATE TABLE public.cuda_summary_run(id uuid PRIMARY KEY,timings jsonb DEFAULT '{}',
            phase text,updated_at timestamptz,attempts integer DEFAULT 0,error text,retry_after timestamptz)'''))
    async with factory() as s,s.begin():
        await store.install(s);await store.install(s)
        assert (await store.status(s))['pending']
        assert (await reader.snapshot(s))[1] is None
        assert await s.scalar(text('SELECT count(*) FROM cuda_summary_run'))==0
    async def request():
        run=dict(id=uuid.uuid4(),metrics=['ply_map'])
        async with factory() as s,s.begin():
            await s.execute(text('INSERT INTO cuda_summary_run(id) VALUES(:id)'),run)
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
        async with factory() as s:
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
        async with factory() as s,s.begin():
            await s.execute(text("INSERT INTO graph_sync.event(kind) VALUES('score')"))
            assert not (await store.status(s))['pending']
            await s.execute(text("INSERT INTO graph_sync.event(kind,completed_at) VALUES('game',clock_timestamp())"))
            assert (await store.status(s))['pending']
        run=await request();await worker.tick(factory,run,xp)
        async with factory() as s,s.begin():
            await s.execute(text("INSERT INTO graph_sync.event(kind) VALUES('player')"))
        await finish(run)
        async with factory() as s,s.begin():
            state=await store.status(s)
            assert state['active_snapshot']==saved and state['pending'] and state['syncing']
            assert await s.scalar(text('SELECT count(*) FROM graph_map.input'))==0
            assert await store.capture(s,uuid.uuid4())==0
            await s.execute(text('UPDATE graph_sync.event SET completed_at=clock_timestamp()'))
        run=await request();await finish(run)
        async with factory() as s:
            state=await store.status(s)
            assert state['active_snapshot']==run['id'] and not state['pending']
            assert (await reader.snapshot(s,saved))[1]['games']==3
    await engine.dispose()
    print('PASS: additive/reentrant install, manual-only capture, checkpointed input, publication, player union, expansion, score-only stability, concurrent topology invalidation, previous snapshot retention and recovery')


if __name__=='__main__':asyncio.run(main())
