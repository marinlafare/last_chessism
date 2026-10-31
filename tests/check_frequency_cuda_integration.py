"""Private PostgreSQL and synthetic inputs only; no cloud/live-data access."""
import asyncio
import os
import tempfile
import uuid
from pathlib import Path
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from tests.check_salience_cuda_integration import FIXTURE

EXTRA='''
ALTER TABLE candidate.game_fen_association ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;
CREATE INDEX frequency_test_game ON candidate.game_fen_association(game_link,id);
CREATE TABLE candidate.fen(fen_id bigint PRIMARY KEY,fen text,score float8,analysis_source text);
INSERT INTO candidate.game_mode VALUES(2,'bullet'),(3,'rapid');
INSERT INTO candidate.game(link,fens_done,mode_id) VALUES(4,true,2),(5,true,3);
INSERT INTO candidate.game_player VALUES(4,1,4,true),(5,1,5,false);
INSERT INTO candidate.game_fen_association(game_link,n_move,is_white,fen_id) VALUES
 (4,1,true,20),(4,2,true,999),(4,3,true,40),(5,1,true,50),(5,2,true,50),(5,3,true,60),(5,4,true,60);
INSERT INTO candidate.game_fen_association(game_link,n_move,is_white,fen_id)
 SELECT 1,21+n,true,100+n FROM generate_series(1,14) n CROSS JOIN generate_series(1,3) repetition;
INSERT INTO candidate.fen SELECT DISTINCT fen_id,'board '||fen_id,
 CASE WHEN fen_id=60 THEN NULL WHEN fen_id=50 THEN 100 ELSE -100 END,'stockfish'
 FROM candidate.game_fen_association;
CREATE TABLE candidate.player_analytics_state(player_name text PRIMARY KEY,revision bigint);
INSERT INTO candidate.player_analytics_state VALUES('alice',1),('bob',1);
'''


async def check(factory, connection, layout):
    from chessism_api.operations.cuda_summaries import salience_store,frequency_store as store,frequency_worker as worker
    from chessism_api.operations.cuda_summaries import frequency_compute as compute,frequency_source as source,input_snapshot
    from chessism_api.operations.player_frequency_position_page import read_ranked_page,FrequencySnapshotChanged
    from chessism_api.operations.player_score_distribution import bins
    from chessism_api.operations.matrix_constructor.artifact_files import file_operation
    xp=SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(2**30,2**30))),
        get_default_memory_pool=lambda:SimpleNamespace(free_all_blocks=lambda:None))
    async def boundary(function,*args,**kwargs):
        if function is compute.calculate:return await file_operation(function,np,*args[1:],require_cuda=False,**kwargs)
        return await file_operation(function,*args,**kwargs)
    async with factory() as s,s.begin():
        await salience_store.install(s,layout)
        await salience_store.activate(s)
        await store.install(s,layout)
        assert not await store.enabled(s)
        await store.activate(s)
        assert len(await store.pending(s))==2
    assert await connection.fetchval('SELECT count(*) FROM public.cuda_summary_run')==0
    async def request():
        run=dict(id=uuid.uuid4(),metrics=['position_frequency'])
        async with factory() as s,s.begin():
            await s.execute(text("INSERT INTO public.cuda_summary_run(id,metrics,status) VALUES(:id,ARRAY['position_frequency'],'running')"),run)
            assert await store.capture(s,run['id'])>0
        return run
    async def finish(run):
        for _ in range(100):
            if not await worker.tick(factory,run,xp):return
        raise AssertionError('Frequency stage did not finish')
    with tempfile.TemporaryDirectory(dir='/restore-test') as root,patch.object(input_snapshot,'ROOT',Path(root)),\
            patch.object(input_snapshot,'FREE_FLOOR',0),patch.object(worker,'file_operation',side_effect=boundary),\
            patch.object(source,'ROW_LIMIT',5),patch.object(source,'GAME_LIMIT',2):
        run=await request();await finish(run)
        expected=await connection.fetch('''WITH counted AS (
            SELECT gp.player_id,gp.is_white,g.mode_id,a.fen_id,count(*) AS appearances,
            sum(count(*)) OVER(PARTITION BY gp.player_id,a.fen_id) AS total
            FROM candidate.game_player gp JOIN candidate.game g ON g.link=gp.game_link
            JOIN candidate.game_fen_association a ON a.game_link=g.link
            GROUP BY gp.player_id,gp.is_white,g.mode_id,a.fen_id)
            SELECT player_id,is_white,mode_id,fen_id,appearances FROM counted WHERE total>1
            ORDER BY player_id,is_white,mode_id,fen_id''')
        actual=await connection.fetch('SELECT * FROM public.player_fen_frequency ORDER BY player_id,is_white,mode_id,fen_id')
        assert actual==expected,(actual,expected)
        assert await connection.fetchval('SELECT count(*) FROM public.player_fen_frequency WHERE fen_id=999')==2
        assert await connection.fetchval('SELECT count(*) FROM public.player_fen_frequency WHERE fen_id=60')==1
        async with factory() as s:
            assert not await store.pending(s)
            first=await read_ranked_page(s,'alice',['blitz','bullet'],bins()[5],color='white')
            second=await read_ranked_page(s,'alice',['blitz','bullet'],bins()[5],first['next_cursor'],color='white')
            rows=first['rows']+second['rows']
            assert len(rows)==17 and len({r['fen_id'] for r in rows})==17
            assert [r['appearances'] for r in rows]==sorted([r['appearances'] for r in rows],reverse=True)
            black=await read_ranked_page(s,'alice',['rapid'],bins()[5],color='black')
            assert [(r['fen_id'],r['score_min'],r['appearances']) for r in black['rows']]==[('50',-100,2)]
            bob=await read_ranked_page(s,'bob',['blitz'],bins()[5])
            assert bob['rows']==[] and bob['sort']=='appearances_desc'
        # A newly scored FEN becomes available without recounting frequencies.
        await connection.execute("UPDATE candidate.fen SET score=100 WHERE fen_id=60; UPDATE candidate.player_stockfish_state SET revision=revision+1 WHERE player_name='alice'")
        async with factory() as s:
            assert not await store.pending(s)
            black=await read_ranked_page(s,'alice',['rapid'],bins()[5],color='black')
            assert {r['fen_id'] for r in black['rows']}=={'50','60'}
            try:await read_ranked_page(s,'alice',['blitz','bullet'],bins()[5],first['next_cursor'],color='white')
            except FrequencySnapshotChanged:pass
            else:raise AssertionError('Changed score snapshot was not rejected')
        # Revision races retain the previous complete snapshot and remain pending.
        await connection.execute("UPDATE candidate.player_salience_summary SET status='stale' WHERE player_id=1")
        run=await request();await worker.tick(factory,run,xp)
        await connection.execute("UPDATE candidate.player_salience_summary SET status='stale' WHERE player_id=1")
        await finish(run)
        assert actual==await connection.fetch('SELECT * FROM public.player_fen_frequency ORDER BY player_id,is_white,mode_id,fen_id')
        async with factory() as s:assert len(await store.pending(s))==1
        # Removal can turn a repeated FEN into a singleton; a full refresh drops it.
        await connection.execute("DELETE FROM candidate.game_fen_association WHERE game_link=4 AND fen_id=999; UPDATE candidate.player_analytics_state SET revision=revision+1 WHERE player_name='alice'")
        run=await request();await finish(run)
        assert not await connection.fetchval('SELECT EXISTS(SELECT 1 FROM public.player_fen_frequency WHERE fen_id=999)')
        async with factory() as s:assert not await store.pending(s)
    print('PASS: private DB; manual-only activation, all plies/unscored FENs, cross-mode singleton contributions, exact counts, bounded checkpoint pagination, rank paging, score refresh without recount, revision race and deletion refresh')


async def main():
    import asyncpg
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage,SESSION_KEY
    from tests.check_db_refactor_candidate_postgres import command
    root=Path(tempfile.mkdtemp(prefix='frequency-integration-',dir='/restore-test'));socket=root/'socket';socket.mkdir()
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl';connection=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute(FIXTURE+EXTRA)
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        layout=RelationalStorage('candidate','archive','reads','relational_v3')
        await check(async_sessionmaker(engine,info={SESSION_KEY:layout}),connection,layout)
    finally:
        if engine:await engine.dispose()
        if connection:await connection.close()
        with suppress(Exception):await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__':asyncio.run(main())
