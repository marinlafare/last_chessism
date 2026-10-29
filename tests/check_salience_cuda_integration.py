"""Isolated plumbing checks, not a live-player experiment or model validation.

Private PostgreSQL, synthetic games, no network/cloud credentials. Exercise the
CUDA boundary with a test double, checkpoints, policy and atomic publication.
"""
import asyncio
import json
import os
from contextlib import suppress
from pathlib import Path
import tempfile
import uuid
from unittest.mock import patch
from types import SimpleNamespace
from sqlalchemy import text

os.environ['DATABASE_URL']='postgresql+asyncpg://test@127.0.0.1:1/postgres'

FIXTURE = '''
CREATE SCHEMA candidate;
CREATE TABLE candidate.player(id bigint PRIMARY KEY,player_name text UNIQUE,joined bigint,deleted_at timestamptz,
 country text,location text,timezone text,timezone_source text);
INSERT INTO candidate.player VALUES(1,'alice',1,NULL,NULL,NULL,'UTC','explicit'),(2,'bob',1,NULL,NULL,NULL,'UTC','explicit');
CREATE TABLE candidate.game_mode(id int PRIMARY KEY,name text);
INSERT INTO candidate.game_mode VALUES(1,'blitz');
CREATE TABLE candidate.game(link bigint PRIMARY KEY,fens_done boolean,
 played_at timestamptz DEFAULT '2026-01-01 12:00+00',mode_id int DEFAULT 1);
INSERT INTO candidate.game(link,fens_done) VALUES(1,true),(2,true),(3,true);
CREATE TABLE candidate.game_player(id bigint PRIMARY KEY,player_id bigint,game_link bigint,is_white boolean);
INSERT INTO candidate.game_player VALUES(1,1,1,true),(2,1,2,true),(3,2,3,false);
CREATE TABLE candidate.game_fen_association(game_link bigint,n_move int,is_white boolean,fen_id bigint);
INSERT INTO candidate.game_fen_association VALUES(1,1,true,10),(1,1,false,10),(1,2,true,20),(1,16,true,999),
 (2,1,true,10),(2,1,false,30),(3,1,true,10);
CREATE TABLE candidate.player_salience_summary(player_id bigint PRIMARY KEY,status text,source_game_count bigint,
 source_position_count bigint,effective_game_count float8,error text);
INSERT INTO candidate.player_salience_summary VALUES(1,'ready',2,3,1.8,NULL),(2,'ready',1,1,.9,NULL);
CREATE TABLE candidate.player_salience_pending_game(game_player_id bigint PRIMARY KEY);
CREATE TABLE candidate.game_player_salience(game_player_id bigint PRIMARY KEY,salience float8,weighted_numerator float8,
 depth_weight_sum float8,position_occurrence_count int,unique_position_count int);
INSERT INTO candidate.game_player_salience VALUES(1,.9,.9,1,1,1),(2,.9,.9,1,1,1),(3,.9,.9,1,1,1);
CREATE TABLE candidate.player_position_frequency(player_id bigint,is_white boolean,fen_id bigint,games_with_position bigint,
 total_occurrences bigint,PRIMARY KEY(player_id,is_white,fen_id));
CREATE TABLE candidate.player_stockfish_state(player_name text PRIMARY KEY,revision bigint,published_revision bigint);
INSERT INTO candidate.player_stockfish_state VALUES('alice',1,1),('bob',1,1);
CREATE TABLE public.cuda_summary_control(id boolean PRIMARY KEY,manual boolean,scores boolean,accuracy boolean);
INSERT INTO public.cuda_summary_control VALUES(true,true,true,true);
CREATE TABLE public.cuda_summary_run(id uuid PRIMARY KEY,metrics text[],status text,phase text,timings jsonb DEFAULT '{}',
 updated_at timestamptz,attempts int DEFAULT 0,error text,retry_after timestamptz);
CREATE VIEW public.player AS SELECT * FROM candidate.player;
'''


async def check(factory, connection, layout, xp):
    from chessism_api.operations.cuda_summaries import salience_store as store,salience_worker as worker
    from chessism_api.operations.cuda_summaries import accuracy_store,input_snapshot,runs,salience_reader
    from chessism_api.operations.player_hero_analytics import AnalyticsScope
    from chessism_api.operations.player_timezone import resolve_player_timezone
    from chessism_api.operations import player_salience,player_salience_calculation
    async with factory() as s,s.begin():
        await accuracy_store.install(s)
        await store.install(s,layout)
        assert not await store.enabled(s)
        await store.activate(s)
        assert await store.enabled(s)
    assert await connection.fetchval('SELECT count(*) FROM public.cuda_summary_run')==0
    assert await connection.fetchval('SELECT min(salience) FROM candidate.game_player_salience')==.9
    # Current data stays untouched at activation, and neither legacy entry can run.
    with patch.object(player_salience,'AsyncDBSession',factory),patch.object(player_salience_calculation,'AsyncDBSession',factory):
        assert (await player_salience.enqueue_player_salience(None,'alice'))['status']=='pending_cuda'
        try:await player_salience_calculation.calculate_player_salience('alice')
        except ValueError as error:assert 'CUDA-only' in str(error)
        else:raise AssertionError('CPU salience was not disabled')
    await connection.execute('''INSERT INTO public.cuda_accuracy_state(player_name,revision,version,timezone)
        VALUES('alice',1,'game_accuracy_v1','{}'),('bob',1,'game_accuracy_v1','{}');
        INSERT INTO public.cuda_accuracy_game VALUES('alice',1,'2026-01-01','2026-01-01 12:00+00','blitz',75,12),
        ('alice',2,'2026-01-01','2026-01-01 13:00+00','blitz',95,5);''')
    async def request():
        id=uuid.uuid4()
        await connection.execute("INSERT INTO public.cuda_summary_run(id,metrics,status) VALUES($1,ARRAY['salience'],'running')",id)
        async with factory() as s,s.begin():assert await store.capture(s,id)==2
        return {'id':id,'metrics':['salience']}
    root=Path(tempfile.mkdtemp(prefix='salience-inputs-',dir='/restore-test'))
    with patch.object(input_snapshot,'ROOT',root),patch.object(input_snapshot,'FREE_FLOOR',0):
        run=await request()
        # A committed extraction checkpoint survives a worker retry. It is not
        # yet an official result, and no production inputs are accessed here.
        await worker.tick(factory,run,xp)
        assert await connection.fetchval('SELECT min(salience) FROM candidate.game_player_salience')==.9
        for _ in range(30):
            if not await worker.tick(factory,run,xp):break
        else:raise AssertionError('Manual salience stage did not finish')
        async with factory() as s:
            state=await runs.progress(s,run['id'])
            pending=(await s.execute(text(store.needed(layout)))).all()
        assert state['timings']['salience']['complete'] and not pending
        assert await connection.fetchval('SELECT count(*) FROM public.cuda_salience_scope WHERE status=\'published\'')==2
        assert await connection.fetchval('SELECT sum(position_occurrence_count) FROM candidate.game_player_salience')==6
        assert await connection.fetchval('SELECT count(*) FROM public.cuda_accuracy_daily')>0
        assert await connection.fetchval("SELECT (payload->>'games')::int FROM public.cuda_salience_daily WHERE player_id=1 AND mode_mask=7")==2
        assert await connection.fetchval("SELECT (payload->>'games')::int FROM public.cuda_accuracy_daily WHERE player_name='alice' AND mode_mask=7")==1
        assert await connection.fetchval("SELECT (payload->>'games')::int FROM public.cuda_salience_daily WHERE player_id=2 AND mode_mask=7")==1
        zone=resolve_player_timezone({'timezone':'UTC','timezone_source':'explicit'})
        async with factory() as s:
            independent=await salience_reader.saved(s,AnalyticsScope('bob','all',None,None,zone),('bullet','blitz','rapid'))
            assert independent['status']=='ready' and independent['points'][0][3]==1
            assert independent['cohort']=='all_games_with_salience'
            assert (await salience_reader.saved(s,AnalyticsScope('bob','rapid',None,None,zone),('rapid',)))['points']==[]
        assert await connection.fetchval('SELECT count(*) FROM public.cuda_salience_game_stage')==0
        # New revisions during extraction must retain every published value.
        before=await connection.fetch('SELECT * FROM candidate.game_player_salience ORDER BY game_player_id')
        await connection.execute("UPDATE candidate.player_salience_summary SET status='stale'")
        run=await request();await worker.tick(factory,run,xp)
        await connection.execute("UPDATE candidate.player_salience_summary SET status='stale'")
        for _ in range(30):
            if not await worker.tick(factory,run,xp):break
        assert before==await connection.fetch('SELECT * FROM candidate.game_player_salience ORDER BY game_player_id')
        assert await connection.fetchval("SELECT count(*) FROM public.cuda_salience_scope WHERE run_id=$1 AND status='superseded'",run['id'])==2
    print('PASS: explicit capture, CPU deferral, binary inputs, device dispatch, checkpoints, daily cache publication and revision-race protection; synthetic data only')


async def main():
    import asyncpg
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    import numpy as np
    from chessism_api.operations.cuda_summaries import salience_worker,salience_publication,salience_compute,accuracy_compute,salience_daily
    from chessism_api.operations.matrix_constructor.artifact_files import file_operation
    from tests.check_db_refactor_candidate_postgres import command
    xp=SimpleNamespace(cuda=SimpleNamespace(runtime=SimpleNamespace(memGetInfo=lambda:(2**30,2**30))),
        get_default_memory_pool=lambda:SimpleNamespace(free_all_blocks=lambda:None))
    async def boundary(function,*args,**kwargs):
        if function in (salience_compute.calculate,salience_compute.reduce_games,salience_compute.totals,accuracy_compute.daily,salience_daily.calculate):
            assert args[0] is xp
            return await file_operation(function,np,*args[1:],require_cuda=False,**kwargs)
        return await file_operation(function,*args,**kwargs)
    root=Path(tempfile.mkdtemp(prefix='salience-integration-',dir='/restore-test'));socket=root/'socket';socket.mkdir()
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl';connection=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute(FIXTURE)
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        layout=RelationalStorage('candidate','archive','reads','relational_v3')
        with patch.object(salience_worker,'file_operation',side_effect=boundary),patch.object(salience_publication,'file_operation',side_effect=boundary):
            await check(async_sessionmaker(engine,info={'relational_storage_v2':layout}),connection,layout,xp)
    finally:
        if engine:await engine.dispose()
        if connection:await connection.close()
        with suppress(Exception):await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__':asyncio.run(main())
