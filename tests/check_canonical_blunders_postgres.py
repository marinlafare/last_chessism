"""Disposable PostgreSQL correctness test. No network or live credentials."""
import asyncio
from contextlib import suppress
import json
from pathlib import Path
import tempfile
from unittest.mock import patch
import numpy as np
from sqlalchemy import text
from tests.check_blunder_training_postgres import FIXTURE
from tests.test_canonical_blunders import report

EXTRA='''
ALTER TABLE candidate.game ADD COLUMN played_at timestamptz DEFAULT now();
CREATE TABLE candidate.game_player(id bigserial,game_link bigint,player_id bigint,is_white boolean);
INSERT INTO candidate.game_player(game_link,player_id,is_white)
 SELECT link,white_player_id,true FROM candidate.game UNION ALL SELECT link,black_player_id,false FROM candidate.game;
CREATE TABLE candidate.moves(link bigint);
CREATE TABLE candidate.game_player_engine_summary(game_player_id bigint,blunder_count integer,analyzed_player_moves integer,game_efficiency float8);
INSERT INTO candidate.game_player_engine_summary SELECT id,7,11,.8 FROM candidate.game_player;
CREATE SCHEMA reads;
CREATE VIEW reads.game_player_engine_summary AS SELECT gp.game_link,p.player_name,s.blunder_count
 FROM candidate.game_player_engine_summary s JOIN candidate.game_player gp ON s.game_player_id=gp.id
 JOIN candidate.player p ON p.id=gp.player_id;
'''


async def check(factory,connection,layout,*,position_reader='legacy'):
    from chessism_api.operations.canonical_blunders import schema,registry,source,compute,publication,readers
    model,config,evidence=registry.freeze(report(),registry.ACK)
    async with factory() as s,s.begin():
        await schema.install(s,layout)
        await s.execute(text('''INSERT INTO candidate.canonical_blunder_model(id,source_run,config,evidence,approved_by,approval)
            VALUES(:id,'fixture',CAST(:config AS jsonb),CAST(:evidence AS jsonb),'test','provisional_user_approved')'''),
            {'id':model,'config':json.dumps(config),'evidence':json.dumps(evidence)})
        await registry.select_target(s,model)
    assert await connection.fetchval('SELECT active_model IS NULL FROM candidate.canonical_blunder_control')
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')==7
    async def load():
        async with factory() as s:
            await source.prepare(s)
            return await source.load(s,position_reader=position_reader)
    async def publish(data):
        mid,cfg,items,(eligible,unsupported),rows=data
        records,info=compute.infer(np,rows,eligible,cfg,require_cuda=False)
        records+=unsupported
        async with factory() as s,s.begin():return await publication.publish(s,mid,items,records,info)
    # Source changed after extraction: stale result must not be published/acknowledged.
    with patch.object(source,'MAX_GAMES',2):data=await load()
    await connection.execute('UPDATE candidate.game SET white_elo=700 WHERE link=1')
    result=await publish(data)
    assert result['written']==1 and not result['activated'],result
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_game WHERE game_link=1')==0
    assert await connection.fetchval('SELECT active_model IS NULL FROM candidate.canonical_blunder_control')
    # Malformed result rolls back, including queue acknowledgement.
    data=await load(); mid,cfg,items,(eligible,unsupported),rows=data
    records,info=compute.infer(np,rows,eligible,cfg,require_cuda=False); records+=unsupported
    records[0]['game_link']=999
    try:
        async with factory() as s,s.begin():await publication.publish(s,mid,items,records,info)
    except ValueError:pass
    else:raise AssertionError('Mismatched result keys accepted')
    before=await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')
    assert before==6
    result=await publish(await load())
    assert result['activated'] and result['remaining']==0,result
    assert await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control')==model
    # Idempotent duplicate result cannot double-count.
    again=await publish(data)
    assert again['written']==0
    with patch.dict('os.environ',{'CHESSISM_CANONICAL_BLUNDERS':'1'}):
        async with factory() as s:
            summary=await readers.saved(s,'alice'); flags=await readers.game_flags(s,1)
        assert summary['source']=='canonical_cuda_blunders' and not summary['stale']
        assert summary['by_mode']['blitz']['games_count']==5,summary
        assert not flags['pending'] and flags['model_id']==model
    assert await connection.fetchval('SELECT min(blunder_count) FROM candidate.game_player_engine_summary')==7
    assert await connection.fetchval('SELECT max(blunder_count) FROM reads.game_player_engine_summary')<7
    # Invalid ratings produce unknown, not zero blunders, and update only that game.
    await connection.execute('UPDATE candidate.game SET white_elo=0 WHERE link=1')
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')==1
    result=await publish(await load()); assert result['written']==1
    assert await connection.fetchval('SELECT white_count IS NULL FROM candidate.canonical_blunder_game WHERE game_link=1')
    # A coverage update from a newly analyzed game creates a summary automatically.
    await connection.execute('UPDATE candidate.game_analysis_coverage SET is_fully_analyzed=true WHERE link=4')
    await publish(await load())
    # Unrelated profile changes do not queue games; eligibility changes do.
    await connection.execute("UPDATE candidate.player SET joined=joined WHERE player_name='alice'")
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')==0
    await connection.execute("UPDATE candidate.player SET deleted_at=now() WHERE player_name='bob'")
    assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')>0
    await publish(await load())
    assert await connection.fetchval("SELECT count(*) FROM candidate.canonical_blunder_game WHERE black_owner='bob'")==0
    # Immutable model records and reinstallable additive schema.
    try:await connection.execute("UPDATE candidate.canonical_blunder_model SET approval='provisional_user_approved'")
    except Exception as error:assert 'immutable' in str(error)
    else:raise AssertionError('Model mutation accepted')
    # Version change and rollback must keep one complete active model throughout.
    config2={**config,'fits':[{**f,'a':f['a']*.9} for f in config['fits']]}
    model2=registry.digest(config2)
    async with factory() as s,s.begin():
        await s.execute(text('''INSERT INTO candidate.canonical_blunder_model(id,source_run,config,evidence,approved_by,approval)
            VALUES(:id,'fixture2',CAST(:config AS jsonb),CAST(:evidence AS jsonb),'test','provisional_user_approved')'''),
            {'id':model2,'config':json.dumps(config2),'evidence':json.dumps(evidence)})
        await registry.select_target(s,model2)
    with patch.object(source,'MAX_GAMES',2):await publish(await load())
    assert await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control')==model
    await publish(await load())
    assert await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control')==model2
    async with factory() as s,s.begin():await registry.select_target(s,model)
    assert await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control')==model2
    await publish(await load())
    assert await connection.fetchval('SELECT active_model FROM candidate.canonical_blunder_control')==model
    assert await connection.fetchval('SELECT previous_model FROM candidate.canonical_blunder_control')==model2
    async with factory() as s,s.begin():await schema.install(s,layout)
    print('PASS: native extraction, unique games, source races, malformed output rollback, atomic activation, idempotent retries, cached histograms, compatible reads, incremental coverage/profile/rating changes, immutable models and version rollback')


async def main():
    import asyncpg
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from tests.check_db_refactor_candidate_postgres import command
    root=Path(tempfile.mkdtemp(prefix='canonical-blunders-',dir='/restore-test')); socket=root/'socket';socket.mkdir()
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'; connection=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute(FIXTURE+EXTRA)
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        layout=RelationalStorage('candidate','archive','reads','relational_v3')
        await check(async_sessionmaker(engine,info={'relational_storage_v2':layout}),connection,layout)
    finally:
        if engine:await engine.dispose()
        if connection:await connection.close()
        with suppress(Exception):await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__':asyncio.run(main())
