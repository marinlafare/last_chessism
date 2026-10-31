"""Synthetic PostgreSQL only. Run --network none with a disposable /restore-test."""
import asyncio
from contextlib import suppress
import io
import os
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock

os.environ['DATABASE_URL']='postgresql+asyncpg://test@127.0.0.1:1/postgres'

FIXTURE='''
CREATE TABLE player(id bigint,player_name text,joined bigint,deleted_at timestamptz);
INSERT INTO player VALUES(1,'alice',1,NULL),(2,'bob',1,NULL),(3,'unknown',0,NULL),(4,'removed',1,now());
CREATE TABLE game(link bigint,mode text,white_elo int,black_elo int,white_result float8,black_result float8,
 white_str_result text,black_str_result text,n_moves int,rules text,white_player_id bigint,black_player_id bigint);
INSERT INTO game SELECT i,'blitz',1500,1500,1,0,'win','resigned',11,'chess',1,2 FROM generate_series(1,11) i;
UPDATE game SET black_str_result='timeout' WHERE link=2;
UPDATE game SET black_elo=1800 WHERE link=3;
UPDATE game SET white_result=NULL WHERE link=5;
UPDATE game SET white_player_id=3,black_player_id=3 WHERE link=6;
UPDATE game SET white_player_id=4,black_player_id=4 WHERE link=7;
UPDATE game SET rules='chess960' WHERE link=11;
CREATE TABLE game_player AS SELECT g.link,p.player_name FROM game g JOIN player p ON p.id IN(g.white_player_id,g.black_player_id);
CREATE TABLE game_analysis_summary(link bigint,is_fully_analyzed bool,total_positions int);
INSERT INTO game_analysis_summary SELECT link,link<>4,22 FROM game;
CREATE TABLE fen(fen_id bigint PRIMARY KEY,score float8,analysis_source text);
INSERT INTO fen VALUES(1,100,'stockfish'),(2,-100,'stockfish'),(3,9998,'stockfish'),(4,100,'unknown'),(5,100,'tablebase');
CREATE TABLE game_fen_association(game_link bigint,n_move int,move_color text,fen_id bigint);
INSERT INTO game_fen_association SELECT link,(ply+1)/2,CASE WHEN ply%2=1 THEN 'white' ELSE 'black' END,
 CASE WHEN link=8 THEN 3 WHEN link=9 THEN 4 WHEN link=10 THEN 5 WHEN ply=22 THEN 2 ELSE 1 END
 FROM game CROSS JOIN generate_series(1,22) ply;
CREATE TABLE cloud_analysis_job(status text);
CREATE SCHEMA candidate;
CREATE TABLE candidate.player AS TABLE player;
CREATE TABLE candidate.game_mode(id int,name text);
INSERT INTO candidate.game_mode VALUES(1,'blitz');
CREATE TABLE candidate.game AS SELECT *,1 AS mode_id FROM game WHERE rules='chess';
ALTER TABLE candidate.game DROP COLUMN rules,DROP COLUMN mode;
CREATE TABLE candidate.game_analysis_coverage AS TABLE game_analysis_summary;
CREATE TABLE candidate.fen AS TABLE fen;
CREATE TABLE candidate.game_fen_association AS SELECT game_link,n_move,move_color='white' AS is_white,fen_id FROM game_fen_association;
'''


async def main():
    import asyncpg
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from chessism_api.operations.research_algorithms.blunder_training import freeze_source,configuration
    from chessism_api.operations.research_algorithms.blunder_model import TRANSITION
    from tests.check_db_refactor_candidate_postgres import command
    root=Path(tempfile.mkdtemp(prefix='blunder-model-',dir='/restore-test'))
    socket=root/'socket'; socket.mkdir()
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'
    connection=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute(FIXTURE)
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        outputs=[]
        for layout in (None,RelationalStorage('candidate','archive','reads','relational_v3')):
            factory=async_sessionmaker(engine,info={'relational_storage_v2':layout})
            stream=io.BytesIO()
            dataset,source,_=await freeze_source(configuration(100),AsyncMock(),stream,session_factory=factory)
            rows=np.frombuffer(stream.getvalue(),dtype=TRANSITION)
            assert source['selected_games']==4 and source['positions']==88,source
            assert source['fitting_games']==1 and source['games_without_eligible_cp']==3,source
            assert source['fitting_positions']==2 and dataset.hist[:,:,:,0].sum()==1
            assert set(rows['game'])=={1,8,9,10}
            game=rows[rows['game']==1]
            assert game['before'][0]==15 and game['before'][1]==-100
            assert game['after'][-1]==100 and len(set(game['split']))==1
            outputs.append(stream.getvalue())
        assert outputs[0]==outputs[1], 'Native and legacy transition semantics differ'
        await connection.execute("INSERT INTO cloud_analysis_job VALUES('running')")
        try:
            await freeze_source(configuration(100),AsyncMock(),io.BytesIO(),session_factory=async_sessionmaker(engine))
        except ValueError as error:
            assert 'active cloud' in str(error)
        else: raise AssertionError('Active cloud work was not protected')
        await connection.execute('DELETE FROM cloud_analysis_job; UPDATE game_analysis_summary SET total_positions=21 WHERE link=1')
        try:
            await freeze_source(configuration(100),AsyncMock(),io.BytesIO(),session_factory=async_sessionmaker(engine))
        except ValueError as error:
            assert 'coverage' in str(error)
        else: raise AssertionError('Corrupt coverage accepted')
        print('PASS: bounded legacy/native selection, eligible profiles/results/ratings/rules, shared games deduplicated, opening/mate/source/tablebase exclusions, exact prior ply, game splits, coverage checks and active-cloud guard')
    finally:
        if engine: await engine.dispose()
        if connection: await connection.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__': asyncio.run(main())
