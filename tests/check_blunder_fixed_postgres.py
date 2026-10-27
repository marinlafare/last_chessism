"""Disposable PostgreSQL: real production summary parity and source guards."""
import asyncio
from contextlib import suppress
import io
import os
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock, patch

os.environ['DATABASE_URL'] = 'postgresql+asyncpg://test@127.0.0.1:1/postgres'

FIXTURE = '''
CREATE TABLE player(player_name text PRIMARY KEY,joined bigint,deleted_at timestamptz);
INSERT INTO player VALUES('alice',1,NULL),('bob',1,NULL),('unknown',0,NULL);
CREATE TABLE game(link bigint PRIMARY KEY,white text,black text,mode text,played_at timestamptz,
 n_moves int,white_elo int,black_elo int,white_result float8,black_result float8,
 white_str_result text,black_str_result text);
INSERT INTO game SELECT i,CASE WHEN i=2 THEN 'bob' ELSE 'alice' END,
 CASE WHEN i=2 THEN 'alice' ELSE 'bob' END,
 CASE i WHEN 1 THEN 'bullet' WHEN 2 THEN 'blitz' ELSE 'rapid' END,
 '2025-01-01'::timestamptz,4,1500,1500,1,0,'win','resigned' FROM generate_series(1,4) i;
CREATE TABLE game_player AS SELECT g.link,side.color,
 CASE WHEN side.color='white' THEN g.white ELSE g.black END AS player_name,
 CASE WHEN side.color='white' THEN 1.0 ELSE 0.0 END AS result,
 1500 AS rating,1500 AS opponent_rating,g.mode,g.played_at,g.n_moves
 FROM game g CROSS JOIN (VALUES('white'),('black')) side(color);
CREATE TABLE game_analysis_summary(link bigint,total_positions int,is_fully_analyzed bool);
INSERT INTO game_analysis_summary SELECT link,8,link<>4 FROM game;
CREATE TABLE fen(fen_id bigint PRIMARY KEY,score float8,analysis_source text);
INSERT INTO fen SELECT i,score,CASE WHEN i=7 THEN 'tablebase' WHEN i=1 THEN 'unknown' ELSE 'stockfish' END
 FROM (VALUES (1,100),(2,-100),(3,9998),(4,700),(5,-9998),(6,-700),(7,100),(8,0)) v(i,score);
CREATE TABLE game_fen_association(game_link bigint,n_move int,move_color text,fen_id bigint);
INSERT INTO game_fen_association SELECT link,(i+1)/2,CASE WHEN i%2=1 THEN 'white' ELSE 'black' END,i
 FROM game CROSS JOIN generate_series(1,8) i;
CREATE TABLE game_player_engine_summary(game_link bigint,player_name text,player_color text,
 analyzed_player_moves int,own_move_cp_gain float8,own_move_cp_loss float8,game_efficiency float8,
 mean_win_percent_loss float8,median_win_percent_loss float8,blunder_count int,
 mate_for_positions int,mate_against_positions int,final_player_cp float8,result text,end_by text,
 PRIMARY KEY(game_link,player_color));
CREATE TABLE cloud_analysis_job(status text);
'''


async def main():
    import asyncpg
    import numpy as np
    from sqlalchemy import text
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
    from chessism_api.operations import player_game_scores as production
    from chessism_api.operations.matrix_constructor.queries import matrix_sql
    from chessism_api.operations.research_algorithms import blunder_fixed as model, blunder_validation_job as job
    from tests.check_db_refactor_candidate_postgres import command
    root = Path(tempfile.mkdtemp(prefix='fixed-blunders-',dir='/restore-test'))
    socket=root/'socket';socket.mkdir()
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'
    connection=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute(FIXTURE)
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        factory=async_sessionmaker(engine)
        # Run the real production writer ONLY inside this synthetic database.
        with patch.object(production,'AsyncDBSession',factory):
            result=await production.refresh_game_player_engine_summaries((1,2,3,4))
        assert result=={'game_players':6,'games':3},result
        saved_before=await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
        async def freeze(reporter=None):
            stream=io.BytesIO()
            games,source,timings=await job.freeze_source(job.configuration('alice'),reporter or AsyncMock(),stream,session_factory=factory)
            return np.frombuffer(stream.getvalue(),dtype=model.DTYPE),games,source,timings
        rows,games,source,_=await freeze()
        assert source['source_positions']==24 and source['games']==3 and len(rows)==12,source
        expected=model.reference(rows)
        np.testing.assert_array_equal(expected==1, rows['sql']==1)
        counts=np.bincount(rows['game'][expected==1],minlength=len(games))
        assert counts.tolist()==[g['saved_blunders'] for g in games],(counts,games)
        # Verify the new default matrix really describes the optimized stream.
        _,query,params=matrix_sql(job.configuration('alice')['matrix'])
        async with factory() as session:
            matrix=(await session.execute(text(query),params)).mappings().all()
        kinds={'cp':0,'mate':1,'tablebase':2}
        lookup={(int(r['game']),int(r['row_key'].split(':')[2])*2-(r['row_key'].endswith(':white'))):r for r in matrix}
        for row in rows:
            original=lookup[int(row['link']),int(row['ply'])]
            assert (original['previous_player_score'],original['player_score'],kinds[original['previous_score_kind']],kinds[original['score_kind']]) == (row['before'],row['after'],row['before_kind'],row['after_kind'])
        # Concurrent updates cannot leak between source extraction and references.
        async def mutate(phase,*args,**kwargs):
            if phase=='selecting_games': await connection.execute('UPDATE fen SET score=-500 WHERE fen_id=1')
        frozen,_,_,_=await freeze(AsyncMock(progress=AsyncMock(side_effect=mutate)))
        np.testing.assert_array_equal(frozen,rows)
        await connection.execute('UPDATE fen SET score=100 WHERE fen_id=1')
        await connection.execute('''CREATE SCHEMA native;
            CREATE TABLE native.player AS SELECT row_number() OVER(ORDER BY player_name)::bigint AS id,* FROM player;
            CREATE TABLE native.game_mode AS SELECT row_number() OVER(ORDER BY mode)::int AS id,mode AS name FROM game GROUP BY mode;
            CREATE TABLE native.game AS SELECT g.link,m.id AS mode_id FROM game g JOIN native.game_mode m ON m.name=g.mode;
            CREATE TABLE native.game_player AS SELECT row_number() OVER(ORDER BY gp.link,gp.color)::bigint AS id,
                gp.link AS game_link,p.id AS player_id,gp.color='white' AS is_white
                FROM game_player gp JOIN native.player p USING(player_name);
            CREATE TABLE native.game_analysis_coverage AS TABLE game_analysis_summary;
            CREATE TABLE native.game_player_engine_summary AS SELECT gp.id AS game_player_id,e.blunder_count,e.analyzed_player_moves
                FROM game_player_engine_summary e JOIN native.game_player gp ON gp.game_link=e.game_link AND gp.is_white=(e.player_color='white');
            CREATE TABLE native.fen AS TABLE fen;
            CREATE TABLE native.game_fen_association AS SELECT game_link,n_move,move_color='white' AS is_white,fen_id FROM game_fen_association;''')
        original_factory=factory
        factory=async_sessionmaker(engine,info={SESSION_KEY:RelationalStorage('native','archive','reads','relational_v3')})
        native_rows,_,_,_=await freeze()
        np.testing.assert_array_equal(native_rows,rows)
        factory=original_factory
        assert saved_before==await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
        for name,limit in [('MAX_ROWS',10),('MAX_GAMES',2)]:
            with patch.object(job,name,limit):
                try: await freeze()
                except ValueError: pass
                else: raise AssertionError('Whole-player cap silently truncated the source')
        for setup,cleanup,message in [
            ("INSERT INTO cloud_analysis_job VALUES('running')",'DELETE FROM cloud_analysis_job','active cloud'),
            ("UPDATE player SET joined=0 WHERE player_name='alice'","UPDATE player SET joined=1 WHERE player_name='alice'",'complete player'),
            ('UPDATE game_player_engine_summary SET blunder_count=NULL WHERE game_link=1 AND player_color=\'white\'',
             f"UPDATE game_player_engine_summary SET blunder_count={games[0]['saved_blunders']} WHERE game_link=1 AND player_color='white'",'missing'),
            ('UPDATE fen SET score=NULL WHERE fen_id=1','UPDATE fen SET score=100 WHERE fen_id=1','invalid scored ply'),
            ('UPDATE game_analysis_summary SET total_positions=9 WHERE link=1','UPDATE game_analysis_summary SET total_positions=8 WHERE link=1','Coverage'),
        ]:
            await connection.execute(setup)
            try: await freeze()
            except ValueError as error: assert message in str(error),error
            else: raise AssertionError('Unsafe source accepted: '+message)
            await connection.execute(cleanup)
        print('PASS: actual production summary/SQL/scalar parity; default matrix; native/legacy; both colors; mates/tablebases/legacy CP; repeatable snapshot; no source writes; cloud/profile/coverage/missing-score guards')
        from tests.shadow_postgres_fixture import verify
        await verify(connection, engine)
        from tests.cohort_postgres_fixture import verify as verify_cohort
        await verify_cohort(connection, engine)
        from tests.strength_postgres_fixture import verify as verify_strength
        await verify_strength(connection, engine)
    finally:
        if engine: await engine.dispose()
        if connection: await connection.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__': asyncio.run(main())
