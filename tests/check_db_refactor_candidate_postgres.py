"""Actual PostgreSQL fixture rehearsal. Run ONLY in a disposable --network none container."""
import asyncio
import os
from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'
from scripts.db_refactor_candidate import C,A,R,V,prepare_reference,build_candidate
from scripts.db_refactor_tuning import apply_tuning,tuned_candidate_case,verify_original_mismatches
from scripts.db_refactor_workload import make_cases,row_digest
from scripts.db_refactor_comparison import candidate_case

FIXTURE = '''
CREATE TABLE player(player_name varchar PRIMARY KEY,joined bigint,title varchar);
INSERT INTO player VALUES ('alice',100,'GM'),('bob',200,NULL),('carol',0,NULL);
CREATE TABLE game(link bigint PRIMARY KEY,white varchar,black varchar,mode varchar(16),
 year integer,month integer,day integer,hour integer,minute integer,second integer,
 white_elo integer,black_elo integer,white_result float8,black_result float8,
 white_str_result varchar,black_str_result varchar,time_control varchar,
 played_at timestamptz,avg_elo float8,eco varchar,time_elapsed integer,n_moves integer,
 fens_done boolean,fens_processing boolean,rules varchar(32),initial_setup varchar(128));
INSERT INTO game VALUES
 (1,'alice','bob','blitz',2025,1,1,1,1,1,2500,2300,1,0,'win','checkmated','180',
 '2025-01-01T01:01:01Z',2400,'https://www.chess.com/openings/Test',12,2,true,false,'chess',NULL),
 (2,'bob','alice','rapid',2025,1,2,1,1,1,2300,2500,0.5,0.5,'agreed','agreed','600',
 '2025-01-02T01:01:01Z',2400,'no_eco',20,2,true,false,'chess',
 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'),
 (3,'alice','carol','blitz',2025,1,3,1,1,1,2500,1900,1,0,'win','resigned','180',
 '2025-01-03T01:01:01Z',2200,'no_eco',10,1,true,false,'chess960','custom-board'),
 (4,'bob','carol','blitz',2025,1,4,1,1,1,2300,1900,1,0,'win','resigned','180',
 '2025-01-04T01:01:01Z',2100,'no_eco',10,1,true,false,'chess','custom-board');
CREATE TABLE moves(id serial PRIMARY KEY,link bigint,n_move integer,white_move varchar,black_move varchar,
 white_time_left float8,black_time_left float8,white_reaction_time float8,black_reaction_time float8);
INSERT INTO moves(link,n_move,white_move,black_move,white_time_left,black_time_left,white_reaction_time,black_reaction_time)
 SELECT link,1,'e4','e5',100,100,1,1 FROM game;
CREATE TABLE fen(fen varchar PRIMARY KEY,fen_id bigint UNIQUE,score float8,next_moves varchar);
INSERT INTO fen VALUES('position-a',10,25,'e2e4'),('position-b',11,-10,'e7e5');
CREATE TABLE game_fen_association(id serial PRIMARY KEY,game_link bigint,fen_fen varchar,n_move integer,move_color varchar(5));
INSERT INTO game_fen_association(game_link,fen_fen,n_move,move_color)
 VALUES(1,'position-a',1,'white'),(1,'position-b',1,'black'),(1,'position-a',2,'white'),(2,'position-a',1,'white');
SELECT setval(pg_get_serial_sequence('game_fen_association','id'),50,true);
CREATE TABLE game_player AS SELECT g.link,side.color,
 CASE WHEN side.color='white' THEN g.white ELSE g.black END AS player_name,
 CASE WHEN side.color='white' THEN g.black ELSE g.white END AS opponent_name,
 CASE WHEN side.color='white' THEN g.white_result ELSE g.black_result END AS result,
 CASE WHEN side.color='white' THEN g.white_elo ELSE g.black_elo END AS rating,
 CASE WHEN side.color='white' THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
 g.mode,g.played_at,g.eco,g.n_moves,g.time_elapsed::float8 AS time_elapsed,g.avg_elo
 FROM game g CROSS JOIN (VALUES('white'::varchar(5)),('black'::varchar(5))) side(color);
CREATE TABLE player_position_frequency(player_name varchar,player_color varchar(5),fen_fen varchar,
 games_with_position bigint,total_occurrences bigint);
INSERT INTO player_position_frequency VALUES('alice','white','position-a',2,3);
-- More than a page of tied frequencies, with IDs opposite lexical FEN order.
INSERT INTO fen SELECT 'tie-'||lpad(i::text,3,'0'),1000-i,NULL,NULL FROM generate_series(1,151) i;
INSERT INTO player_position_frequency
 SELECT 'alice','white',fen,2,2 FROM fen WHERE fen LIKE 'tie-%';
CREATE TABLE game_player_salience(game_link bigint,player_color varchar(5),player_name varchar,
 salience float8,weighted_numerator float8,depth_weight_sum float8,position_occurrence_count integer,
 unique_position_count integer,repeated_position_count integer);
INSERT INTO game_player_salience VALUES(1,'white','alice',0.5,1,2,3,2,1),(1,'black','bob',0.75,1.5,2,3,2,1);
CREATE TABLE game_player_engine_summary(game_link bigint,player_color varchar(5),player_name varchar,
 analyzed_player_moves integer,own_move_cp_loss float8,result varchar,end_by varchar);
INSERT INTO game_player_engine_summary VALUES(1,'white','alice',2,0,'win','checkmate');
CREATE TABLE player_salience_summary(player_name varchar,status varchar,source_game_count bigint,
 source_position_count bigint,effective_game_count float8,error varchar);
INSERT INTO player_salience_summary VALUES('alice','ready',2,4,1.5,NULL),('bob','stale',2,4,1.5,NULL);
CREATE TABLE player_salience_pending_game(player_name varchar,game_link bigint,player_color varchar(5));
INSERT INTO player_salience_pending_game VALUES('alice',2,'black');
CREATE TABLE game_opening(link bigint,n_moves integer,opening varchar,mode varchar(16),avg_elo float8,played_at timestamptz);
INSERT INTO game_opening SELECT link,1,'e4 e5',mode,avg_elo,played_at FROM game;
CREATE TABLE game_analysis_summary(link bigint,total_positions integer,analyzed_positions integer,
 unscored_positions integer,is_fully_analyzed boolean,score_sum float8,abs_score_sum float8,
 avg_score float8,avg_abs_score float8,max_abs_score float8,equal_positions integer,small_positions integer,
 clear_positions integer,decisive_positions integer,mate_positions integer,refreshed_at timestamptz);
INSERT INTO game_analysis_summary VALUES(1,3,3,0,true,40,60,13.3,20,25,3,0,0,0,0,'2025-01-01Z');
'''


async def command(*args):
    process=await asyncio.create_subprocess_exec(*args,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
    output,_=await process.communicate()
    if process.returncode:
        raise RuntimeError(output.decode()[-2000:])


async def main():
    import asyncpg
    base=Path('/restore-test/db-refactor-baseline-fixture')
    base.mkdir(mode=0o700)
    pgdata,socket=base/'pgdata',base/'socket'
    socket.mkdir(mode=0o700)
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'
    await command('/usr/lib/postgresql/15/bin/initdb','-D',str(pgdata),'-U','chessism_user','--auth-local=trust','--auth-host=reject')
    conn=None
    try:
        await command(pgctl,'-D',str(pgdata),'-l',str(base/'postgres.log'),'-w','-o',
            f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await conn.execute(FIXTURE)
        await conn.execute('''ALTER TABLE fen ALTER COLUMN fen_id SET NOT NULL;
            ALTER TABLE fen ALTER COLUMN fen_id ADD GENERATED BY DEFAULT AS IDENTITY (START WITH 2000);
            ALTER TABLE fen ADD COLUMN n_games integer NOT NULL DEFAULT 0,
                ADD COLUMN moves_counter varchar NOT NULL DEFAULT '',ADD COLUMN piece_count integer,
                ADD COLUMN wdl_win float8, ADD COLUMN wdl_draw float8,ADD COLUMN wdl_loss float8,
                ADD COLUMN analysis_source varchar,ADD COLUMN tablebase_wdl integer,
                ADD COLUMN tablebase_dtz integer,ADD COLUMN analyzed_at timestamptz;
            CREATE TABLE fen_continuation(id serial PRIMARY KEY,fen_id bigint REFERENCES fen(fen_id),
                rank integer,move varchar,score float8,UNIQUE(fen_id,rank));''')
        scope=await prepare_reference(conn,pgdata)
        assert scope=={'standard_games':2,'archived_games':2},scope
        # A failed stage must roll back every candidate/archive object while
        # leaving the old dataset and the scoped reference usable.
        async def interrupt(phase,**detail):
            if phase=='building_compact_game_players':
                raise RuntimeError('fixture interruption')
        try:
            await build_candidate(conn,pgdata,interrupt)
        except RuntimeError as error:
            assert str(error)=='fixture interruption'
        else:
            raise AssertionError('Expected injected interruption')
        for namespace in (C,A,V):
            assert not await conn.fetchval('SELECT to_regnamespace($1) IS NOT NULL',namespace)
        assert await conn.fetchval('SELECT count(*) FROM public.game')==4

        # Nonstandard games with FEN contributions need a different cache plan;
        # do not silently retain invalid frequencies in that case.
        tx=conn.transaction()
        await tx.start()
        try:
            await conn.execute("INSERT INTO game_fen_association VALUES(49,3,'position-a',1,'white')")
            try:
                await build_candidate(conn,pgdata,interrupt)
            except RuntimeError as error:
                assert 'cache recalculation' in str(error)
            else:
                raise AssertionError('Expected archived-FEN preflight refusal')
        finally:
            await tx.rollback()
        phases=[]
        async def publish(phase,**detail):
            phases.append(phase)
        result=await build_candidate(conn,pgdata,publish)
        assert result['game_fen_association']['rows']==4
        assert all(r['mismatches']==0 for r in result.values())
        assert await conn.fetchval(f'SELECT count(*) FROM {A}.moves')==2
        assert await conn.fetchval('SELECT count(*) FROM public.game')==4
        assert await conn.fetchval(f"SELECT eco FROM {C}.game WHERE link=1")=='Test'
        assert await conn.fetchval(f"SELECT eco FROM {V}.game WHERE link=1")=='https://www.chess.com/openings/Test'
        assert await conn.fetchval(f'SELECT count(*) FROM {C}.opening_sequence')==1
        assert await conn.fetchval(f"SELECT nextval(pg_get_serial_sequence('{C}.game_fen_association','id'))")==51
        assert await conn.fetchval("SELECT last_value FROM public.game_fen_association_id_seq")==50
        assert not await conn.fetchval('''SELECT EXISTS(SELECT 1 FROM information_schema.columns
            WHERE table_schema=$1 AND table_name='game_analysis_coverage' AND column_name='score_sum')''',C)
        # The measured query adapter uses actual numeric joins, not a hidden
        # round-trip through a compatibility text-FEN join.
        await conn.execute(f'SET search_path={V},public')
        rows=await conn.fetch('''SELECT a.n_move,a.move_color,f.fen,f.score
            FROM game_fen_association a JOIN fen f ON f.fen_id=a.fen_id
            WHERE a.game_link=1 ORDER BY a.id''')
        assert len(rows)==3 and rows[0]['fen']=='position-a'
        # Native queries must preserve old values, types, order and LIMIT ties.
        inputs={'players':[{'player_name':name,'cohort':name,'mode':'blitz','year':2025,'link':1}
                           for name in ('alice','bob','carol','missing')],
                'opening':{'mode':'blitz','n_moves':1,'opening':'e4 e5'}}
        cases=[c for c in make_cases(inputs,Path('/app'))
               if not c['name'].endswith(('engine_insights_sql','playing_patterns_sql'))]
        await conn.execute(f'SET search_path={R},public')
        before={c['name']:row_digest(await conn.fetch(c['sql'],*c['args'])) for c in cases}
        original=next(c for c in cases if c['name']=='alice/recent_games')
        v1=candidate_case(original)
        await conn.execute(f'SET search_path={V},public')
        v1_hash=row_digest(await conn.fetch(v1['sql'],*v1['args']))
        assert v1_hash!=before[original['name']],'Fixture must reproduce the actual int/float difference'
        prior={'manifest':{'reference_cases':[original],'candidate_cases':[v1]},
               'reference':{'results':[{'name':original['name'],'result_sha256':before[original['name']]}]},
               'candidate':{'results':[{'name':original['name'],'result_sha256':v1_hash}]},
               'comparison':{'comparisons':[{'name':original['name'],'results_match':False}]}}
        explained=await verify_original_mismatches(conn,prior)
        assert explained[0]['type_changes']==2 and explained[0]['unexplained_differences']==0
        await apply_tuning(conn,pgdata,publish)
        await conn.execute(f'SET search_path={V},public')
        for case in cases:
            new=tuned_candidate_case(case)
            rows=await conn.fetch(new['sql'],*new['args'])
            assert row_digest(rows)==before[case['name']],case['name']
        # Index-only tuning is repeat-safe and refuses another database path.
        await apply_tuning(conn,pgdata,publish)
        try:
            await apply_tuning(conn,Path('/var/lib/postgresql/data'),publish)
        except RuntimeError:
            pass
        else:
            raise AssertionError('Tuning must reject the production path')
        try:
            await prepare_reference(conn,pgdata)
        except RuntimeError:
            pass
        else:
            raise AssertionError('Must not overwrite existing candidate namespaces')
        print(f'PostgreSQL candidate fixture passed: {len(result)} exact comparisons; {len(cases)} tuned query comparisons; float diagnosis and frequency boundary ties checked.',flush=True)
        from scripts.db_refactor_write_checks import run_checks
        writes=await run_checks(socket,pgdata,database='postgres')
        assert all(row['passed'] for row in writes['checks'])
        print(f"Normalized write fixture passed: {len(writes['checks'])} checks; probe writes rolled back.",flush=True)
        from scripts.run_db_refactor_write_validation import cloud_import_checks
        from chessism_api.operations import database_restore_tests as restore
        try:
            cloud=await cloud_import_checks(conn,socket,base,restore)
        except Exception:
            print((base/'cloud-import-tests.log').read_text()[-4000:],flush=True)
            raise
        assert cloud['passed']==4
        print('Four real cloud-import transaction/retry tests passed in an empty private database.',flush=True)
    finally:
        if conn is not None:
            await conn.close()
        await command(pgctl,'-D',str(pgdata),'-m','fast','-w','stop')

    # Same backup engine as the app, with a fresh repo and a different PGDATA.
    from scripts.db_refactor_roundtrip import OfflineRoundTrip
    token=uuid4().hex[:12]
    restored=Path('/restore-test')/('db-refactor-baseline-verify-'+token)/'pgdata'
    work=Path('/restore-test')/('db-refactor-acceptance-'+token)
    state={}
    async def backup_publish(phase,**detail):
        state.update(phase=phase,**detail)
    roundtrip=OfflineRoundTrip(pgdata,socket,work,restored,backup_publish,state)
    roundtrip.prepare()
    try:
        await roundtrip.run()
    except Exception:
        for log in sorted(work.glob('*.log')):
            print(log.name,log.read_text()[-2500:],flush=True)
        raise
    restored_socket=restored.parent/'socket'
    try:
        await command(pgctl,'-D',str(restored),'-l',str(restored.parent/'postgres.log'),'-w','-o',
            f"-c listen_addresses='' -c port=55432 -c archive_mode=off -c unix_socket_directories='{restored_socket}'",'start')
        result=await run_checks(restored_socket,restored,database='postgres')
        assert len(result['checks'])==7
        print('Private pgBackRest backup, verify, restore and restored write checks passed.',flush=True)
    finally:
        await command(pgctl,'-D',str(restored),'-m','fast','-w','stop')


if __name__=='__main__':
    asyncio.run(main())
