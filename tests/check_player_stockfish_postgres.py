"""Synthetic PostgreSQL tests. Run in a disposable --network none container only."""
import asyncio
from contextlib import suppress
from datetime import date
import os
from pathlib import Path
import tempfile
from unittest.mock import patch
import time

os.environ['DATABASE_URL'] = 'postgresql+asyncpg://chessism_user@127.0.0.1:1/postgres'

FIXTURE = '''
CREATE TABLE player(id int PRIMARY KEY,player_name text UNIQUE,joined bigint,deleted_at timestamptz);
INSERT INTO player VALUES(1,'alice',1,NULL),(2,'bob',1,NULL),(3,'empty',1,NULL),(4,'unknown',0,NULL);
CREATE TABLE game(link bigint PRIMARY KEY,white text,black text,mode text,played_at timestamptz,
 n_moves int,white_result float8,black_result float8,white_str_result text,black_str_result text);
INSERT INTO game VALUES(1,'alice','bob','blitz','2025-01-01T01:00Z',20,1,0,'win','checkmated'),
 (2,'bob','alice','rapid','2025-01-02T01:00Z',2,.5,.5,'agreed','agreed'),
 (3,'alice','bob','bullet',NULL,1,.5,.5,'stalemate','stalemate');
CREATE TABLE game_player AS SELECT link,side AS color,mode,played_at,
 CASE WHEN side='white' THEN white ELSE black END AS player_name FROM game
 CROSS JOIN (VALUES('white'),('black')) s(side);
CREATE TABLE moves(link bigint REFERENCES game ON DELETE CASCADE,n_move int,white_move text,black_move text);
INSERT INTO moves VALUES(1,20,'a4','a5'),(2,2,'a4','a5'),(3,1,'a4','--');
CREATE TABLE fen(fen_id bigint PRIMARY KEY,score float8,analysis_source text,analyzed_at timestamptz DEFAULT now());
INSERT INTO fen(fen_id,score,analysis_source) VALUES
 (1,-551,'stockfish'),(2,-550,'stockfish'),(3,-150,'stockfish'),(4,-50,'stockfish'),(5,0,'stockfish'),
 (6,50,'stockfish'),(7,150,'stockfish'),(8,550,'stockfish'),(9,551,'stockfish'),(10,NULL,NULL),
 (11,9998,'stockfish'),(12,-9996,'stockfish_backup'),(13,10000,'stockfish'),(14,9001,NULL),
 (15,9998,'tablebase'),(16,-20000,'stockfish');
CREATE TABLE game_fen_association(id serial PRIMARY KEY,game_link bigint REFERENCES game ON DELETE CASCADE,
 fen_id bigint REFERENCES fen);
INSERT INTO game_fen_association(game_link,fen_id) SELECT 1,fen_id FROM fen;
INSERT INTO game_fen_association(game_link,fen_id) VALUES(1,11),(2,11),(2,5),(3,5);
CREATE TABLE game_analysis_summary(link bigint PRIMARY KEY REFERENCES game ON DELETE CASCADE,
 is_fully_analyzed boolean,total_positions int);
INSERT INTO game_analysis_summary VALUES(1,false,17),(2,true,2),(3,true,1);
CREATE TABLE database_summary(id int,n_games_in_db bigint,n_positions bigint,analyzed_fens bigint,
 unscored_fens bigint,nonzero_scored_fens bigint);
INSERT INTO database_summary VALUES(1,3,16,15,1,14);
CREATE TABLE cloud_analysis_job(status text);
'''


async def main():
    import asyncpg
    from sqlalchemy import text
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import sessionmaker
    from tests.check_db_refactor_candidate_postgres import command
    from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
    from chessism_api.database.stockfish_summary_schema import install_stockfish_summaries, summary_table
    from chessism_api.operations import player_stockfish_summary as summaries, player_score_distribution as scores
    from chessism_api.operations import player_stockfish_worker as worker

    root = Path(tempfile.mkdtemp(prefix='stockfish-summary-', dir='/restore-test'))
    socket = root / 'socket'; socket.mkdir()
    pgctl = '/usr/lib/postgresql/15/bin/pg_ctl'
    conn, engines = None, []
    try:
        await command('/usr/lib/postgresql/15/bin/initdb', '-D', str(root / 'pgdata'), '-U', 'chessism_user', '--auth-local=trust', '--auth-host=reject')
        await command(pgctl, '-D', str(root / 'pgdata'), '-l', str(root / 'postgres.log'), '-w', '-o',
                      f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'", 'start')
        conn = await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await conn.execute(FIXTURE)
        await conn.execute('''CREATE SCHEMA candidate; CREATE SCHEMA reads; CREATE SCHEMA archive;
          CREATE TABLE candidate.player(LIKE public.player INCLUDING ALL); INSERT INTO candidate.player SELECT * FROM public.player;
          CREATE TABLE candidate.game_mode(id int PRIMARY KEY,name text);
          INSERT INTO candidate.game_mode VALUES(1,'bullet'),(2,'blitz'),(3,'rapid');
          CREATE TABLE candidate.game AS SELECT g.link,w.id AS white_player_id,b.id AS black_player_id,m.id AS mode_id,
            g.played_at,g.n_moves,g.white_result,g.black_result,g.white_str_result,g.black_str_result
            FROM public.game g JOIN candidate.player w ON w.player_name=g.white JOIN candidate.player b ON b.player_name=g.black
            JOIN candidate.game_mode m ON m.name=g.mode;
          ALTER TABLE candidate.game ADD PRIMARY KEY(link);
          CREATE TABLE candidate.game_player AS SELECT gp.link AS game_link,p.id AS player_id,gp.color='white' AS is_white
            FROM public.game_player gp JOIN candidate.player p USING(player_name);
          ALTER TABLE candidate.game_player ADD FOREIGN KEY(game_link) REFERENCES candidate.game ON DELETE CASCADE;
          CREATE TABLE candidate.fen(LIKE public.fen INCLUDING ALL); INSERT INTO candidate.fen SELECT * FROM public.fen;
          CREATE TABLE candidate.game_fen_association AS SELECT * FROM public.game_fen_association;
          ALTER TABLE candidate.game_fen_association ADD FOREIGN KEY(game_link) REFERENCES candidate.game ON DELETE CASCADE;
          CREATE TABLE candidate.moves AS SELECT * FROM public.moves;
          ALTER TABLE candidate.moves ADD FOREIGN KEY(link) REFERENCES candidate.game ON DELETE CASCADE;
          CREATE TABLE candidate.game_analysis_coverage AS SELECT * FROM public.game_analysis_summary;
          ALTER TABLE candidate.game_analysis_coverage ADD PRIMARY KEY(link),ADD FOREIGN KEY(link) REFERENCES candidate.game ON DELETE CASCADE;''')
        class Redis:
            async def get(self, key): return None
            async def set(self, *args, **kwargs): pass

        checks = 0
        for normalized in (False, True):
            layout = RelationalStorage('candidate','archive','reads','relational_v3') if normalized else None
            engine = create_async_engine(URL.create('postgresql+asyncpg',username='chessism_user',database='postgres',port=55432,query={'host':str(socket)}),
                connect_args={'server_settings':{'search_path':layout.search_path if layout else 'public'}})
            engines.append(engine)
            factory = sessionmaker(engine,class_=AsyncSession,expire_on_commit=False,info={SESSION_KEY:layout} if layout else {})
            table = lambda name: summary_table(layout,name)
            src = lambda name: layout.table(name) if layout else 'public.'+name
            coverage = src('game_analysis_coverage' if layout else 'game_analysis_summary')
            async with engine.begin() as cx:
                await install_stockfish_summaries(cx,layout)
                await install_stockfish_summaries(cx,layout)  # repeat-safe, no state reset
            async def finish(player):
                for _ in range(20):
                    result = await summaries.refresh_player(player,batch_days=1,session_factory=factory)
                    if result['status'] != 'building': return result
                raise AssertionError(result)
            async def sql(query):
                async with factory.begin() as s: return await s.execute(text(query))
            async def revision(player):
                return (await sql(f"SELECT revision FROM {table('state')} WHERE player_name='{player}'")).scalar()
            with patch.object(summaries,'AsyncDBSession',factory), patch.object(scores,'AsyncDBSession',factory), \
                 patch.object(worker,'AsyncDBSession',factory), patch.dict(os.environ,{'CHESSISM_STOCKFISH_SUMMARIES':'1'}):
                assert not await worker.cloud_busy()
                await sql("INSERT INTO public.cloud_analysis_job VALUES('running')")
                assert await worker.cloud_busy()
                await sql("DELETE FROM public.cloud_analysis_job")
                assert (await scores.get_distribution('alice',Redis()))['summary_pending']
                readiness=await summaries.get_readiness('alice')
                assert not readiness['summary_pending'] and readiness['by_mode']['rapid']['coverage']['eligible_games']==1
                assert await revision('unknown') is None
                for player in ('alice','bob','empty'):
                    assert (await finish(player))['status']=='ready'
                    expected = await scores.calculate_distribution(player,scores.MODES,Redis())
                    actual = await scores.get_distribution(player,Redis())
                    for mode in scores.MODES:
                        assert {k:actual['by_mode'][mode][k] for k in expected['by_mode'][mode]} == expected['by_mode'][mode]
                        checks += 1
                alice = await scores.get_distribution('alice',Redis())
                assert {'moves':2,'side':'for','positions':2} in alice['by_mode']['blitz']['mates']
                assert {'moves':4,'side':'against','positions':1} in alice['by_mode']['blitz']['mates']
                assert {'moves':None,'side':'for','positions':1} in alice['by_mode']['blitz']['mates']
                assert {'moves':None,'side':'against','positions':1} in alice['by_mode']['blitz']['mates']
                assert {'moves':2,'side':'against','positions':1} in alice['by_mode']['rapid']['mates']
                assert (await summaries.get_readiness('alice'))['by_mode']['rapid']['coverage']['eligible_games']==1
                with patch.object(scores,'calculate_distribution',side_effect=AssertionError('request-side calculation')):
                    await scores.get_distribution('alice',Redis(),refresh=True)
                a,b,e = [await revision(p) for p in ('alice','bob','empty')]
                # Rescoring: existing pipelines map changed FENs to every owning game,
                # then refresh coverage, even when counts are numerically unchanged.
                await sql(f"UPDATE {src('fen')} SET score=9994 WHERE fen_id=11")
                await sql(f"UPDATE {coverage} SET total_positions=total_positions WHERE link IN(1,2)")
                assert await revision('alice')==a+1 and await revision('bob')==b+1 and await revision('empty')==e
                stale=await scores.get_distribution('alice',Redis())
                assert stale['stale'] and stale['by_mode']==alice['by_mode']
                for player in ('alice','bob'): await finish(player)
                alice=await scores.get_distribution('alice',Redis())
                assert {'moves':6,'side':'for','positions':2} in alice['by_mode']['blitz']['mates']
                assert not alice['stale']
                # All supported invalidations, old/new days, rollback and idempotent rebuild.
                prior=await revision('alice')
                async with factory() as session:
                    await session.execute(text(f"UPDATE {coverage} SET total_positions=total_positions WHERE link=1"))
                    await session.rollback()
                assert await revision('alice')==prior
                await sql(f"UPDATE {src('game')} SET played_at='2025-01-04T23:00-03:00' WHERE link=1")
                if not normalized:
                    await sql("UPDATE public.game_player SET played_at='2025-01-04T23:00-03:00' WHERE link=1")
                dirty=(await sql(f"SELECT day FROM {table('dirty')} WHERE player_name='alice'")).scalars().all()
                assert date(2025,1,1) in dirty and date(2025,1,5) in dirty
                await finish('alice')
                assert (await scores.get_distribution('alice',Redis()))['by_mode']==alice['by_mode']
                await sql(f"UPDATE {src('moves')} SET white_move='b4' WHERE link=3")
                assert (await scores.get_distribution('alice',Redis()))['stale']
                await finish('alice')
                # A simultaneous import must abort publication, leaving the event queued.
                await sql(f"UPDATE {coverage} SET total_positions=total_positions WHERE link IN(1,2)")
                entered,release=asyncio.Event(),asyncio.Event()
                real=summaries.calculate_day
                calls=0
                async def concurrent(*args):
                    nonlocal calls
                    payload=await real(*args)
                    calls+=1
                    if calls==2:
                        entered.set(); await release.wait()
                    return payload
                with patch.object(summaries,'calculate_day',side_effect=concurrent):
                    task=asyncio.create_task(summaries.refresh_player('alice',session_factory=factory))
                    await asyncio.wait_for(entered.wait(),10)
                    await asyncio.wait_for(sql(f"UPDATE {coverage} SET total_positions=total_positions WHERE link=2"),2)
                    release.set()
                    try: await task
                    except Exception as exc: assert 'could not serialize' in str(exc),str(exc)
                    else: raise AssertionError('Concurrent write must prevent stale publication')
                assert (await scores.get_distribution('alice',Redis()))['stale']
                await finish('alice')
                await sql(f"UPDATE {src('player')} SET joined=0 WHERE player_name='alice'")
                assert (await finish('alice'))['status']=='ineligible'
                try: await scores.get_distribution('alice',Redis())
                except LookupError: pass
                else: raise AssertionError('Incomplete profile accepted')
                await sql(f"UPDATE {src('player')} SET joined=1 WHERE player_name='alice'")
                await finish('alice')
                assert (await scores.get_distribution('alice',Redis()))['by_mode']==alice['by_mode']
                if not normalized: await sql('DELETE FROM public.game_player WHERE link=1')
                await sql(f"DELETE FROM {src('game')} WHERE link=1")
                await finish('alice')
                assert (await scores.get_distribution('alice',Redis()))['by_mode']['blitz']['cp_positions']==0
                start=time.perf_counter()
                for _ in range(20): await scores.get_distribution('alice',Redis())
                print(f'normalized={normalized}: parity, invalidation, replay, concurrent publication, demotion/deletion passed; saved reads {(time.perf_counter()-start)*50:.2f} ms average',flush=True)
                if normalized:
                    await sql("INSERT INTO candidate.player VALUES(5,'benchmark',1,NULL)")
                    await sql('''INSERT INTO candidate.game SELECT n,5,2,2,'2025-01-06T12:00Z',20,1,0,'win','checkmated'
                        FROM generate_series(100,2099) n''')
                    await sql('''INSERT INTO candidate.game_player SELECT g.link,p.player_id,p.is_white
                        FROM candidate.game g CROSS JOIN (VALUES(5,true),(2,false)) p(player_id,is_white) WHERE g.link>=100''')
                    await sql('''INSERT INTO candidate.game_fen_association SELECT 100000+g.link*16+f.fen_id,g.link,f.fen_id
                        FROM candidate.game g CROSS JOIN candidate.fen f WHERE g.link>=100''')
                    assert await revision('benchmark') is not None
                    direct_start=time.perf_counter()
                    expected=await scores.calculate_distribution('benchmark',scores.MODES,Redis())
                    direct_ms=(time.perf_counter()-direct_start)*1000
                    await finish('benchmark')
                    saved_start=time.perf_counter()
                    actual=await scores.get_distribution('benchmark',Redis())
                    saved_ms=(time.perf_counter()-saved_start)*1000
                    for mode in scores.MODES:
                        assert {k:actual['by_mode'][mode][k] for k in expected['by_mode'][mode]}==expected['by_mode'][mode]
                    print(f'Synthetic 2,000-game / 32,000-occurrence parity: direct {direct_ms:.1f} ms; saved snapshot {saved_ms:.1f} ms (not a production estimate).',flush=True)
                    await sql('TRUNCATE candidate.game_fen_association')
                    assert (await scores.get_distribution('benchmark',Redis()))['stale']
                    await finish('benchmark')
                    assert (await scores.get_distribution('benchmark',Redis()))['by_mode']['blitz']['cp_positions']==0
                checks+=20
        print(f'PASS: {checks} grouped parity and lifecycle checks; no live database/cloud access.',flush=True)
    finally:
        for engine in engines: await engine.dispose()
        if conn: await conn.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'pgdata'),'-m','immediate','-w','stop')


if __name__=='__main__': asyncio.run(main())
