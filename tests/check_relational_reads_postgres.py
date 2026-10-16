"""Exercise optimized application readers on real private PostgreSQL fixtures."""
import asyncio
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def main():
    import asyncpg
    from tests.check_db_refactor_candidate_postgres import FIXTURE,command
    from scripts.db_refactor_candidate import prepare_reference,build_candidate
    from scripts.db_refactor_application_checks import measure_application_reads
    base=Path('/restore-test/db-refactor-baseline-reads-fixture')
    base.mkdir(mode=0o700)
    data,socket=base/'pgdata',base/'socket'
    socket.mkdir(mode=0o700)
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'
    await command('/usr/lib/postgresql/15/bin/initdb','-D',str(data),'-U','chessism_user','--auth-local=trust','--auth-host=reject')
    conn=None
    try:
        await command(pgctl,'-D',str(data),'-l',str(base/'postgres.log'),'-w','-o',
            f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await conn.execute(FIXTURE)
        await conn.execute('''ALTER TABLE player ADD COLUMN country text,ADD COLUMN location text,
            ADD COLUMN timezone text,ADD COLUMN timezone_source text,ADD COLUMN deleted_at timestamptz;
            UPDATE player SET timezone='UTC',timezone_source='user';
            INSERT INTO player(player_name,joined) VALUES('no-games',1);
            INSERT INTO game SELECT (jsonb_populate_record(NULL::game,to_json(game)::jsonb ||
                '{"link":5,"white":"alice","black":"alice","mode":null,"played_at":null}'::jsonb)).* FROM game WHERE link=1;
            INSERT INTO game_player(link,color,player_name,opponent_name,result,rating,opponent_rating,mode,played_at,eco,n_moves,time_elapsed,avg_elo)
                SELECT 5,color,'alice','alice',result,rating,opponent_rating,NULL,NULL,eco,n_moves,time_elapsed,avg_elo
                FROM game_player WHERE link=1;
            INSERT INTO game SELECT (jsonb_populate_record(NULL::game,to_json(game)::jsonb ||
                '{"link":6,"white":"alice","black":"alice"}'::jsonb)).* FROM game WHERE link=1;
            INSERT INTO game_player(link,color,player_name,opponent_name,result,rating,opponent_rating,mode,played_at,eco,n_moves,time_elapsed,avg_elo)
                SELECT 6,color,'alice','alice',result,rating,opponent_rating,mode,played_at,eco,n_moves,time_elapsed,avg_elo
                FROM game_player WHERE link=1;
            INSERT INTO game SELECT (jsonb_populate_record(NULL::game,to_json(game)::jsonb ||
                jsonb_build_object('link',v.link,'played_at',v.played_at))).* FROM game
                CROSS JOIN (VALUES (7,'2025-03-09T06:59:00Z'),(8,'2025-03-09T07:01:00Z')) v(link,played_at)
                WHERE game.link=1;
            INSERT INTO game_player(link,color,player_name,opponent_name,result,rating,opponent_rating,mode,played_at,eco,n_moves,time_elapsed,avg_elo)
                SELECT g.link,gp.color,gp.player_name,gp.opponent_name,gp.result,gp.rating,gp.opponent_rating,
                    g.mode,g.played_at,g.eco,g.n_moves,g.time_elapsed,g.avg_elo
                FROM game g CROSS JOIN game_player gp WHERE g.link IN (7,8) AND gp.link=1;
            -- A game row alone must never invent a missing player association.
            DELETE FROM game_player WHERE link=8 AND color='black';''')
        async def publish(*args,**kwargs): pass
        await prepare_reference(conn,data)
        await build_candidate(conn,data,publish)
        methods={'get_player_games_page','get_player_game_summary','get_player_performance_summary',
                 'get_player_game_averages','get_player_termination_stats','get_player_behavioural_activity',
                 'get_player_behavioural_ratings'}
        rows=await measure_application_reads(socket,[{'cohort':name,'player_name':name}
            for name in ('alice','bob','carol','no-games')],publish,lambda result:None,
            database='postgres',methods=methods,repeats=3,capture_plans=True)
        bad=[{k:r[k] for k in ('name','errors','differences','within_side_variations')} for r in rows if not r['matches']]
        assert not bad,bad
        print('PASS 28 application read comparisons: numeric ownership, both sides, self-play, archived-only/empty player, null mode/date, aggregates, charts, exact order and types',flush=True)
        # Include true local-midnight boundaries and DST, not just UTC/no filter.
        from scripts.db_refactor_candidate import C
        from scripts.db_refactor_application_checks import factory_for,comparable_response
        from chessism_api.operations import player_hero_analytics as hero
        from datetime import date
        from unittest.mock import patch
        from scripts.db_refactor_workload import digest
        engines,factories=zip(*(factory_for(socket,normalized=side,database='postgres',readonly=True)
                               for side in (False,True)))
        comparisons=0
        try:
            for zone in ('UTC','America/New_York','Asia/Kolkata'):
                for schema in ('public',C):
                    await conn.execute(f'UPDATE {schema}.player SET timezone=$1',zone)
                for player in ('alice','bob','carol','no-games'):
                    for mode in ('all','bullet','blitz','rapid'):
                        for start,end in ((None,None),(date(2025,1,1),date(2025,1,1)),
                                          (None,date(2025,1,1)),(date(2025,1,2),None),
                                          (date(2025,3,9),date(2025,3,9))):
                            values=[]
                            for factory in factories:
                                with patch.object(hero,'AsyncDBSession',factory):
                                    values.append(comparable_response('get_player_behavioural_ratings',
                                        await hero.get_player_behavioural_ratings(player,mode,start,end)))
                            assert digest(values[0])==digest(values[1]),(zone,player,mode,start,end)
                            comparisons+=1
        finally:
            for engine in engines:
                await engine.dispose()
        print(f'PASS {comparisons} rating comparisons: timezones, single-day/open date ranges, modes, empty results and self-play ties',flush=True)
    finally:
        if conn: await conn.close()
        await command(pgctl,'-D',str(data),'-m','fast','-w','stop')


if __name__=='__main__':
    asyncio.run(main())
