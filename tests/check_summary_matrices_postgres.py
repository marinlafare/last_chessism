"""Disposable PostgreSQL only: run with --network none, no live credentials."""
import asyncio
from contextlib import suppress
import os
from pathlib import Path
import tempfile

os.environ['DATABASE_URL'] = 'postgresql+asyncpg://test@127.0.0.1:1/postgres'

FIXTURE = '''
CREATE TABLE game_player(link bigint,player_name text,color text,mode text,played_at timestamptz,
 n_moves int,rating int,opponent_rating int,result float8);
INSERT INTO game_player VALUES
 (1,'alice','white','blitz','2025-01-01Z',2,1500,1700,1),
 (1,'bob','black','blitz','2025-01-01Z',2,1700,1500,0),
 (2,'alice','black','rapid','2025-01-02Z',2,1600,1800,.5);
CREATE TABLE fen(fen_id bigint PRIMARY KEY,score float8,analysis_source text);
INSERT INTO fen VALUES(1,50,'stockfish'),(2,9998,'stockfish'),(3,NULL,NULL),(4,-100,'stockfish');
CREATE TABLE game_fen_association(game_link bigint,n_move int,move_color text,fen_id bigint);
INSERT INTO game_fen_association VALUES
 (1,1,'white',1),(1,1,'black',2),(1,2,'white',1),(1,2,'black',4),
 (2,1,'white',3),(2,1,'black',4);
CREATE TABLE game_analysis_summary(link bigint,is_fully_analyzed bool,total_positions int);
INSERT INTO game_analysis_summary VALUES(1,true,4),(2,false,2);
'''


async def main():
    import asyncpg
    from sqlalchemy import text
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine
    from tests.check_db_refactor_candidate_postgres import command
    from chessism_api.operations.matrix_constructor.defaults import default_formula
    from chessism_api.operations.matrix_constructor.definitions import resolve_definition
    from chessism_api.operations.matrix_constructor.queries import matrix_sql
    from chessism_api.operations.research_algorithms.summary_validation import reference
    root = Path(tempfile.mkdtemp(prefix='summary-matrices-', dir='/restore-test'))
    socket = root / 'socket'; socket.mkdir()
    pgctl = '/usr/lib/postgresql/15/bin/pg_ctl'
    conn, engine = None, None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',
                      f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        conn = await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await conn.execute(FIXTURE)
        engine = create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        async def read(key, *, contract=2, **filters):
            config = resolve_definition(default_formula(key)['config'], {'filters': {'max_rows': 100, **filters}})
            config['summary_input_version'] = contract
            count, query, params = matrix_sql(config)
            async with engine.begin() as session:
                total = (await session.execute(text(count), params)).scalar_one()
                rows = [dict(row) for row in (await session.execute(text(query), params)).mappings()]
            return rows, total
        rows, total = await read('score-distribution')
        assert len(rows) == total == 9
        assert all(set(row)=={'row_key','player','mode','player_score','score_kind','mate_moves'} for row in rows)
        alice = [row for row in rows if row['row_key'].startswith('1:') and row['player']=='alice']
        bob = [row for row in rows if row['row_key'].startswith('1:') and row['player']=='bob']
        assert [row['player_score'] for row in alice] == [50,9998,50,-100]
        assert [row['player_score'] for row in bob] == [-50,-9998,-50,100]
        assert reference(alice).tolist() == [[6,-1,0],[13,2,1],[6,-1,0],[5,-1,0]]
        assert all(row['player_score'] is not None and row['score_kind']!='unscored' for row in rows)
        legacy, legacy_total = await read('score-distribution',contract=1)
        assert len(legacy)==legacy_total==10 and sum(row['score_kind']=='unscored' for row in legacy)==1
        limited, _ = await read('score-distribution',max_rows=1,analyzed_only=False)
        assert len(limited)==1 and limited[0]['player_score']==100  # NULL preceding ply did not consume the limit.
        rows, total = await read('blunder-moves')
        assert len(rows) == total == 4
        a1,a2,b1,b2 = sorted(rows, key=lambda row:(row['player'],row['row_key']))
        assert (a1['previous_player_score'],a1['initial_prior'],a1['player_rating']) == (15,1,1500)
        assert all(not {'player_color','started_at','ply','result'}.intersection(row) for row in rows)
        assert (a2['previous_player_score'],a2['previous_score_kind']) == (9998,'mate')
        assert (b1['previous_player_score'],b1['player_score'],b1['opponent_rating']) == (-50,-9998,1500)
        rows, total = await read('blunder-moves',players=['bob'],max_rows=1)
        assert len(rows)==1 and total==2 and rows[0]['previous_player_score']==-50
        rows, _ = await read('score-distribution',players=['alice'],modes=['blitz'],date_to='2025-01-01')
        assert len(rows)==4
        # Inconsistent coverage must not silently jump across a missing predecessor.
        await conn.execute('UPDATE game_analysis_summary SET is_fully_analyzed=true WHERE link=2')
        rows, _ = await read('blunder-moves',modes=['rapid'])
        assert len(rows)==1 and rows[0]['previous_player_score'] is None and rows[0]['previous_score_kind']=='unscored'
        rows, _ = await read('score-distribution',modes=['rapid'],analyzed_only=True)
        assert len(rows)==1
        await conn.execute('UPDATE fen SET score=0 WHERE fen_id=1')
        rows, _ = await read('score-distribution',modes=['blitz'])
        assert sum(row['player_score']==0 for row in rows)==4
        # Normalization preserves tablebase and unknown mate provenance without
        # requiring the raw analysis_source column in the output matrix.
        for source, distance, score_kind in [('tablebase',None,'tablebase'),(None,None,'mate'),('Stockfish',None,'mate'),('stockfish_backup',2,'mate')]:
            await conn.execute('UPDATE fen SET analysis_source=$1 WHERE fen_id=2',source)
            rows, _ = await read('score-distribution',players=['alice'],modes=['blitz'])
            mate = next(row for row in rows if row['player_score']==9998)
            assert (mate['mate_moves'],mate['score_kind'])==(distance,score_kind)
        print('PASS: scored-only five-column input, filter-before-limit, zero/repeated scores, normalized mate provenance, both perspectives, prior-ply boundaries, simplified blunder input, dates and v1 compatibility')
    finally:
        if engine: await engine.dispose()
        if conn: await conn.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__ == '__main__':
    asyncio.run(main())
