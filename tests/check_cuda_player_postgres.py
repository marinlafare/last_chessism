"""One-player frozen validation input against disposable PostgreSQL only."""
import asyncio
from contextlib import suppress
import json
import io
import os
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock

os.environ['DATABASE_URL'] = 'postgresql+asyncpg://test@127.0.0.1:1/postgres'


async def main():
    import asyncpg
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from tests.check_db_refactor_candidate_postgres import command
    from tests.check_summary_matrices_postgres import FIXTURE
    from chessism_api.operations.player_stockfish_summary import empty_modes
    from chessism_api.operations.research_algorithms import score_validation_job as job, summary_validation as val
    from chessism_api.operations.research_algorithms.score_stream import DTYPE, cpu_reference
    from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
    from chessism_api.operations.matrix_constructor.queries import matrix_sql
    root = Path(tempfile.mkdtemp(prefix='cuda-player-', dir='/restore-test'))
    socket = root/'socket'; socket.mkdir()
    pgctl = '/usr/lib/postgresql/15/bin/pg_ctl'
    conn, engine = None, None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        conn = await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await conn.execute(FIXTURE)
        await conn.execute('''CREATE TABLE player(player_name text,joined bigint,deleted_at timestamptz);
            INSERT INTO player VALUES('alice',1,NULL);
            CREATE TABLE cloud_analysis_job(status text);
            CREATE TABLE algorithm_run(status text);
            CREATE TABLE game(link bigint,n_moves int,white_result float8,black_result float8,white_str_result text,black_str_result text);
            INSERT INTO game VALUES(1,2,1,0,'win','resigned'),(2,2,.5,.5,'draw','draw');
            CREATE TABLE player_stockfish_state(player_name text,revision bigint,published_revision bigint DEFAULT 1);
            INSERT INTO player_stockfish_state(player_name,revision) VALUES('alice',1);
            CREATE TABLE player_stockfish_snapshot(player_name text,revision bigint,payload jsonb,generated_at timestamptz DEFAULT now());''')
        payload = empty_modes()
        payload['blitz']['counts'][6]=2; payload['blitz']['counts'][5]=1
        payload['blitz']['cp_positions']=3; payload['blitz']['mate_positions']=1
        payload['blitz']['mates']=[{'moves':2,'side':'for','positions':1}]
        payload['rapid']['counts'][7]=1; payload['rapid']['cp_positions']=1
        payload['rapid']['unscored_positions']=1
        await conn.execute('INSERT INTO player_stockfish_snapshot(player_name,revision,payload) VALUES($1,1,$2::jsonb)','alice',json.dumps(payload))
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        factory=async_sessionmaker(engine)
        async def freeze():
            stream=io.BytesIO()
            sql,saved,source,timings=await job.freeze_source(job.configuration('alice'),AsyncMock(),stream,session_factory=factory)
            return np.frombuffer(stream.getvalue(),dtype=DTYPE),sql,saved,source,timings
        rows,sql,saved,source,_=await freeze()
        assert len(rows)==5 and source['expected_rows']==5 and source['truncated'] is False
        assert np.isfinite(rows['score']).all()
        # Compare the optimized extraction to the original five-column matrix.
        _,query,params=matrix_sql(job.configuration('alice')['matrix'])
        original=await conn.fetch(query.replace(':players','$1').replace(':modes','$2').replace(':max_rows','$3'),['alice'],list(val.MODES),job.MAX_ROWS)
        assert sorted((val.MODES[int(r['mode'])],float(r['score']),int(r['kind']),int(r['mate'])) for r in rows)==sorted(
            (r['mode'],r['player_score'],val.KINDS[r['score_kind']],-1 if r['mate_moves'] is None else r['mate_moves']) for r in original)
        assert job.compare_groups(sql,saved)['status']=='passed'
        actual={}
        for index,mode in enumerate(val.MODES):
            selected=rows[rows['mode']==index]
            if not len(selected): continue
            decisions=cpu_reference(selected)
            groups=np.zeros(len(selected),dtype=np.int64)
            counts=val.group_counts(np,decisions,groups,1)[0].tolist()
            mates=val.mate_records(dict(zip(*val.group_mates(np,decisions,groups))),0)
            actual[mode]={'counts':counts,'mates':mates}
        assert job.compare_groups(sql,actual)['status']=='passed'
        assert job.compare_groups(saved,actual)['status']=='passed'
        # A source update between extraction and SQL verification must not leak
        # into either reference; both are tied to the same repeatable snapshot.
        async def mutate(phase,processed=0,total=None,detail=''):
            if phase=='sql_reference':
                await conn.execute('UPDATE fen SET score=550 WHERE fen_id=1')
        frozen=io.BytesIO()
        frozen_sql,frozen_saved,_,_=await job.freeze_source(job.configuration('alice'),
            AsyncMock(progress=AsyncMock(side_effect=mutate)),frozen,session_factory=factory)
        assert job.compare_groups(frozen_sql,frozen_saved)['status']=='passed'
        assert sorted(np.frombuffer(frozen.getvalue(),dtype=DTYPE).tolist())==sorted(rows.tolist())
        await conn.execute('UPDATE fen SET score=50 WHERE fen_id=1')
        # Native compact storage uses the same multiplicity/perspective as the
        # compatibility matrix, without projecting its expensive view columns.
        await conn.execute('''CREATE SCHEMA native;
            CREATE TABLE native.player AS SELECT 1::bigint AS id,* FROM public.player;
            CREATE TABLE native.game_mode(id int,name text);
            INSERT INTO native.game_mode VALUES(1,'blitz'),(2,'rapid');
            CREATE TABLE native.game AS SELECT *,link::int AS mode_id FROM public.game;
            CREATE TABLE native.game_player AS SELECT link AS game_link,1::bigint AS player_id,color='white' AS is_white FROM public.game_player WHERE player_name='alice';
            CREATE TABLE native.game_fen_association AS SELECT game_link,n_move,move_color='white' AS is_white,fen_id FROM public.game_fen_association;
            CREATE TABLE native.fen AS SELECT * FROM public.fen;
            CREATE TABLE native.player_stockfish_state AS SELECT * FROM public.player_stockfish_state;
            CREATE TABLE native.player_stockfish_snapshot AS SELECT * FROM public.player_stockfish_snapshot;''')
        original_factory=factory
        factory=async_sessionmaker(engine,info={SESSION_KEY:RelationalStorage('native','archive','reads','relational_v3')})
        native_rows,native_sql,_,_,_=await freeze()
        assert sorted(native_rows.tolist())==sorted(rows.tolist())
        assert job.compare_groups(native_sql,sql)['status']=='passed'
        # All-player experiment uses exactly the same v2 projection/classifier,
        # but freezes 1,000-game chunks without raising per-window validation caps.
        from scripts import benchmark_all_score_distributions as cohort
        for candidate in (original_factory, factory):
            selected = await cohort.inventory(session_factory=candidate)
            assert len(selected)==1 and selected[0]['scored']==5
            frozen=io.BytesIO()
            chunk_sql,chunk_saved,_,source=await cohort.freeze_player(selected[0],frozen,
                lambda *args: None,session_factory=candidate)
            assert source['rows']==5 and source['game_batches']==1
            assert sorted(np.frombuffer(frozen.getvalue(),dtype=DTYPE).tolist())==sorted(rows.tolist())
            assert job.compare_groups(chunk_sql,chunk_saved)['status']=='passed'
            assert job.compare_groups(chunk_sql,sql)['status']=='passed'
            await conn.execute("INSERT INTO algorithm_run VALUES('running')")
            try:
                await cohort.inventory(session_factory=candidate)
                raise AssertionError('busy research queue accepted')
            except ValueError as error: assert 'idle' in str(error)
            await conn.execute('DELETE FROM algorithm_run')
        factory=original_factory
        # The helper never writes to the saved summary or source data.
        assert await conn.fetchval('SELECT revision FROM player_stockfish_state')==1
        await conn.execute('UPDATE player_stockfish_state SET revision=2')
        try: await freeze(); raise AssertionError('stale summary accepted')
        except ValueError as error: assert 'updating' in str(error)
        await conn.execute('UPDATE player_stockfish_state SET revision=1')
        # A stale/incorrect row count must never produce a partial pass, in
        # either direction, even if the summary revision claims to be current.
        for count in (1,3):
            payload['blitz']['counts'][6]=count
            await conn.execute('UPDATE player_stockfish_snapshot SET payload=$1::jsonb',json.dumps(payload))
            try: await freeze(); raise AssertionError('incorrect source count accepted')
            except ValueError as error: assert 'count' in str(error)
        payload['blitz']['counts'][6]=2
        payload['blitz']['counts'][0]=job.MAX_ROWS+1
        await conn.execute('UPDATE player_stockfish_snapshot SET payload=$1::jsonb',json.dumps(payload))
        try: await freeze(); raise AssertionError('oversized input accepted')
        except ValueError as error: assert 'No rows were truncated' in str(error)
        await conn.execute("INSERT INTO cloud_analysis_job VALUES('running')")
        try: await freeze(); raise AssertionError('active cloud accepted')
        except ValueError as error: assert 'active cloud' in str(error)
        print('PASS: streamed scored-only input, legacy/native/matrix/SQL/saved-summary parity, repeated occurrences, stale/oversize/cloud guards, no source writes')
    finally:
        if engine: await engine.dispose()
        if conn: await conn.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__': asyncio.run(main())
