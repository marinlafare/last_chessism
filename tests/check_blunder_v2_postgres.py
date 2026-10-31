"""Private disposable PostgreSQL; no networking, cloud credentials or live data."""
import asyncio
from contextlib import suppress
import io
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock

from tests.check_blunder_training_postgres import FIXTURE


async def main():
    import asyncpg
    import numpy as np
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from chessism_api.operations.research_algorithms.blunder_v2_job import configuration
    from chessism_api.operations.research_algorithms.blunder_v2_source import freeze
    from chessism_api.operations.research_algorithms.blunder_v2_model import DTYPE
    from tests.check_db_refactor_candidate_postgres import command
    root=Path(tempfile.mkdtemp(prefix='blunder-v2-',dir='/restore-test'))
    socket=root/'socket'; socket.mkdir()
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'
    connection=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pgctl,'-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection=await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute(FIXTURE)
        await connection.execute('''ALTER TABLE game ADD COLUMN white text,ADD COLUMN black text,ADD COLUMN played_at timestamptz,ADD COLUMN initial_setup text;
            UPDATE game g SET white=p.player_name FROM player p WHERE p.id=g.white_player_id;
            UPDATE game g SET black=p.player_name FROM player p WHERE p.id=g.black_player_id;
            UPDATE game SET played_at='2020-01-01'::timestamptz+link*interval '1 day';
            ALTER TABLE candidate.game ADD COLUMN played_at timestamptz;
            UPDATE candidate.game g SET played_at=p.played_at FROM game p WHERE p.link=g.link;''')
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database='postgres',port=55432,query={'host':str(socket)}))
        outputs=[]
        for layout in (None,RelationalStorage('candidate','archive','reads','relational_v3')):
            factory=async_sessionmaker(engine,info={'relational_storage_v2':layout})
            stream=io.BytesIO()
            meta,games,source=await freeze(configuration(100),AsyncMock(),stream,session_factory=factory)
            assert {g['link'] for g in meta}=={1,2,3,8,9,10},meta
            assert source['positions']==132 and source['selected_games']==6
            rows=np.frombuffer(stream.getvalue(),dtype=DTYPE)
            assert rows['before'][0]==15 and rows['before'][1]==100
            assert sum(games['cp_count'])==66
            assert rows['fen'][0]==1
            outputs.append(stream.getvalue())
        assert outputs[0]==outputs[1],'Native/legacy input changed'
        await connection.execute("INSERT INTO cloud_analysis_job VALUES('running')")
        try:
            await freeze(configuration(100),AsyncMock(),io.BytesIO(),session_factory=async_sessionmaker(engine))
        except ValueError as error: assert 'active cloud' in str(error)
        else: raise AssertionError('Active cloud was not protected')
        await connection.execute('DELETE FROM cloud_analysis_job; UPDATE game_analysis_summary SET total_positions=21 WHERE link=1')
        try:
            await freeze(configuration(100),AsyncMock(),io.BytesIO(),session_factory=async_sessionmaker(engine))
        except ValueError as error: assert 'Coverage' in str(error)
        else: raise AssertionError('Invalid source coverage accepted')
        print('PASS: v2 read-only native/legacy source, historical ratings/dates, timeouts and wider gaps, exact predecessor/FEN identity, coverage and cloud guards')
        await connection.execute('UPDATE game_analysis_summary SET total_positions=22 WHERE link=1')
        from tests.full_development_postgres_fixture import verify as verify_full_development
        await verify_full_development(connection,engine)
        await connection.execute('DROP TABLE algorithm_run')
        from tests.balanced_postgres_fixture import verify
        await verify(connection,engine)
    finally:
        if engine: await engine.dispose()
        if connection: await connection.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'db'),'-m','fast','-w','stop')


if __name__=='__main__': asyncio.run(main())
