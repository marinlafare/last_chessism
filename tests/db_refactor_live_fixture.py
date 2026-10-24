"""Exercise the actual live transaction implementation on tiny private data."""
import asyncio
import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.db_refactor_release_rehearsal import private_url,check_runtime
from scripts.db_refactor_live import migrate
from scripts.db_refactor_live_target import identity
from scripts.db_refactor_corpus import snapshot,validate_summaries,InjectedFinalizationFailure
from scripts.db_refactor_switch_rehearsal import catalog_identity
from chessism_api.operations import database_restore_tests as restore


async def exercise_live(socket,pgdata,start):
    observer=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres',
        server_settings={'default_transaction_read_only':'on'})
    engine=create_async_engine(private_url(socket,'postgres'),isolation_level='REPEATABLE READ')
    events=[]
    outsider=None
    async def publish(phase,**details):events.append((phase,details))
    async def checkpoint():pass
    async def connect():
        return await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                await connection.execute(text('SELECT 1'))
                driver=(await connection.get_raw_connection()).driver_connection
                expected=await identity(driver);expected.pop('pid')
                original=await catalog_identity(driver)
                summaries=await snapshot(connection)
            async def unchanged():
                async with connection.begin():
                    assert await catalog_identity(driver)==original
                    assert await snapshot(connection)==summaries
            try:await migrate(connection,{**expected,'system_identifier':'wrong'},observer.get_server_pid(),publish,checkpoint)
            except RuntimeError as error:assert 'identity' in str(error)
            else:raise AssertionError('Wrong database identity accepted')
            await unchanged()
            outsider=await connect()
            try:await migrate(connection,expected,observer.get_server_pid(),publish,checkpoint)
            except RuntimeError as error:assert 'Other database clients' in str(error)
            else:raise AssertionError('Concurrent database client accepted')
            await outsider.close();outsider=None
            await unchanged()
            print('PASS live guard rejects wrong cluster and concurrent clients before copying',flush=True)
            async with connection.begin():
                await connection.execute(text("INSERT INTO cloud_analysis_job VALUES('blocked','running')"))
            try:await migrate(connection,expected,observer.get_server_pid(),publish,checkpoint)
            except RuntimeError as error:assert 'Cloud analysis is still active' in str(error)
            else:raise AssertionError('Running cloud work accepted')
            async with connection.begin():await connection.execute(text('DELETE FROM cloud_analysis_job'))
            try:await migrate(connection,expected,observer.get_server_pid(),publish,checkpoint,inject=True)
            except InjectedFinalizationFailure:pass
            else:raise AssertionError('Combined cutover failure was not reached')
            await unchanged()
            assert any(phase=='live_transaction_rolled_back' and info['rollback_verified'] for phase,info in events)
            print('PASS complete live-path build, summary failure and whole-transaction rollback',flush=True)
            async def interrupted():raise asyncio.CancelledError()
            try:await migrate(connection,expected,observer.get_server_pid(),publish,interrupted)
            except asyncio.CancelledError:pass
            else:raise AssertionError('Cancellation was not propagated')
            await unchanged()
            async def new_client(phase,**details):
                nonlocal outsider
                if phase=='source_tables_locked':outsider=await connect()
            try:await migrate(connection,expected,observer.get_server_pid(),new_client,checkpoint)
            except RuntimeError as error:assert 'Other database clients' in str(error)
            else:raise AssertionError('A client arriving during migration was not detected')
            await outsider.close();outsider=None
            await unchanged()
            print('PASS cancellation and mid-migration client abort safely, preserving original tables',flush=True)
            result=await migrate(connection,expected,observer.get_server_pid(),publish,checkpoint)
            assert result['committed'] and len(result['rows'])==16
            async with connection.begin():
                assert await connection.scalar(text('SELECT count(*) FROM chessism_legacy.game'))==4
                assert await connection.scalar(text('SELECT count(*) FROM public.game'))==2
                assert await connection.scalar(text('SELECT count(*) FROM public.fen'))==153
                assert await connection.scalar(text('SELECT application_ready AND NOT rehearsal_only FROM public.relational_storage_release')) is True
                for table,last in (('moves',10000),('months',20000)):
                    assert await connection.scalar(text(f'SELECT last_value FROM public.{table}_id_seq'))==last
                after=await snapshot(connection)
        await engine.dispose()
        await observer.close();observer=None
        await restore._stop_restored_postgres(pgdata)
        await start()
        await check_runtime(socket,pgdata,database='postgres')
        async with engine.connect() as connection:
            assert await snapshot(connection)==after
            assert await validate_summaries(connection)==result['summaries']
        print('PASS live-path atomic commit, retained originals, sequence high-water and runtime restart',flush=True)
    finally:
        if outsider:await outsider.close()
        if observer:await observer.close()
        await engine.dispose()
