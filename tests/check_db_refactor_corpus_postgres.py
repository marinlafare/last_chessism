"""Disposable, network-none PostgreSQL proof of the summary transaction."""
import asyncio
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def main():
    import asyncpg
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    from chessism_api.database.models import Base
    from chessism_api.operations import database_restore_tests as restore
    from tests.check_relational_application_postgres import setup
    from tests.check_db_refactor_candidate_postgres import command
    from scripts.db_refactor_candidate import C,A,V
    from scripts.db_refactor_cutover import TABLE_MAP,NEW_TABLES
    from scripts.db_refactor_release_rehearsal import private_url
    from scripts.db_refactor_corpus import finalize,snapshot,InjectedFinalizationFailure,SUMMARY_TABLES

    if {p.name for p in Path('/sys/class/net').iterdir()}!={'lo'}:
        raise RuntimeError('Only disposable network-none fixture containers are allowed')
    base=Path('/restore-test/db-refactor-baseline-corpus-fixture')
    base.mkdir(mode=0o700)
    data=base/'pgdata'
    socket=restore._create_restore_socket_directory()
    conn=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(data),'-U','chessism_user',
                      '--auth-local=trust','--auth-host=reject')
        await command(restore.PG_CTL,'-D',str(data),'-l',str(base/'postgres.log'),'-w',
                      '-o',restore._postgres_start_options(socket),'start')
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await setup(conn,data)
        # This fixture has no external FKs/triggers. The full switch/rollback
        # fixture tests their relocation separately; here only summary math.
        await conn.execute(f'CREATE TABLE {C}.months (LIKE public.months INCLUDING ALL); CREATE SCHEMA chessism_legacy')
        for source,target in TABLE_MAP.items():
            await conn.execute(f'ALTER TABLE public.{source} SET SCHEMA chessism_legacy')
        for target in (*TABLE_MAP.values(),*NEW_TABLES):
            await conn.execute(f'ALTER TABLE {C}.{target} SET SCHEMA public')
        await conn.execute(f'''ALTER SCHEMA {A} RENAME TO chessism_archive;
            ALTER SCHEMA {V} RENAME TO chessism_reads;
            CREATE TABLE public.relational_storage_release(version text PRIMARY KEY,application_ready boolean,rehearsal_only boolean);
            INSERT INTO public.relational_storage_release VALUES('relational_v2',false,true);''')
        await conn.close();conn=None
        engine=create_async_engine(private_url(socket,'postgres'),isolation_level='REPEATABLE READ')
        async with engine.begin() as connection:
            tables=[Base.metadata.tables[name] for name in (*SUMMARY_TABLES,'home_summary_source')
                    if name!='player_salience_summary']
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync,tables=tables))
            await connection.execute(text('INSERT INTO home_summary_source(id,revision) VALUES(1,1)'))
        async def publish(*args,**kwargs): pass
        async with engine.connect() as connection:
            async with connection.begin(): before=await snapshot(connection)
            try:
                async with connection.begin(): await finalize(connection,data,publish,inject=True)
            except InjectedFinalizationFailure: pass
            else: raise AssertionError('Failure injection was not reached')
            async with connection.begin(): assert await snapshot(connection)==before
            print('PASS summary savepoint commits roll back with the outer transaction',flush=True)
            transaction=await connection.begin()
            try:
                await finalize(connection,data,publish)
            finally:
                await transaction.rollback()
            async with connection.begin(): assert await snapshot(connection)==before
            print('PASS rollback of ALL refresh stages including ratings and homepage',flush=True)
            async with connection.begin(): result=await finalize(connection,data,publish)
            assert result['counts']['games']==2 and result['counts']['archived_games']==2
            assert all(result['checks'].values())
            async with connection.begin():
                assert await connection.scalar(text('SELECT n_games_in_db FROM database_summary WHERE id=1'))==2
                assert await connection.scalar(text('SELECT count(*) FROM chessism_legacy.game'))==4
                assert await connection.scalar(text('SELECT count(*) FROM public.fen'))==153
                assert await connection.scalar(text("SELECT status FROM public.player_salience_summary s JOIN player p ON p.id=s.player_id WHERE p.player_name='alice'"))=='stale'
            print('PASS active/archive scope, all summary validators, salience staleness, unchanged original games/FENs',flush=True)
            try:
                async with connection.begin():
                    await connection.execute(text("""INSERT INTO chessism_legacy.game_fen_association
                        (game_link,fen_fen,n_move,move_color) VALUES(3,'position-a',1,'white')"""))
                    await finalize(connection,data,publish)
            except RuntimeError as error:
                assert 'Archived games contributed FENs' in str(error)
            else:
                raise AssertionError('Archived position contributions must block preservation of frequency caches')
            print('PASS archive-with-FENs fails closed before cache refresh',flush=True)
    finally:
        if conn: await conn.close()
        if engine: await engine.dispose()
        await restore._stop_restored_postgres(data)
        if not (data/'postmaster.pid').exists(): socket.rmdir()


if __name__=='__main__': asyncio.run(main())
