"""Whole copy/switch/summary failure and restart fixture, network-none only."""
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
    from scripts.db_refactor_candidate import C,prepare_reference,build_candidate
    from scripts.db_refactor_tuning import apply_tuning
    from scripts.db_refactor_application_checks import prepare_application_copy
    from scripts.db_refactor_migration_sequence import reset_cloned_shadows,validate_application_copies,finish_private_sequence
    from scripts.db_refactor_release_rehearsal import private_url,check_runtime
    from scripts.db_refactor_switch_rehearsal import exercise_switch
    from scripts.db_refactor_corpus import SUMMARY_TABLES,snapshot,InjectedFinalizationFailure,validate_summaries

    if {p.name for p in Path('/sys/class/net').iterdir()}!={'lo'}:
        raise RuntimeError('Disposable network-none fixture required')
    base=Path('/restore-test/db-refactor-baseline-sequence-fixture')
    base.mkdir(mode=0o700)
    (base/'migration-sequence-owner.json').write_text('{"fixture":true}')
    data=base/'pgdata'
    socket=restore._create_restore_socket_directory()
    conn=engine=None
    try:
        await command('/usr/lib/postgresql/15/bin/initdb','-D',str(data),'-U','chessism_user','--auth-local=trust','--auth-host=reject')
        async def start():
            await command(restore.PG_CTL,'-D',str(data),'-l',str(base/'postgres.log'),'-w','-o',restore._postgres_start_options(socket),'start')
        async def publish(*args,**kwargs): pass
        await start()
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await setup(conn,data)
        await conn.execute(f'CREATE VIEW public.unexpected_dependency AS SELECT * FROM {C}.player')
        try: await reset_cloned_shadows(conn,data)
        except RuntimeError as error: assert 'external dependency' in str(error)
        else: raise AssertionError('External view must block shadow reset')
        assert await conn.fetchval("SELECT to_regclass('public.unexpected_dependency') IS NOT NULL")
        assert await conn.fetchval(f"SELECT to_regclass('{C}.player') IS NOT NULL")
        await conn.execute('DROP VIEW public.unexpected_dependency')
        await reset_cloned_shadows(conn,data)
        print('PASS shadow reset preserves outside objects; unexpected dependency rolls back',flush=True)
        await conn.execute('''ALTER TABLE months ADD COLUMN id serial UNIQUE;
            ALTER TABLE months ADD FOREIGN KEY(player_name) REFERENCES public.player(player_name);
            INSERT INTO months(player_name,year,month,n_games) VALUES('alice',2025,1,3);
            SELECT setval(pg_get_serial_sequence('public.moves','id'),10000,true);
            SELECT setval(pg_get_serial_sequence('public.months','id'),20000,true);
            CREATE TABLE player_stats(player_name varchar PRIMARY KEY REFERENCES player(player_name) ON DELETE CASCADE);
            CREATE FUNCTION invalidate_home_summary_v1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                UPDATE public.home_summary_source SET revision=revision+1 WHERE id=1; RETURN NULL; END $$;''')
        engine=create_async_engine(private_url(socket,'postgres'),isolation_level='REPEATABLE READ')
        async with engine.begin() as connection:
            tables=[Base.metadata.tables[name] for name in (*SUMMARY_TABLES,'home_summary_source') if name!='player_salience_summary']
            await connection.run_sync(lambda sync:Base.metadata.create_all(sync,tables=tables))
            await connection.execute(text('INSERT INTO home_summary_source(id,revision) VALUES(1,1)'))
        for table,cols in (('player','title,deleted_at,player_name'),('game','white,black'),('fen','fen'),('game_fen_association','game_link,fen_fen')):
            for suffix,event in (('rows','INSERT OR DELETE OR TRUNCATE'),('values',f'UPDATE OF {cols}')):
                await conn.execute(f'CREATE TRIGGER home_summary_v1_{table}_{suffix} AFTER {event} ON {table} FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary_v1()')
        if '--live-path' in sys.argv:
            from tests.db_refactor_live_fixture import exercise_live
            await conn.execute('CREATE TABLE cloud_analysis_job(id text PRIMARY KEY,status text NOT NULL)')
            await conn.close();conn=None
            await engine.dispose();engine=None
            await exercise_live(socket,data,start)
            return
        await prepare_reference(conn,data)
        rows=await build_candidate(conn,data,publish)
        await apply_tuning(conn,data,publish)
        await prepare_application_copy(conn,data,publish)
        rows.update(await validate_application_copies(conn,data,publish))
        assert len(rows)==16
        for table in ('moves','months'):
            transaction=conn.transaction();await transaction.start()
            try:
                field='white_move' if table=='moves' else 'n_games'
                value="'wrong'" if table=='moves' else '999'
                await conn.execute(f'UPDATE {C}.{table} SET {field}={value}')
                try: await validate_application_copies(conn,data,publish)
                except RuntimeError as error: assert f'copy differs: {table}' in str(error)
                else: raise AssertionError('Value mismatch was accepted')
            finally: await transaction.rollback()
        await conn.close();conn=None
        await exercise_switch(socket,data,publish,database='postgres')
        async with engine.connect() as connection: before=await snapshot(connection)
        try: await finish_private_sequence(socket,data,publish,database='postgres',inject=True)
        except InjectedFinalizationFailure: pass
        else: raise AssertionError('Combined failure was not injected')
        async with engine.connect() as connection:
            assert await snapshot(connection)==before
            assert await connection.scalar(text("SELECT to_regnamespace('chessism_legacy') IS NULL"))
            assert await connection.scalar(text("SELECT to_regclass('public.relational_storage_release') IS NULL"))
        print('PASS full row comparison and combined cutover/summary rollback',flush=True)
        result=await finish_private_sequence(socket,data,publish,database='postgres')
        assert result['summaries']['counts']['games']==2 and result['summaries']['counts']['archived_games']==2
        async with engine.connect() as connection:
            for table,last in (('moves',10000),('months',20000)):
                assert await connection.scalar(text(f"SELECT last_value FROM public.{table}_id_seq"))==last
            assert await connection.scalar(text('SELECT count(*) FROM chessism_legacy.game'))==4
            after=await snapshot(connection)
        await engine.dispose();engine=None
        await restore._stop_restored_postgres(data)
        await start()
        await check_runtime(socket,data,database='postgres')
        engine=create_async_engine(private_url(socket,'postgres'))
        async with engine.connect() as connection:
            assert await validate_summaries(connection)==result['summaries']
            assert await snapshot(connection)==after
        print('PASS rebuilt migration, atomic commit, sequences, originals retained, runtime and restart',flush=True)
    finally:
        if conn: await conn.close()
        if engine: await engine.dispose()
        await restore._stop_restored_postgres(data)
        if not (data/'postmaster.pid').exists():socket.rmdir()


if __name__=='__main__':asyncio.run(main())
