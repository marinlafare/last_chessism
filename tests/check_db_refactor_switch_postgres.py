"""Canonical switch/rollback fixture. Disposable network-none PostgreSQL ONLY."""
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
    from scripts.db_refactor_application_checks import prepare_application_copy
    from scripts.db_refactor_switch_rehearsal import exercise_switch
    base=Path('/restore-test/db-refactor-baseline-switch-fixture')
    base.mkdir(mode=0o700)
    data,socket=base/'pgdata',base/'socket';socket.mkdir(mode=0o700)
    pgctl='/usr/lib/postgresql/15/bin/pg_ctl'
    await command('/usr/lib/postgresql/15/bin/initdb','-D',str(data),'-U','chessism_user','--auth-local=trust','--auth-host=reject')
    conn=None
    try:
        await command(pgctl,'-D',str(data),'-l',str(base/'postgres.log'),'-w','-o',
            f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await conn.execute(FIXTURE)
        await conn.execute('''ALTER TABLE player ADD COLUMN deleted_at timestamptz;
            CREATE TABLE months(id serial PRIMARY KEY,player_name varchar NOT NULL REFERENCES player(player_name),
                year int,month int,n_games int,UNIQUE(player_name,year,month));
            INSERT INTO months(player_name,year,month,n_games) VALUES('alice',2025,1,3);
            CREATE TABLE home_summary_source(id int PRIMARY KEY,revision bigint NOT NULL);
            INSERT INTO home_summary_source VALUES(1,1);
            CREATE TABLE player_stats(player_name varchar PRIMARY KEY REFERENCES player(player_name) ON DELETE CASCADE);
            INSERT INTO player_stats VALUES('alice');
            CREATE TABLE main_character_mode_summary(player_name varchar PRIMARY KEY REFERENCES player(player_name) ON DELETE CASCADE);
            INSERT INTO main_character_mode_summary VALUES('bob');
            CREATE FUNCTION invalidate_home_summary_v1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                UPDATE public.home_summary_source SET revision=revision+1 WHERE id=1; RETURN NULL; END $$;''')
        for table,columns in (('player','title,deleted_at,player_name'),('game','white,black'),
                              ('fen','fen'),('game_fen_association','game_link,fen_fen')):
            for suffix,event in (('rows','INSERT OR DELETE OR TRUNCATE'),('values',f'UPDATE OF {columns}')):
                await conn.execute(f'CREATE TRIGGER home_summary_v1_{table}_{suffix} AFTER {event} ON {table} '
                                   'FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary_v1()')
        async def publish(*args,**kwargs): pass
        # Deleted/rolled-back rows can leave sequence state well above max(id).
        await conn.execute("SELECT setval(pg_get_serial_sequence('public.moves','id'),10000,true)")
        await conn.execute("SELECT setval(pg_get_serial_sequence('public.months','id'),20000,true)")
        await prepare_reference(conn,data)
        await build_candidate(conn,data,publish)
        await prepare_application_copy(conn,data,publish)
        for table,last in (('moves',10000),('months',20000)):
            for schema in (('refactor_candidate_v1','refactor_archive_v1') if table=='moves' else ('refactor_candidate_v1',)):
                sequence=await conn.fetchval('SELECT pg_get_serial_sequence($1,$2)',schema+'.'+table,'id')
                self_sql=await conn.fetchval("SELECT format('SELECT last_value FROM %s',$1::regclass)",sequence)
                assert await conn.fetchval(self_sql)==last
        await conn.close();conn=None
        report=await exercise_switch(socket,data,publish,database='postgres')
        assert 'complete_rollback_exact' in report['checks'] and 'injected_rollback_exact' in report['checks']
        assert report['committed'] is False
        print('PASS canonical promotion: contract, active FKs, numeric triggers, read views, injected-failure rollback and success rollback; no release marker',flush=True)
        # Model the controller's lifetime lock on a separate private connection;
        # ingestion/projection repair must still serialize only with each other.
        from chessism_api.database.relational_storage import INGESTION_COMMIT_LOCK
        from chessism_api.operations.ingestion_pipeline import relational_repository
        from chessism_api.operations import relational_projection
        assert relational_repository.INGESTION_COMMIT_LOCK==relational_projection.INGESTION_COMMIT_LOCK==INGESTION_COMMIT_LOCK
        controller=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        writer=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        repair=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        try:
            await controller.fetchval('SELECT pg_advisory_lock(731946219)')
            async with writer.transaction():
                assert await writer.fetchval('SELECT pg_try_advisory_xact_lock($1)',INGESTION_COMMIT_LOCK)
                async with repair.transaction():
                    assert not await repair.fetchval('SELECT pg_try_advisory_xact_lock($1)',INGESTION_COMMIT_LOCK)
            async with repair.transaction():
                assert await repair.fetchval('SELECT pg_try_advisory_xact_lock($1)',INGESTION_COMMIT_LOCK)
            print('PASS controller/ingestion lock independence and ingestion/projection mutual exclusion',flush=True)
        finally:
            await repair.close();await writer.close();await controller.close()
        from scripts.db_refactor_release_rehearsal import commit_private_release,check_runtime
        release=await commit_private_release(socket,data,publish,database='postgres')
        assert release['committed_to_private_copy'] and release['production_ready'] is False
        print('PASS private committed switch, approval gate, shared API/worker/controller initializers and sequence high-water preservation',flush=True)
        await command(pgctl,'-D',str(data),'-m','fast','-w','stop')
        await command(pgctl,'-D',str(data),'-l',str(base/'restart-postgres.log'),'-w','-o',
            f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        restarted=await check_runtime(socket,data,database='postgres')
        assert len(restarted['checks'])==4
        await command(sys.executable,'/candidate/scripts/db_refactor_runtime_probe.py',
            '--workspace',str(base),'--database','postgres')
        print('PASS canonical startup after PostgreSQL restart; no cloud clients or queue consumers started',flush=True)
    finally:
        if conn: await conn.close()
        await command(pgctl,'-D',str(data),'-m','fast','-w','stop')


if __name__=='__main__':
    asyncio.run(main())
