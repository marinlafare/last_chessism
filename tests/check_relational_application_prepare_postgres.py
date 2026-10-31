"""Schema-completion smoke test before touching the production-sized private copy."""
import asyncio
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def main():
    import asyncpg
    from tests.check_db_refactor_candidate_postgres import FIXTURE,command
    from scripts.db_refactor_candidate import C,A,prepare_reference,build_candidate
    from scripts.db_refactor_application_checks import prepare_application_copy,read_cases
    base=Path('/restore-test/db-refactor-baseline-app-prepare')
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
        await conn.execute('''CREATE TABLE months(id serial PRIMARY KEY,player_name varchar NOT NULL REFERENCES player(player_name),
            year int,month int,n_games int,UNIQUE(player_name,year,month));
            INSERT INTO months(player_name,year,month,n_games) VALUES('alice',2025,1,3);''')
        async def publish(*args,**kwargs): pass
        await prepare_reference(conn,pgdata)
        await build_candidate(conn,pgdata,publish)
        await prepare_application_copy(conn,pgdata,publish)
        assert await conn.fetchval(f'SELECT count(*) FROM {C}.moves')==2
        assert await conn.fetchval(f'SELECT count(*) FROM {A}.moves')==2
        await conn.execute(f"INSERT INTO {C}.player(player_name,joined) VALUES('only-in-normalized',0)")
        new_id=await conn.fetchval(f"INSERT INTO {C}.months(player_name,year,month,n_games) VALUES('only-in-normalized',2025,1,0) RETURNING id")
        assert new_id>1 and await conn.fetchval('SELECT count(*) FROM public.months')==1
        assert len(read_cases([{'cohort':'test','player_name':'alice'}]))==9
        print('PASS schema completion: moves, independent sequences, real month-ledger FK, unchanged originals and application read cases',flush=True)
        from scripts.db_refactor_cutover import inspect_cutover
        await conn.execute('''CREATE TABLE player_stats(player_name varchar PRIMARY KEY REFERENCES player(player_name));
            CREATE VIEW public.player_name_list AS SELECT player_name FROM player;
            CREATE FUNCTION public.fixture_notice() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
            CREATE TRIGGER fixture_notice AFTER UPDATE ON game FOR EACH STATEMENT EXECUTE FUNCTION public.fixture_notice();''')
        original_oid=await conn.fetchval("SELECT 'public.player'::regclass::oid")
        try:
            await inspect_cutover(conn,pgdata)
        except RuntimeError as error:
            assert 'read-only transaction' in str(error)
        else:
            raise AssertionError('Inventory must require read-only mode')
        async with conn.transaction(readonly=True):
            inventory=await inspect_cutover(conn,pgdata)
        assert any(r['source_table']=='player_stats' and r['action']=='retarget_active_foreign_key'
                   for r in inventory['foreign_keys'])
        assert any(r['name']=='fixture_notice' for r in inventory['triggers'])
        assert any(r['name']=='player_name_list' for r in inventory['dependent_views'])
        assert any(r['table']=='months' and r['table_schema']==C for r in inventory['sequences'])
        assert await conn.fetchval("SELECT 'public.player'::regclass::oid")==original_oid
        print('PASS read-only cutover inventory: external FK, dependent view, trigger/function, sequences and unchanged original table identity',flush=True)
    finally:
        if conn: await conn.close()
        await command(pgctl,'-D',str(pgdata),'-m','fast','-w','stop')


if __name__=='__main__':
    asyncio.run(main())
