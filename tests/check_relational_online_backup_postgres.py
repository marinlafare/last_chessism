"""Real online backup-worker and WAL restore in a disposable network-none container."""
import asyncio
import os
from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'


async def main():
    import asyncpg
    from tests.check_relational_application_postgres import setup
    from tests.check_db_refactor_candidate_postgres import command
    from scripts.db_refactor_online_backup import OnlineApplicationBackup
    from scripts.fen_rehearsal_readiness import wait_for_recovery_completion
    from chessism_api.operations import database_restore_tests as restore
    token=uuid4().hex[:12]
    base=Path('/restore-test')/('db-refactor-baseline-online-fixture-'+token)
    base.mkdir(mode=0o700)
    pgdata,socket=base/'pgdata',base/'socket'
    socket.mkdir(mode=0o700)
    await command('/usr/lib/postgresql/15/bin/initdb','-D',str(pgdata),'-U','chessism_user','--auth-local=trust','--auth-host=reject')
    work=Path('/restore-test')/('db-refactor-acceptance-'+token)
    restored=Path('/restore-test')/('db-refactor-baseline-online-verify-'+token)/'pgdata'
    async def publish(phase,**detail):
        print(phase,flush=True)
    backup=OnlineApplicationBackup(pgdata,socket,work,restored,publish,{})
    backup.prepare()
    await backup.command('fixture_stanza','--no-online','stanza-create')
    conn=None
    owned=set()
    try:
        owned.add(pgdata)
        await command(restore.PG_CTL,'-D',str(pgdata),'-l',str(base/'postgres.log'),'-w','-o',backup.start_options(),'start')
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await setup(conn,pgdata)
        await conn.execute('''CREATE TABLE matrix_definition(id text,name text,row_type text,config jsonb);
            CREATE TABLE matrix_artifact(id text,status text);
            CREATE TABLE algorithm_definition(id text,name text,config jsonb);
            CREATE TABLE algorithm_run(id text,name text,config jsonb,result jsonb,status text);
            INSERT INTO matrix_definition VALUES('matrix-test','fixture recipe','game','{"test":true}');
            INSERT INTO algorithm_definition VALUES('algorithm-test','fixture algorithm','{"test":true}');
            INSERT INTO algorithm_run VALUES('run-test','fixture run','{}','{"value":42}','complete');''')
        from tests.test_matrix_storage import fixture_snapshot
        artifact_id=str(uuid4())
        artifacts=base/'matrices'
        fixture_snapshot(artifacts,artifact_id)
        await conn.execute("INSERT INTO matrix_artifact VALUES($1,'complete')",artifact_id)
        result=await backup.backup(conn,database='postgres',artifact_root=artifacts)
        await conn.close(); conn=None
        await restore._stop_restored_postgres(pgdata); owned.remove(pgdata)
        await backup.restore(result)
        owned.add(restored)
        await command(restore.PG_CTL,'-D',str(restored),'-l',str(restored.parent/'postgres.log'),'-w',
                      '-o',restore._postgres_start_options(restored.parent/'socket'),'start')
        conn=await asyncpg.connect(host=str(restored.parent/'socket'),port=55432,user='chessism_user',database='postgres')
        await wait_for_recovery_completion(conn)
        verified=await backup.validate(conn,result,database='postgres')
        assert verified['matrices']['count']==1 and verified['definitions']['count']==1
        print('PASS online full + incremental worker backups, v7 manifest, post-backup WAL replay, recovery boundary, recipes, algorithms and matrix file hashes',flush=True)
    finally:
        if conn:
            await conn.close()
        for data in owned:
            await restore._stop_restored_postgres(data)


if __name__=='__main__':
    asyncio.run(main())
