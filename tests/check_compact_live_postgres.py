"""Exercise real compact LIVE transaction on tiny schema-only backup restore.

Requires network none and /restore-test tmpfs. No live database/socket/volume.
"""
import asyncio
import json
import os
from pathlib import Path
import tempfile

import asyncpg
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker

from chessism_api.operations import database_restore_tests as restore
from chessism_api.database.relational_storage import SESSION_KEY,create_relational_engine
from chessism_api.database.backup_contract import contract_differences
from scripts.compact_db_live import migrate,contract,V2,V3
from scripts.db_refactor_live_target import identity


async def run():
    if {p.name for p in Path('/sys/class/net').iterdir()}!={'lo'} or Path('/var/lib/postgresql/data/PG_VERSION').exists():
        raise RuntimeError('Network-none disposable fixture required, without live data')
    root=Path(tempfile.mkdtemp(prefix='compact-db-live-',dir='/restore-test'))
    data,socket=root/'pgdata',root/'socket'
    socket.mkdir()
    conn=observer=engine=outsider=None
    async def start():
        await restore._run_command_capture(restore.PG_CTL,'-D',str(data),'-l',str(root/'postgres.log'),'-w',
            '-o',restore._postgres_start_options(socket),'start')
    try:
        await restore._run_command_capture(str(Path(restore.PG_CTL).with_name('initdb')),'-D',str(data),
            '--username=chessism_user','--auth=trust','--locale=C')
        await start()
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        await conn.execute('CREATE DATABASE chessism_db')
        await conn.close();conn=None
        backup=Path('/backup-volume/chessism/database/snapshots/snapshot-20261013T040040-9175018e24b3.dump')
        await restore._run_command_capture('pg_restore','--schema-only','--exit-on-error','-h',str(socket),
            '-p','55432','-U','chessism_user','-d','chessism_db',str(backup))
        url=URL.create('postgresql+asyncpg',username='chessism_user',host=str(socket),port=55432,database='chessism_db')
        seed_engine=create_async_engine(url,connect_args={'server_settings':{'search_path':V2.search_path}})
        factory=async_sessionmaker(seed_engine,info={SESSION_KEY:V2})
        from scripts.db_refactor_write_checks import fixture_bundle,PLAYER,LINKS
        from chessism_api.operations.ingestion_pipeline.relational_repository import ingest_bundle
        async with factory.begin() as session:
            bundle=fixture_bundle()
            await ingest_bundle(session,bundle['games'],bundle['moves'],bundle['game_openings'])
        async with seed_engine.begin() as c:
            await c.execute(text("INSERT INTO public.relational_storage_release(version,application_ready,rehearsal_only) VALUES('relational_v2',true,false)"))
            await c.execute(text("INSERT INTO public.fen(fen,fen_id,n_games,moves_counter,score,wdl_win,wdl_draw,wdl_loss) VALUES('test-fen',1,1,'#0_1',15,10,980,10)"))
            await c.execute(text("INSERT INTO public.fen_continuation(fen_id,rank,move,score) VALUES(1,2,'e2e4',12)"))
            await c.execute(text("INSERT INTO public.game_fen_association(game_link,fen_id,n_move,move_color) VALUES(:link,1,1,'white')"),{'link':LINKS[0]})
            await c.execute(text("INSERT INTO public.months(player_name,year,month,n_games) VALUES(:p,2025,1,3)"),{'p':PLAYER})
            await c.execute(text("INSERT INTO public.player_stats(player_name,chess_blitz_last_rating,last_updated) VALUES(:p,2500,now())"),{'p':PLAYER})
            await c.execute(text("INSERT INTO public.home_titled_player_summary(player_name,title,games,game_half_units) VALUES(:p,'GM',3,6)"),{'p':PLAYER})
        await seed_engine.dispose()
        observer=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='chessism_db')
        expected=await identity(observer);expected.pop('pid')
        engine=create_async_engine(url,isolation_level='REPEATABLE READ')
        events=[]
        async def publish(phase,**details):events.append((phase,details))
        async def checkpoint():pass
        candidate=json.loads(Path('/candidate/.local-migrations/db-refactor/compact-index-retune-001/candidate-contract.json').read_text())
        async with engine.connect() as c:
            async with c.begin():original=await contract(c,V2)
            async def unchanged():
                async with c.begin():
                    assert not contract_differences(original,await contract(c,V2))
                    assert await c.scalar(text('SELECT version FROM public.relational_storage_release'))=='relational_v2'
                    assert await c.scalar(text('SELECT score FROM public.fen WHERE fen_id=1'))==15.0
                    assert await c.scalar(text('SELECT count(*) FROM public.moves'))==12
            async def rejected(target=expected,**kw):
                try:await migrate(c,target,observer.get_server_pid(),original,candidate,publish,checkpoint,**kw)
                except RuntimeError:return
                raise AssertionError('Invalid live apply accepted')
            await rejected({**expected,'system_identifier':'wrong'})
            outsider=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='chessism_db')
            await rejected()
            await outsider.close();outsider=None
            await unchanged()
            async with c.begin():await c.execute(text("INSERT INTO public.cloud_analysis_job(id,status,target,imported,created_at,updated_at) VALUES('test-live-guard','running',1,0,now(),now())"))
            await rejected()
            async with c.begin():await c.execute(text("DELETE FROM public.cloud_analysis_job WHERE id='test-live-guard'"))
            print('PASS wrong identity, connected client and active cloud work rejected',flush=True)
            try:
                await migrate(c,expected,observer.get_server_pid(),original,candidate,publish,checkpoint,inject_failure=True)
            except RuntimeError as error:assert 'Injected compact live failure' in str(error),str(error)
            else:raise AssertionError('Injected rollback not reached')
            await unchanged()
            assert any(p=='compact_live_transaction_rolled_back' and d['rollback_verified'] for p,d in events)
            print('PASS exact full schema, integer conversions, release update and transaction rollback',flush=True)
            async def cancel():raise asyncio.CancelledError()
            try:await migrate(c,expected,observer.get_server_pid(),original,candidate,publish,cancel)
            except asyncio.CancelledError:pass
            else:raise AssertionError('Cancellation ignored')
            await unchanged()
            result=await migrate(c,expected,observer.get_server_pid(),original,candidate,publish,checkpoint)
            assert result['committed'] and not contract_differences(candidate,result['contract'])
        await engine.dispose();engine=None
        await observer.close();observer=None
        await restore._stop_restored_postgres(data)
        await start()
        ready=await create_relational_engine(url,V3)
        async with ready.connect() as c:
            assert await c.scalar(text('SELECT score FROM public.fen WHERE fen_id=1'))==15
            assert await c.scalar(text('SELECT count(*) FROM public.moves'))==12
            assert await c.scalar(text('SELECT count(*) FROM chessism_archive.moves'))==4
            assert await c.scalar(text('SELECT is_white FROM public.game_fen_association')) is True
        await ready.dispose()
        try:wrong=await create_relational_engine(url,V2)
        except RuntimeError:pass
        else:
            await wrong.dispose()
            raise AssertionError('Old runtime profile accepted compact schema')
        print('PASS cancellation, atomic commit, PostgreSQL restart, v3 activation and v2 rejection',flush=True)
    finally:
        if outsider:await outsider.close()
        if conn:await conn.close()
        if observer:await observer.close()
        if engine:await engine.dispose()
        await restore._stop_restored_postgres(data)


if __name__=='__main__':asyncio.run(run())
