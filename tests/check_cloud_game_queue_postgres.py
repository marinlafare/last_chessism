"""Disposable, network-isolated PG checks. Never connects to an environment DB."""
import asyncio
from contextlib import suppress
import json
from pathlib import Path
import tempfile
import time
from unittest.mock import patch
from uuid import uuid4

import asyncpg
from sqlalchemy import event, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from chessism_api.database import models as m, cloud_columns
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.database.ask_db import get_game_set_fens_for_analysis
from chessism_api.operations import database_restore_tests as restore
from chessism_api.operations.cloud_analysis import game_fen_queue as queue
from chessism_api.operations.cloud_analysis.preparation import PreparationTimings
from chessism_api.operations.cloud_analysis.migrate_game_fen_queue import upgrade


async def fixture(engine, conn):
    classes = [m.Fen, m.CloudAnalysisJob, m.CloudAnalysisRun, m.CloudBatchUnit,
               m.CloudFenClaim, m.CloudPhaseTiming, m.CloudGameFenQueue, m.CloudGameFenCandidate, m.CloudResultBatch,
               m.CloudBatchSequence, m.CloudBatchCycle, m.CloudSequenceControl, m.CloudJobCancellation]
    tables = [cloud_columns.ROOTS, *cloud_columns.TABLES.values(), *(model.__table__ for model in classes)]
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: m.Base.metadata.create_all(c, tables=tables))
    await conn.execute('''CREATE TABLE game_fen_association(
        game_link bigint NOT NULL, fen_id bigint NOT NULL REFERENCES fen(fen_id),
        n_move integer NOT NULL, is_white boolean NOT NULL,
        move_color text GENERATED ALWAYS AS (CASE WHEN is_white THEN 'white' ELSE 'black' END) STORED);
        CREATE INDEX fixture_game_moves ON game_fen_association(game_link,n_move,is_white)''')
    # Text order differs from insertion/ID order; shared FENs and repeated moves
    # deliberately have their earliest move in a later-priority game.
    await conn.execute('''INSERT INTO fen(fen,n_games,moves_counter,score) VALUES
        ('z-shared',1,'{}',NULL),('a-shared',1,'{}',NULL),('m-white',1,'{}',NULL),
        ('n-black',1,'{}',NULL),('already-scored',1,'{}',0),('other-game',1,'{}',NULL);
        INSERT INTO game_fen_association VALUES
        (11,1,10,true),(11,2,10,true),(22,1,1,true),(22,2,1,true),
        (11,3,2,true),(11,4,2,false),(11,3,20,true),(22,5,2,true),(33,6,1,true)''')


async def new_job(factory, links):
    job = m.CloudAnalysisJob(id=uuid4().hex, status='running', target=1000, imported=0,
        selection={'mode': 'games', 'game_links': links, 'backend': 'batch_spot',
                   'execution_mode': 'batch_sequence_v2', 'n_vms': 1})
    async with factory.begin() as session:
        session.add(job)
        await session.flush()
        run = m.CloudAnalysisRun(id=uuid4().hex, job_id=job.id, status='selecting',
                                positions=[], launch={}, receipts={})
        session.add(run)
    return job, run


async def claim(session, fens, run):
    await session.execute(text('''INSERT INTO cloud_fen_claim(fen,run_id)
        SELECT unnest(CAST(:fens AS text[])), :run'''), {'fens': fens, 'run': run.id})
    await session.commit()
    await session.close()


async def count_rows(conn, job):
    return await conn.fetchval('SELECT count(*) FROM cloud_game_fen_candidate WHERE job_id=$1', job.id)


async def correctness(engine, conn, factory):
    job, run = await new_job(factory, [11,22,11])
    timings = PreparationTimings()
    old, expected = await get_game_set_fens_for_analysis([11,22,11], 50, raise_errors=True)
    await old.close()
    assert expected == ['a-shared','z-shared','m-white','n-black'], expected
    session, actual = await queue.get_game_queue_fens(job.id, [11,22,11], 2, timings=timings)
    assert actual == expected[:2]
    await session.close()  # Validation failure/crash before claims: all rows return.
    assert await count_rows(conn, job) == 4
    session, actual = await queue.get_game_queue_fens(job.id, [11,22,11], 2, timings=timings)
    assert actual == expected[:2]
    await claim(session, actual, run)
    assert await count_rows(conn, job) == 2
    # New engine/session factory simulates a controller restart, not an in-memory cache.
    restarted = async_sessionmaker(engine, expire_on_commit=False, info=factory.kw['info'])
    with patch.object(queue, 'AsyncDBSession', restarted):
        session, rest = await queue.get_game_queue_fens(job.id, [11,22,11], 2, timings=timings)
        assert rest == expected[2:]
        await claim(session, rest, run)
    assert await count_rows(conn, job) == 0
    assert sum(row['phase'] == 'prepare_queue_build' for row in timings.rows) == 1
    assert await queue.get_game_queue_fens(job.id,[11,22,11],2) == (None,None)
    try:
        await queue.get_game_queue_fens(job.id,[22,11],2)
        raise AssertionError('Changed ordering was accepted')
    except ValueError as error:
        assert 'selection changed' in str(error)
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=$1',run.id)
    # Both compact and legacy SQL preserve the old order, including empty scopes.
    legacy = async_sessionmaker(engine, expire_on_commit=False)
    other, _ = await new_job(factory, [11,22])
    with patch.object(queue, 'AsyncDBSession', legacy):
        session, actual = await queue.get_game_queue_fens(other.id,[11,22],10)
        assert actual == expected
        await session.close()
    empty, _ = await new_job(factory, [])
    assert await queue.get_game_queue_fens(empty.id,[],10) == (None,None)
    print('PASS exact legacy/compact ordering, deduplication, empty scope, rollback, restart and one-time build', flush=True)

    # Two reservers for the same job serialize; different jobs still share FEN locks.
    concurrent, owner = await new_job(factory,[11,22])
    await queue.ensure_queue(concurrent.id,[11,22])
    async def reserve_one(job, run, limit):
        session, selected = await queue.get_game_queue_fens(job.id,[11,22],limit)
        if session:
            await asyncio.sleep(.02)
            await claim(session,selected,run)
        return selected or []
    groups = await asyncio.wait_for(asyncio.gather(
        reserve_one(concurrent,owner,2),reserve_one(concurrent,owner,2)),10)
    assert sorted(groups[0]+groups[1]) == sorted(expected) and not set(groups[0]) & set(groups[1])
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=$1',owner.id)
    first, r1 = await new_job(factory,[11,22]); second,r2 = await new_job(factory,[11,22])
    groups = await asyncio.wait_for(asyncio.gather(reserve_one(first,r1,2),reserve_one(second,r2,2)),10)
    assert sorted(groups[0]+groups[1]) == sorted(expected) and not set(groups[0]) & set(groups[1])
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=ANY($1::text[])',[r1.id,r2.id])

    locked_job, locked_owner = await new_job(factory,[11,22])
    await queue.ensure_queue(locked_job.id,[11,22])
    async with factory.begin() as blocker:
        await blocker.execute(text("SELECT fen FROM fen WHERE fen='a-shared' FOR UPDATE"))
        session, actual = await asyncio.wait_for(queue.get_game_queue_fens(locked_job.id,[11,22],1),3)
        assert actual == ['z-shared']
        await claim(session,actual,locked_owner)
        assert await count_rows(conn,locked_job) == 3
    session, actual = await queue.get_game_queue_fens(locked_job.id,[11,22],1)
    assert actual == ['a-shared']
    await session.close()
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=$1',locked_owner.id)
    locked_job,_=await new_job(factory,[11,22])
    await queue.ensure_queue(locked_job.id,[11,22])
    # Eligibility can change AFTER building the candidate snapshot.
    await conn.execute("UPDATE fen SET score=20 WHERE fen='a-shared'")
    await conn.execute("INSERT INTO cloud_fen_claim VALUES('z-shared',$1)",run.id)
    session, actual = await queue.get_game_queue_fens(locked_job.id,[11,22],10)
    assert actual == ['m-white','n-black']
    await session.close()
    await conn.execute("UPDATE fen SET score=NULL WHERE fen='a-shared'")
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=$1',run.id)
    # Inject a claim inside the fixture transaction before the safety check;
    # a separate inserter correctly cannot cross these real FEN row locks.
    real_recheck = queue.unclaimed_locked_fens
    async def late_claim(session, fens):
        await session.execute(text("INSERT INTO cloud_fen_claim VALUES('a-shared',:run) ON CONFLICT DO NOTHING"),{'run':run.id})
        return await real_recheck(session,fens)
    with patch.object(queue,'unclaimed_locked_fens',side_effect=late_claim):
        session, actual = await queue.get_game_queue_fens(locked_job.id,[11,22],2)
        assert actual == ['z-shared','m-white']
        await session.close()
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=$1',run.id)
    print('PASS same-job/different-job concurrency, locked-row retry and fresh score/claim rechecks', flush=True)

    before = await count_rows(conn,other)
    try:
        async with factory.begin() as session:
            await queue.discard_queue(session,other.id)
            raise RuntimeError('interrupted cleanup')
    except RuntimeError:
        pass
    assert await count_rows(conn,other) == before
    async with factory.begin() as session:
        await queue.discard_queue(session,other.id)
        await queue.discard_queue(session,other.id)
    assert await count_rows(conn,other) == 0 and await count_rows(conn,locked_job) == 4
    interrupted,_=await new_job(factory,[11,22])
    def fail_build(connection,cursor,statement,parameters,context,executemany):
        if statement.startswith('ANALYZE cloud_game_fen_candidate'):
            raise RuntimeError('interrupted queue build')
    event.listen(engine.sync_engine,'before_cursor_execute',fail_build)
    try:
        await queue.ensure_queue(interrupted.id,[11,22])
        raise AssertionError('Build interruption was ignored')
    except RuntimeError as error:
        assert 'interrupted queue build' in str(error)
    finally:
        event.remove(engine.sync_engine,'before_cursor_execute',fail_build)
    assert await count_rows(conn,interrupted)==0
    assert await conn.fetchval('SELECT built_at FROM cloud_game_fen_queue WHERE job_id=$1',interrupted.id) is None
    await queue.ensure_queue(interrupted.id,[11,22])
    assert await count_rows(conn,interrupted)==4
    # The migration refuses live jobs and a running controller, even in this sandbox.
    try:
        async with engine.begin() as connection:
            await connection.run_sync(upgrade)
        raise AssertionError('Migration allowed an active job')
    except ValueError as error:
        assert 'Finish/recover' in str(error)
    async with engine.connect() as blocker:
        await blocker.execute(text('SELECT pg_advisory_lock(731946219)'))
        try:
            async with engine.begin() as connection:
                await connection.run_sync(upgrade)
            raise AssertionError('Migration allowed a running controller')
        except ValueError as error:
            assert 'controller is running' in str(error)
        finally:
            await blocker.execute(text('SELECT pg_advisory_unlock(731946219)'))
    print('PASS interrupted-build recovery, job-scoped cleanup/rollback and migration live-job/controller guards',flush=True)


async def coordinator(conn,factory):
    import chess
    from chessism_api.operations.cloud_analysis import work_units_controller as fleet, preparation, sequence, controller, cancellation
    board=chess.Board(); fens=[]
    for move in ['e2e4','e7e5','g1f3']:
        board.push_uci(move)
        fens.append(' '.join(board.fen().split()[:4]))
    for index,fen in enumerate(fens):
        ident=await conn.fetchval("INSERT INTO fen(fen,n_games,moves_counter) VALUES($1,1,'{}') RETURNING fen_id",fen)
        await conn.execute('INSERT INTO game_fen_association(game_link,fen_id,n_move,is_white) VALUES(55,$1,$2,true)',ident,index+1)
    job,root=await new_job(factory,[55])
    with patch.object(fleet,'AsyncDBSession',factory),patch.object(preparation,'AsyncDBSession',factory), \
            patch.object(sequence,'AsyncDBSession',factory),patch.object(controller,'AsyncDBSession',factory), \
            patch.object(cancellation,'AsyncDBSession',factory):
        assert await asyncio.wait_for(fleet.reserve_unit(job,root,{'units':[]},0,2),10)==2
        async with factory() as session: root=await session.get(m.CloudAnalysisRun,root.id)
        assert root.position_count==2 and len(root.launch['units'])==1
        assert await asyncio.wait_for(fleet.reserve_unit(job,root,root.launch,0,1),10)==1
        assert await conn.fetchval('SELECT count(*) FROM cloud_batch_unit WHERE root_run_id=$1',root.id)==2
        assert await conn.fetchval("SELECT count(*) FROM cloud_phase_timing WHERE job_id=$1 AND phase='prepare_queue_build'",job.id)==1
        async with factory.begin() as session:
            session.add(m.CloudBatchSequence(job_id=job.id,repeat_count=1,requested_count=3,reserved_count=3))
            await session.flush()
            session.add(m.CloudBatchCycle(job_id=job.id,cycle_index=0,run_id=root.id,target_count=3))
        await sequence.freeze(job.id)
        assert await conn.fetchval('SELECT count(*) FROM cloud_game_fen_queue WHERE job_id=$1',job.id)==0
        assert await conn.fetchval('SELECT target FROM cloud_analysis_job WHERE id=$1',job.id)==3
        assert await conn.fetchval('SELECT count(*) FROM cloud_fen_claim WHERE fen=ANY($1::text[])',fens)==3
        # Invalid payload rolls back queue deletion, child, parent and claims.
        bad,owner=await new_job(factory,[33])
        try:
            await fleet.reserve_unit(bad,owner,{'units':[]},0,1)
            raise AssertionError('Invalid FEN was accepted')
        except ValueError:
            pass
        assert await count_rows(conn,bad)==1
        assert await conn.fetchval('SELECT count(*) FROM cloud_batch_unit WHERE root_run_id=$1',owner.id)==0
        assert await conn.fetchval('SELECT position_count FROM cloud_analysis_run WHERE id=$1',owner.id)==0
        await controller.save_job(bad.id,status='cancelled')
        assert await count_rows(conn,bad)==0
        stopped,stop_root=await new_job(factory,[11,22])
        await queue.ensure_queue(stopped.id,[11,22])
        async with factory.begin() as session:
            session.add(m.CloudJobCancellation(job_id=stopped.id,phase='finishing',batch_plan=None,artifact_plan=None))
        await cancellation.finish(stopped.id,[stop_root.id])
        assert await count_rows(conn,stopped)==0
        assert await conn.fetchval('SELECT status FROM cloud_analysis_job WHERE id=$1',stopped.id)=='cancelled'
        assert await conn.fetchval('SELECT count(*) FROM cloud_fen_claim WHERE fen=ANY($1::text[])',fens)==3
    print('PASS real unit coordinator, atomic claims/checkpoints, validation rollback, freeze, cancellation and terminal cleanup',flush=True)


async def benchmark(engine,conn,factory):
    size,batch=120000,10000
    await conn.execute('''INSERT INTO fen(fen,n_games,moves_counter)
        SELECT 'bench-'||lpad(i::text,6,'0'),1,'{}' FROM generate_series(1,120000) i;
        INSERT INTO game_fen_association(game_link,fen_id,n_move,is_white)
        SELECT 20000+(substring(fen FROM 7)::int-1)/100,fen_id,
               1+(substring(fen FROM 7)::int-1)%100,true FROM fen WHERE fen LIKE 'bench-%';
        INSERT INTO game_fen_association(game_link,fen_id,n_move,is_white)
        SELECT game_link+2000,fen_id,n_move,false FROM game_fen_association WHERE game_link>=20000;
        ANALYZE fen; ANALYZE game_fen_association; ANALYZE cloud_fen_claim;''')
    links=list(range(20000,21200))+list(range(22000,23200))
    old_job,old_run=await new_job(factory,links)
    outputs=[]; started=time.monotonic()
    for _ in range(size//batch):
        await conn.execute('ANALYZE cloud_fen_claim')
        session,fens=await get_game_set_fens_for_analysis(links,batch,raise_errors=True)
        outputs.extend(fens)
        await claim(session,fens,old_run)
    old_seconds=time.monotonic()-started
    await conn.execute('DELETE FROM cloud_fen_claim WHERE run_id=$1',old_run.id)
    new,owner=await new_job(factory,links)
    timings=PreparationTimings(); actual=[]; started=time.monotonic()
    for _ in range(size//batch):
        await conn.execute('ANALYZE cloud_fen_claim')
        session,fens=await queue.get_game_queue_fens(new.id,links,batch,timings=timings)
        actual.extend(fens)
        await claim(session,fens,owner)
    new_seconds=time.monotonic()-started
    assert actual == outputs and len(set(actual)) == size
    assert sum(row['phase']=='prepare_queue_build' for row in timings.rows)==1
    # Plan-only check with a populated queue: all FEN reads must be indexed.
    plan_job,_=await new_job(factory,links)
    await queue.ensure_queue(plan_job.id,links)
    async with factory() as session:
        plan=await session.scalar(text('EXPLAIN (FORMAT JSON) '+queue.page_sql(queue.storage_for(session))),
            {'job_id':plan_job.id,'after':0,'limit':batch})
    def nodes(node):
        yield node
        for child in node.get('Plans',[]): yield from nodes(child)
    assert not any(n.get('Relation Name') in {'fen','game_fen_association'} and n['Node Type']=='Seq Scan'
                   for n in nodes(plan[0]['Plan'])),plan
    print(json.dumps({'PASS':'equal ordered output; bounded indexed page plan', 'synthetic_fens':size,
        'associations':size*2,'batches':size//batch,'old_seconds':round(old_seconds,3),
        'new_seconds_including_build':round(new_seconds,3),
        'build_seconds':round(sum(row['duration_seconds'] for row in timings.rows if row['phase']=='prepare_queue_build'),3),
        'speedup':round(old_seconds/new_seconds,2)}),flush=True)


async def main():
    if Path('/var/lib/postgresql/data/PG_VERSION').exists():
        raise RuntimeError('Production PostgreSQL data must not be mounted')
    root=Path(tempfile.mkdtemp(prefix='game-queue-',dir='/restore-test'))
    pgdata,socket=root/'pgdata',root/'socket'; pgdata.mkdir(); socket.mkdir()
    conn=engine=None
    try:
        await restore._run_command_capture(str(Path(restore.PG_CTL).with_name('initdb')),
            '-D',str(pgdata),'--username=chessism_user','--auth=trust','--locale=C')
        await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(root/'postgres.log'),
            '-w','-o',restore._postgres_start_options(socket),'start')
        conn=await asyncpg.connect(host=str(socket),port=55432,user='chessism_user',database='postgres')
        engine=create_async_engine(URL.create('postgresql+asyncpg',username='chessism_user',
            host=str(socket),port=55432,database='postgres'),connect_args={'server_settings':{'statement_timeout':'30000'}})
        factory=async_sessionmaker(engine,expire_on_commit=False,
            info={SESSION_KEY:RelationalStorage('public','archive','reads','relational_v3')})
        await fixture(engine,conn)
        with patch.object(queue,'AsyncDBSession',factory),patch('chessism_api.database.ask_db.AsyncDBSession',factory):
            await asyncio.wait_for(correctness(engine,conn,factory),90)
            await asyncio.wait_for(coordinator(conn,factory),45)
            await asyncio.wait_for(benchmark(engine,conn,factory),150)
        await conn.execute("UPDATE cloud_analysis_job SET status='complete'")
        for _ in range(2):
            async with engine.begin() as connection:
                await connection.run_sync(upgrade)
        print('PASS additive migration is repeatable after jobs are terminal',flush=True)
    finally:
        if engine: await engine.dispose()
        if conn: await conn.close()
        with suppress(Exception):
            await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-m','fast','-w','stop')


if __name__=='__main__':
    asyncio.run(main())
