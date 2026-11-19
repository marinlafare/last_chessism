"""Native export ownership, atomic admission and cancellation/resource regressions."""
import asyncio
from contextlib import redirect_stdout
import io
import hashlib
from datetime import timedelta
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import uuid
from unittest.mock import AsyncMock, patch

import asyncpg
from fastapi import HTTPException
from sqlalchemy import text

from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations.exports import analytical_reader, cli, config, files, jobs, service
from chessism_api.operations.exports.models import ExportClient, ExportJob
from tests.test_exports_database import ExportDatabaseFixture
from tests.test_analytical_exports_database import AnalyticalDatabaseFixture


class CanonicalExportJobTests(ExportDatabaseFixture):
    async def test_initializer_owns_metadata_and_owner_idempotency_constraints(self):
        self.assertFalse(await canonical.initialize(self.db))
        self.assertEqual(await canonical.validate(self.db),12)
        first=await self.submit()
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute('UPDATE export_job SET client_id=$1 WHERE id=$2','missing',first['id'])
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.db.execute('UPDATE export_job SET rows_written=-1 WHERE id=$1',first['id'])
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute('DELETE FROM export_client WHERE id=$1',self.client.id)
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO export_job SELECT * FROM export_job WHERE id=$1',first['id'])

    async def test_concurrent_same_key_admission_keeps_one_immutable_request(self):
        from chessism_api.operations.exports.query import ExportSpec
        spec=ExportSpec(source='games',select=['game_id'])
        a,b=await asyncio.gather(service.submit(self.client,spec,'same-request',self.redis),
                                 service.submit(self.client,spec,'same-request',self.redis))
        self.assertEqual(a['id'],b['id'])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM export_job'),1)
        self.assertEqual([p.name for p in Path(self.directory.name).iterdir()],[a['id']])
        self.assertEqual(files.read_request(a['id']),service.request_document(spec))

    async def test_owner_checks_protect_status_cancel_delete_and_independent_keys(self):
        from chessism_api.operations.exports.query import ExportSpec
        other=ExportClient(id=uuid.uuid4().hex,name='other',token_hash='f'*64,
            allowed_sources=['games'],max_rows=1000,expires_at=service.now()+timedelta(days=1))
        async with AsyncDBSession() as session:session.add(other);await session.commit()
        spec=ExportSpec(source='games',select=['game_id'],limit=2)
        first=await service.submit(self.client,spec,'shared-key',self.redis)
        second=await service.submit(other,spec,'shared-key',self.redis)
        self.assertNotEqual(first['id'],second['id'])
        for method in (service.owned_job,service.cancel,service.delete):
            with self.assertRaises(HTTPException) as error:await method(other,first['id'])
            self.assertEqual(error.exception.status_code,404)
        row=await self.db.fetchrow('SELECT status,cancel_requested FROM export_job WHERE id=$1',first['id'])
        self.assertEqual(tuple(row),('queued',False));self.assertTrue(files.job_dir(first['id']).exists())

    async def test_failed_admission_rolls_back_metadata_and_removes_only_its_request(self):
        first=await self.submit();original=AsyncDBSession
        def broken():
            session=original()
            async def flush():raise RuntimeError('synthetic admission failure')
            session.flush=flush;return session
        with patch.object(service,'AsyncDBSession',broken),self.assertRaisesRegex(RuntimeError,'synthetic admission'):
            await self.submit()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM export_job'),1)
        self.assertEqual([p.name for p in Path(self.directory.name).iterdir()],[first['id']])
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_cancellation_after_commit_preserves_recoverable_request_and_identity(self):
        original=AsyncDBSession; first_close=True
        def factory():
            session=original();close=session.close
            async def cancelled_close():
                nonlocal first_close
                await close()
                if first_close:first_close=False;raise asyncio.CancelledError()
            session.close=cancelled_close;return session
        from chessism_api.operations.exports.query import ExportSpec
        spec=ExportSpec(source='games',select=['game_id'])
        with patch.object(service,'AsyncDBSession',factory),self.assertRaises(asyncio.CancelledError):
            await service.submit(self.client,spec,'committed-request',self.redis)
        identity=await self.db.fetchval('SELECT id FROM export_job')
        self.assertIsNotNone(identity);self.assertTrue((files.job_dir(identity)/'request.json').exists())
        self.assertEqual((await service.submit(self.client,spec,'committed-request',self.redis))['id'],identity)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_worker_cancellation_drains_child_before_releasing_lock_and_removing_files(self):
        for rollback_fails in (False,True):
            job=await self.submit();entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
            def factory():
                session=AsyncDBSession();rollback,close=session.rollback,session.close
                async def delayed():
                    rolling.set();await release.wait()
                    if rollback_fails:raise RuntimeError('synthetic rollback failure')
                    await rollback()
                async def closing():
                    try:await close()
                    finally:closed.set()
                session.rollback=delayed;session.close=closing;return session
            async def constructing(job):
                async with read_transaction(factory) as session:
                    await session.execute(text('SELECT 1'));entered.set();await asyncio.Future()
            with patch.object(jobs,'construct',constructing):
                task=asyncio.create_task(jobs.run_export({},job['id']))
                try:
                    await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
                    for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
                    self.assertFalse(closed.is_set());self.assertTrue(files.job_dir(job['id']).exists())
                    self.assertEqual(await jobs.run_export({},job['id']),{'status':'busy'})
                finally:release.set();await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
            self.assertEqual(await self.db.fetchval('SELECT status FROM export_job WHERE id=$1',job['id']),'cancelled')
            self.assertFalse(files.job_dir(job['id']).exists());self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_timeout_waits_for_cleanup_and_publishes_failure(self):
        job=await self.submit();entered,release,closed=(asyncio.Event() for _ in range(3))
        async def constructing(job):
            try:entered.set();await asyncio.Future()
            finally:await release.wait();closed.set()
        with patch.object(jobs,'construct',constructing),patch.object(config,'TIMEOUT',.1):
            task=asyncio.create_task(jobs.run_export({},job['id']))
            try:
                await asyncio.wait_for(entered.wait(),2);await asyncio.sleep(.15)
                self.assertFalse(task.done());self.assertTrue(files.job_dir(job['id']).exists());release.set()
                self.assertEqual(await task,{'status':'failed'})
            finally:release.set();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(closed.is_set());self.assertFalse(files.job_dir(job['id']).exists())
        self.assertEqual(await self.db.fetchval('SELECT error_code FROM export_job WHERE id=$1',job['id']),'EXPORT_TIMEOUT')
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_cancelled_admission_waits_for_file_write_before_removal(self):
        loop=asyncio.get_running_loop();entered=asyncio.Event();release=threading.Event();finished=threading.Event()
        original=files.write_json
        def write(*args):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):raise RuntimeError('test release timeout')
            original(*args);finished.set()
        with patch.object(files,'write_json',write):
            task=asyncio.create_task(self.submit())
            try:
                await asyncio.wait_for(entered.wait(),2)
                for _ in range(3):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
            finally:release.set();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(finished.is_set());self.assertTrue(task.cancelled())
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM export_job'),0)
        self.assertEqual(list(Path(self.directory.name).iterdir()),[]);self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_credential_cli_validates_schema_and_preserves_scopes_without_schema_writes(self):
        path=Path(self.directory.name)/'synthetic.token'
        args=SimpleNamespace(action='create-client',name='synthetic',sources=['games'],max_rows=50,days=1,output=str(path))
        with patch.object(cli,'bind_runtime_engine',AsyncMock(return_value=self.engine)),redirect_stdout(io.StringIO()):
            await cli.main(SimpleNamespace(action='install'))
            await cli.main(args)
            identity=await self.db.fetchval("SELECT id FROM export_client WHERE name='synthetic'")
            await cli.main(SimpleNamespace(action='enable-analytics',id=identity))
            await cli.main(SimpleNamespace(action='revoke-client',id=identity))
        row=await self.db.fetchrow('SELECT * FROM export_client WHERE id=$1',identity)
        token=path.read_text().strip()
        self.assertEqual(row['token_hash'],hashlib.sha256(token.encode()).hexdigest())
        self.assertEqual(row['allowed_sources'],['games']);self.assertEqual(row['max_rows'],50)
        self.assertTrue(row['allow_analytics']);self.assertIsNotNone(row['revoked_at'])
        self.assertEqual(path.stat().st_mode & 0o777,0o600);self.assertEqual(await canonical.validate(self.db),12)

    async def test_cancelled_credential_commit_keeps_the_private_token_for_committed_client(self):
        original=AsyncDBSession;first=True;path=Path(self.directory.name)/'synthetic.token'
        def factory():
            session=original();close=session.close
            async def cancelled():
                nonlocal first
                await close()
                if first:first=False;raise asyncio.CancelledError()
            session.close=cancelled;return session
        args=SimpleNamespace(action='create-client',name='synthetic',sources=['games'],max_rows=50,days=1,output=str(path))
        with patch.object(cli,'bind_runtime_engine',AsyncMock(return_value=self.engine)),patch.object(cli,'AsyncDBSession',factory),self.assertRaises(asyncio.CancelledError):
            await cli.main(args)
        self.assertTrue(path.exists())
        self.assertEqual(await self.db.fetchval("SELECT token_hash FROM export_client WHERE name='synthetic'"),hashlib.sha256(path.read_text().strip().encode()).hexdigest())


class CanonicalAnalyticalCleanupTests(AnalyticalDatabaseFixture):
    async def test_repeated_cancellation_waits_for_reader_close_even_when_rollback_fails(self):
        original=analytical_reader.asyncpg.connect
        for rollback_fails in (False,True):
            entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
            class Connection:
                def __init__(self,conn):self.conn=conn
                def __getattr__(self,key):return getattr(self.conn,key)
                def transaction(self,**kwargs):
                    transaction=self.conn.transaction(**kwargs)
                    async def rollback():
                        rolling.set();await release.wait()
                        if rollback_fails:raise RuntimeError('synthetic reader rollback failure')
                        await transaction.rollback()
                    return SimpleNamespace(start=transaction.start,rollback=rollback)
                async def close(self,**kwargs):
                    try:await self.conn.close(**kwargs)
                    finally:closed.set()
            async def connect(*args,**kwargs):return Connection(await original(*args,**kwargs))
            async def reading():
                async with analytical_reader.connection() as conn:
                    self.assertEqual(await conn.fetchval('SELECT count(*) FROM exports_data_v1.games'),3)
                    entered.set();await asyncio.Future()
            with patch.object(analytical_reader.asyncpg,'connect',connect):
                task=asyncio.create_task(reading())
                try:
                    await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
                    for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
                finally:release.set();await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
            self.assertEqual(await self.db.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='chessism_export_reader'"),0)
