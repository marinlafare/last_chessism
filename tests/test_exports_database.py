"""Destructive only inside an explicitly named disposable test database.

Run with EXPORT_TEST_DATABASE=1 and DATABASE_URL ending /chessism_export_test.
Never point this suite at the application database.
"""
import asyncio
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch
import uuid

import httpx
from fastapi import FastAPI, HTTPException
import pyarrow.parquet as pq
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations.exports import config, files, jobs, service
from chessism_api.operations.exports.auth import token_hash
from chessism_api.operations.exports.catalog import SOURCES, tables_for
from chessism_api.operations.exports.models import Base, ExportClient, ExportJob
from chessism_api.operations.exports.query import ExportSpec, compile_query
from chessism_api.redis_client import get_redis_pool
from chessism_api.routers.exports import router

SCHEMA = 'export_test_source'
DDL = [
    f'CREATE SCHEMA {SCHEMA}',
    f'''CREATE TABLE {SCHEMA}.player(id bigint PRIMARY KEY,player_name text,name text,title text,
        joined integer,country text,verified boolean,deleted_at timestamptz)''',
    f'CREATE TABLE {SCHEMA}.game_mode(id smallint PRIMARY KEY,name text)',
    f'''CREATE TABLE {SCHEMA}.game(link bigint PRIMARY KEY,played_at timestamptz,white_player_id bigint,
        black_player_id bigint,white_elo integer,black_elo integer,avg_elo double precision,
        white_result double precision,black_result double precision,white_str_result text,black_str_result text,
        mode_id smallint,time_control text,eco text,n_moves integer,time_elapsed integer)''',
    f'''CREATE TABLE {SCHEMA}.fen(fen_id bigint PRIMARY KEY,fen text,n_games bigint,score integer,
        wdl_win smallint,wdl_draw smallint,wdl_loss smallint,piece_count smallint,analysis_source text,
        analyzed_at timestamptz,tablebase_wdl smallint,tablebase_dtz integer)''',
    f'''CREATE TABLE {SCHEMA}.moves(link bigint,n_move integer,white_move text,black_move text,
        white_reaction_time double precision,black_reaction_time double precision,
        white_time_left double precision,black_time_left double precision,PRIMARY KEY(link,n_move))''',
    f'''CREATE TABLE {SCHEMA}.game_fen_association(game_link bigint,n_move integer,is_white boolean,
        fen_id bigint,UNIQUE(game_link,n_move,is_white))''',
    f'''CREATE TABLE {SCHEMA}.game_player(id bigint PRIMARY KEY,game_link bigint,player_id bigint,is_white boolean)''',
    f"INSERT INTO {SCHEMA}.player(id,player_name,verified,joined) VALUES(1,'alice',NULL,0),(2,'hikaru',TRUE,1),(3,'bob',FALSE,2)",
    f"INSERT INTO {SCHEMA}.game_mode VALUES(1,'blitz')",
    f'''INSERT INTO {SCHEMA}.game(link,played_at,white_player_id,black_player_id,mode_id,n_moves,time_elapsed,white_elo)
        VALUES(10,'2026-01-01T00:00:00.123456Z',1,2,1,1,300,2000),
              (11,'2026-01-01T00:00:00.123456Z',2,1,1,1,300,2100),
              (9007199254740993,'2025-01-01T00:00:00Z',2,3,1,1,300,2200)''',
    f"INSERT INTO {SCHEMA}.fen(fen_id,fen,n_games,score) VALUES(1,'position-one',2,NULL),(2,'position-two',1,-30)",
    f"INSERT INTO {SCHEMA}.moves(link,n_move,white_move,black_move) VALUES(10,1,'e4','e5'),(11,1,'d4','--')",
    f'INSERT INTO {SCHEMA}.game_fen_association VALUES(10,1,TRUE,1),(10,1,FALSE,2),(11,1,TRUE,1)',
    f'INSERT INTO {SCHEMA}.game_player VALUES(1,10,1,TRUE),(2,10,2,FALSE),(3,11,2,TRUE),(4,11,1,FALSE)',
]


@unittest.skipUnless(os.environ.get('EXPORT_TEST_DATABASE') == '1', 'isolated database test only')
class ExportDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        url=os.environ['DATABASE_URL']
        if not url.endswith('/chessism_export_test'):
            raise RuntimeError('Refusing any database other than chessism_export_test')
        self.engine=create_async_engine(url)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.execute(text(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE'))
            await conn.run_sync(Base.metadata.create_all)
            for statement in DDL:await conn.execute(text(statement))
        self.old_bind=AsyncDBSession.kw.get('bind');self.old_info=AsyncDBSession.kw.get('info',{})
        AsyncDBSession.configure(bind=self.engine,info={SESSION_KEY:RelationalStorage(SCHEMA,'unused_archive','unused_reads','relational_v3')})
        self.directory=TemporaryDirectory()
        self.patches=[patch.object(config,'ROOT',Path(self.directory.name)),patch.object(config,'FREE_FLOOR',0)]
        for item in self.patches:item.start()
        self.client=ExportClient(id=uuid.uuid4().hex,name='test',token_hash=token_hash('chx_test-secret'),
            allowed_sources=list(SOURCES),max_rows=100000,expires_at=service.now()+timedelta(days=1))
        async with AsyncDBSession() as session:session.add(self.client);await session.commit()
        self.redis=AsyncMock()

    async def asyncTearDown(self):
        AsyncDBSession.configure(bind=self.old_bind,info=self.old_info)
        for item in reversed(self.patches):item.stop()
        self.directory.cleanup();await self.engine.dispose()

    async def submit(self, **kwargs):
        spec=ExportSpec(source='games',select=['game_id','played_at','white_player'],**kwargs)
        return await service.submit(self.client,spec,uuid.uuid4().hex,self.redis)

    async def test_all_sources_have_exact_row_semantics_and_no_unneeded_fanout(self):
        expected={'games':3,'players':3,'positions':2,'game_positions':3,'moves':3,'player_games':4}
        async with AsyncDBSession() as session:
            for name,source in SOURCES.items():
                spec=ExportSpec(source=name,select=list(source.columns),limit=100)
                query,params=compile_query(spec,tables_for(session))
                rows=(await session.execute(query,params)).mappings().all()
                self.assertEqual(len(rows),expected[name],name)
                identities=[tuple(row[k] for k in source.keys) for row in rows]
                self.assertEqual(len(set(identities)),len(rows),name)

    async def test_real_http_auth_preview_export_manifest_file_ownership_and_revocation(self):
        app=FastAPI();app.include_router(router,prefix='/exports/v1')
        app.dependency_overrides[get_redis_pool]=lambda:self.redis
        headers={'Authorization':'Bearer chx_test-secret','Idempotency-Key':'integration-001'}
        spec={'source':'games','select':['game_id','played_at','white_player','average_rating'],'limit':2}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as http:
            self.assertEqual((await http.get('/exports/v1/catalog',headers=headers)).status_code,200)
            preview=(await http.post('/exports/v1/preview',json=spec,headers=headers)).json()
            self.assertEqual(preview['row_count'],2)
            response=await http.post('/exports/v1',json=spec,headers=headers)
            self.assertEqual(response.status_code,202,response.text);identity=response.json()['id']
            again=await http.post('/exports/v1',json=spec,headers=headers)
            self.assertEqual(again.json()['id'],identity)
            conflict=await http.post('/exports/v1',json={**spec,'limit':1},headers=headers)
            self.assertEqual(conflict.status_code,409)
            self.assertEqual((await http.get(f'/exports/v1/{identity}/file',headers=headers)).status_code,409)
            result=await jobs.run_export({'redis':self.redis},identity);self.assertEqual(result['status'],'complete')
            manifest=(await http.get(f'/exports/v1/{identity}/manifest',headers=headers)).json()
            self.assertTrue(manifest['limit_reached']);self.assertEqual(manifest['row_count'],2)
            download=await http.get(f'/exports/v1/{identity}/file',headers=headers)
            self.assertEqual(hashlib.sha256(download.content).hexdigest(),manifest['file']['sha256'])
            self.assertEqual(len(download.content),manifest['file']['size_bytes'])
            table=pq.read_table(files.job_dir(identity)/'data.parquet')
            self.assertEqual(table['game_id'].to_pylist(),[10,11])
            self.assertEqual(table['played_at'].to_pylist()[0].microsecond,123456)
            self.assertEqual(table['average_rating'].to_pylist(),[None,None])
            other=ExportClient(id=uuid.uuid4().hex,name='other',token_hash=token_hash('chx_other'),
                allowed_sources=['players'],max_rows=100000,expires_at=service.now()+timedelta(days=1))
            async with AsyncDBSession() as session:session.add(other);await session.commit()
            self.assertEqual((await http.get(f'/exports/v1/{identity}',headers={'Authorization':'Bearer chx_other'})).status_code,404)
            self.assertEqual((await http.delete(f'/exports/v1/{identity}',headers=headers)).status_code,204)
            self.assertFalse(files.job_dir(identity).exists())
            self.assertEqual((await http.get(f'/exports/v1/{identity}',headers=headers)).status_code,410)
            async with AsyncDBSession() as session:
                row=await session.get(ExportClient,self.client.id);row.revoked_at=service.now();await session.commit()
            self.assertEqual((await http.get('/exports/v1/catalog',headers=headers)).status_code,401)

    async def test_cancellation_empty_snapshot_and_restart_cleanup(self):
        job=await self.submit();await service.cancel(self.client,job['id'])
        result=await jobs.run_export({'redis':self.redis},job['id']);self.assertEqual(result['status'],'cancelled')
        self.assertFalse(files.job_dir(job['id']).exists())
        empty=await self.submit(where={'field':'game_id','op':'eq','value':0})
        self.assertEqual((await jobs.run_export({'redis':self.redis},empty['id']))['status'],'complete')
        self.assertEqual(pq.read_table(files.job_dir(empty['id'])/'data.parquet').num_rows,0)
        interrupted=await self.submit()
        async with AsyncDBSession() as session:
            row=await session.get(ExportJob,interrupted['id']);row.status='running'
            expired=await session.get(ExportJob,empty['id']);expired.expires_at=service.now()-timedelta(seconds=1)
            await session.commit()
        await jobs.maintain_exports({'redis':self.redis})
        async with AsyncDBSession() as session:
            self.assertEqual((await session.get(ExportJob,interrupted['id'])).status,'failed')
            self.assertEqual((await session.get(ExportJob,empty['id'])).status,'expired')
        self.assertFalse(files.job_dir(empty['id']).exists());self.assertFalse(files.job_dir(interrupted['id']).exists())

    async def test_consistent_snapshot_and_exact_limit_not_reported_as_truncation(self):
        async def change_data():
            async with self.engine.begin() as conn:
                await conn.execute(text(f"UPDATE {SCHEMA}.game SET white_elo=9999 WHERE link=11"))
        called=False
        async def disk(function,*args):
            nonlocal called
            result=function(*args)
            if getattr(function,'__name__','')=='write' and not called:
                called=True;await change_data()
            return result
        spec=ExportSpec(source='games',select=['game_id','white_rating'],limit=3)
        job=await service.submit(self.client,spec,'snapshot-test',self.redis)
        with patch.object(config,'BATCH_ROWS',1),patch.object(jobs,'disk_work',disk):
            result=await jobs.run_export({'redis':self.redis},job['id'])
        self.assertEqual(result['status'],'complete')
        table=pq.read_table(files.job_dir(job['id'])/'data.parquet')
        self.assertEqual(table['white_rating'].to_pylist(),[2000,2100,2200])
        self.assertEqual(table['game_id'].to_pylist()[-1],2**53+1)
        manifest=json.loads((files.job_dir(job['id'])/'manifest.json').read_bytes())
        self.assertFalse(manifest['limit_reached'])
        self.assertTrue(called)

    async def test_active_cancellation_size_failure_and_queue_repair_never_publish_files(self):
        request=await self.submit(limit=1)
        original_disk=jobs.disk_work
        async def cancel_while_writing(function,*args):
            result=await original_disk(function,*args)
            if getattr(function,'__name__','')=='write':
                await service.cancel(self.client,request['id'])
            return result
        with patch.object(jobs,'disk_work',cancel_while_writing):
            self.assertEqual((await jobs.run_export({'redis':self.redis},request['id']))['status'],'cancelled')
        self.assertFalse(files.job_dir(request['id']).exists())
        large=await self.submit()
        with patch.object(config,'MAX_BYTES',0):
            self.assertEqual((await jobs.run_export({'redis':self.redis},large['id']))['status'],'failed')
        self.assertFalse(files.job_dir(large['id']).exists())
        self.redis.enqueue_job.side_effect=ConnectionError('offline')
        queued=await self.submit()
        self.assertEqual(queued['status'],'queued')
        self.redis.enqueue_job.side_effect=None;self.redis.enqueue_job.reset_mock()
        await jobs.maintain_exports({'redis':self.redis})
        self.redis.enqueue_job.assert_awaited_once()

    async def test_scope_and_queue_limits_and_revocation_during_execution(self):
        first=await self.submit();second=await self.submit()
        with self.assertRaises(HTTPException) as error:await self.submit()
        self.assertEqual(error.exception.status_code,429)
        with self.assertRaises(HTTPException) as error:await service.delete(self.client,first['id'])
        self.assertEqual(error.exception.status_code,409)
        async with AsyncDBSession() as session:
            client=await session.get(ExportClient,self.client.id);client.revoked_at=service.now();await session.commit()
        self.assertEqual((await jobs.run_export({'redis':self.redis},first['id']))['status'],'cancelled')
        self.assertFalse(files.job_dir(first['id']).exists())


if __name__=='__main__':unittest.main()
