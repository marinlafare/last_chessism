"""Real reader-role and analytical lifecycle tests, disposable database ONLY."""
import json
import os
from pathlib import Path
from unittest.mock import patch
import uuid

import asyncpg
import httpx
from fastapi import FastAPI
import pyarrow.parquet as pq
from sqlalchemy import text

from tests.test_exports_database import ExportDatabaseFixture, SCHEMA
from chessism_api.database.engine import AsyncDBSession
from chessism_api.operations.exports import analytical_admin, analytical_reader, jobs, service
from chessism_api.operations.exports.catalog import tables_for
from chessism_api.operations.exports.analytical_spec import SQLExportSpec, MoveMatrixSpec, StoredLegacyMoveMatrixSpec
from chessism_api.operations.exports.analytical_spec import AnalyticalPreviewSpec
from chessism_api.operations.exports.analytical_preview import preview
from chessism_api.operations.exports.analytical_reader import AnalyticalError
from chessism_api.operations.exports import config
from chessism_api.operations.exports.models import ExportClient
from chessism_api.redis_client import get_redis_pool
from chessism_api.routers.exports import router


class AnalyticalDatabaseFixture(ExportDatabaseFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # Stable fixture selection; dedicated year-expansion tests override it.
        year_choice=patch('chessism_api.operations.exports.analytical_query.random.Random.choice',
                          lambda _random, years: years[-1])
        year_choice.start()
        self.addCleanup(year_choice.stop)
        self.credential=Path(self.directory.name)/'reader.json'
        async with AsyncDBSession() as session:tables=tables_for(session)
        await analytical_admin.install(self.engine,tables,str(self.credential))
        self.reader_env=patch.dict(os.environ,{'CHESSISM_EXPORT_READER_FILE':str(self.credential)})
        self.reader_env.start()
        async with AsyncDBSession() as session:
            client=await session.get(ExportClient,self.client.id)
            client.allow_analytics=True;await session.commit()
        self.client.allow_analytics=True

    async def asyncTearDown(self):
        self.reader_env.stop()
        await super().asyncTearDown()

    async def run_spec(self,spec):
        job=await service.submit(self.client,spec,uuid.uuid4().hex,self.redis)
        await jobs.run_export({},job['id'])
        status=await service.owned_job(self.client,job['id'])
        return status,Path(self.directory.name)/job['id']


class AnalyticalDatabaseTests(AnalyticalDatabaseFixture):
    async def test_preview_matrix_uses_entire_game_selection_and_preserves_schema(self):
        spec=MoveMatrixSpec(player='hikaru',game_mode='blitz',max_games=2,
                            first_move=1,last_move=40,score_perspective='player')
        result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1))
        self.assertEqual(result['row_count'],1);self.assertTrue(result['has_more'])
        self.assertFalse(result['entire_result_checked'])
        self.assertEqual(result['selection']['selected_games'],2)
        self.assertEqual(result['selection']['selected_participants'],2)
        self.assertEqual(len(result['columns']),44)
        self.assertEqual(result['rows'][0][:5],[10,'hikaru','black','2026-01-01T00:00:00.123456+00:00',30])
        self.assertEqual(result['columns'][3]['type'],'timestamp[us, tz=UTC]')
        self.assertEqual(result['columns'][4]['type'],'int32')
        empty=await preview(AnalyticalPreviewSpec(design=spec.model_copy(update={'player':'unknown'})))
        self.assertEqual(empty['rows'],[]);self.assertFalse(empty['has_more'])
        self.assertEqual(empty['total_row_count'],0);self.assertEqual(len(empty['columns']),44)
        both=await preview(AnalyticalPreviewSpec(design=spec.model_copy(update={'players':'both'}),preview_rows=1))
        self.assertEqual(both['selection']['selected_participants'],4)
        self.assertEqual(both['selection']['result_row_bound'],4)
        async with self.engine.begin() as conn:
            # Corrupt the second game's participant move, outside the first
            # preview row: the same full-selection duplicate guard must fire.
            await conn.execute(text('ALTER VIEW exports_data_v1.moves RENAME TO moves_original'))
            await conn.execute(text('''CREATE VIEW exports_data_v1.moves AS
                SELECT * FROM exports_data_v1.moves_original UNION ALL
                SELECT * FROM exports_data_v1.moves_original WHERE game_id=11'''))
            await conn.execute(text('ALTER VIEW exports_data_v1.moves OWNER TO chessism_export_view_owner'))
            await conn.execute(text('GRANT SELECT ON exports_data_v1.moves TO chessism_export_reader'))
        with self.assertRaises(AnalyticalError) as error:
            await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1))
        self.assertEqual(error.exception.code,'DUPLICATE_MOVE')

    async def test_preview_sql_final_rows_lookahead_dates_nulls_zeros_and_large_ids(self):
        spec=SQLExportSpec(sql='''SELECT game_id, played_at, played_at::date AS day,
            played_at::timestamp AS local_time, 0 AS zero, NULL::int4 AS missing
            FROM exports_data_v1.games ORDER BY game_id''',max_result_rows=3)
        result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=2))
        self.assertTrue(result['has_more']);self.assertIsNone(result['total_row_count'])
        result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=3))
        self.assertFalse(result['has_more']);self.assertEqual(result['total_row_count'],3)
        self.assertEqual(result['rows'][-1][0],'9007199254740993')
        self.assertEqual(result['rows'][0][2],'2026-01-01')
        self.assertEqual(result['rows'][0][3],'2026-01-01T00:00:00.123456')
        self.assertEqual(result['rows'][0][-2:],[0,None])
        aggregate=SQLExportSpec(sql='''SELECT g.game_id, count(m.move_number) AS moves
            FROM exports_data_v1.games g LEFT JOIN exports_data_v1.moves m USING(game_id)
            GROUP BY g.game_id ORDER BY g.game_id''',max_result_rows=3)
        grouped=await preview(AnalyticalPreviewSpec(design=aggregate,preview_rows=1))
        self.assertEqual(grouped['rows'],[[10,2]]);self.assertTrue(grouped['has_more'])

    async def test_null_flag_checks_beyond_preview_and_saved_export_matches(self):
        spec=SQLExportSpec(sql='''SELECT game_id, CASE WHEN game_id=11 THEN NULL ELSE 0 END AS score,
            FALSE AS flag, '' AS empty_text FROM exports_data_v1.games ORDER BY game_id''',
            max_result_rows=3,exclude_null_columns=True)
        with patch.object(config,'BATCH_ROWS',1):
            result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1))
            status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(result['rows'],[[10,False,'']])
        self.assertTrue(result['has_more']);self.assertEqual(result['total_row_count'],3)
        self.assertTrue(result['entire_result_checked'])
        self.assertEqual(result['column_selection']['dropped_columns'],['score'])
        table=pq.read_table(path/'data.parquet')
        self.assertEqual(table.column_names,['game_id','flag','empty_text'])
        self.assertEqual(table.num_rows,3)
        self.assertTrue(all(not field.nullable for field in table.schema))
        self.assertEqual(table['game_id'].to_pylist()[-1],9007199254740993)
        manifest=json.loads((path/'manifest.json').read_text())
        self.assertEqual(manifest['column_selection'],result['column_selection'])
        self.assertEqual(manifest['columns'],result['columns'])
        # Previously admitted matrix exports still honor their stored projection.
        matrix=StoredLegacyMoveMatrixSpec(player='hikaru',game_mode='blitz',max_games=2,
            max_result_rows=2,last_move=2,exclude_null_columns=True)
        status,path=await self.run_spec(matrix)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(path/'data.parquet').num_rows,2)
        self.assertEqual(pq.read_table(path/'data.parquet').column_names,['game_id','player_name','color'])
        # New designs preserve all requested columns, including missing scores.
        status,path=await self.run_spec(MoveMatrixSpec(player='hikaru',game_mode='blitz',max_games=2,last_move=2))
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(path/'data.parquet').column_names,['game_id','player_name','color','played_at','move_1','move_2'])
        empty=SQLExportSpec(sql='SELECT NULL::int4 AS score WHERE FALSE',exclude_null_columns=True)
        result=await preview(AnalyticalPreviewSpec(design=empty))
        self.assertEqual(result['columns'],[{'name':'score','type':'int32','nullable':False}])
        status,path=await self.run_spec(empty)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(path/'data.parquet').num_rows,0)

    async def test_preview_failures_and_null_exports_publish_no_partial_data(self):
        cases=[
            (SQLExportSpec(sql='SELECT NULL::int4 AS a, NULL::text AS b',exclude_null_columns=True),'NO_NON_NULL_COLUMNS'),
            (SQLExportSpec(sql='SELECT game_id FROM exports_data_v1.games',max_result_rows=2,exclude_null_columns=True),'RESULT_ROWS_EXCEEDED'),
            (SQLExportSpec(sql='SELECT avg(white_rating) AS average FROM exports_data_v1.games'),'UNSUPPORTED_RESULT_TYPE')]
        for spec,code in cases:
            with self.subTest(code=code),self.assertRaises(AnalyticalError) as error:
                await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1))
            self.assertEqual(error.exception.code,code)
            status,path=await self.run_spec(spec)
            self.assertEqual(status.error_code,code,status.error)
            self.assertFalse(path.exists())
        small=AnalyticalPreviewSpec(design=SQLExportSpec(sql='SELECT 1 AS n'))
        with patch('chessism_api.operations.exports.analytical_preview.MAX_RESPONSE_BYTES',1),self.assertRaises(AnalyticalError) as error:
            await preview(small)
        self.assertEqual(error.exception.code,'PREVIEW_SIZE_EXCEEDED')
        with patch.dict(os.environ,{'CHESSISM_EXPORT_MAX_PLAN_COST':'0'}),self.assertRaises(AnalyticalError) as error:
            await preview(small)
        self.assertEqual(error.exception.code,'QUERY_PLAN_LIMIT')

    async def test_real_http_preview_has_no_job_files_and_idempotency_remains_compatible(self):
        app=FastAPI();app.include_router(router,prefix='/exports/v1')
        app.dependency_overrides[get_redis_pool]=lambda:self.redis
        headers={'Authorization':'Bearer chx_test-secret'}
        before=set(Path(self.directory.name).iterdir())
        body={'design':{'mode':'sql','sql':'SELECT 0 AS score','max_result_rows':1,'exclude_null_columns':True},'preview_rows':10}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            self.assertEqual((await client.post('/exports/v1/analytical-preview',json=body)).status_code,401)
            response=await client.post('/exports/v1/analytical-preview',headers=headers,json=body)
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.json()['rows'],[[0]])
            self.assertEqual(response.headers['cache-control'],'private, no-store')
            body['design']['sql']='SELECT NULL::int4 AS score'
            failed=await client.post('/exports/v1/analytical-preview',headers=headers,json=body)
            self.assertEqual(failed.status_code,422,failed.text)
            self.assertEqual(failed.json()['detail']['code'],'NO_NON_NULL_COLUMNS')
        self.assertEqual(set(Path(self.directory.name).iterdir()),before)
        async with AsyncDBSession() as session:
            self.assertEqual(await session.scalar(text('SELECT count(*) FROM public.export_job')),0)
        spec=SQLExportSpec(sql='SELECT 0 AS score',max_result_rows=1)
        submitted=await service.submit(self.client,spec,'pre-preview-key',self.redis)
        stored=json.loads((Path(self.directory.name)/submitted['id']/'request.json').read_text())
        self.assertNotIn('exclude_null_columns',stored)
        again=await service.submit(self.client,spec.model_copy(update={'exclude_null_columns':False}),'pre-preview-key',self.redis)
        self.assertEqual(submitted['id'],again['id'])

    async def test_reader_role_has_no_base_metadata_write_temp_or_admin_access(self):
        async with analytical_reader.connection() as conn:
            self.assertEqual(await conn.fetchval('SELECT count(*) FROM exports_data_v1.games'),3)
            self.assertEqual(await conn.fetchval('SELECT current_user'),'chessism_export_reader')
        credential=json.loads(self.credential.read_text())
        owner = self.engine.url.username.replace('"', '""')
        for sql in ('SELECT * FROM public.export_client',f'SELECT * FROM {SCHEMA}.game',
                    f'SET ROLE "{owner}"','CREATE TEMP TABLE hacked(id int)',
                    'CREATE SCHEMA hacked','UPDATE exports_data_v1.players SET name=\'changed\'',
                    "SELECT pg_read_file('/etc/passwd')"):
            conn=await asyncpg.connect(**credential)
            try:
                await conn.execute('SET default_transaction_read_only=off')
                with self.subTest(sql=sql),self.assertRaises(asyncpg.InsufficientPrivilegeError):
                    await conn.execute(sql)
            finally:await conn.close()

    async def test_sql_runs_finished_aggregation_and_preserves_large_ids(self):
        spec=SQLExportSpec(sql='''WITH selected AS (SELECT game_id FROM exports_data_v1.games ORDER BY game_id LIMIT $1)
            SELECT g.game_id,count(m.move_number) AS moves,row_number() OVER(ORDER BY g.game_id) AS rank
            FROM selected g LEFT JOIN exports_data_v1.moves m USING(game_id)
            GROUP BY g.game_id ORDER BY g.game_id''',parameters=[{'type':'int64','value':3}],max_result_rows=3)
        status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        rows=pq.read_table(path/'data.parquet').to_pylist()
        self.assertEqual([r['moves'] for r in rows],[2,1,0])
        self.assertEqual(rows[-1]['game_id'],9007199254740993)
        manifest=json.loads((path/'manifest.json').read_text())
        self.assertTrue(manifest['complete_result']);self.assertEqual(manifest['row_count'],3)

    async def test_matrix_selects_games_first_keeps_nulls_zeros_side_and_rows(self):
        async with self.engine.begin() as conn:
            await conn.execute(text(f"INSERT INTO {SCHEMA}.fen(fen_id,fen,score) VALUES(3,'zero-position',0)"))
            await conn.execute(text(f"INSERT INTO {SCHEMA}.moves(link,n_move,white_move) VALUES(11,2,'Nf3')"))
            await conn.execute(text(f'INSERT INTO {SCHEMA}.game_fen_association VALUES(11,2,TRUE,3)'))
        spec=MoveMatrixSpec(player='hikaru',game_mode='blitz',max_games=2,
                            first_move=1,last_move=40,score_perspective='player')
        status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        rows=pq.read_table(path/'data.parquet').to_pylist()
        self.assertEqual(len(rows),2);self.assertEqual(len(rows[0]),44)
        self.assertEqual(rows[0]['color'],'black');self.assertEqual(rows[0]['move_1'],30)
        self.assertIsNone(rows[1]['move_1']);self.assertEqual(rows[1]['move_2'],0)
        self.assertIsNone(rows[1]['move_40'])
        manifest=json.loads((path/'manifest.json').read_text())
        self.assertEqual(manifest['selection']['selected_games'],2)
        self.assertEqual(manifest['selection']['selected_participants'],2)
        # Both players produce four complete rows, not a truncated two-row file.
        status,path=await self.run_spec(spec.model_copy(update={'players':'both'}))
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_metadata(path/'data.parquet').num_rows,4)
        self.assertEqual(status.row_limit,4)

    async def test_overflow_duplicate_moves_and_unsupported_types_publish_nothing(self):
        for spec,code in ((SQLExportSpec(sql='SELECT game_id FROM exports_data_v1.games',max_result_rows=2),'RESULT_ROWS_EXCEEDED'),
                          (SQLExportSpec(sql='SELECT avg(white_rating) AS average FROM exports_data_v1.games'),'UNSUPPORTED_RESULT_TYPE')):
            status,path=await self.run_spec(spec)
            self.assertEqual(status.error_code,code,status.error);self.assertFalse(path.exists())
        async with AsyncDBSession() as session:tables=tables_for(session)
        from chessism_api.operations.exports.catalog import SOURCES
        query=analytical_admin.view_select(SOURCES['moves'],tables)
        async with self.engine.begin() as conn:
            await conn.execute(text(f'CREATE OR REPLACE VIEW exports_data_v1.moves AS {query} UNION ALL {query}'))
        status,path=await self.run_spec(MoveMatrixSpec(player='hikaru',game_mode='blitz'))
        self.assertEqual(status.error_code,'DUPLICATE_MOVE',status.error);self.assertFalse(path.exists())

    async def test_empty_typed_matrix_and_analytical_http_scope_and_idempotency(self):
        status,path=await self.run_spec(MoveMatrixSpec(player='unknown',game_mode='bullet'))
        self.assertEqual(status.status,'complete',status.error)
        table=pq.read_table(path/'data.parquet')
        self.assertEqual(table.num_rows,0);self.assertEqual(str(table.schema.field('move_1').type),'int32')
        app=FastAPI();app.include_router(router,prefix='/exports/v1')
        app.dependency_overrides[get_redis_pool]=lambda:self.redis
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            headers={'Authorization':'Bearer chx_test-secret','Idempotency-Key':'analytics-test-1'}
            catalog=await client.get('/exports/v1/analytical-catalog',headers=headers)
            self.assertTrue(catalog.json()['enabled'])
            body={'sql':'SELECT count(*) AS n FROM exports_data_v1.games','max_result_rows':1}
            first=await client.post('/exports/v1/sql',headers=headers,json=body)
            self.assertEqual(first.status_code,202,first.text)
            second=await client.post('/exports/v1/sql',headers=headers,json=body)
            self.assertEqual(first.json()['id'],second.json()['id'])
            blocked=await client.post('/exports/v1/sql',headers=headers,json={'sql':'SELECT pg_sleep(10)'})
            self.assertEqual(blocked.status_code,422)
            async with AsyncDBSession() as session:
                row=await session.get(ExportClient,self.client.id);row.allow_analytics=False;await session.commit()
            blocked=await client.post('/exports/v1/sql',headers=headers,json=body)
            self.assertEqual(blocked.status_code,403)
