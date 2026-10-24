"""Complete-score eligibility must precede game limits, identically for preview/save."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
import httpx
import pyarrow.parquet as pq
from pydantic import ValidationError
from sqlalchemy import text

from tests import test_analytical_exports_database as base
from tests.test_exports_database import SCHEMA
from chessism_api.operations.exports import config, service
from chessism_api.operations.exports.analytical_reader import AnalyticalError
from chessism_api.operations.exports.analytical_query import check_scores, prepare
from chessism_api.operations.exports.analytical_spec import (MoveMatrixSpec, AnalyticalPreviewSpec, SQLExportSpec,
    StoredLegacyMoveMatrixSpec, StoredRangeMoveMatrixSpec, parse_request, MATRIX_CONTRACT_KEY,
    MATRIX_CONTRACT_VERSION)
from chessism_api.operations.exports.analytical_preview import preview
from chessism_api.operations.exports.move_matrix import plan
from chessism_api.redis_client import get_redis_pool
from chessism_api.routers.exports import router


class CompleteScoreContractTests(unittest.TestCase):
    def test_strict_flag_modes_conflict_and_query_has_no_candidate_limit(self):
        spec=MoveMatrixSpec(player='hikaru',first_move=5,last_move=6,require_complete_scores=True)
        sql,params,checks=plan(spec,start_year=2022)
        self.assertLess(sql.index('bool_and(eligible.score_cp IS NOT NULL)'),sql.index('LIMIT $3'))
        self.assertLess(sql.index('ORDER BY CASE WHEN played_at'),sql.index('bool_and(eligible.score_cp IS NOT NULL)'))
        self.assertEqual(sql.count('LIMIT'),1)
        self.assertIn('count(DISTINCT (eligible.is_white,eligible.move_number))',sql)
        self.assertIn('BETWEEN 5 AND 6',sql)
        self.assertIn(') = 2',sql)
        self.assertEqual(params,['hikaru','bullet',1000,2022])
        self.assertIn(') = 4',plan(spec.model_copy(update={'players':'both'}))[0])
        self.assertNotIn('eligible.score_cp',plan(spec.model_copy(update={'require_complete_scores':False}))[0])
        self.assertNotIn('ORDER BY game_id ASC OFFSET 0',plan(spec.model_copy(update={'require_complete_scores':False}))[0])
        for options in ({'require_complete_scores':'true'},{'require_complete_scores':1},
                        {'require_complete_scores':True,'exclude_null_columns':True}):
            with self.subTest(options=options),self.assertRaises(ValidationError):
                MoveMatrixSpec(player='hikaru',**options)
        with self.assertRaises(ValidationError):SQLExportSpec(sql='SELECT 1',require_complete_scores=True)
        check_scores(spec,[{'move_5':0,'move_6':-30}])
        with self.assertRaises(AnalyticalError) as error:check_scores(spec,[{'move_5':0,'move_6':None}])
        self.assertEqual(error.exception.code,'INCOMPLETE_MATRIX')

    def test_internal_contract_version_is_not_a_user_setting(self):
        spec=MoveMatrixSpec(player='hikaru',require_complete_scores=True)
        document=service.request_document(spec)
        self.assertEqual(document[MATRIX_CONTRACT_KEY],MATRIX_CONTRACT_VERSION)
        self.assertEqual(parse_request(document).complete_score_scope,'whole_game')
        old=dict(document);old.pop(MATRIX_CONTRACT_KEY)
        self.assertEqual(parse_request(old).complete_score_scope,'requested_moves')
        for version in (1,2,3,4):
            with self.assertRaises(ValidationError):MoveMatrixSpec(player='hikaru',**{MATRIX_CONTRACT_KEY:version})
        for version in (0,5,True,'2'):
            with self.assertRaises(ValueError):parse_request(dict(document,**{MATRIX_CONTRACT_KEY:version}))
        self.assertEqual(service.request_document(spec.model_copy(update={'require_complete_scores':False}))[MATRIX_CONTRACT_KEY],MATRIX_CONTRACT_VERSION)


class CompleteMatrixDatabaseTests(base.AnalyticalDatabaseFixture):
    async def test_whole_game_rejects_opponent_or_later_missing_scores_before_limit(self):
        await self.complete_fixture()
        async with self.engine.begin() as conn:
            # Game 12 has a perfect requested prefix, but unscored later Black move.
            await conn.execute(text(f"INSERT INTO {SCHEMA}.moves(link,n_move,white_move,black_move) VALUES(12,3,'Bb5','a6')"))
            await conn.execute(text(f'INSERT INTO {SCHEMA}.game_fen_association VALUES(12,3,TRUE,4),(12,3,FALSE,1)'))
            # Game 13 has a perfect Hikaru prefix but an unscored opponent move.
            await conn.execute(text(f'UPDATE {SCHEMA}.game_fen_association SET fen_id=1 WHERE game_link=13 AND is_white AND n_move=1'))
        spec=self.design(max_games=1)
        for mode in ('sample','design'):
            result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1,preview_mode=mode))
            self.assertEqual(result['rows'],[[14,'hikaru','white',None,0,20]])
            self.assertEqual(result['selection']['complete_score_scope'],'whole_game')
            self.assertEqual(result['column_selection']['dropped_columns'],[])
        status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(path/'data.parquet')['game_id'].to_pylist(),[14])
        nullable=await preview(AnalyticalPreviewSpec(design=self.design(require_complete_scores=False,max_games=1)))
        self.assertEqual(nullable['rows'][0][0],10)
        self.assertEqual(nullable['selection']['complete_score_scope'],'none')
        self.assertIsNone(nullable['rows'][0][-1])

    async def test_old_queued_range_rules_survive_and_new_request_needs_a_new_key(self):
        await self.complete_fixture()
        async with self.engine.begin() as conn:
            await conn.execute(text(f'UPDATE {SCHEMA}.game_fen_association SET fen_id=1 WHERE game_link=12 AND NOT is_white AND n_move=2'))
        old=StoredRangeMoveMatrixSpec(**self.design(max_games=1).model_dump())
        submitted=await service.submit(self.client,old,'old-range-eligibility',self.redis)
        request_path=Path(self.directory.name)/submitted['id']/'request.json'
        before=request_path.read_bytes()
        self.assertNotIn(MATRIX_CONTRACT_KEY,json.loads(before))
        with self.assertRaises(HTTPException) as error:
            await service.submit(self.client,self.design(max_games=1),'old-range-eligibility',self.redis)
        self.assertEqual(error.exception.status_code,409)
        self.assertEqual(request_path.read_bytes(),before)
        await base.jobs.run_export({},submitted['id'])
        status=await service.owned_job(self.client,submitted['id'])
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(request_path.parent/'data.parquet')['game_id'].to_pylist(),[12])
        manifest=json.loads((request_path.parent/'manifest.json').read_text())
        self.assertEqual(manifest['selection']['complete_score_scope'],'requested_moves')
        current=self.design(max_games=1)
        fresh=await service.submit(self.client,current,'new-whole-game-eligibility',self.redis)
        repeat=await service.submit(self.client,current,'new-whole-game-eligibility',self.redis)
        self.assertEqual(fresh['id'],repeat['id'])
        await base.jobs.run_export({},fresh['id'])
        self.assertEqual(pq.read_table(Path(self.directory.name)/fresh['id']/'data.parquet')['game_id'].to_pylist(),[13])

    async def test_final_unplayed_black_move_is_not_a_missing_score(self):
        await self.complete_fixture()
        async with self.engine.begin() as conn:
            await conn.execute(text(f"INSERT INTO {SCHEMA}.moves(link,n_move,white_move,black_move) VALUES(14,3,'Qh5','--')"))
            await conn.execute(text(f'INSERT INTO {SCHEMA}.game_fen_association VALUES(14,3,TRUE,4)'))
        spec=self.design(first_move=3,last_move=3,max_games=100)
        result=await preview(AnalyticalPreviewSpec(design=spec))
        self.assertEqual(result['rows'],[[14,'hikaru','white',None,20]])
        both=await preview(AnalyticalPreviewSpec(design=spec.model_copy(update={'players':'both'})))
        self.assertEqual(both['rows'],[])  # requested Black move 3 never happened

    async def test_legacy_key_replay_keeps_original_document_and_safe_limits(self):
        legacy=StoredLegacyMoveMatrixSpec(player='hikaru',game_mode='blitz',max_games=2,
            max_result_rows=100000,last_move=2)
        submitted=await service.submit(self.client,legacy,'old-matrix-key',self.redis)
        request_path=Path(self.directory.name)/submitted['id']/'request.json'
        before=request_path.read_bytes()
        new=MoveMatrixSpec(player='hikaru',game_mode='blitz',max_games=2,last_move=2)
        # New year selection/schema must not reuse an old dataset's key.
        with self.assertRaises(HTTPException) as error:
            await service.submit(self.client,new,'old-matrix-key',self.redis)
        self.assertEqual(error.exception.status_code,409)
        self.assertEqual(request_path.read_bytes(),before)
        with self.assertRaises(HTTPException) as error:
            await service.submit(self.client,new.model_copy(update={'last_move':3}),'old-matrix-key',self.redis)
        self.assertEqual(error.exception.status_code,409)
        # A smaller old guard is NOT semantically equivalent to the new bound.
        smaller=legacy.model_copy(update={'players':'both','max_result_rows':2})
        old=await service.submit(self.client,smaller,'old-smaller-key',self.redis)
        with self.assertRaises(HTTPException) as error:
            await service.submit(self.client,new.model_copy(update={'players':'both'}),'old-smaller-key',self.redis)
        self.assertEqual(error.exception.status_code,409)
        await base.jobs.run_export({},old['id'])
        status=await service.owned_job(self.client,old['id'])
        self.assertEqual(status.error_code,'RESULT_ROWS_EXCEEDED')

    async def test_http_derived_bounds_legacy_inputs_and_client_ceiling(self):
        app=FastAPI();app.include_router(router,prefix='/exports/v1')
        app.dependency_overrides[get_redis_pool]=lambda:self.redis
        headers={'Authorization':'Bearer chx_test-secret'}
        design={'mode':'move_matrix','player':'hikaru','game_mode':'blitz',
                'max_games':2,'players':'both','first_move':1,'last_move':2}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            for extra in ({},{'max_result_rows':4,'exclude_null_columns':False},{'max_result_rows':100000}):
                body={'design':dict(design,**extra),'preview_rows':1,'preview_mode':'sample'}
                response=await client.post('/exports/v1/analytical-preview',headers=headers,json=body)
                self.assertEqual(response.status_code,200,response.text)
                self.assertEqual(response.json()['selection']['result_row_bound'],4)
            for extra in ({'max_result_rows':2},{'exclude_null_columns':True},{'max_games':100000}):
                for endpoint,body in (
                    ('/analytical-preview',{'design':dict(design,**extra),'preview_rows':1,'preview_mode':'sample'}),
                    ('/move-matrix',dict(design,**extra))):
                    response=await client.post('/exports/v1'+endpoint,
                        headers={**headers,'Idempotency-Key':'reject-unsafe-input'},json=body)
                    self.assertEqual(response.status_code,422,response.text)
            response=await client.post('/exports/v1/move-matrix',headers={**headers,'Idempotency-Key':'derived-bound-save'},json=design)
            self.assertEqual(response.status_code,202,response.text)
            submitted=response.json()
            self.assertEqual(submitted['row_limit'],4)
            request_path=Path(self.directory.name)/submitted['id']/'request.json'
            stored=json.loads(request_path.read_text())
            self.assertNotIn('max_result_rows',stored);self.assertNotIn('exclude_null_columns',stored)
            repeat=await client.post('/exports/v1/move-matrix',headers={**headers,'Idempotency-Key':'derived-bound-save'},
                json=dict(design,max_result_rows=100000,exclude_null_columns=False))
            self.assertEqual(repeat.json()['id'],submitted['id'])
        await base.jobs.run_export({},submitted['id'])
        status=await service.owned_job(self.client,submitted['id'])
        self.assertEqual(status.status,'complete',status.error)
        table=pq.read_table(request_path.parent/'data.parquet')
        self.assertEqual(table.num_rows,4)
        self.assertEqual(table.column_names,['game_id','player_name','color','played_at','move_1','move_2'])

    async def complete_fixture(self):
        # Original game 10 has only one scored Black move; game 11 is unscored.
        # First qualifying requested-player games are 12 and 13, not 10 and 11.
        async with self.engine.begin() as conn:
            statements=[
                f"INSERT INTO {SCHEMA}.fen(fen_id,fen,score) VALUES(3,'zero',0),(4,'positive',20)",
                f"INSERT INTO {SCHEMA}.game_mode VALUES(2,'rapid')",
                f'''INSERT INTO {SCHEMA}.game(link,white_player_id,black_player_id,mode_id,n_moves) VALUES
                    (12,2,1,1,2),(13,1,2,1,2),(14,2,3,1,2),(15,2,1,1,2),
                    (16,2,1,1,2),(17,2,1,1,2),(18,2,1,1,3),(19,2,1,2,2),(20,1,3,1,2)''',
                f'''INSERT INTO {SCHEMA}.moves(link,n_move,white_move,black_move) VALUES
                    (12,1,'e4','e5'),(12,2,'Nf3','Nc6'),
                    (13,1,'e4','e5'),(13,2,'Nf3','Nc6'),
                    (14,1,'e4','e5'),(14,2,'Nf3','Nc6'),
                    (15,1,'e4','e5'),(15,2,'Nf3','Nc6'),
                    (16,1,'e4','e5'),(16,2,'Nf3','Nc6'),
                    (17,1,'e4','e5'),(17,2,'--','--'),
                    (18,1,'e4','e5'),(18,3,'Bb5','a6'),
                    (19,1,'e4','e5'),(19,2,'Nf3','Nc6'),
                    (20,1,'e4','e5'),(20,2,'Nf3','Nc6')''',
                f'''INSERT INTO {SCHEMA}.game_fen_association VALUES
                    (12,1,TRUE,3),(12,2,TRUE,4),(12,1,FALSE,2),(12,2,FALSE,4),
                    (13,1,TRUE,4),(13,2,TRUE,4),(13,1,FALSE,2),(13,2,FALSE,3),
                    (14,1,TRUE,3),(14,2,TRUE,4),(14,1,FALSE,2),(14,2,FALSE,2),
                    (15,1,TRUE,3),(15,2,TRUE,1),
                    (16,1,TRUE,3),
                    (17,1,TRUE,3),(17,2,TRUE,4),
                    (18,1,TRUE,3),(18,3,TRUE,4),
                    (19,1,TRUE,3),(19,2,TRUE,4),(20,1,TRUE,3),(20,2,TRUE,4)'''
            ]
            for statement in statements:await conn.execute(text(statement))

    def design(self,**options):
        return MoveMatrixSpec(**dict({'player':'hikaru','game_mode':'blitz','max_games':2,
            'first_move':1,'last_move':2,'score_perspective':'player','require_complete_scores':True},**options))

    async def test_sample_plans_only_needed_games_preserves_design_and_save_is_full(self):
        await self.complete_fixture()
        spec=self.design(max_games=1000,max_result_rows=1000)
        before=spec.model_dump()
        with patch('chessism_api.operations.exports.analytical_preview.prepare',wraps=prepare) as prepared:
            result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1,preview_mode='sample'))
        executed=prepared.call_args.args[1]
        self.assertEqual(executed.max_games,1)
        self.assertEqual(executed.limit,1)
        self.assertEqual(spec.model_dump(),before)
        self.assertEqual(result['kind'],'analytical_sample')
        self.assertEqual(result['rows'],[[12,'hikaru','white',None,0,20]])
        self.assertEqual(result['row_count'],1)
        self.assertEqual(len(result['columns']),6)
        self.assertEqual(result['column_selection']['dropped_columns'],[])
        self.assertFalse(result['entire_result_checked'])
        for name in ('has_more','total_row_count'):self.assertIsNone(result[name])
        selection=result['selection']
        self.assertEqual(selection['max_games'],1000)
        self.assertEqual(selection['sample_game_limit'],1)
        self.assertEqual(selection['returned_games'],1)
        self.assertEqual(selection['returned_participants'],1)
        self.assertEqual(selection['counts_scope'],'sample_only')
        self.assertIsNone(selection['selected_games']);self.assertIsNone(selection['selected_participants'])
        status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        table=pq.read_table(path/'data.parquet')
        self.assertEqual(table['game_id'].to_pylist(),[12,13,14])
        manifest=json.loads((path/'manifest.json').read_text())
        self.assertEqual(manifest['selection']['max_games'],1000)
        self.assertEqual(manifest['selection']['selected_games'],3)
        self.assertEqual(manifest['columns'],result['columns'])

    async def test_sample_both_players_odd_row_count_and_game_cap(self):
        await self.complete_fixture()
        spec=self.design(players='both',max_games=1000,max_result_rows=2000)
        with patch('chessism_api.operations.exports.analytical_preview.prepare',wraps=prepare) as prepared:
            result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=3,preview_mode='sample'))
        self.assertEqual(prepared.call_args.args[1].max_games,2)
        self.assertEqual(result['rows'],[
            [12,'hikaru','white',None,0,20],[12,'alice','black',None,30,-20],[13,'alice','white',None,20,20]])
        self.assertEqual(result['selection']['returned_games'],2)
        self.assertEqual(result['selection']['returned_participants'],3)
        # A sample never selects beyond the design's max_games either.
        limited=await preview(AnalyticalPreviewSpec(design=self.design(players='both',max_games=1,max_result_rows=2),
            preview_rows=10,preview_mode='sample'))
        self.assertEqual(limited['row_count'],2)
        self.assertEqual(limited['selection']['returned_games'],1)
        self.assertIsNone(limited['total_row_count'])
        # One sample row does not reduce the saved matrix's derived row bound.
        full=self.design(players='both',max_games=1000)
        tiny=await preview(AnalyticalPreviewSpec(design=full,preview_rows=1,preview_mode='sample'))
        self.assertEqual(tiny['row_count'],1)
        self.assertEqual(tiny['selection']['result_row_bound'],2000)
        status,path=await self.run_spec(full)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(path/'data.parquet').num_rows,6)

    async def test_sample_does_not_validate_later_games_but_rejects_duplicate_sample_moves(self):
        await self.complete_fixture()
        async with self.engine.begin() as conn:
            await conn.execute(text('ALTER VIEW exports_data_v1.moves RENAME TO moves_original'))
            await conn.execute(text('''CREATE VIEW exports_data_v1.moves AS
                SELECT * FROM exports_data_v1.moves_original UNION ALL
                SELECT * FROM exports_data_v1.moves_original WHERE game_id=13'''))
            await conn.execute(text('ALTER VIEW exports_data_v1.moves OWNER TO chessism_export_view_owner'))
            await conn.execute(text('GRANT SELECT ON exports_data_v1.moves TO chessism_export_reader'))
        spec=self.design(max_games=1000,max_result_rows=1000)
        result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1,preview_mode='sample'))
        self.assertEqual(result['rows'][0][0],12)
        self.assertFalse(result['entire_result_checked'])
        # Reading only one sample row must not run preflight over later games.
        status,path=await self.run_spec(spec)
        self.assertEqual(status.error_code,'DUPLICATE_MOVE');self.assertFalse(path.exists())
        with self.assertRaises(AnalyticalError) as error:
            await preview(AnalyticalPreviewSpec(design=spec,preview_rows=2,preview_mode='sample'))
        self.assertEqual(error.exception.code,'DUPLICATE_MOVE')

    async def test_empty_and_nullable_samples_keep_every_requested_column(self):
        await self.complete_fixture()
        empty=await preview(AnalyticalPreviewSpec(design=self.design(first_move=40,last_move=41),
            preview_mode='sample',preview_rows=2))
        self.assertEqual(empty['rows'],[])
        self.assertEqual([c['name'] for c in empty['columns']],['game_id','player_name','color','played_at','move_40','move_41'])
        self.assertIsNone(empty['has_more']);self.assertIsNone(empty['total_row_count'])
        self.assertEqual(empty['selection']['returned_games'],0)
        nullable=await preview(AnalyticalPreviewSpec(design=self.design(require_complete_scores=False),
            preview_mode='sample',preview_rows=2))
        self.assertEqual([row[0] for row in nullable['rows']],[10,11])
        self.assertIsNone(nullable['rows'][0][-1])
        self.assertEqual(nullable['column_selection']['dropped_columns'],[])

    async def test_sample_http_contract_auth_and_no_artifacts(self):
        await self.complete_fixture()
        app=FastAPI();app.include_router(router,prefix='/exports/v1')
        app.dependency_overrides[get_redis_pool]=lambda:self.redis
        headers={'Authorization':'Bearer chx_test-secret'}
        body={'design':self.design(max_games=1000,max_result_rows=1000).model_dump(),
              'preview_rows':1,'preview_mode':'sample'}
        before=set(Path(self.directory.name).iterdir())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            self.assertEqual((await client.post('/exports/v1/analytical-preview',json=body)).status_code,401)
            response=await client.post('/exports/v1/analytical-preview',headers=headers,json=body)
            self.assertEqual(response.status_code,200,response.text)
            result=response.json()
            self.assertEqual(result['kind'],'analytical_sample');self.assertIsNone(result['has_more'])
            self.assertEqual(result['row_count'],1)
            self.assertEqual(response.headers['cache-control'],'private, no-store')
        self.assertEqual(set(Path(self.directory.name).iterdir()),before)

    async def test_limit_counts_qualifying_games_and_preview_save_keep_every_column(self):
        await self.complete_fixture()
        spec=self.design()
        result=await preview(AnalyticalPreviewSpec(design=spec,preview_rows=1))
        self.assertEqual(result['rows'],[[12,'hikaru','white',None,0,20]])
        self.assertTrue(result['has_more'])
        self.assertEqual(result['selection']['selected_games'],2)
        self.assertTrue(result['selection']['require_complete_scores'])
        self.assertEqual(result['selection']['game_limit_applies_to'],'qualifying_games')
        self.assertEqual(result['column_selection']['dropped_columns'],[])
        with patch.object(config,'BATCH_ROWS',1):status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        table=pq.read_table(path/'data.parquet')
        self.assertEqual(table.column_names,['game_id','player_name','color','played_at','move_1','move_2'])
        self.assertEqual(table.to_pylist(),[
            {'game_id':12,'player_name':'hikaru','color':'white','played_at':None,'move_1':0,'move_2':20},
            {'game_id':13,'player_name':'hikaru','color':'black','played_at':None,'move_1':30,'move_2':0}])
        self.assertFalse(table.schema.field('move_1').nullable)
        manifest=json.loads((path/'manifest.json').read_text())
        self.assertEqual(manifest['columns'],result['columns'])
        self.assertEqual(manifest['selection'],result['selection'])
        # Limit one means the first eligible game, not first candidate then discard.
        one=await preview(AnalyticalPreviewSpec(design=self.design(max_games=1,max_result_rows=1)))
        self.assertEqual(one['rows'][0][0],12)
        default=await preview(AnalyticalPreviewSpec(design=self.design(require_complete_scores=False)))
        self.assertEqual([row[0] for row in default['rows']],[10,11])
        self.assertIsNone(default['rows'][0][-1])

    async def test_both_players_qualify_before_limit_and_bound_is_derived(self):
        await self.complete_fixture()
        spec=self.design(players='both')
        result=await preview(AnalyticalPreviewSpec(design=spec))
        self.assertEqual([row[:3] for row in result['rows']],
            [[12,'hikaru','white'],[12,'alice','black'],[13,'alice','white'],[13,'hikaru','black']])
        self.assertEqual(result['selection']['selected_games'],2)
        self.assertEqual(result['selection']['selected_participants'],4)
        self.assertEqual(result['selection']['result_row_bound'],4)
        self.assertEqual(result['selection']['result_row_bound_source'],'max_games_times_selected_sides')
        self.assertTrue(all(row[-1] is not None and row[-2] is not None for row in result['rows']))
        status,path=await self.run_spec(spec)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_metadata(path/'data.parquet').num_rows,4)
        # A queued job from before the change must retain its old failure guard.
        insufficient=StoredLegacyMoveMatrixSpec(**spec.model_dump(),max_result_rows=2)
        status,path=await self.run_spec(insufficient)
        self.assertEqual(status.error_code,'RESULT_ROWS_EXCEEDED');self.assertFalse(path.exists())

    async def test_missing_score_position_move_and_holes_do_not_qualify_range_is_exact(self):
        await self.complete_fixture()
        spec=self.design(max_games=100,max_result_rows=100)
        result=await preview(AnalyticalPreviewSpec(design=spec))
        self.assertEqual([row[0] for row in result['rows']],[12,13,14])
        self.assertFalse(result['has_more']);self.assertEqual(result['total_row_count'],3)
        # NULLs outside the output range (including the opponent) disqualify it.
        third_only=await preview(AnalyticalPreviewSpec(design=self.design(first_move=3,last_move=3,max_games=100,max_result_rows=100)))
        self.assertEqual(third_only['rows'],[])
        self.assertEqual([c['name'] for c in third_only['columns']],['game_id','player_name','color','played_at','move_3'])
        absent=self.design(first_move=4,last_move=5)
        empty=await preview(AnalyticalPreviewSpec(design=absent))
        self.assertEqual(empty['rows'],[]);self.assertEqual(empty['selection']['selected_games'],0)
        self.assertEqual([c['name'] for c in empty['columns']],['game_id','player_name','color','played_at','move_4','move_5'])
        status,path=await self.run_spec(absent)
        self.assertEqual(status.status,'complete',status.error)
        self.assertEqual(pq.read_table(path/'data.parquet').num_rows,0)

    async def test_duplicates_cannot_fill_missing_moves_or_hide_in_a_qualifying_matrix(self):
        await self.complete_fixture()
        async with self.engine.begin() as conn:
            await conn.execute(text('ALTER VIEW exports_data_v1.moves RENAME TO moves_original'))
            await conn.execute(text('''CREATE VIEW exports_data_v1.moves AS
                SELECT * FROM exports_data_v1.moves_original UNION ALL
                SELECT * FROM exports_data_v1.moves_original WHERE game_id=10'''))
            await conn.execute(text('ALTER VIEW exports_data_v1.moves OWNER TO chessism_export_view_owner'))
            await conn.execute(text('GRANT SELECT ON exports_data_v1.moves TO chessism_export_reader'))
        result=await preview(AnalyticalPreviewSpec(design=self.design(max_games=1,max_result_rows=1)))
        self.assertEqual(result['rows'][0][0],12) # duplicate move 1 does not replace missing move 2
        async with self.engine.begin() as conn:
            await conn.execute(text('''CREATE OR REPLACE VIEW exports_data_v1.moves AS
                SELECT * FROM exports_data_v1.moves_original UNION ALL
                SELECT * FROM exports_data_v1.moves_original WHERE game_id=12'''))
        with self.assertRaises(AnalyticalError) as error:
            await preview(AnalyticalPreviewSpec(design=self.design(),preview_rows=1))
        self.assertEqual(error.exception.code,'DUPLICATE_MOVE')
        status,path=await self.run_spec(self.design())
        self.assertEqual(status.error_code,'DUPLICATE_MOVE');self.assertFalse(path.exists())

    async def test_real_http_complete_preview_export_and_old_default_fingerprints(self):
        await self.complete_fixture()
        app=FastAPI();app.include_router(router,prefix='/exports/v1')
        app.dependency_overrides[get_redis_pool]=lambda:self.redis
        headers={'Authorization':'Bearer chx_test-secret'}
        spec=self.design()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            response=await client.post('/exports/v1/analytical-preview',headers=headers,
                json={'design':spec.model_dump(),'preview_rows':1})
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.json()['rows'][0][0],12)
            conflict=await client.post('/exports/v1/analytical-preview',headers=headers,
                json={'design':{**spec.model_dump(),'exclude_null_columns':True},'preview_rows':1})
            self.assertEqual(conflict.status_code,422,conflict.text)
            response=await client.post('/exports/v1/move-matrix',
                headers={**headers,'Idempotency-Key':'complete-matrix-001'},json=spec.model_dump())
            self.assertEqual(response.status_code,202,response.text)
            submitted=response.json()
            from chessism_api.operations.exports.jobs import run_export
            await run_export({},submitted['id'])
            manifest=(await client.get('/exports/v1/'+submitted['id']+'/manifest',headers=headers)).json()
            self.assertTrue(manifest['selection']['require_complete_scores'])
            self.assertEqual(manifest['row_count'],2)
        legacy=self.design(require_complete_scores=False)
        submitted=await service.submit(self.client,legacy,'legacy-matrix-key',self.redis)
        stored=json.loads((Path(self.directory.name)/submitted['id']/'request.json').read_text())
        self.assertNotIn('require_complete_scores',stored)
        replay=await service.submit(self.client,parse_request(stored),'legacy-matrix-key',self.redis)
        self.assertEqual(replay['id'],submitted['id'])
