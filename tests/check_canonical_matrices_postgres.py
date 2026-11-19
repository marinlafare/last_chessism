"""Saved matrix recipes on native keys; private PostgreSQL only, no GPU/network."""
import asyncio
from unittest.mock import patch

from sqlalchemy import text

from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations.matrix_constructor import definitions, live_preview
from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
from chessism_api.operations.matrix_constructor.config import normalize_matrix_config
from chessism_api.operations.matrix_constructor.queries import matrix_sql
from chessism_api.operations.matrix_constructor.move_scores import score_matrix_sql, expand_sample_moves
from tests.check_canonical_cuda_postgres import CanonicalCudaTests


class CanonicalMatrixTests(CanonicalCudaTests):
    def recipe(self, kind, columns=None, **filters):
        fields = columns or list(ROW_TYPES[kind].columns_by_key)
        if kind == 'move_scores' and columns is None:
            fields = ['player', 'game', 'result', 'move_score_1', 'move_score_2']
        return normalize_matrix_config({'row_type':kind, 'feature_columns':fields,
            'filters':{'max_rows':1000, **filters}, 'summary_input_version':1 if kind=='accuracy_positions' else 3,
            **({'move_scores':{'first_move':1,'last_move':2}} if kind == 'move_scores' else {})})

    async def query(self, config):
        count, query, params = matrix_sql(config)
        async with read_transaction(self.factory) as session:
            total = await session.scalar(text(count),params)
            rows = [dict(r) for r in (await session.execute(text(query),params)).mappings()]
        self.assertEqual(self.engine.pool.checkedout(),0)
        return total, rows

    async def test_every_matrix_catalog_column_runs_on_canonical_tables(self):
        expected = {'game':3,'game_player':6,'move':44,'position':4,
                    'game_position':44,'player_period':2,'move_scores':6,
                    'score_distribution':88,'blunder_moves':44,'accuracy_positions':44}
        for kind in ROW_TYPES:
            with self.subTest(kind=kind):
                config=self.recipe(kind)
                total, rows=await self.query(config)
                self.assertEqual(total,expected[kind]);self.assertEqual(len(rows),total)
                self.assertEqual(len({r['row_key'] for r in rows}),total)
                self.assertEqual(set(rows[0]),{'row_key',*config['feature_columns']})

    async def test_filters_dates_modes_sides_and_empty_game_scope(self):
        total, rows = await self.query(self.recipe('game_player',['player','opponent','player_color'],
            players=['alice'],modes=['blitz'],date_from='2026-01-01',date_to='2026-01-01'))
        self.assertEqual(total,4)
        self.assertEqual({r['row_key'] for r in rows}, {'1:white',f'{self.links[1]}:white',f'{self.links[1]}:black','3:white'})
        for kind in ROW_TYPES:
            with self.subTest(kind=kind):
                total, rows=await self.query(self.recipe(kind,players=['absent'],date_from='2026-01-01'))
                self.assertEqual((total,rows),(0,[]))
        total, rows=await self.query(self.recipe('position',['score'],players=['bob'],min_moves=12))
        self.assertEqual((total,rows),(0,[]))

    async def test_integer_extrema_and_unknown_source_keep_perspective_without_overflow(self):
        await self.db.execute('UPDATE fen SET score=-2147483648,analysis_source=NULL,analyzed_at=NULL')
        _, rows=await self.query(self.recipe('game',['average_abs_score']))
        self.assertEqual([r['average_abs_score'] for r in rows if r['row_key']!='3'],[2147483648.0]*2)
        for kind in ['score_distribution','blunder_moves']:
            _, rows=await self.query(self.recipe(kind))
            self.assertEqual({r['player_score'] for r in rows},{-2147483648,2147483648})
            self.assertEqual({r['score_kind'] for r in rows},{'unknown'})
            self.assertTrue(all(r['mate_moves'] is None for r in rows))
        _, rows=await self.query(self.recipe('move_scores'))
        self.assertEqual({r['move_score_1'] for r in rows},{None,-2147483648,2147483648})
        _, rows=await self.query(self.recipe('accuracy_positions'))
        self.assertEqual({r['score'] for r in rows},{-2147483648.0})
        self.assertEqual({r['score_kind'] for r in rows},{'unknown'})

    async def test_move_scores_complete_selection_and_dynamic_preview_preserve_keys(self):
        config=self.recipe('move_scores',analyzed_only=True)
        total, rows=await self.query(config)
        self.assertEqual(total,4)
        self.assertEqual({r['result'] for r in rows},{'w','l'})
        # A missing selected score excludes affected games before the row cap.
        await self.db.execute('UPDATE fen SET score=NULL,analysis_source=NULL,analyzed_at=NULL WHERE fen_id=$1',next(iter(self.fens.values())))
        self.assertEqual(await self.query(config),(0,[]))
        config['filters']['analyzed_only']=False
        config['move_scores']={'all_moves':True,'score_perspective':'player'}
        config['feature_columns']=['player','game','result']
        _,query,params=score_matrix_sql(config,sample_all_moves=True)
        async with read_transaction(self.factory) as session:
            raw=[dict(r) for r in (await session.execute(text(query),params)).mappings()]
        records,keys=expand_sample_moves(raw)
        self.assertEqual(len(records),6);self.assertEqual(len(keys),11)
        self.assertTrue(all(r['move_score_1'] is None for r in records if r['row_key'].endswith(':white')))

    async def test_estimate_and_preview_use_guarded_native_queries_and_bound_limits(self):
        config=self.recipe('game_player',['player','rating'],max_rows=3)
        with patch.object(definitions,'AsyncDBSession',self.factory),patch.object(live_preview,'AsyncDBSession',self.factory):
            estimate=await definitions.estimate_definition(config)
            preview=await live_preview.preview_definition(config,limit=2)
        self.assertEqual(estimate['selected_rows'],3)
        self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual(preview['total_rows'],2)

    async def test_cancelled_preview_closes_its_read_transaction(self):
        config=self.recipe('game',['white_rating'])
        entered=asyncio.Event();release=asyncio.Event();sessions=[]
        def factory():
            session=self.factory();sessions.append(session);original=session.execute
            async def execute(statement,*a,**kw):
                if 'SELECT' in str(statement):
                    entered.set();await release.wait()
                return await original(statement,*a,**kw)
            session.execute=execute
            return session
        with patch.object(live_preview,'AsyncDBSession',factory):
            task=asyncio.create_task(live_preview.preview_definition(config))
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
            finally:
                release.set()
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertTrue(all(not s.in_transaction() for s in sessions))
