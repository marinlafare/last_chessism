"""Typed expressions, graph contracts, branching, missing values and bounded outputs."""

from copy import deepcopy
import json
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
import uuid

from fastapi import FastAPI
import httpx

import numpy as np

from chessism_api.operations.research_algorithms import builder_runner
from chessism_api.operations.research_algorithms.builder_frames import Frame, apply_step, prepare
from chessism_api.operations.research_algorithms.builder_outputs import render_output
from chessism_api.operations.research_algorithms.builder_schema import normalize_builder
from chessism_api.operations.research_algorithms.expressions import evaluate, parse_expression
from chessism_api.operations.research_algorithms.repository import RunCancelled
from chessism_api.routers import research_algorithms as routes
from chessism_api.auth import get_current_account
from chessism_api.redis_client import get_redis_pool

MATRIX = {'row_type': 'game_player', 'feature_columns': ['moves', 'elapsed_seconds', 'result', 'mode'],
          'filters': {'max_rows': 100, 'modes': ['blitz']}}
SPEC = {'operation': 'pipeline', 'columns': ['moves', 'elapsed_seconds', 'result', 'mode'],
        'steps': [
            {'id': 'long_games', 'op': 'filter', 'input': 'input', 'expression': 'moves > 10'},
            {'id': 'pace', 'op': 'calculate', 'input': 'long_games', 'column': 'seconds_per_full_move',
             'expression': 'safe_divide(elapsed_seconds, moves)'},
            {'id': 'totals', 'op': 'aggregate', 'input': 'pace', 'group_by': ['result'],
             'metrics': [{'name': 'games', 'op': 'count'}, {'name': 'mean_seconds', 'op': 'mean', 'column': 'seconds_per_full_move'}]},
        ], 'outputs': [
            {'name': 'Summary', 'type': 'table', 'input': 'totals'},
            {'name': 'Scatter', 'type': 'scatter', 'input': 'pace', 'x': 'moves', 'y': 'seconds_per_full_move'},
        ], 'max_rows': 100}


def frame():
    return Frame({'moves': np.array([10., 20., 40., 30.]), 'elapsed_seconds': np.array([20., 40., 120., np.nan]),
                  'result': np.array([0., 1., 1., .5]), 'mode': np.array(['blitz', 'blitz', None, 'rapid'], dtype=object)},
                 {'moves': 'number', 'elapsed_seconds': 'number', 'result': 'number', 'mode': 'category'},
                 np.array(['a', 'b', 'c', 'd'], dtype=object))


class ExpressionTests(unittest.TestCase):
    def test_no_python_execution_or_unbounded_integer_math(self):
        for expression in ['__import__("os")', 'moves.__class__', 'moves[0]', '[x for x in moves]',
                           '(lambda: 1)()', 'sum(moves)', 'open("/etc/passwd")', '"x" * 100000',
                           '1' * 400, '(' * 300 + '1' + ')' * 300]:
            with self.subTest(expression=expression), self.assertRaises(ValueError):
                parse_expression(expression, frame().schema)
        values = evaluate('10 ** 1000000000000', frame().columns, frame().schema, 4)
        self.assertTrue(np.isnan(values).all())

    def test_vector_formulas_types_boolean_and_missing(self):
        source = frame()
        np.testing.assert_allclose(evaluate('safe_divide(elapsed_seconds, moves)', source.columns, source.schema, 4), [2., 2., 3., np.nan])
        np.testing.assert_array_equal(evaluate('(moves > 10) and (mode == "blitz")', source.columns, source.schema, 4), [False, True, False, False])
        np.testing.assert_array_equal(evaluate('mode != "blitz"', source.columns, source.schema, 4), [False, False, False, True])
        np.testing.assert_allclose(evaluate('fill_missing(elapsed_seconds, 0)', source.columns, source.schema, 4), [20., 40., 120., 0.])
        np.testing.assert_allclose(evaluate('where(moves > 10, moves, 0)', source.columns, source.schema, 4), [0., 20., 40., 30.])
        with self.assertRaises(ValueError): parse_expression('mode + moves', source.schema)
        with self.assertRaises(ValueError): evaluate('moves / 0', source.columns, source.schema, 4, invalid='error')

    def test_utc_group_keys_and_scalar_broadcast(self):
        values = {'timestamp': np.array([0., 86400., np.nan])}
        result = evaluate('utc_day(timestamp)', values, {'timestamp': 'number'}, 3)
        self.assertEqual(result.tolist(), ['1970-01-01', '1970-01-02', None])
        self.assertEqual(evaluate('True', values, {'timestamp': 'number'}, 3).tolist(), [True] * 3)


class SchemaTests(unittest.TestCase):
    def test_normalized_schema_keeps_categories_and_independent_outputs(self):
        original = deepcopy(SPEC)
        config = normalize_builder(SPEC, MATRIX)
        self.assertEqual(config['schemas']['input']['mode'], 'category')
        self.assertEqual(config['schemas']['pace']['seconds_per_full_move'], 'number')
        self.assertEqual(config['schemas']['totals'], {'result': 'number', 'games': 'number', 'mean_seconds': 'number'})
        self.assertEqual(SPEC, original)
        config['matrix']['filters']['modes'].append('bullet')
        self.assertEqual(MATRIX['filters']['modes'], ['blitz'])

    def test_invalid_graphs_and_outputs_rejected_before_extraction(self):
        bad = []
        value = deepcopy(SPEC); value['steps'][0]['input'] = 'pace'; bad.append(value)
        value = deepcopy(SPEC); value['steps'][1]['column'] = 'moves'; bad.append(value)
        value = deepcopy(SPEC); value['steps'][0]['expression'] = 'moves + 1'; bad.append(value)
        value = deepcopy(SPEC); value['outputs'][1]['x'] = 'mode'; bad.append(value)
        value = deepcopy(SPEC); value['outputs'][0]['type'] = 'heatmap'; bad.append(value)
        value = deepcopy(SPEC); value['max_rows'] = 100001; bad.append(value)
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError): normalize_builder(value, MATRIX)


class StepTests(unittest.TestCase):
    def test_weighted_mean_population_std_null_groups_and_zero_weight(self):
        source = Frame({'value': np.array([2., 4., 9., np.nan]), 'weight': np.array([1., 3., 0., 1.]),
                        'group': np.array(['a', 'a', None, None], dtype=object)},
                       {'value': 'number', 'weight': 'number', 'group': 'category'}, np.array(['a', 'b', 'c', 'd']))
        step = {'op': 'aggregate', 'group_by': ['group'], 'metrics': [
            {'name': 'weighted', 'op': 'weighted_mean', 'column': 'value', 'weight': 'weight'},
            {'name': 'std', 'op': 'std', 'column': 'value'}, {'name': 'count', 'op': 'count'}]}
        result = apply_step(source, step, {'group': 'category', 'weighted': 'number', 'std': 'number', 'count': 'number'}, 'null')
        index = result.columns['group'].tolist().index('a')
        self.assertAlmostEqual(result.columns['weighted'][index], 3.5)
        self.assertAlmostEqual(result.columns['std'][index], 1)
        self.assertEqual(result.columns['count'].tolist(), [2, 2])
        self.assertTrue(np.isnan(result.columns['weighted'][1 - index]))
        source.columns['weight'][0] = -1
        with self.assertRaisesRegex(ValueError, 'nonnegative'): apply_step(source, step, result.schema, 'null')

    def test_preparation_counts_missing_categories_and_imputes_only_numeric(self):
        prepared, counts, excluded, imputed = prepare(frame(), 'column_mean')
        self.assertEqual(imputed, 1); self.assertEqual(excluded, 0)
        self.assertIsNone(prepared.columns['mode'][2])
        self.assertEqual(counts['mode'], 1)
        self.assertEqual(prepare(frame(), 'drop_rows')[0].row_keys.tolist(), ['a', 'b'])

    def test_constant_transform_and_correlations(self):
        source = Frame({'a': np.ones(3), 'b': np.array([1., 2., 3.])}, {'a': 'number', 'b': 'number'}, np.arange(3))
        normalized = apply_step(source, {'op': 'transform', 'columns': ['a'], 'method': 'normalize'}, source.schema, 'null')
        np.testing.assert_array_equal(normalized.columns['a'], np.zeros(3))
        correlated = apply_step(source, {'op': 'correlate', 'columns': ['a', 'b']}, {'variable': 'category', 'a': 'number', 'b': 'number'}, 'null')
        result = render_output(correlated, {'type': 'heatmap'}, 42)
        self.assertIsNone(result['correlations'][0][0])
        self.assertAlmostEqual(result['correlations'][1][1], 1)
        json.dumps(result, allow_nan=False)

    def test_outputs_are_bounded_and_scalar_requires_one_row(self):
        source = Frame({'a': np.arange(2000.), 'b': np.arange(2000.)}, {'a': 'number', 'b': 'number'}, np.arange(2000))
        output = {'type': 'scatter', 'x': 'a', 'y': 'b'}
        one = render_output(source, output, 42)
        self.assertEqual(one, render_output(source, output, 42))
        self.assertEqual(len(one['points']), 1000)
        self.assertTrue(one['sampled'])
        self.assertEqual(len(render_output(source, {'type': 'table', 'columns': ['a']}, 42)['rows']), 100)
        self.assertEqual(len(render_output(source, {'type': 'line', 'x': 'a', 'y': 'b'}, 42)['points']), 200)
        with self.assertRaises(ValueError): render_output(source, {'type': 'scalar', 'column': 'a'}, 42)


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_formula_graph_branches_intermediates_and_json(self):
        config = normalize_builder(SPEC, MATRIX)
        source = {'taken_at': 'now', 'input_sha256': 'test', 'inputs_retained': False}
        with patch.object(builder_runner, 'extract', AsyncMock(return_value=(frame(), source, .1))):
            result = await builder_runner.execute(config, AsyncMock())
        self.assertEqual([step['output_rows'] for step in result['steps']], [3, 3, 2])
        self.assertEqual(result['outputs'][0]['rows'], [[.5, 1., None], [1., 2., 2.5]])
        self.assertEqual([point['row_key'] for point in result['outputs'][1]['points']], ['b', 'c'])
        self.assertEqual(result['final_rows'], 2)
        json.dumps(result, allow_nan=False)

    async def test_cancel_between_steps(self):
        config = normalize_builder(SPEC, MATRIX)
        reporter = AsyncMock()
        reporter.check.side_effect = RunCancelled('cancelled')
        with patch.object(builder_runner, 'extract', AsyncMock(return_value=(frame(), {}, .1))):
            with self.assertRaises(RunCancelled): await builder_runner.execute(config, reporter)


class BuilderApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_revision_uses_frozen_recipe_even_after_matrix_deletion(self):
        parent_id = uuid.uuid4()
        parent_config = {'matrix': deepcopy(MATRIX), 'revision': 3}
        session = AsyncMock()
        session.get.return_value = SimpleNamespace(config=parent_config, matrix_definition_id=None)
        request = routes.BuilderRequest(**SPEC, name='Revision', parent_definition_id=parent_id)
        value = await routes.request_config(request, session)
        self.assertEqual(value['revision'], 4)
        self.assertEqual(value['parent_definition_id'], str(parent_id))
        self.assertEqual(parent_config['revision'], 3)
        self.assertEqual(session.get.await_count, 1)

    async def test_sample_is_explicitly_queued_with_frozen_capped_config(self):
        app = FastAPI()
        app.include_router(routes.router, prefix='/algorithms')
        app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(id=None)
        app.dependency_overrides[get_redis_pool] = lambda: AsyncMock()
        from unittest.mock import MagicMock
        factory = MagicMock()
        factory.return_value.__aenter__.return_value = AsyncMock()
        normalized = normalize_builder(SPEC, {**MATRIX, 'filters': {'max_rows': 10000}})
        normalized['max_rows'] = 10000
        queued = AsyncMock(return_value={'id': 'sample', 'status': 'queued'})
        with patch.object(routes, 'AsyncDBSession', factory), patch.object(routes, 'request_config', AsyncMock(return_value=normalized)), patch.object(routes, 'enqueue_run', queued):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.post('/algorithms/sample', json={**SPEC, 'name': 'Draft', 'matrix_definition_id': str(uuid.uuid4())})
        self.assertEqual(response.status_code, 202, response.text)
        args = queued.call_args
        self.assertIsNone(args.args[0])
        self.assertEqual(args.kwargs['draft'][1]['max_rows'], 500)
        self.assertTrue(args.kwargs['draft'][1]['sample_run'])


if __name__ == '__main__': unittest.main()
