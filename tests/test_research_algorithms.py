"""CPU reference, allowlists, cleanup, queue and backup integrity regressions."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import numpy as np
from fastapi import FastAPI, Depends
import httpx

from chessism_api.auth import get_current_account, require_superuser
from chessism_api.database.models import AccountRole

from chessism_api.operations.research_algorithms import config, jobs, storage
from chessism_api.operations.research_algorithms.backups import validate_algorithm_backup
from chessism_api.operations.research_algorithms.numerics import ColumnStats, Relationships, prepare_block
from chessism_api.operations.research_algorithms.repository import RunCancelled
from chessism_api.routers import research_algorithms as routes


MATRIX = {"row_type": "game_player", "feature_columns": ["rating", "opponent_rating", "moves", "mode"],
          "label_columns": ["result"], "filters": {"players": ["lafareto"], "modes": ["blitz"], "max_rows": 100}}
REQUEST = {"columns": ["rating", "opponent_rating"], "x": "rating", "y": "opponent_rating", "max_rows": 100}


class ConfigTests(unittest.TestCase):
    def test_recipe_is_frozen_and_source_projects_only_needed_columns(self):
        original = deepcopy(MATRIX)
        value = config.normalize_algorithm(REQUEST, MATRIX)
        value["matrix"]["filters"]["players"].append("other")
        self.assertEqual(MATRIX, original)
        source = config.source_config(value)
        self.assertEqual(source["feature_columns"], REQUEST["columns"])
        self.assertEqual(source["label_columns"], [])
        self.assertEqual(source["filters"]["max_rows"], 100)

    def test_bad_columns_categories_axes_and_limits_are_rejected(self):
        for changes in ({"columns": ["rating", "mode"]}, {"columns": ["rating", "raw_sql"]},
                        {"columns": ["rating"]}, {"x": "unknown"}, {"y": "rating"},
                        {"seed": -1}, {"seed": 2**32}, {"max_rows": 1000001},
                        {"missing": "zero"}, {"scaling": "magic"}, {"operation": "eval_python"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                config.normalize_algorithm({**REQUEST, **changes}, MATRIX)

    def test_cap_and_derived_column(self):
        value = config.normalize_algorithm({**REQUEST, "rating_difference": True, "x": "rating_difference", "max_rows": 500}, MATRIX)
        self.assertEqual(value["max_rows"], 100)
        self.assertEqual(value["backend"], "numpy_cpu")
        with self.assertRaises(ValueError):
            config.normalize_algorithm({**REQUEST, "columns": ["rating", "moves"], "rating_difference": True}, MATRIX)


class NumericalTests(unittest.TestCase):
    def test_streaming_stats_match_numpy_and_preserve_source_keys(self):
        data = np.array([[1., 4., 7.], [2., 3., 7.], [4., 2., 7.], [8., 1., 7.]])
        calc = Relationships(["a", "b", "c"], seed=42, sample_limit=100)
        calc.add(data[:2], ['first', 'second']); calc.add(data[2:], ['third', 'fourth'])
        result = calc.finish(scaling="none", x="a", y="b")
        self.assertAlmostEqual(result["correlations"][0][1], np.corrcoef(data[:, :2].T)[0, 1])
        self.assertIsNone(result["correlations"][0][2])
        self.assertTrue(result["summaries"][2]["constant"])
        np.testing.assert_allclose([s['mean'] for s in result['summaries']], data.mean(axis=0))
        np.testing.assert_allclose([s['std'] for s in result['summaries']], data.std(axis=0))
        self.assertEqual(result['scatter']['points'][2]['row_key'], 'third')

    def test_missing_values_mean_imputation_and_derived_difference(self):
        data = np.array([[1., 4.], [np.nan, 6.], [3., np.inf]])
        stats = ColumnStats(2); stats.add(data[:1]); stats.add(data[1:])
        np.testing.assert_allclose(stats.mean, [2, 5])
        np.testing.assert_array_equal(stats.count, [2, 2])
        settings = config.normalize_algorithm({**REQUEST, 'rating_difference': True}, MATRIX)
        block, keys, affected = prepare_block(data, ['a', 'b', 'c'], settings, stats.mean)
        np.testing.assert_allclose(block, [[1, 4, 3]])
        self.assertEqual(keys, ['a']); self.assertEqual(affected, 2)
        settings['missing'] = 'column_mean'
        block, keys, affected = prepare_block(data, ['a', 'b', 'c'], settings, stats.mean)
        np.testing.assert_allclose(block, [[1, 4, 3], [2, 6, 4], [3, 5, 2]])
        self.assertEqual(keys, ['a', 'b', 'c'])

    def test_standardization_and_sampling_are_deterministic(self):
        data = np.random.default_rng(11).normal(size=(2000, 2))
        results = []
        for _ in range(2):
            calc = Relationships(['a', 'b'], seed=42, sample_limit=10)
            calc.add(data, [str(i) for i in range(len(data))])
            results.append(calc.finish(scaling='standardize', x='a', y='b'))
        self.assertEqual(results[0], results[1])
        self.assertEqual(len(results[0]['scatter']['points']), 10)
        self.assertTrue(results[0]['scatter']['sampled'])
        point = results[0]['scatter']['points'][0]
        self.assertAlmostEqual(point['x'], (data[int(point['row_key']), 0] - data[:, 0].mean()) / data[:, 0].std())

    def test_fewer_than_two_rows_is_not_a_fabricated_correlation(self):
        calc = Relationships(['a', 'b'], seed=42, sample_limit=10)
        calc.add(np.array([[1., 2.]]), ['a'])
        with self.assertRaisesRegex(ValueError, 'two usable'):
            calc.finish(scaling='none', x='a', y='b')


class StorageTests(unittest.TestCase):
    def test_cleanup_success_failure_crash_and_worker_exclusion(self):
        with TemporaryDirectory() as temporary, patch.object(storage, 'WORK_ROOT', Path(temporary)):
            with storage.workspace(str(uuid.uuid4())) as folder:
                self.assertTrue(folder.is_dir())
            self.assertFalse(folder.exists())
            with self.assertRaises(ValueError):
                with storage.workspace(str(uuid.uuid4())) as failed:
                    raise ValueError('test error')
            self.assertFalse(failed.exists())
            stale = Path(temporary) / f'run-{uuid.uuid4()}-abc'; stale.mkdir()
            keep = Path(temporary) / 'unrelated'; keep.mkdir()
            link = Path(temporary) / f'run-{uuid.uuid4()}-link'; link.symlink_to(keep, target_is_directory=True)
            handle = storage.acquire_worker_lock()
            try:
                with self.assertRaisesRegex(RuntimeError, 'Another'):
                    storage.acquire_worker_lock()
                self.assertEqual(storage.cleanup_interrupted_workspaces(), [stale.name])
                self.assertTrue(keep.exists()); self.assertTrue(link.is_symlink())
            finally:
                handle.close()

    def test_capacity_rejects_low_disk_without_creating_data(self):
        with patch.object(storage.shutil, 'disk_usage', return_value=SimpleNamespace(free=1)):
            self.assertFalse(storage.capacity(config.normalize_algorithm(REQUEST, MATRIX))['safe_to_run'])
            with self.assertRaisesRegex(ValueError, 'Insufficient'):
                storage.ensure_capacity(config.normalize_algorithm(REQUEST, MATRIX))

    def test_backup_validation_detects_corruption(self):
        data = {'schema_version': 1, 'definitions': {'count': 1, 'md5': 'a'}, 'completed_runs': {'count': 2, 'md5': 'b'}}
        self.assertEqual(validate_algorithm_backup(data, deepcopy(data))['status'], 'passed')
        for bad in ({}, {**data, 'completed_runs': {'count': 2, 'md5': 'bad'}}):
            with self.assertRaises(ValueError): validate_algorithm_backup(data, bad)


class JobTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_superuser_boundary(self):
        app = FastAPI()
        app.include_router(routes.router, prefix='/research/algorithms', dependencies=[Depends(require_superuser)])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            self.assertEqual((await client.get('/research/algorithms/catalog')).status_code, 401)
            app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(role=AccountRole.user)
            self.assertEqual((await client.get('/research/algorithms/catalog')).status_code, 403)
            app.dependency_overrides[get_current_account] = lambda: SimpleNamespace(role=AccountRole.admin)
            result = await client.get('/research/algorithms/catalog')
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()['backends'], ['numpy_cpu'])

    async def test_publish_waits_for_backup_and_cancellation_stays_available(self):
        from chessism_api.operations.matrix_constructor.storage import MatrixCatalogBusy
        @asynccontextmanager
        async def busy(**kwargs):
            raise MatrixCatalogBusy('backup active')
            yield
        reporter = AsyncMock()
        reporter.check.side_effect = [None, RunCancelled('cancelled while waiting')]
        with patch.object(jobs, 'matrix_catalog_lock', busy), patch.object(jobs.asyncio, 'sleep', AsyncMock()):
            with self.assertRaises(RunCancelled):
                await jobs.publish('waiting', {}, reporter)
        self.assertEqual(reporter.check.await_count, 2)

    async def test_all_missing_imputation_fails_before_opening_arrays(self):
        with self.assertRaisesRegex(ValueError, 'no available'):
            await jobs.calculate({'missing': 'column_mean'}, Path('/not-created'), {'non_missing': np.array([0, 2])}, AsyncMock())

    async def test_failure_and_cancellation_clean_workspace(self):
        for exception in (ValueError('broken source'), RunCancelled('cancelled')):
            with self.subTest(exception=exception), TemporaryDirectory() as temporary, patch.object(storage, 'WORK_ROOT', Path(temporary)):
                row = SimpleNamespace(status='queued', config=config.normalize_algorithm(REQUEST, MATRIX), started_at=None)
                session = AsyncMock(); session.begin = MagicMock(); session.get.return_value = row
                factory = MagicMock(); factory.return_value.__aenter__.return_value = session
                async def fail(_config, folder, reporter):
                    self.assertTrue(folder.is_dir())
                    raise exception
                with patch.object(jobs, 'AsyncDBSession', factory), patch.object(jobs, 'Reporter', return_value=AsyncMock()), patch.object(jobs, 'materialize', side_effect=fail), patch.object(jobs, 'terminal', AsyncMock()) as terminal:
                    if isinstance(exception, RunCancelled):
                        self.assertEqual((await jobs.run_algorithm_job({}, run_id=str(uuid.uuid4())))['status'], 'cancelled')
                    else:
                        with self.assertRaisesRegex(ValueError, 'broken source'):
                            await jobs.run_algorithm_job({}, run_id=str(uuid.uuid4()))
                    terminal.assert_awaited_once()
                self.assertEqual(list(Path(temporary).iterdir()), [])

    async def test_cancelled_queued_run_is_never_materialized(self):
        session = AsyncMock(); session.begin = MagicMock(); session.get.return_value = SimpleNamespace(status='cancelled')
        factory = MagicMock(); factory.return_value.__aenter__.return_value = session
        with patch.object(jobs, 'AsyncDBSession', factory), patch.object(jobs, 'materialize', AsyncMock()) as materialize:
            self.assertEqual((await jobs.run_algorithm_job({}, run_id='skip'))['status'], 'not_queued')
            materialize.assert_not_awaited()

    async def test_save_is_instructions_only(self):
        session = AsyncMock(); session.get.return_value = SimpleNamespace(config=MATRIX)
        def add(row): row.created_at = datetime.now(timezone.utc)
        session.add = MagicMock(side_effect=add)
        @asynccontextmanager
        async def lock(**kwargs): yield session
        request = routes.AlgorithmRequest(**REQUEST, name='Test', matrix_definition_id=uuid.uuid4())
        with patch.object(routes, 'matrix_catalog_lock', lock), patch.object(routes, 'estimate_definition', AsyncMock()) as estimate:
            saved = await routes.save_definition(request, SimpleNamespace(id=None))
            self.assertEqual(saved['name'], 'Test')
            self.assertEqual(saved['config']['backend'], 'numpy_cpu')
            estimate.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
