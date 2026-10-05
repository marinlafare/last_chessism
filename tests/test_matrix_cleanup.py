"""Regression checks for definition/legacy boundaries and narrow preview queries."""

import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
import httpx

from chessism_api.operations.matrix_constructor import live_preview
from chessism_api.operations.matrix_constructor.config import normalize_matrix_config
from chessism_api.operations.matrix_constructor.queries import matrix_sql
from chessism_api.routers import research_matrices
from chessism_api.routers.matrices import definitions, legacy_snapshots


class MatrixBoundariesTests(unittest.TestCase):
    def test_importing_definition_routes_does_not_load_materialization(self):
        result = subprocess.run([sys.executable, "-c", """
import sys
import chessism_api.routers.research_matrices
root = 'chessism_api.operations.matrix_constructor.'
assert not any(root + name in sys.modules for name in ('jobs', 'arrays', 'legacy_estimates', 'preview'))
"""], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_empty_column_projection_is_valid_for_every_row_type(self):
        from chessism_api.operations.matrix_constructor.catalog import ROW_TYPES
        for key, row_type in ROW_TYPES.items():
            config = normalize_matrix_config({"row_type": key, "feature_columns": [row_type.columns[0].key]})
            config["feature_columns"] = []
            _, query, _ = matrix_sql(config)
            self.assertNotIn('row_key,', query, key)
            self.assertIn('AS row_key', query)


class MatrixCleanupQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_features_preview_skips_label_enrichment_and_vice_versa(self):
        session = AsyncMock()
        result = MagicMock()
        result.mappings.return_value = [{"row_key": "1:white", "moves": 20, "game_salience": 0.25}]
        session.execute.return_value = result
        factory = MagicMock()
        factory.return_value.__aenter__.return_value = session
        config = {"row_type": "game_player", "feature_columns": ["moves"], "label_columns": ["game_salience"]}
        with patch.object(live_preview, "AsyncDBSession", factory):
            features = await live_preview.preview_definition(config, role="features")
            query = str(session.execute.call_args.args[0])
            self.assertNotIn('JOIN game_player_salience', query)
            self.assertEqual(features["rows"], [[20]])
            labels = await live_preview.preview_definition(config, role="labels")
            query = str(session.execute.call_args.args[0])
            self.assertIn('JOIN game_player_salience', query)
            self.assertNotIn('AS "moves"', query)
            self.assertEqual(labels["rows"], [[0.25]])

    async def test_preview_without_labels_still_has_row_identity(self):
        session = AsyncMock()
        result = MagicMock()
        result.mappings.return_value = [{"row_key": "1:white"}]
        session.execute.return_value = result
        factory = MagicMock()
        factory.return_value.__aenter__.return_value = session
        with patch.object(live_preview, "AsyncDBSession", factory):
            preview = await live_preview.preview_definition({"row_type": "game_player", "feature_columns": ["game_salience"]}, role="labels")
        self.assertEqual(preview["rows"], [[]])
        self.assertEqual(preview["row_keys"], ["1:white"])
        self.assertNotIn('game_player_salience', str(session.execute.call_args.args[0]))

    async def test_legacy_progress_reads_redis_only_for_active_jobs(self):
        with patch.object(legacy_snapshots, 'get_redis_pool', AsyncMock(side_effect=AssertionError('must not connect'))):
            self.assertEqual(await legacy_snapshots._active_progress([SimpleNamespace(id='a', job_id='old', status='complete')]), {})
        redis = AsyncMock()
        redis.mget.return_value = [b'{"phase":"running"}']
        with patch.object(legacy_snapshots, 'get_redis_pool', AsyncMock(return_value=redis)):
            progress = await legacy_snapshots._active_progress([SimpleNamespace(id='a', job_id='job', status='running')])
        self.assertEqual(progress, {'a': {'phase': 'running'}})
        redis.mget.assert_awaited_once_with(['chessism:job_progress:job'])
        self.assertIsNone(legacy_snapshots._decode_progress(b'\xff'))

    async def test_router_keeps_definition_and_legacy_urls_distinct(self):
        app = FastAPI()
        app.include_router(research_matrices.router, prefix='/matrices')
        paths = app.openapi()['paths']
        for path in ('/matrices', '/matrices/preview', '/matrices/estimate',
                     '/matrices/definitions/{definition_id}', '/matrices/definitions/{definition_id}/preview',
                     '/matrices/snapshots', '/matrices/snapshots/{artifact_id}',
                     '/matrices/snapshots/{artifact_id}/preview', '/matrices/{artifact_id}/preview'):
            self.assertIn(path, paths)
        with patch.object(definitions, 'preview_definition', AsyncMock(return_value={'rows': []})):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.post('/matrices/preview', json={'row_type': 'game_player', 'feature_columns': ['moves']})
                self.assertEqual(response.status_code, 200)
                self.assertEqual((await client.get('/matrices/catalog')).json()['workflow'], 'definition_only')
