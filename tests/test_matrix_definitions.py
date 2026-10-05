from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from fastapi import FastAPI, HTTPException
import httpx

from chessism_api.database.models import MatrixDefinition
from chessism_api.operations.matrix_constructor import definitions, live_preview
from chessism_api.operations.matrix_constructor.definition_backups import definition_fingerprint, validate_definition_backup
from chessism_api.routers import research_matrices as routes


CONFIG = {"name": "Small recipe", "row_type": "game_player",
          "feature_columns": ["moves", "elapsed_seconds"], "label_columns": ["result"],
          "filters": {"players": ["lafareto"], "max_rows": 100}}


class DefinitionTests(unittest.TestCase):
    def test_config_contains_only_allowlisted_instructions(self):
        config = definitions.definition_config({**CONFIG, "selection_order": "DROP TABLE game", "rows": [1, 2]})
        self.assertEqual(config["definition_version"], 1)
        self.assertNotIn("DROP", config["selection_order"])
        self.assertNotIn("rows", config)
        self.assertEqual(config["filters"]["players"], ["lafareto"])
        self.assertLess(len(json.dumps(config)), 2000)

    def test_source_values_preserve_nulls_categories_and_large_integers(self):
        self.assertIsNone(live_preview.source_value(None))
        self.assertIsNone(live_preview.source_value(float('nan')))
        self.assertEqual(live_preview.source_value('bullet'), 'bullet')
        self.assertEqual(live_preview.source_value(0), 0)
        self.assertEqual(live_preview.source_value(2 ** 53 + 1), str(2 ** 53 + 1))

    def test_backup_fingerprint_checks_exact_instructions_and_membership(self):
        records = [{"id": "a", "name": "a", "row_type": "game_player", "config": CONFIG},
                   {"id": "b", "name": "b", "row_type": "game_player", "config": CONFIG}]
        recorded = definition_fingerprint(records)
        self.assertEqual(recorded, definition_fingerprint(list(reversed(records))))
        self.assertEqual(validate_definition_backup(recorded, records)["count"], 2)
        with self.assertRaisesRegex(ValueError, "differ"):
            validate_definition_backup(recorded, records[:1])
        records[0] = {**records[0], "config": {**CONFIG, "name": "changed"}}
        with self.assertRaisesRegex(ValueError, "differ"):
            validate_definition_backup(recorded, records)
        self.assertEqual(validate_definition_backup(definition_fingerprint([]), [])["count"], 0)


class DefinitionQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_preview_only_queries_bounded_prefix_without_counting(self):
        session = AsyncMock()
        result = MagicMock()
        result.mappings.return_value = [{"row_key": str(i), "moves": i, "elapsed_seconds": None, "result": 0.5} for i in range(51)]
        session.execute.return_value = result
        factory = MagicMock(); factory.return_value.__aenter__.return_value = session
        with patch.object(live_preview, "AsyncDBSession", factory):
            preview = await live_preview.preview_definition(CONFIG)
        sql, params = session.execute.call_args.args
        self.assertNotIn("COUNT(*)", str(sql))
        self.assertEqual(params["max_rows"], 51)
        self.assertEqual(len(preview["rows"]), 50)
        self.assertEqual(preview["rows"][0], [0, None, 0.5])
        self.assertTrue(preview["more_matching_rows"])
        self.assertFalse(preview["has_more"])
        self.assertFalse(preview["materialized"])
        self.assertTrue(any('statement_timeout' in str(call.args[0]) for call in session.execute.call_args_list))
        session.commit.assert_not_awaited()

    async def test_preview_respects_recipe_cap_empty_results_and_category_labels(self):
        config = {**CONFIG, "feature_columns": ["mode"], "filters": {"max_rows": 1}}
        session = AsyncMock(); result = MagicMock()
        result.mappings.return_value = [{"row_key": "1:white", "mode": "bullet", "result": 1}]
        session.execute.return_value = result
        factory = MagicMock(); factory.return_value.__aenter__.return_value = session
        with patch.object(live_preview, "AsyncDBSession", factory):
            preview = await live_preview.preview_definition(config, limit=100)
            self.assertEqual(preview["rows"], [["bullet", 1]])
            self.assertEqual(preview["columns"][0]["encoding"], "source_category")
            self.assertEqual(session.execute.call_args.args[1]["max_rows"], 1)
            result.mappings.return_value = []
            empty = await live_preview.preview_definition(config)
            self.assertEqual(empty["rows"], [])
            self.assertEqual(empty["shape"], [0, 2])

    async def test_large_estimate_is_a_lower_bound_and_never_checks_filesystem(self):
        session = AsyncMock(); result = MagicMock(); result.scalar_one.return_value = 10001
        session.execute.return_value = result
        factory = MagicMock(); factory.return_value.__aenter__.return_value = session
        with patch.object(definitions, "AsyncDBSession", factory):
            estimate = await definitions.estimate_definition({**CONFIG, "filters": {"max_rows": 5000000}})
        self.assertEqual(session.execute.call_args.args[1]["count_limit"], 10001)
        self.assertTrue(estimate["selected_rows_is_lower_bound"])
        self.assertNotIn("storage", estimate)
        self.assertFalse(estimate["materialized"])

    async def test_save_is_metadata_only_without_query_estimate_or_redis(self):
        session = AsyncMock()
        inserted = []
        def add(row):
            row.created_at = datetime.now(timezone.utc)
            inserted.append(row)
        session.add = MagicMock(side_effect=add)
        @asynccontextmanager
        async def lock(**kwargs):
            self.assertFalse(kwargs['wait'])
            yield session
        with patch.object(routes, "matrix_catalog_lock", lock), patch.object(routes, "estimate_definition", AsyncMock(side_effect=AssertionError('must not query data'))):
            saved = await routes.save_matrix_definition(routes.MatrixRequest(**CONFIG), account=SimpleNamespace(id=None))
        self.assertEqual(saved["status"], "saved")
        self.assertEqual(saved["storage_kind"], "definition")
        self.assertIsInstance(inserted[0], MatrixDefinition)
        self.assertNotIn("artifact_path", saved)
        self.assertNotIn("job_id", saved)
        session.execute.assert_not_awaited()

    async def test_delete_definition_never_calls_snapshot_deletion(self):
        session = AsyncMock(); definition = SimpleNamespace(id=str(uuid.uuid4()))
        session.get.return_value = definition
        @asynccontextmanager
        async def lock(**kwargs):
            yield session
        with patch.object(routes, 'matrix_catalog_lock', lock), patch.object(routes, '_delete_working_artifact', AsyncMock(side_effect=AssertionError('must keep snapshot'))):
            await routes.delete_matrix_definition(uuid.UUID(definition.id))
        session.delete.assert_awaited_once_with(definition)

    async def test_live_endpoint_validation_and_auth_scope(self):
        app = FastAPI()
        app.include_router(routes.router, prefix='/matrices')
        with patch.object(routes, 'preview_definition', AsyncMock(return_value={"rows": [], "materialized": False})) as preview:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.post('/matrices/preview', json=CONFIG)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual((await client.post('/matrices/preview?limit=101', json=CONFIG)).status_code, 422)
                self.assertEqual((await client.post('/matrices/preview', json={**CONFIG, 'feature_columns': ['raw_sql']})).status_code, 422)
                self.assertEqual(preview.await_count, 1)


if __name__ == '__main__':
    unittest.main()
