"""Exercise server-owned preview bounds without a database or billable jobs."""
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException
from chessism_api.routers import cloud_analysis as jobs, cloud_game_drafts as drafts
from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest


class SelectionAccepted(Exception):
    """Stop at the first mocked operation after selection-bound validation."""


class CloudRunSelectionLimitTests(unittest.IsolatedAsyncioTestCase):
    def test_combined_cpu_limit_matches_single_selection(self):
        for cpus in (1, 100, 101, 200):
            self.assertEqual(drafts.SubmitDraft(revision=1, backend='cloud_run', n_cpus=cpus).n_cpus, cpus)
        for cpus in (0, 201, 1.5, '200', True):
            with self.assertRaises(ValueError):
                drafts.SubmitDraft(revision=1, backend='cloud_run', n_cpus=cpus)

    async def test_saved_game_preview_bound(self):
        for count in (200001, 3700000, 6000000, 6000001):
            with self.subTest(count=count):
                redis = SimpleNamespace(get=AsyncMock(return_value=json.dumps({
                    'game_links': [101], 'player_name': 'example', 'fens_to_analyze': count})))
                session = MagicMock()
                with patch.object(jobs, 'AsyncDBSession', return_value=session), \
                     patch.object(jobs, 'controller_online', AsyncMock(side_effect=SelectionAccepted)) as accepted:
                    request = CloudJobRequest(backend='cloud_run', mode='games', plan_id='a' * 32)
                    if count <= 6000000:
                        with self.assertRaises(SelectionAccepted):
                            await jobs.create_cloud_job(request, redis)
                        accepted.assert_awaited_once()
                    else:
                        with self.assertRaises(HTTPException) as error:
                            await jobs.create_cloud_job(request, redis)
                        self.assertEqual(error.exception.status_code, 409)
                        self.assertIn('6,000,000', error.exception.detail)
                        accepted.assert_not_awaited()

    async def test_combined_server_recount_bound(self):
        for count in (200001, 3700000, 6000000, 6000001):
            with self.subTest(count=count):
                session = MagicMock()
                session.__aenter__.return_value = session
                draft = SimpleNamespace(id='b' * 32, submitted_job_id=None,
                    revision=1, preview_revision=1, preview_fens=count)
                with patch.object(drafts, 'AsyncDBSession', return_value=session), \
                     patch.object(drafts.drafts, 'locked_draft', AsyncMock(return_value=draft)), \
                     patch.object(jobs, 'controller_online', AsyncMock(return_value=True)), \
                     patch.object(drafts, 'require_available', AsyncMock()), \
                     patch.object(drafts.drafts, 'count_union', AsyncMock(return_value={'eligible': count})), \
                     patch.object(drafts.drafts, 'game_links', AsyncMock(side_effect=SelectionAccepted)) as accepted:
                    request = drafts.SubmitDraft(revision=1, backend='cloud_run', n_cpus=200)
                    if count <= 6000000:
                        with self.assertRaises(SelectionAccepted):
                            await drafts.submit_draft(draft.id, request)
                        accepted.assert_awaited_once()
                    else:
                        with self.assertRaises(HTTPException) as error:
                            await drafts.submit_draft(draft.id, request)
                        self.assertEqual(error.exception.status_code, 409)
                        self.assertIn('6,000,000', error.exception.detail)
                        accepted.assert_not_awaited()
