"""Fast contract tests; no database or Google resources."""
import unittest
from fastapi import HTTPException
from pydantic import ValidationError
from chessism_api.operations.cloud_analysis.game_drafts import preview_entry
from chessism_api.routers.cloud_game_drafts import AddSelection, SubmitDraft


def plan(links=(101, 102), player='hikaru'):
    return {'game_links': list(links), 'player_name': player, 'selection_mode': 'latest',
            'selection_order': 'latest', 'requested_games': 2, 'fens_to_analyze': 3,
            'date_from': None, 'date_to': None}


class DraftContractTests(unittest.TestCase):
    def test_server_preview_is_normalized_and_deduplicated(self):
        values, links = preview_entry(plan((101, 102, 101), ' Hikaru '))
        self.assertEqual(links, [101, 102])
        self.assertEqual(values['player_name'], 'hikaru')
        self.assertEqual(values['selected_games'], 2)

    def test_all_game_selection_keeps_dates_and_no_limit(self):
        values, _ = preview_entry({**plan(), 'selection_mode': 'range', 'requested_games': None,
                                   'date_from': '2025-01-01', 'date_to': '2026-01-01'})
        self.assertIsNone(values['requested_games'])
        self.assertEqual(str(values['date_from']), '2025-01-01')

    def test_invalid_saved_previews_fail_closed(self):
        for changes in ({'game_links': []}, {'game_links': [True]}, {'game_links': ['101']},
                        {'selection_mode': 'anything'}, {'fens_to_analyze': -1},
                        {'date_from': 'bad'}, {'player_name': ''}):
            with self.subTest(changes=changes), self.assertRaises(HTTPException):
                preview_entry({**plan(), **changes})

    def test_clients_cannot_supply_fens_counts_games_or_engine_settings(self):
        for field in ('game_links', 'total_fens', 'nodes', 'backend', 'threads'):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                SubmitDraft.model_validate({'revision': 1, field: 1})
        with self.assertRaises(ValidationError):
            AddSelection(revision=1, plan_id='untrusted-id')
        for value in (0, 11, 1.5, '2', True):
            with self.assertRaises(ValidationError):
                SubmitDraft(revision=1, n_vms=value)


class PreparedPreviewContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_combined_preview_skips_unused_scope_and_retains_prepared_token(self):
        import json
        from unittest.mock import AsyncMock, patch
        from chessism_api.routers import analysis
        request = analysis.PlayerGamePreviewRequest(player_name='hikaru',game_limit=200,prepare_combined=True)
        redis = AsyncMock()
        result = {**plan(), 'selected_games':2, 'prepared_fens_version':1}
        with patch.object(analysis,'get_player_game_analysis_scope',side_effect=AssertionError('Unused scope scan')), \
                patch.object(analysis,'preview_player_games_for_analysis',new=AsyncMock(return_value=result)) as preview:
            reply = await analysis.api_preview_player_game_analysis(request,redis)
        self.assertEqual(preview.await_args.kwargs['prepared_plan_id'],reply['plan_id'])
        saved=json.loads(redis.set.await_args.args[1])
        self.assertEqual(saved['game_links'],result['game_links'])
        self.assertEqual(saved['prepared_fens_version'],1)
        self.assertNotIn('game_links',reply)
        self.assertNotIn('estimated_seconds',reply)

    async def test_scope_request_keeps_its_original_contract(self):
        from unittest.mock import AsyncMock,patch
        from chessism_api.routers import analysis
        with patch.object(analysis,'get_player_game_analysis_scope',new=AsyncMock(return_value={'incomplete_games':3})):
            value=await analysis.api_get_player_game_analysis_scope(analysis.PlayerGameScopeRequest(player_name='hikaru'))
        self.assertEqual(value['incomplete_games'],3)
