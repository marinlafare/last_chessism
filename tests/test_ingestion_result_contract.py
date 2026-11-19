"""Structured completion results, without touching a database or Redis service."""
import asyncio
from contextlib import ExitStack, asynccontextmanager
import unittest
from unittest.mock import AsyncMock, patch
from chessism_api.operations.ingestion_pipeline import jobs, game_importer


class IngestionResultTests(unittest.IsolatedAsyncioTestCase):
    def harness(self, operation, fen):
        stack = ExitStack()
        self.addCleanup(stack.close)
        @asynccontextmanager
        async def reporter(*args):
            yield None
        stack.enter_context(patch.object(jobs, 'StepReporter', reporter))
        mocks = {}
        for name in ['start_ingestion_run', 'start_ingestion_stage', 'finish_ingestion_stage',
                     'finish_ingestion_run', '_write_game_job_progress', 'ensure_fen_pipeline_enqueued']:
            mocks[name] = stack.enter_context(patch.object(jobs, name, new_callable=AsyncMock))
        mocks['start_ingestion_run'].return_value = 'run-42'
        mocks['ensure_fen_pipeline_enqueued'].return_value = fen
        return mocks

    async def run_job(self, operation):
        return await jobs._run_game_job({'redis': object(), 'job_id': 'job-42'}, {'player_name': 'Alice'},
            operation=operation, fallback_job_id='fallback', queued_detail='Queued {player_name}.')

    async def test_committed_counts_and_run_survive_with_pending_followup(self):
        for phase in ['queued', 'already_active', 'up_to_date']:
            with self.subTest(phase=phase):
                operation = AsyncMock(return_value={'message': 'Imported.', 'committed_games': 7, 'zero_move_games': 2})
                fen = {'status': phase, 'job_id': 'fen-42' if phase != 'up_to_date' else None, 'pending_games': 9}
                mocks = self.harness(operation, fen)
                result = await self.run_job(operation)
                self.assertEqual(result['status'], 'complete')
                self.assertEqual(result['player_name'], 'alice')
                self.assertEqual(result['committed_games'], 7)
                self.assertEqual(result['zero_move_games'], 2)
                self.assertEqual(result['ingestion_run_id'], 'run-42')
                self.assertEqual(result['fen_extraction'], fen)
                self.assertEqual(mocks['finish_ingestion_run'].await_count, int(phase == 'up_to_date'))

    async def test_no_archives_returns_zero_counts_without_message_inference(self):
        with patch.object(jobs, 'missing_archive_months', AsyncMock(return_value=[])), \
             patch.object(jobs, 'update_archive_months', AsyncMock(return_value=[])):
            for operation in [jobs.create_games, jobs.update_player_games]:
                result = await operation({'player_name': 'alice'})
                self.assertEqual(result['committed_games'], 0)
                self.assertEqual(result['zero_move_games'], 0)

    async def test_persistence_error_propagates_and_cannot_queue_followup(self):
        error = RuntimeError('commit failed')
        operation = AsyncMock(side_effect=error)
        mocks = self.harness(operation, {})
        with self.assertRaises(RuntimeError) as caught:
            await self.run_job(operation)
        self.assertIs(caught.exception, error)
        mocks['ensure_fen_pipeline_enqueued'].assert_not_awaited()
        self.assertEqual(mocks['_write_game_job_progress'].call_args.kwargs['phase'], 'failed')

    async def test_cancellation_propagates_without_success_or_followup(self):
        operation = AsyncMock(side_effect=asyncio.CancelledError())
        mocks = self.harness(operation, {})
        with self.assertRaises(asyncio.CancelledError):
            await self.run_job(operation)
        mocks['ensure_fen_pipeline_enqueued'].assert_not_awaited()
        self.assertFalse(any(call.kwargs.get('phase') == 'complete' for call in mocks['_write_game_job_progress'].call_args_list))

    async def test_followup_enqueue_error_preserves_arq_failure(self):
        operation = AsyncMock(return_value={'message': 'Saved.', 'committed_games': 3, 'zero_move_games': 0})
        mocks = self.harness(operation, {})
        mocks['ensure_fen_pipeline_enqueued'].side_effect = RuntimeError('redis unavailable')
        with self.assertRaisesRegex(RuntimeError, 'redis unavailable'):
            await self.run_job(operation)
        self.assertEqual(mocks['_write_game_job_progress'].call_args.kwargs['phase'], 'failed')

    async def test_importer_counts_only_committed_identities_including_zero_move_games(self):
        for inserted, zero in [([1, 2, 3], 1), ([], 0)]:
            with self.subTest(inserted=inserted):
                connection = AsyncMock()
                connection.fetchval.return_value = zero
                committed = []
                @asynccontextmanager
                async def transaction(_):
                    yield object()
                    committed.append(True)
                bundle = {'games': [object()], 'moves': [], 'game_openings': [], 'affected_months': set(), 'affected_players': set()}
                with patch.object(game_importer, '_prepare_game_bundle', return_value=bundle), \
                     patch.object(game_importer, 'write_transaction', transaction), \
                     patch.object(game_importer, 'driver_connection', AsyncMock(return_value=connection)), \
                     patch.object(game_importer.game_imports, 'ingest_games', AsyncMock(return_value={'inserted_links': inserted, 'analyzable_links': []})), \
                     patch.object(game_importer.game_imports, 'sync_month_counts', AsyncMock()), \
                     patch.object(game_importer, 'refresh_import_summaries', AsyncMock()):
                    result = await game_importer.insert_games_months_moves_and_players([], 'alice')
                self.assertEqual(committed, [True])
                self.assertEqual(result['committed_games'], len(inserted))
                self.assertEqual(result['zero_move_games'], zero)

    async def test_commit_failure_cannot_return_counts(self):
        @asynccontextmanager
        async def transaction(_):
            yield object()
            raise RuntimeError('commit rejected')
        bundle = {'games': [object()], 'moves': [], 'game_openings': [], 'affected_months': set(), 'affected_players': set()}
        connection = AsyncMock(); connection.fetchval.return_value = 0
        with patch.object(game_importer, '_prepare_game_bundle', return_value=bundle), \
             patch.object(game_importer, 'write_transaction', transaction), \
             patch.object(game_importer, 'driver_connection', AsyncMock(return_value=connection)), \
             patch.object(game_importer.game_imports, 'ingest_games', AsyncMock(return_value={'inserted_links': [1], 'analyzable_links': [1]})), \
             patch.object(game_importer.game_imports, 'sync_month_counts', AsyncMock()), \
             patch.object(game_importer, 'refresh_import_summaries', AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, 'commit rejected'):
                await game_importer.insert_games_months_moves_and_players([], 'alice')

    def test_jobs_response_preserves_structured_ingestion_result_and_execution_authority(self):
        from types import SimpleNamespace
        from chessism_api.routers.jobs import _serialize_result
        result = {'status': 'complete', 'committed_games': 7, 'zero_move_games': 2,
                  'player_name': 'alice', 'ingestion_run_id': 'run-42',
                  'fen_extraction': {'status': 'queued', 'job_id': 'fen-42', 'pending_games': 9}}
        serialized = _serialize_result(SimpleNamespace(success=True, result=result,
            start_time=None, finish_time=None, queue_name='pipeline_queue'))
        self.assertTrue(serialized['success'])
        self.assertEqual(serialized['result'], result)
        failed = _serialize_result(SimpleNamespace(success=False, result=RuntimeError('commit failed'),
            start_time=None, finish_time=None, queue_name='pipeline_queue'))
        self.assertFalse(failed['success'])
        self.assertEqual(failed['result'], {'type': 'RuntimeError', 'message': 'commit failed'})
