"""Real HTTP adapter, parsing, importer and ARQ orchestration; synthetic persistence."""
import asyncio
from contextlib import ExitStack, asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from chessism_api.operations.ingestion_pipeline import chesscom, game_importer, jobs
from chessism_api.routers.jobs import _serialize_result


def game(link=101):
    return {'url': f'https://www.chess.com/game/live/{link}', 'rules': 'chess',
            'time_control': '600', 'white': {'username': 'alice', 'rating': 1500, 'result': 'win'},
            'black': {'username': 'bob', 'rating': 1600, 'result': 'resigned'},
            'pgn': '[Date "2026.01.31"]\n[StartTime "23:59:00"]\n[EndDate "2026.02.01"]\n'
                   '[EndTime "00:00:00"]\n\n1. e4 {[%clk 0:10:00]} e5 {[%clk 0:09:59]} 1-0'}


class ArchiveOutcomeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.responses = {}
        self.requested = []
        self.saved = {}
        self.months = set()
        self.commits = 0
        self.writes = 0
        self.selected = ['2026-01']
        real_client = httpx.AsyncClient

        def upstream(request):
            month = '-'.join(request.url.path.split('/')[-2:])
            self.requested.append(month)
            result = self.responses[month]
            if isinstance(result, BaseException):
                raise result
            return result

        self.stack.enter_context(patch.object(chesscom.httpx, 'AsyncClient',
            side_effect=lambda **kwargs: real_client(transport=httpx.MockTransport(upstream), **kwargs)))
        for name in ['missing_archive_months', 'update_archive_months']:
            self.stack.enter_context(patch.object(jobs, name, AsyncMock(side_effect=lambda *_: self.selected)))

        @asynccontextmanager
        async def reporter(*_):
            yield None

        @asynccontextmanager
        async def transaction(_):
            before = deepcopy(self.saved), self.months.copy()
            try:
                yield object()
                self.commits += 1
            except BaseException:
                self.saved, self.months = before
                raise

        async def ingest(connection, games, moves, openings):
            self.writes += 1
            inserted = []
            for row in games:
                if row['link'] not in self.saved:
                    self.saved[row['link']] = deepcopy(row)
                    inserted.append(row['link'])
            return {'inserted_links': inserted, 'analyzable_links': inserted}

        async def sync(connection, player, months):
            self.months.update(months)

        self.stack.enter_context(patch.object(game_importer, 'write_transaction', transaction))
        connection = SimpleNamespace(fetchval=AsyncMock(return_value=0))
        self.stack.enter_context(patch.object(game_importer, 'driver_connection', AsyncMock(return_value=connection)))
        self.stack.enter_context(patch.object(game_importer.game_imports, 'ingest_games', AsyncMock(side_effect=ingest)))
        self.stack.enter_context(patch.object(game_importer.game_imports, 'sync_month_counts', AsyncMock(side_effect=sync)))
        self.stack.enter_context(patch.object(game_importer, 'refresh_import_summaries', AsyncMock()))
        self.stack.enter_context(patch.object(jobs, 'StepReporter', reporter))
        self.lifecycle = {}
        for name in ['start_ingestion_run', 'start_ingestion_stage', 'finish_ingestion_stage',
                     'finish_ingestion_run', '_write_game_job_progress', 'ensure_fen_pipeline_enqueued']:
            self.lifecycle[name] = self.stack.enter_context(patch.object(jobs, name, AsyncMock()))
        self.lifecycle['start_ingestion_run'].return_value = 'synthetic-run'
        self.lifecycle['ensure_fen_pipeline_enqueued'].return_value = {
            'status': 'queued', 'job_id': 'fen-followup', 'pending_games': 1}

    async def run_job(self, operation=jobs.update_player_games):
        return await jobs._run_game_job({'redis': object(), 'job_id': 'synthetic-job'},
            {'player_name': 'alice'}, operation=operation, fallback_job_id='unused',
            queued_detail='Queued {player_name}.')

    async def assert_incomplete(self):
        previous = deepcopy(self.saved), self.months.copy(), self.commits, self.writes
        followups = self.lifecycle['ensure_fen_pipeline_enqueued'].await_count
        with self.assertRaisesRegex(RuntimeError, 'Incomplete archive download') as caught:
            await self.run_job()
        self.assertEqual((self.saved, self.months, self.commits, self.writes), previous)
        self.assertEqual(self.lifecycle['ensure_fen_pipeline_enqueued'].await_count, followups)
        self.assertEqual(self.lifecycle['_write_game_job_progress'].call_args.kwargs['phase'], 'failed')
        failed = _serialize_result(SimpleNamespace(success=False, result=caught.exception,
            start_time=None, finish_time=None, queue_name='pipeline_queue'))
        self.assertFalse(failed['success'])
        self.assertIn('Incomplete archive download', failed['result']['message'])
        self.assertNotIn('committed_games', failed['result'])  # Failure is not a fabricated zero-count success.
        return caught.exception

    async def test_http_503_is_failed_arq_outcome_not_empty_success(self):
        self.responses['2026-01'] = httpx.Response(503, text='unavailable')
        error = await self.assert_incomplete()
        self.assertIn('2026-01', str(error))
        self.assertIn('503', str(error))

    async def test_mixed_archives_report_missing_coverage_before_any_import(self):
        self.selected = ['2026-01', '2026-02', '2026-03']
        self.responses = {'2026-01': httpx.Response(200, json={'games': [game()]}),
                          '2026-02': httpx.Response(503, text='unavailable'),
                          '2026-03': httpx.Response(200, json={'games': []})}
        error = await self.assert_incomplete()
        self.assertIn('2026-02', str(error))
        self.assertIn('2/3', str(error))
        self.assertEqual(self.requested, self.selected)

    async def test_successful_empty_archive_is_recorded_with_zero_counts(self):
        self.responses['2026-01'] = httpx.Response(200, json={'games': []})
        for operation in [jobs.create_games, jobs.update_player_games]:
            result = await self.run_job(operation)
            self.assertEqual(result['committed_games'], 0)
            self.assertEqual(result['zero_move_games'], 0)
            self.assertEqual(result['status'], 'complete')
        self.assertEqual(self.months, {(2026, 1)})

    async def test_actual_parsing_and_import_count_new_games_after_commit(self):
        self.responses['2026-01'] = httpx.Response(200, json={'games': [game(), game(102)]})
        result = await self.run_job()
        self.assertEqual(result['committed_games'], 2)
        self.assertEqual(set(self.saved), {101, 102})
        self.assertEqual(self.commits, 1)
        self.assertEqual(result['fen_extraction']['status'], 'queued')
        self.assertEqual(result['ingestion_run_id'], 'synthetic-run')

    async def test_successful_duplicate_archive_returns_zero_new_games(self):
        self.responses['2026-01'] = httpx.Response(200, json={'games': [game()]})
        self.assertEqual((await self.run_job())['committed_games'], 1)
        saved = deepcopy(self.saved)
        self.assertEqual((await self.run_job())['committed_games'], 0)
        self.assertEqual(self.saved, saved)

    async def test_later_download_failure_preserves_previous_committed_games(self):
        self.responses['2026-01'] = httpx.Response(200, json={'games': [game()]})
        self.assertEqual((await self.run_job())['committed_games'], 1)
        self.selected = ['2026-01', '2026-02']
        self.responses['2026-02'] = httpx.Response(503, text='unavailable')
        await self.assert_incomplete()
        self.assertEqual(set(self.saved), {101})

    async def test_http_404_is_not_a_successful_empty_archive(self):
        self.responses['2026-01'] = httpx.Response(404, text='missing')
        await self.assert_incomplete()

    async def test_transport_failure_is_not_success(self):
        self.responses['2026-01'] = httpx.ReadTimeout('synthetic timeout')
        await self.assert_incomplete()

    async def test_malformed_archive_payloads_do_not_prove_empty_coverage(self):
        for payload in ['bad json', '{}', '{"games":null}', '{"games":"bad"}', '[]']:
            with self.subTest(payload=payload):
                self.responses['2026-01'] = httpx.Response(200, text=payload)
                await self.assert_incomplete()

    async def test_cancellation_stays_cancellation_and_cannot_publish_success(self):
        self.responses['2026-01'] = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.run_job()
        self.assertEqual(self.commits, 0)
        self.lifecycle['ensure_fen_pipeline_enqueued'].assert_not_awaited()
        self.assertFalse(any(call.kwargs.get('phase') == 'complete'
            for call in self.lifecycle['_write_game_job_progress'].call_args_list))
