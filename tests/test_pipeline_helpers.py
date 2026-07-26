import gzip
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from sqlalchemy.dialects import postgresql

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "stockfish-service"))

import chess
import operations.engine as stockfish_engine
from chessism_api.operations import analysis, analysis_backups, fens, games, player_deletion, tablebase
from chessism_api.database.ask_db import (
    _player_fens_for_analysis_stmt,
    fair_sample_player_games_by_month,
    get_player_fen_score_counts,
)
from chessism_api.operations.fens import _aggregate_fen_data_in_memory, split_list
from chessism_api.operations.format_games import (
    create_game_dict,
    create_moves_table,
    get_moves_data,
    get_pgn_item,
    insert_games_months_moves_and_players,
)
from operations.engine import EnginePool, clean_engine_result, convert_to_serializable
from chessism_api.routers.analysis import (
    AnalysisJobRequest,
    AnalysisLoopJobRequest,
    PlayerGamePreviewRequest,
    PlayerGameScopeRequest,
    _player_game_date_bounds,
    api_preview_player_game_analysis,
)
from chessism_api.routers import jobs as jobs_router
from arq.jobs import JobStatus


class GameFormattingTests(unittest.IsolatedAsyncioTestCase):
    def test_pgn_header_lookup_matches_the_exact_header(self):
        pgn = '[UTCDate "2024.07.03"]\n[Date "2024.07.02"]\n'

        self.assertEqual(get_pgn_item(pgn, "Date"), "2024.07.02")

    def test_move_table_rejects_missing_clocks_without_mutating_inputs(self):
        moves = ["e4", "e5", "Nf3"]
        times = []

        with self.assertRaisesRegex(ValueError, "Every played half-move"):
            create_moves_table(
                "https://www.chess.com/game/live/123",
                times,
                moves,
                time_bonus=0,
            )

        self.assertEqual(moves, ["e4", "e5", "Nf3"])
        self.assertEqual(times, [])

    def test_crlf_pgn_move_section_is_parsed(self):
        game = {
            "url": "https://www.chess.com/game/live/123",
            "time_control": "600",
            "pgn": (
                '[Date "2024.07.02"]\r\n\r\n'
                '1. e4 {[%clk 0:10:00]} e5 {[%clk 0:09:59]} '
                '2. Nf3 {[%clk 0:09:55]} Nc6 {[%clk 0:09:54]} 1-0'
            ),
        }

        move_count, moves = get_moves_data(game)

        self.assertEqual(move_count, 2)
        self.assertEqual(moves["white_moves"], ["e4", "Nf3"])
        self.assertEqual(moves["black_moves"], ["e5", "Nc6"])

    def test_partial_clock_annotations_are_rejected(self):
        game = {
            "url": "https://www.chess.com/game/live/123",
            "time_control": "600",
            "pgn": (
                '[Date "2024.07.02"]\n\n'
                '1. e4 {[%clk 0:10:00]} e5 {[%clk 0:09:59]} '
                '2. Nf3 {[%clk 0:09:55]} Nc6 1-0'
            ),
        }

        with self.assertRaisesRegex(ValueError, "3 clocks for 4 moves"):
            get_moves_data(game)

    def test_game_without_clock_annotations_is_discarded(self):
        pgn = "\n".join([
            '[Date "2024.07.02"]',
            '[StartTime "10:00:00"]',
            '[EndDate "2024.07.02"]',
            '[EndTime "10:05:00"]',
            "",
            "1. e4 e5 2. Nf3 Nc6 1-0",
        ])
        raw_game = {
            "url": "https://www.chess.com/game/live/123",
            "time_control": "600",
            "white": {"username": "White", "rating": 1500, "result": "win"},
            "black": {"username": "Black", "rating": 1400, "result": "checkmated"},
            "eco": "https://www.chess.com/openings/Kings-Pawn-Game",
            "pgn": pgn,
        }

        result = create_game_dict(raw_game)

        self.assertFalse(result)

    def test_game_with_clock_annotation_for_every_move_is_kept(self):
        pgn = "\n".join([
            '[Date "2024.07.02"]',
            '[StartTime "10:00:00"]',
            '[EndDate "2024.07.02"]',
            '[EndTime "10:05:00"]',
            "",
            (
                "1. e4 {[%clk 0:10:00]} e5 {[%clk 0:09:59]} "
                "2. Nf3 {[%clk 0:09:55]} Nc6 {[%clk 0:09:54]} 1-0"
            ),
        ])
        raw_game = {
            "url": "https://www.chess.com/game/live/123",
            "time_control": "600",
            "white": {"username": "White", "rating": 1500, "result": "win"},
            "black": {"username": "Black", "rating": 1400, "result": "checkmated"},
            "eco": "https://www.chess.com/openings/Kings-Pawn-Game",
            "pgn": pgn,
        }

        result = create_game_dict(raw_game)

        self.assertIsInstance(result, dict)
        self.assertEqual(result["n_moves"], 2)
        self.assertEqual(result["moves_data"]["white_time_left"], [600.0, 595.0])

    async def test_invalid_game_does_not_queue_orphan_moves_or_mutate_input(self):
        formatted_game = {
            "link": 123,
            "moves_data": {
                "link": 123,
                "white_moves": ["e4"],
                "black_moves": ["e5"],
                "white_reaction_times": [0.0],
                "black_reaction_times": [0.0],
                "white_time_left": [0.0],
                "black_time_left": [0.0],
            },
        }

        with patch(
            "chessism_api.operations.format_games.insert_new_data",
            new_callable=AsyncMock,
        ) as insert_new_data:
            result = await insert_games_months_moves_and_players(
                [formatted_game],
                "white",
            )

        insert_new_data.assert_not_awaited()
        self.assertIn("moves_data", formatted_game)
        self.assertIn("No new data", result)


class FenAggregationTests(unittest.TestCase):
    def test_split_list_always_returns_requested_chunk_count(self):
        self.assertEqual(split_list([1, 2], 4), [[1], [2], [], []])

    def test_aggregation_deduplicates_associations_and_exact_counters(self):
        base = {
            "game_link": 1,
            "fen_fen": "8/8/8/8/8/8/8/K6k w - -",
            "n_move": 1,
            "move_color": "white",
        }
        associations = [
            {**base, "move_counter_string": "#1_10"},
            {**base, "move_counter_string": "#1_10"},
            {**base, "n_move": 2, "move_counter_string": "#1_1"},
        ]

        fens, unique_associations = _aggregate_fen_data_in_memory(associations)

        self.assertEqual(fens[0]["moves_counter"], "#1_10#1_1")
        self.assertEqual(len(unique_associations), 2)
        self.assertNotIn("move_counter_string", unique_associations[0])
        self.assertEqual(fens[0]["piece_count"], 2)

    def test_piece_count_ignores_fen_digits_and_metadata(self):
        self.assertEqual(
            fens.count_fen_pieces("8/8/8/3k4/8/8/3Q4/3K4 w - -"),
            3,
        )


class TablebaseAnalysisTests(unittest.TestCase):
    def test_tablebase_scores_are_stored_from_white_perspective(self):
        white_to_move = chess.Board("8/8/8/8/8/2K5/4Q3/7k w - -")
        black_to_move = chess.Board("8/8/8/8/8/2K5/4Q3/7k b - -")

        white_win = tablebase._tablebase_score_payload(white_to_move, 2)
        black_win = tablebase._tablebase_score_payload(black_to_move, 2)
        cursed_win = tablebase._tablebase_score_payload(white_to_move, 1)

        self.assertEqual(white_win["score"], 1_000.0)
        self.assertEqual(white_win["wdl_win"], 1_000.0)
        self.assertEqual(black_win["score"], -1_000.0)
        self.assertEqual(black_win["wdl_loss"], 1_000.0)
        self.assertEqual(cursed_win["score"], 0.0)
        self.assertEqual(cursed_win["wdl_draw"], 1_000.0)

    def test_tablebase_probe_persists_exact_metadata(self):
        row = {
            "fen": "8/8/8/8/8/2K5/4Q3/7k w - -",
            "piece_count": 3,
        }
        fake_tablebase = MagicMock()
        fake_tablebase.get_wdl.return_value = 2
        fake_tablebase.get_dtz.return_value = 7

        with patch.object(tablebase, "_best_tablebase_move", return_value="d2d5"):
            solved, unavailable = tablebase._probe_batch_sync(fake_tablebase, [row])

        self.assertEqual(unavailable, [])
        self.assertEqual(solved[0]["tablebase_wdl"], 2)
        self.assertEqual(solved[0]["tablebase_dtz"], 7)
        self.assertEqual(solved[0]["next_moves"], "d2d5")
        self.assertEqual(solved[0]["score"], 1_000.0)


class AutomaticFenPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_games_queue_one_automatic_pipeline(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock(return_value=True)
        redis.delete = AsyncMock()
        redis.enqueue_job = AsyncMock(return_value=SimpleNamespace(job_id="fen-job"))

        with patch.object(
            fens,
            "_get_remaining_fens_count_committed",
            new_callable=AsyncMock,
            return_value=125,
        ):
            result = await fens.ensure_fen_pipeline_enqueued(redis)

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["job_id"], "fen-job")
        redis.enqueue_job.assert_awaited_once_with(
            "run_fen_pipeline",
            total_games_to_process=125,
            batch_size=1_000,
            num_workers=3,
            _queue_name="pipeline_queue",
        )

    async def test_active_pipeline_is_reused_instead_of_duplicated(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=b"active-fen-job")
        redis.enqueue_job = AsyncMock()

        class FakeJob:
            def __init__(self, job_id, *_args, **_kwargs):
                self.job_id = job_id

            async def status(self):
                return JobStatus.in_progress

        with (
            patch.object(
                fens,
                "_get_remaining_fens_count_committed",
                new_callable=AsyncMock,
                return_value=50,
            ),
            patch.object(fens, "Job", FakeJob),
        ):
            result = await fens.ensure_fen_pipeline_enqueued(redis)

        self.assertEqual(result["status"], "already_active")
        self.assertEqual(result["job_id"], "active-fen-job")
        redis.enqueue_job.assert_not_awaited()

    async def test_game_ingestion_automatically_starts_fen_extraction(self):
        operation = AsyncMock(return_value="DATA UPDATED FOR hikaru")
        redis = MagicMock()

        with (
            patch.object(games, "_write_game_job_progress", new_callable=AsyncMock),
            patch.object(
                games,
                "ensure_fen_pipeline_enqueued",
                new_callable=AsyncMock,
                return_value={
                    "status": "queued",
                    "job_id": "fen-job",
                    "pending_games": 12,
                },
            ) as ensure_pipeline,
        ):
            result = await games._run_game_job(
                {"redis": redis, "job_id": "games-job"},
                {"player_name": "Hikaru"},
                operation=operation,
                fallback_job_id="games-job",
                queued_detail="Queued update for {player_name}.",
            )

        ensure_pipeline.assert_awaited_once_with(redis)
        self.assertIn("Automatic FEN extraction queued for 12 games", result)

    async def test_finished_pass_queues_follow_up_for_games_arriving_mid_run(self):
        redis = MagicMock()
        context = {"redis": redis, "job_id": "fen-job"}

        with (
            patch.object(fens, "_run_fen_pipeline", new_callable=AsyncMock),
            patch.object(fens, "_write_fen_pipeline_progress", new_callable=AsyncMock),
            patch.object(fens, "_release_fen_pipeline_coordination", new_callable=AsyncMock),
            patch.object(
                fens,
                "ensure_fen_pipeline_enqueued",
                new_callable=AsyncMock,
                return_value={
                    "status": "queued",
                    "job_id": "follow-up",
                    "pending_games": 3,
                },
            ) as ensure_pipeline,
        ):
            await fens.run_fen_pipeline(
                context,
                total_games_to_process=10,
                batch_size=1_000,
                num_workers=3,
            )

        ensure_pipeline.assert_awaited_once_with(
            redis,
            batch_size=1_000,
            num_workers=3,
        )


class AnalysisFormattingTests(unittest.IsolatedAsyncioTestCase):
    def test_analysis_job_batch_is_capped_at_stockfish_service_limit(self):
        request = AnalysisJobRequest(batch_size=analysis.MAX_ANALYSIS_BATCH_SIZE)

        self.assertEqual(request.batch_size, 500)
        with self.assertRaises(ValueError):
            AnalysisJobRequest(batch_size=501)

    def test_analysis_loop_accepts_1000_batch_maximum(self):
        request = AnalysisLoopJobRequest(
            scope="all",
            positions_per_run=20_000,
            runs=4,
            batches=1_000,
            cool_off=300,
        )

        self.assertEqual(request.batches, 1_000)
        self.assertEqual(request.positions_per_run * request.runs, 80_000)
        with self.assertRaises(ValueError):
            AnalysisLoopJobRequest(batches=1_001)

    def test_player_game_date_range_is_inclusive_and_validated(self):
        request = PlayerGameScopeRequest(
            player_name="hikaru",
            selection_mode="range",
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 31),
        )
        date_from, date_to_exclusive = _player_game_date_bounds(request)

        self.assertEqual(date_from.isoformat(), "2026-01-01T00:00:00+00:00")
        self.assertEqual(date_to_exclusive.isoformat(), "2026-02-01T00:00:00+00:00")

        invalid = PlayerGameScopeRequest(
            player_name="hikaru",
            selection_mode="range",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 1, 1),
        )
        with self.assertRaisesRegex(Exception, "Start date"):
            _player_game_date_bounds(invalid)

    def test_fair_range_balances_games_across_calendar_months(self):
        rows = []
        link = 1
        for month in range(1, 5):
            for day in (1, 10, 20):
                rows.append({
                    "link": link,
                    "played_at": datetime(2026, month, day, tzinfo=timezone.utc),
                })
                link += 1

        selected, sampling = fair_sample_player_games_by_month(rows, 4)

        self.assertEqual(len(selected), 4)
        self.assertEqual(
            [row["played_at"].month for row in selected],
            [1, 2, 3, 4],
        )
        self.assertEqual(sampling["available_periods"], 4)
        self.assertEqual(sampling["sampled_periods"], 4)

    def test_fair_range_spreads_small_sample_over_full_history(self):
        rows = [
            {
                "link": month,
                "played_at": datetime(2026, month, 15, tzinfo=timezone.utc),
            }
            for month in range(1, 7)
        ]

        selected, sampling = fair_sample_player_games_by_month(rows, 3)

        self.assertEqual(
            [row["played_at"].month for row in selected],
            [1, 4, 6],
        )
        self.assertEqual(sampling["available_periods"], 6)
        self.assertEqual(sampling["sampled_periods"], 3)

    def test_fair_range_request_is_valid_without_dates(self):
        request = PlayerGamePreviewRequest(
            player_name="hikaru",
            selection_mode="fair_range",
            game_limit=200,
        )

        self.assertEqual(request.selection_mode, "fair_range")
        self.assertEqual(_player_game_date_bounds(request), (None, None))

    async def test_player_game_preview_freezes_exact_game_ids_in_redis(self):
        redis = MagicMock()
        redis.set = AsyncMock()
        scope = {
            "player_name": "hikaru",
            "games_with_fens": 300,
            "complete_games": 20,
            "incomplete_games": 280,
            "earliest_game": None,
            "latest_game": None,
        }
        preview = {
            "player_name": "hikaru",
            "game_links": [3, 2, 1],
            "selected_games": 3,
            "position_occurrences": 250,
            "analyzed_occurrences": 50,
            "unscored_occurrences": 200,
            "unique_fens": 220,
            "analyzed_unique_fens": 40,
            "fens_to_analyze": 180,
        }
        request = PlayerGamePreviewRequest(
            player_name="Hikaru",
            selection_mode="latest",
            game_limit=3,
        )

        with (
            patch(
                "chessism_api.routers.analysis.get_player_game_analysis_scope",
                new_callable=AsyncMock,
                return_value=scope,
            ),
            patch(
                "chessism_api.routers.analysis.preview_player_games_for_analysis",
                new_callable=AsyncMock,
                return_value=preview,
            ),
        ):
            result = await api_preview_player_game_analysis(request, redis)

        self.assertEqual(result["selected_games"], 3)
        self.assertEqual(result["fens_to_analyze"], 180)
        self.assertNotIn("game_links", result)
        saved_plan = json.loads(redis.set.await_args.args[1])
        self.assertEqual(saved_plan["game_links"], [3, 2, 1])

    async def test_fair_range_preview_uses_balanced_selection_order(self):
        redis = MagicMock()
        redis.set = AsyncMock()
        request = PlayerGamePreviewRequest(
            player_name="Hikaru",
            selection_mode="fair_range",
            game_limit=12,
        )
        scope = {
            "player_name": "hikaru",
            "games_with_fens": 100,
            "complete_games": 10,
            "incomplete_games": 90,
            "earliest_game": "2020-01-01T00:00:00+00:00",
            "latest_game": "2026-01-01T00:00:00+00:00",
        }
        preview = {
            "player_name": "hikaru",
            "game_links": list(range(1, 13)),
            "selected_games": 12,
            "fens_to_analyze": 500,
            "tablebase_fens": 0,
            "available_periods": 72,
            "sampled_periods": 12,
        }

        with (
            patch(
                "chessism_api.routers.analysis.get_player_game_analysis_scope",
                new_callable=AsyncMock,
                return_value=scope,
            ),
            patch(
                "chessism_api.routers.analysis.preview_player_games_for_analysis",
                new_callable=AsyncMock,
                return_value=preview,
            ) as preview_query,
        ):
            result = await api_preview_player_game_analysis(request, redis)

        self.assertEqual(result["selection_order"], "fair_range")
        self.assertEqual(result["sampled_periods"], 12)
        preview_query.assert_awaited_once_with(
            "hikaru",
            order="fair_range",
            game_limit=12,
            date_from=None,
            date_to_exclusive=None,
        )

    async def test_engine_client_error_is_not_retried_forever(self):
        request = httpx.Request("POST", analysis.ENGINE_URL)
        response = httpx.Response(422, request=request, text="invalid batch")
        client = AsyncMock()
        client.post.return_value = response

        with self.assertRaises(analysis.EngineServiceRequestError):
            await analysis._call_engine_service(
                client,
                analysis.ENGINE_URL,
                ["fen-1"],
                100,
            )

    def test_engine_result_formatting_keeps_best_line_and_continuations(self):
        engine_output = [{
            "fen": "fen-1",
            "is_valid": True,
            "analysis": [
                {"score": 25, "pv": ["e2e4", "e7e5"], "wdl": [500, 400, 100]},
                {"score": 12, "pv": ["d2d4"]},
            ],
        }]

        rows, continuations = analysis._format_engine_results(engine_output)

        self.assertEqual(rows[0]["next_moves"], "e2e4 e7e5")
        self.assertEqual(rows[0]["wdl_win"], 500)
        self.assertEqual(continuations[0]["rank"], 2)
        self.assertEqual(continuations[0]["move"], "d2d4")

    async def test_analysis_concurrency_does_not_claim_more_than_target(self):
        requested_batch_sizes = []

        class FakeSession:
            execute = AsyncMock()
            commit = AsyncMock()
            rollback = AsyncMock()
            close = AsyncMock()

        async def fetch_batch(limit):
            requested_batch_sizes.append(limit)
            start = sum(requested_batch_sizes[:-1])
            return FakeSession(), [f"fen-{index}" for index in range(start, start + limit)]

        async def call_engine(client, url, fens, nodes, **kwargs):
            return [
                {
                    "fen": fen,
                    "is_valid": True,
                    "analysis": {"score": 0, "pv": [], "time": 0.01},
                }
                for fen in fens
            ]

        with (
            patch.object(analysis, "ANALYSIS_CONCURRENCY", 3),
            patch.object(analysis, "_call_engine_service", side_effect=call_engine),
            patch.object(analysis, "record_analysis_times", new_callable=AsyncMock),
            patch.object(analysis, "_increment_summary_for_analysis_results", new_callable=AsyncMock),
            patch.object(analysis, "_refresh_scored_projections_after_analysis", new_callable=AsyncMock),
            patch.object(
                analysis.fen_interface,
                "update_fen_analysis_data",
                new_callable=AsyncMock,
            ),
        ):
            await analysis._run_analysis_job(
                {},
                total_fens_to_process=5,
                batch_size=3,
                nodes_limit=100,
                fetch_batch=fetch_batch,
                timing_source="test",
                job_id="TEST",
                fallback_arq_job_id="test",
                no_more_message="done",
            )

        self.assertEqual(sum(requested_batch_sizes), 5)
        self.assertTrue(all(size <= 3 for size in requested_batch_sizes))

    async def test_worker_clamps_legacy_oversized_batch_payloads(self):
        requested_batch_sizes = []

        class FakeSession:
            execute = AsyncMock()
            commit = AsyncMock()
            rollback = AsyncMock()
            close = AsyncMock()

        async def fetch_batch(limit):
            requested_batch_sizes.append(limit)
            return FakeSession(), [f"fen-{index}" for index in range(limit)]

        async def call_engine(client, url, fens, nodes, **kwargs):
            return [
                {
                    "fen": fen,
                    "is_valid": True,
                    "analysis": {"score": 0, "pv": [], "time": 0.01},
                }
                for fen in fens
            ]

        with (
            patch.object(analysis, "_call_engine_service", side_effect=call_engine),
            patch.object(analysis, "record_analysis_times", new_callable=AsyncMock),
            patch.object(analysis, "_increment_summary_for_analysis_results", new_callable=AsyncMock),
            patch.object(analysis, "_refresh_scored_projections_after_analysis", new_callable=AsyncMock),
            patch.object(
                analysis.fen_interface,
                "update_fen_analysis_data",
                new_callable=AsyncMock,
            ),
        ):
            await analysis._run_analysis_job(
                {},
                total_fens_to_process=501,
                batch_size=1_000,
                nodes_limit=100,
                fetch_batch=fetch_batch,
                timing_source="test",
                job_id="TEST",
                fallback_arq_job_id="test",
                no_more_message="done",
            )

        self.assertEqual(requested_batch_sizes, [500, 1])

    async def test_analysis_loop_runs_four_passes_with_three_cool_offs(self):
        run_result = {
            "processed": 20_000,
            "engine_processed": 20_000,
            "failed": 0,
            "failed_batches": 0,
        }

        with (
            patch.object(
                analysis,
                "_run_analysis_job",
                new_callable=AsyncMock,
                side_effect=[run_result] * 4,
            ) as run_job,
            patch.object(analysis, "_reset_job_progress", new_callable=AsyncMock),
            patch.object(analysis, "_write_job_progress", new_callable=AsyncMock),
            patch.object(
                analysis,
                "_refresh_scored_projections_after_analysis",
                new_callable=AsyncMock,
            ),
            patch.object(analysis.asyncio, "sleep", new_callable=AsyncMock) as cool_off,
        ):
            result = await analysis.run_analysis_loop_job(
                {},
                scope="all",
                runs=4,
                positions_per_run=20_000,
                batches=500,
                cool_off=300,
                nodes_limit=1_000_000,
            )

        self.assertEqual(run_job.await_count, 4)
        self.assertEqual(cool_off.await_count, 3)
        cool_off.assert_awaited_with(300)
        self.assertEqual(result["processed"], 80_000)
        self.assertEqual(result["runs"], 4)
        self.assertTrue(all(
            call.kwargs["max_batch_size"] == 1_000
            for call in run_job.await_args_list
        ))

    async def test_player_game_completion_chunks_work_and_refreshes_frozen_games(self):
        run_result = {
            "processed": 5_000,
            "engine_processed": 5_000,
            "failed": 0,
            "failed_batches": 0,
        }
        completion = {
            "selected_games": 200,
            "fully_analyzed_games": 200,
            "incomplete_games": 0,
        }

        with (
            patch.object(
                analysis,
                "count_game_set_fens_for_analysis",
                new_callable=AsyncMock,
                return_value=10_000,
            ),
            patch.object(
                analysis,
                "analyze_tablebase_positions",
                new_callable=AsyncMock,
                return_value={"processed": 0, "solved": 0, "unavailable": 0},
            ),
            patch.object(
                analysis,
                "_run_analysis_job",
                new_callable=AsyncMock,
                side_effect=[run_result, run_result],
            ) as run_job,
            patch.object(analysis, "_reset_job_progress", new_callable=AsyncMock),
            patch.object(analysis, "_write_job_progress", new_callable=AsyncMock),
            patch.object(analysis, "refresh_game_analysis_summary", new_callable=AsyncMock),
            patch.object(
                analysis,
                "get_game_set_analysis_completion",
                new_callable=AsyncMock,
                return_value=completion,
            ),
            patch.object(
                analysis,
                "_refresh_scored_projections_after_analysis",
                new_callable=AsyncMock,
            ),
            patch.object(analysis.asyncio, "sleep", new_callable=AsyncMock) as cool_off,
        ):
            result = await analysis.run_player_games_analysis_job(
                {},
                player_name="hikaru",
                game_links=[3, 2, 1],
                planned_fens=10_000,
                batch_size=500,
                nodes_limit=1_000_000,
                cool_off=120,
                chunk_size=5_000,
            )

        self.assertEqual(run_job.await_count, 2)
        cool_off.assert_awaited_once_with(120)
        self.assertEqual(result["processed"], 10_000)
        self.assertEqual(result["fully_analyzed_games"], 200)


class AnalysisSelectionTests(unittest.TestCase):
    def test_player_fen_query_locks_rows_before_satisfying_limit(self):
        statement = _player_fens_for_analysis_stmt("lafareto", 500)
        sql = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).upper()

        self.assertIn("EXISTS (SELECT", sql)
        self.assertIn("LIMIT 500 FOR UPDATE OF FEN SKIP LOCKED", sql)
        self.assertNotIn("GROUP BY", sql)


class AnalysisJobsApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_analysis_jobs_lists_running_and_queued_from_server_queue(self):
        redis = MagicMock()
        redis.zrange = AsyncMock(return_value=[(b"queued-job", 2), (b"running-job", 1)])
        redis.get = AsyncMock(side_effect=[None, json.dumps({
            "total": 20_000,
            "processed": 14_000,
            "phase": "analyzing",
        })])

        statuses = {
            "queued-job": JobStatus.queued,
            "running-job": JobStatus.in_progress,
        }

        class FakeJob:
            def __init__(self, job_id, *_args, **_kwargs):
                self.job_id = job_id

            async def status(self):
                return statuses[self.job_id]

            async def info(self):
                return SimpleNamespace(
                    function="run_analysis_loop_job",
                    args=(),
                    kwargs={"scope": "all", "positions_per_run": 5_000, "runs": 4},
                    job_try=None,
                    enqueue_time=None,
                    score=None,
                )

        with patch.object(jobs_router, "Job", FakeJob):
            response = await jobs_router.api_get_analysis_jobs(redis)

        payload = json.loads(response.body)
        self.assertEqual(
            [job["job_id"] for job in payload["jobs"]],
            ["running-job", "queued-job"],
        )
        self.assertEqual(payload["jobs"][0]["progress"]["processed"], 14_000)

    async def test_delete_endpoint_refuses_to_delete_running_analysis(self):
        redis = MagicMock()
        redis.eval = AsyncMock()

        job = MagicMock()
        job.status = AsyncMock(return_value=JobStatus.in_progress)

        with (
            patch.object(jobs_router, "KNOWN_QUEUES", ("analysis_queue",)),
            patch.object(jobs_router, "Job", return_value=job),
            self.assertRaisesRegex(Exception, "already started"),
        ):
            await jobs_router.api_delete_queued_analysis_job("running-job", redis)

        redis.eval.assert_not_awaited()


class EnginePoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_pool_leases_four_independent_engines(self):
        created = []

        class FakeEngine:
            def __init__(self, number):
                self.number = number
                self.quit = AsyncMock()

        async def start_engine(number):
            transport = MagicMock()
            engine = FakeEngine(number)
            created.append((transport, engine))
            return transport, engine

        with patch.object(stockfish_engine, "_start_engine", side_effect=start_engine):
            pool = EnginePool(size=4)
            await pool.initialize()

            self.assertEqual(pool.status()["workers"], {
                "total": 4,
                "busy": 0,
                "idle": 4,
            })

            async with pool.acquire() as first:
                async with pool.acquire() as second:
                    async with pool.acquire() as third:
                        async with pool.acquire() as fourth:
                            slots = [first, second, third, fourth]
                            self.assertEqual(
                                {slot.number for slot in slots},
                                {1, 2, 3, 4},
                            )
                            self.assertEqual(
                                len({id(slot.engine) for slot in slots}),
                                4,
                            )
                            self.assertEqual(pool.status()["workers"]["busy"], 4)

            self.assertEqual(pool.status()["workers"]["idle"], 4)
            await pool.shutdown()

        for transport, engine in created:
            engine.quit.assert_awaited_once()
            transport.close.assert_called_once()


class FenAnalysisBackupTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.backup_directory = Path(self.temporary_directory.name)

    def test_backup_filename_validation_blocks_path_traversal(self):
        with patch.object(analysis_backups, "BACKUP_DIR", self.backup_directory):
            valid = analysis_backups._backup_path(
                "fen-analysis-20240703T120000_000001Z.jsonl.gz"
            )
            self.assertEqual(valid.parent, self.backup_directory)

            with self.assertRaisesRegex(ValueError, "Invalid"):
                analysis_backups._backup_path(
                    "../fen-analysis-20240703T120000_000001Z.jsonl.gz"
                )

    def test_compressed_backup_records_round_trip(self):
        backup_path = self.backup_directory / (
            "fen-analysis-20240703T120000_000001Z.jsonl.gz"
        )
        header = {
            "type": "metadata",
            "format": analysis_backups.BACKUP_FORMAT,
            "version": analysis_backups.BACKUP_VERSION,
            "records": 1,
        }
        record = {
            "type": "fen_analysis",
            "fen": "8/8/8/8/8/8/8/K6k w - - 0 1",
            "score": 0.25,
            "next_moves": "a1a2",
            "wdl_win": 200,
            "wdl_draw": 700,
            "wdl_loss": 100,
            "continuations": [{"rank": 2, "move": "a1b1", "score": 0.1}],
        }
        with gzip.open(backup_path, "wt", encoding="utf-8") as output:
            output.write(json.dumps(header) + "\n")
            output.write(json.dumps(record) + "\n")

        loaded_header, loaded_records = analysis_backups._backup_records(backup_path)

        self.assertEqual(loaded_header, header)
        self.assertEqual(list(loaded_records), [record])

    def test_backup_listing_uses_sidecar_metadata_and_newest_first(self):
        older_name = "fen-analysis-20240703T120000_000001Z.jsonl.gz"
        newer_name = "fen-analysis-20240704T120000_000001Z.jsonl.gz"
        for filename in (older_name, newer_name):
            backup_path = self.backup_directory / filename
            backup_path.write_bytes(b"backup")
            analysis_backups._atomic_write_json(
                analysis_backups._metadata_path(backup_path),
                {
                    "filename": filename,
                    "created_at": "2024-07-03T12:00:00+00:00",
                    "records": 42,
                    "bytes": 6,
                    "sha256": "abc123",
                },
            )

        with patch.object(analysis_backups, "BACKUP_DIR", self.backup_directory):
            backups = analysis_backups.list_fen_analysis_backups()

        self.assertEqual([item["filename"] for item in backups], [newer_name, older_name])
        self.assertEqual(backups[0]["records"], 42)
        self.assertEqual(backups[0]["sha256"], "abc123")


class PlayerCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_player_coverage_includes_complete_games_and_fen_occurrences(self):
        query_result = MagicMock()
        query_result.mappings.return_value.first.return_value = {
            "total_games": 120,
            "analyzed_games": 45,
            "total_positions": 6_400,
            "analyzed_positions": 3_200,
            "unscored_positions": 3_200,
        }
        session = AsyncMock()
        session.execute.return_value = query_result
        session_context = MagicMock()
        session_context.__aenter__ = AsyncMock(return_value=session)
        session_context.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "chessism_api.database.ask_db.AsyncDBSession",
            return_value=session_context,
        ):
            coverage = await get_player_fen_score_counts("hikaru")

        self.assertEqual(coverage["total_games"], 120)
        self.assertEqual(coverage["analyzed_games"], 45)
        self.assertEqual(coverage["total_fens"], 6_400)
        self.assertEqual(coverage["analyzed_fens"], 3_200)
        self.assertEqual(coverage["total_positions"], coverage["total_fens"])
        self.assertEqual(coverage["analyzed_positions"], coverage["analyzed_fens"])


class PlayerDeletionSafetyTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _session_context(session):
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=session)
        context.__aexit__ = AsyncMock(return_value=False)
        return context

    async def test_preview_classifies_shell_opponents_as_exclusive(self):
        player_result = MagicMock()
        player_result.mappings.return_value.first.return_value = {
            "player_name": "hikaru",
            "joined": 1,
            "deleted_at": None,
        }
        counts_result = MagicMock()
        counts_result.mappings.return_value.first.return_value = {
            "total_games": 100,
            "exclusive_games": 75,
            "shared_games": 25,
            "fen_associations": 4_000,
            "analyzed_fen_associations": 1_500,
        }
        session = AsyncMock()
        session.execute.side_effect = [player_result, counts_result]

        with patch.object(
            player_deletion,
            "AsyncDBSession",
            return_value=self._session_context(session),
        ):
            preview = await player_deletion.get_player_deletion_preview("Hikaru")

        self.assertTrue(preview["is_main_player"])
        self.assertEqual(preview["exclusive_games"], 75)
        self.assertEqual(preview["shared_games"], 25)
        self.assertEqual(preview["fen_rows_deleted"], 0)
        self.assertEqual(preview["analysis_rows_deleted"], 0)
        classification_sql = str(session.execute.await_args_list[1].args[0])
        self.assertIn("COALESCE(opponent.joined, 0) = 0", classification_sql)
        self.assertIn("COALESCE(opponent.joined, 0) <> 0", classification_sql)

    async def test_game_batch_updates_frequency_but_never_deletes_fens(self):
        links_result = MagicMock()
        links_result.scalars.return_value.all.return_value = [101, 102]
        update_result = MagicMock()
        delete_result = MagicMock()
        delete_result.rowcount = 2
        session = AsyncMock()
        session.execute.side_effect = [
            links_result,
            update_result,
            update_result,
            update_result,
            delete_result,
        ]

        with patch.object(
            player_deletion,
            "AsyncDBSession",
            return_value=self._session_context(session),
        ):
            deleted = await player_deletion._delete_exclusive_game_batch("hikaru", [101, 102])

        self.assertEqual(deleted, 2)
        session.commit.assert_awaited_once()
        sql = "\n".join(str(call.args[0]) for call in session.execute.await_args_list)
        self.assertIn("UPDATE fen current_fen", sql)
        self.assertIn("DELETE FROM game_fen_association", sql)
        self.assertIn("DELETE FROM moves", sql)
        self.assertIn("DELETE FROM game", sql)
        self.assertNotIn("DELETE FROM fen\n", sql)
        self.assertNotIn("DELETE FROM fen_continuation", sql)

    async def test_demotion_keeps_the_player_identity_as_a_deleted_shell(self):
        remaining_result = MagicMock()
        remaining_result.scalar.return_value = 0
        mutation_result = MagicMock()
        updated_result = MagicMock()
        updated_result.rowcount = 1
        session = AsyncMock()
        session.execute.side_effect = [
            remaining_result,
            mutation_result,
            mutation_result,
            mutation_result,
            updated_result,
        ]

        with patch.object(
            player_deletion,
            "AsyncDBSession",
            return_value=self._session_context(session),
        ):
            await player_deletion._demote_player("hikaru")

        sql = "\n".join(str(call.args[0]) for call in session.execute.await_args_list)
        self.assertIn("UPDATE player", sql)
        self.assertIn("joined = 0", sql)
        self.assertIn("deleted_at = CURRENT_TIMESTAMP", sql)
        self.assertNotIn("DELETE FROM player WHERE", sql)
        session.commit.assert_awaited_once()


class StockfishSerializationTests(unittest.TestCase):
    def test_negative_mate_score_preserves_its_sign(self):
        score = chess.engine.PovScore(chess.engine.Mate(-2), chess.WHITE)

        self.assertEqual(convert_to_serializable(score), -9998)

    def test_nested_move_keys_are_json_serializable(self):
        move = chess.Move.from_uci("e2e4")

        result = clean_engine_result(
            {"refutation": {move: [move]}},
            original_fen="fen-1",
            is_valid=True,
        )

        self.assertEqual(
            result["analysis"]["refutation"],
            {"e2e4": ["e2e4"]},
        )


if __name__ == "__main__":
    unittest.main()
