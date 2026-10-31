import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from chessism_api.operations.ingestion_pipeline.steps import StepReporter, merge_steps


class PositionBatchTests(unittest.TestCase):
    def test_position_bound_keeps_a_whole_game_prefix(self):
        from chessism_api.operations.ingestion_pipeline.fen_batches import bounded_game_prefix
        moves={1:[{'white_move':'e4','black_move':'e5'}]*2,
               2:[{'white_move':'e4','black_move':'--'}]*3,
               3:[{'white_move':'e4','black_move':'e5'}]*4}
        self.assertEqual(bounded_game_prefix([1,2,3],moves,7),[1,2])
        self.assertEqual(bounded_game_prefix([3],moves,7),[3])
        self.assertEqual(bounded_game_prefix([],moves,7),[])
        self.assertEqual(bounded_game_prefix([4,1],moves,7),[4,1])


class StepTests(unittest.IsolatedAsyncioTestCase):
    async def test_batches_accumulate_without_double_counting_advances(self):
        async with StepReporter({}, None) as reporter:
            reporter.goal("extract_positions", 10)
            for size in (3, 7):
                async with reporter.step("extract_positions", size, "games"):
                    await reporter.advance(size)
        row = reporter.steps["extract_positions"]
        self.assertEqual((row["processed"], row["total"]), (10, 10))
        self.assertEqual(row["status"], "complete")
        self.assertTrue(row["fixed_total"])

    async def test_unknown_total_is_not_guessed(self):
        async with StepReporter({}, None) as reporter:
            async with reporter.step("discover_endgames", None, "positions"):
                await reporter.advance(5)
                self.assertIsNone(reporter.steps["discover_endgames"]["total"])
        self.assertEqual(reporter.steps["discover_endgames"]["total"], 5)

    async def test_exception_is_retained_as_failed(self):
        reporter = StepReporter({}, None)
        with self.assertRaises(ValueError):
            async with reporter:
                async with reporter.step("save_positions", 20, "positions"):
                    raise ValueError("write failed")
        self.assertEqual(reporter.steps["save_positions"]["status"], "failed")
        self.assertEqual(reporter.steps["save_positions"]["processed"], 0)

    async def test_heartbeat_does_not_pretend_progress_advanced(self):
        redis = AsyncMock()
        reporter = StepReporter({"redis": redis}, "run")
        async with reporter.step("probe_endgames", 10, "positions"):
            original = reporter.steps["probe_endgames"]["updated_at"]
            await reporter.publish()
            self.assertEqual(reporter.steps["probe_endgames"]["updated_at"], original)
            payload = json.loads(redis.hset.call_args.args[2])
            self.assertGreaterEqual(payload[0]["heartbeat_at"], original)

    def test_parallel_progress_preserves_unknown_totals_and_failure(self):
        rows = [
            {"step": "save_positions", "processed": 5, "total": 10,
             "elapsed_seconds": 2, "status": "working"},
            {"step": "save_positions", "processed": 2, "total": None,
             "elapsed_seconds": 1, "status": "failed"},
        ]
        merged = merge_steps(rows)[0]
        self.assertEqual(merged["processed"], 7)
        self.assertIsNone(merged["total"])
        self.assertEqual(merged["status"], "failed")
        self.assertEqual(merged["elapsed_seconds"], 2)

    async def test_no_endgames_still_reports_final_statistics_and_player_queue(self):
        from chessism_api.operations.ingestion_pipeline import fen_orchestrator as fens

        reporters = []

        def reporter_for_test(*_args):
            reporter = StepReporter({}, None)
            reporters.append(reporter)
            return reporter

        with (
            patch("chessism_api.operations.ingestion_pipeline.steps.StepReporter",
                  side_effect=reporter_for_test),
            patch.object(fens, "increment_stage_progress", new_callable=AsyncMock),
            patch.object(fens, "refresh_scored_rating_summary", new_callable=AsyncMock),
            patch.object(fens, "enqueue_stale_player_salience_jobs",
                         new_callable=AsyncMock, return_value=["one", "two"]),
            patch.object(fens, "finish_ingestion_stage", new_callable=AsyncMock),
            patch.object(fens, "finish_ingestion_run", new_callable=AsyncMock),
        ):
            await fens._finish_drained_chain(None, "job", "run")

        rows = {name: row for reporter in reporters for name, row in reporter.steps.items()}
        self.assertEqual(rows["rating_projection"]["processed"], 1)
        self.assertEqual(rows["queue_player_summaries"]["processed"], 2)
        self.assertTrue(all(row["status"] == "complete" for row in rows.values()))
