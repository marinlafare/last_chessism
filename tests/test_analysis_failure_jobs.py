"""Fatal acquisition, cancellation, committed work and entry-point regressions."""
import asyncio
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from chessism_api.database.postgresql import ask_db
from chessism_api.operations import analysis


def session_for(fens=()):
    rows = Mock()
    rows.scalars.return_value.all.return_value = list(fens)
    recheck = Mock()
    recheck.all.return_value = list(fens)
    return SimpleNamespace(begin=AsyncMock(), execute=AsyncMock(return_value=rows),
                           scalars=AsyncMock(return_value=recheck), commit=AsyncMock(),
                           rollback=AsyncMock(), close=AsyncMock())


def engine_result(fen="fen"):
    return [{"fen": fen, "is_valid": True, "analysis": {"score": 0, "pv": []}}]


class AnalysisFailureJobTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = {"job_id": "failure-regression"}
        self.concurrency = patch.object(analysis, "ANALYSIS_CONCURRENCY", 1)
        self.concurrency.start()
        self.addCleanup(self.concurrency.stop)
        self.progress = self.stub("_write_job_progress")
        self.stub("_reset_job_progress")
        self.engine = self.stub("_call_engine_service", return_value=engine_result())
        self.stage = self.stub("stage_fen_results")
        self.timings = self.stub("record_analysis_times")
        self.summary = self.stub("_increment_summary_for_analysis_results")
        self.projections = self.stub("_refresh_scored_projections_after_analysis")
        self.tablebase = self.stub("analyze_tablebase_positions", return_value={"solved": 2})
        self.stub("count_game_set_fens_for_analysis", return_value=3)
        self.game_summary = self.stub("refresh_game_analysis_summary")
        self.completion = self.stub("get_game_set_analysis_completion", return_value={
            "selected_games": 2, "fully_analyzed_games": 0, "incomplete_games": 2})
        self.sleep = AsyncMock()
        sleeper = patch.object(analysis.asyncio, "sleep", self.sleep)
        sleeper.start()
        self.addCleanup(sleeper.stop)

    def stub(self, name, **kwargs):
        replacement = AsyncMock(**kwargs)
        patcher = patch.object(analysis, name, replacement)
        patcher.start()
        self.addCleanup(patcher.stop)
        return replacement

    async def run_job(self, fetch, **kwargs):
        options = dict(total_fens_to_process=3, batch_size=1, nodes_limit=100000,
                       fetch_batch=fetch, timing_source="test", job_id="FAILURE TEST",
                       fallback_arq_job_id="unused", no_more_message="empty")
        options.update(kwargs)
        return await analysis._run_analysis_job(self.ctx, **options)

    def phases(self):
        return [item.kwargs["phase"] for item in self.progress.await_args_list]

    def entry_points(self):
        return (
            ("global", "get_fens_for_analysis", (1,),
             lambda: analysis.run_analysis_job(self.ctx, 3, 1, 100000)),
            ("player", "get_player_fens_for_analysis", ("fixture", 1),
             lambda: analysis.run_player_analysis_job(self.ctx, "fixture", 3, 1, 100000)),
            ("all_loop", "get_fens_for_analysis", (1,),
             lambda: analysis.run_analysis_loop_job(self.ctx, "all", 3, 3, 1, 60, 100000)),
            ("player_loop", "get_player_fens_for_analysis", ("fixture", 1),
             lambda: analysis.run_analysis_loop_job(self.ctx, "player", 3, 3, 1, 60, 100000, " Fixture ")),
            ("frozen_games", "get_game_set_fens_for_analysis", ([33, 11, 33], 1),
             lambda: analysis.run_player_games_analysis_job(
                 self.ctx, " Fixture ", [33, None, 11, 33], 3, 1, 100000, cool_off=60, chunk_size=1)),
        )

    async def test_actual_entry_point_callbacks_propagate_failure_without_success_finalization(self):
        for name, selector_name, expected_args, entry in self.entry_points():
            with self.subTest(entry=name):
                self.progress.reset_mock()
                failure = RuntimeError("database unavailable")
                failed = session_for()
                failed.begin.side_effect = failure
                with patch.object(ask_db, "AsyncDBSession", side_effect=[failed, session_for()]) as create, \
                     patch.object(analysis, selector_name, wraps=getattr(analysis, selector_name)) as selector:
                    with self.assertRaises(RuntimeError) as caught:
                        await entry()
                self.assertIs(caught.exception, failure)
                selector.assert_awaited_once_with(*expected_args)
                self.assertEqual(create.call_count, 1)
                failed.rollback.assert_awaited_once()
                failed.close.assert_awaited_once()
                self.assertEqual(self.phases()[-1], "failed")
                self.assertNotIn("complete", self.phases())
                self.assertNotIn("loop_complete", self.phases())
                self.assertNotIn("cooling", self.phases())
        self.engine.assert_not_awaited()
        self.stage.assert_not_awaited()
        self.timings.assert_not_awaited()
        self.sleep.assert_not_awaited()
        self.projections.assert_not_awaited()
        self.game_summary.assert_not_awaited()
        self.completion.assert_not_awaited()

    async def test_actual_empty_callbacks_complete_without_engine_or_result_writes(self):
        for name, selector_name, expected_args, entry in self.entry_points():
            with self.subTest(entry=name):
                self.progress.reset_mock()
                session = session_for()
                with patch.object(ask_db, "AsyncDBSession", return_value=session), \
                     patch.object(analysis, selector_name, wraps=getattr(analysis, selector_name)) as selector:
                    await entry()
                selector.assert_awaited_once_with(*expected_args)
                session.rollback.assert_awaited_once()
                session.close.assert_awaited_once()
                self.assertEqual(self.phases()[-1], "complete")
                self.assertNotIn("failed", self.phases())
        self.engine.assert_not_awaited()
        self.stage.assert_not_awaited()
        self.sleep.assert_not_awaited()

    async def test_propagated_acquisition_error_escapes_processing_retries(self):
        failure = RuntimeError("fetch failed")
        fetch = AsyncMock(side_effect=[failure, (None, None)])
        with self.assertRaises(RuntimeError) as caught:
            await self.run_job(fetch)
        self.assertIs(caught.exception, failure)
        fetch.assert_awaited_once_with(1)
        self.sleep.assert_not_awaited()
        self.engine.assert_not_awaited()
        self.stage.assert_not_awaited()
        self.assertEqual(self.phases(), ["queued", "failed"])

    async def test_failure_after_commit_preserves_results_counts_and_offsets(self):
        session = session_for()
        failure = RuntimeError("next selection failed")
        fetch = AsyncMock(side_effect=[(session, ["fen"]), failure, (None, None)])
        with self.assertRaises(RuntimeError) as caught:
            await self.run_job(fetch, progress_total=100, progress_offset=10, failed_offset=2)
        self.assertIs(caught.exception, failure)
        self.assertEqual(fetch.await_count, 2)
        session.commit.assert_awaited_once()
        session.close.assert_awaited_once()
        session.rollback.assert_not_awaited()
        self.stage.assert_awaited_once()
        final = self.progress.await_args.kwargs
        self.assertEqual((final["phase"], final["processed"], final["failed"], final["total"]),
                         ("failed", 11, 2, 100))
        self.projections.assert_not_awaited()

    async def test_progress_publication_failure_does_not_replace_original_error(self):
        failure = RuntimeError("database unavailable")

        async def publish(*_args, **kwargs):
            if kwargs["phase"] == "failed":
                raise OSError("Redis unavailable")

        self.progress.side_effect = publish
        with self.assertRaises(RuntimeError) as caught:
            await self.run_job(AsyncMock(side_effect=[failure, (None, None)]))
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.phases()[-1], "failed")

    async def test_failure_cancels_sibling_acquisition_before_session_handoff(self):
        entered = asyncio.Event()
        session = session_for(["fen"])
        failure = RuntimeError("first selection failed")
        calls = 0

        async def select(*_args, **_kwargs):
            entered.set()
            await asyncio.Event().wait()

        session.execute.side_effect = select
        session.rollback.side_effect = OSError("rollback failed during cancellation")

        async def fetch(limit):
            nonlocal calls
            calls += 1
            if calls == 1:
                await entered.wait()
                raise failure
            return await ask_db.get_fens_for_analysis(limit)

        self.sleep.side_effect = AssertionError("acquisition failure was retried")
        with patch.object(analysis, "ANALYSIS_CONCURRENCY", 2), \
             patch.object(ask_db, "AsyncDBSession", return_value=session):
            with self.assertRaises(RuntimeError) as caught:
                await asyncio.wait_for(self.run_job(fetch), 2)
        self.assertIs(caught.exception, failure)
        self.assertEqual(calls, 2)
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        self.engine.assert_not_awaited()
        self.assertEqual(self.phases()[-1], "failed")

    async def test_coordinator_awaits_owned_sibling_cleanup_before_publishing_failure(self):
        engine_entered, cleanup_entered, release_cleanup = (asyncio.Event() for _ in range(3))
        session = session_for()
        failure = RuntimeError("other worker selection failed")
        calls = 0

        async def engine(*_args, **_kwargs):
            engine_entered.set()
            await asyncio.Event().wait()

        async def close():
            cleanup_entered.set()
            await release_cleanup.wait()

        async def fetch(_limit):
            nonlocal calls
            calls += 1
            if calls == 1:
                await engine_entered.wait()
                raise failure
            return session, ["fen"]

        self.engine.side_effect = engine
        session.rollback.side_effect = OSError("rollback unavailable")
        session.close.side_effect = close
        self.sleep.side_effect = AssertionError("acquisition failure was retried")
        with patch.object(analysis, "ANALYSIS_CONCURRENCY", 2):
            task = asyncio.create_task(self.run_job(fetch))
            try:
                await asyncio.wait_for(cleanup_entered.wait(), 2)
                self.assertFalse(task.done())
                self.assertNotIn("failed", self.phases())
                release_cleanup.set()
                with self.assertRaises(RuntimeError) as caught:
                    await asyncio.wait_for(task, 2)
                self.assertIs(caught.exception, failure)
            finally:
                release_cleanup.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
        session.commit.assert_not_awaited()
        self.assertEqual(self.phases()[-1], "failed")

    async def test_committed_sibling_is_counted_even_if_cancelled_during_summary_update(self):
        summary_entered = asyncio.Event()
        session = session_for()
        failure = RuntimeError("other worker selection failed")
        calls = 0

        async def summary(*_args):
            summary_entered.set()
            await asyncio.Event().wait()

        async def fetch(_limit):
            nonlocal calls
            calls += 1
            if calls == 1:
                await summary_entered.wait()
                raise failure
            return session, ["fen"]

        self.summary.side_effect = summary
        self.sleep.side_effect = AssertionError("acquisition failure was retried")
        with patch.object(analysis, "ANALYSIS_CONCURRENCY", 2):
            with self.assertRaises(RuntimeError) as caught:
                await asyncio.wait_for(self.run_job(fetch), 2)
        self.assertIs(caught.exception, failure)
        session.commit.assert_awaited_once()
        session.rollback.assert_not_awaited()
        session.close.assert_awaited_once()
        self.assertEqual(self.progress.await_args.kwargs["processed"], 1)
        self.assertEqual(self.phases()[-1], "failed")

    async def test_external_cancellation_remains_cancellation_and_cleans_owned_sessions(self):
        for stage in ("selection", "engine"):
            for cleanup_fails in (False, True):
                with self.subTest(stage=stage, cleanup_fails=cleanup_fails):
                    self.progress.reset_mock()
                    session = session_for(["fen"])
                    entered = asyncio.Event()

                    async def blocked(*_args, **_kwargs):
                        entered.set()
                        await asyncio.Event().wait()

                    if stage == "selection":
                        session.execute.side_effect = blocked
                    else:
                        self.engine.side_effect = blocked
                    if cleanup_fails:
                        session.rollback.side_effect = OSError("rollback failed")
                        session.close.side_effect = OSError("close failed")
                    with patch.object(ask_db, "AsyncDBSession", return_value=session):
                        task = asyncio.create_task(self.run_job(ask_db.get_fens_for_analysis))
                        try:
                            await asyncio.wait_for(entered.wait(), 2)
                            task.cancel("external cancellation")
                            with self.assertRaises(asyncio.CancelledError):
                                await asyncio.wait_for(task, 2)
                        finally:
                            if not task.done():
                                task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                    session.rollback.assert_awaited_once()
                    session.close.assert_awaited_once()
                    self.assertNotIn("failed", self.phases())
                    self.assertNotIn("complete", self.phases())

    async def test_engine_request_failure_keeps_its_error_even_if_cleanup_fails(self):
        session = session_for()
        failure = analysis.EngineServiceRequestError("invalid engine request")
        self.engine.side_effect = failure
        session.rollback.side_effect = OSError("rollback failed")
        session.close.side_effect = OSError("close failed")
        with self.assertRaises(analysis.EngineServiceRequestError) as caught:
            await self.run_job(AsyncMock(return_value=(session, ["fen"])))
        self.assertIs(caught.exception, failure)
        session.close.assert_awaited_once()
        self.sleep.assert_not_awaited()
        self.assertEqual(self.phases()[-1], "failed")

    async def test_existing_processing_retry_behavior_is_preserved(self):
        for kind in ("no_result", "invalid_result", "write_failure"):
            with self.subTest(kind=kind):
                self.sleep.reset_mock()
                first, second = session_for(), session_for()
                fetch = AsyncMock(side_effect=[(first, ["fen"]), (second, ["fen"])])
                self.engine.side_effect = ([None, engine_result()] if kind == "no_result" else
                                           [[{"is_valid": False}], engine_result()] if kind == "invalid_result" else None)
                self.stage.side_effect = [RuntimeError("write failed"), None] if kind == "write_failure" else None
                result = await self.run_job(fetch, total_fens_to_process=1)
                self.assertEqual(result["processed"], 1)
                self.assertEqual(result["failed_batches"], 1)
                self.assertEqual(result["failed"], int(kind == "invalid_result"))
                first.rollback.assert_awaited_once()
                first.close.assert_awaited_once()
                second.commit.assert_awaited_once()
                second.close.assert_awaited_once()
                self.sleep.assert_awaited_once_with(1)

    async def test_later_loop_failure_preserves_previous_pass_and_stops_further_cooldowns(self):
        committed, failed, empty = session_for(["fen"]), session_for(), session_for()
        failure = RuntimeError("second pass unavailable")
        failed.begin.side_effect = failure
        with patch.object(ask_db, "AsyncDBSession", side_effect=[committed, failed, empty]):
            with self.assertRaises(RuntimeError) as caught:
                await analysis.run_analysis_loop_job(self.ctx, "all", 3, 1, 1, 60, 100000)
        self.assertIs(caught.exception, failure)
        committed.commit.assert_awaited_once()
        committed.rollback.assert_not_awaited()
        self.sleep.assert_has_awaits([call(60)])
        self.assertEqual(self.sleep.await_count, 1)
        self.assertEqual(self.progress.await_args.kwargs["processed"], 1)
        self.assertEqual(self.phases()[-1], "failed")
        self.assertNotIn("complete", self.phases())
        self.projections.assert_not_awaited()
