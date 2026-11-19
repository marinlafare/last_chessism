"""Fatal acquisition, cancellation, committed work and entry-point regressions."""
import asyncio
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from chessism_api.database.postgresql import analysis_selection
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
        self.real_reset = analysis._reset_job_progress
        self.real_progress = analysis._write_job_progress
        self.concurrency = patch.object(analysis, "ANALYSIS_CONCURRENCY", 1)
        self.concurrency.start()
        self.addCleanup(self.concurrency.stop)
        self.progress = self.stub("_write_job_progress")
        self.reset = self.stub("_reset_job_progress", return_value="test-attempt")
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
                with patch.object(analysis_selection, "AsyncDBSession", side_effect=[failed, session_for()]) as create, \
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
                with patch.object(analysis_selection, "AsyncDBSession", return_value=session), \
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
        self.assertEqual(final["attempt_id"], "test-attempt")
        self.assertEqual(self.engine.await_args.kwargs["progress_attempt_id"], "test-attempt")
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
            return await analysis_selection.get_fens_for_analysis(limit)

        self.sleep.side_effect = AssertionError("acquisition failure was retried")
        with patch.object(analysis, "ANALYSIS_CONCURRENCY", 2), \
             patch.object(analysis_selection, "AsyncDBSession", return_value=session):
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
                    with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                        task = asyncio.create_task(self.run_job(analysis_selection.get_fens_for_analysis))
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

    async def _assert_staggered_cancellation_cleanup(self, *, rollback_fails, cancel_again):
        self.progress.reset_mock()
        engine_entered, close_entered, release_close = (asyncio.Event() for _ in range(3))
        close_finished, close_interrupted, draining = (asyncio.Event() for _ in range(3))
        sessions = [session_for(), session_for()]
        workers = []
        engine_calls = 0
        real_gather = asyncio.gather

        def observe_drain(*tasks, **kwargs):
            result = real_gather(*tasks, **kwargs)
            if kwargs.get("return_exceptions"):
                asyncio.get_running_loop().call_soon(draining.set)
            return result

        async def checkpoint():
            # Run callbacks already queued by cancellation; no elapsed-time assumption.
            ready = asyncio.Event()
            asyncio.get_running_loop().call_soon(ready.set)
            await ready.wait()

        async def engine(*_args, **_kwargs):
            nonlocal engine_calls
            engine_calls += 1
            if engine_calls == 2:
                engine_entered.set()
            await asyncio.Event().wait()

        async def fetch(_limit):
            index = len(workers)
            workers.append(asyncio.current_task())
            return sessions[index], ["fen"]

        async def first_close():
            await close_entered.wait()

        async def second_close():
            close_entered.set()
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                close_interrupted.set()
                raise
            close_finished.set()

        self.engine.side_effect = engine
        sessions[0].close.side_effect = first_close
        sessions[1].close.side_effect = second_close
        if rollback_fails:
            sessions[1].rollback.side_effect = OSError("rollback unavailable")

        with patch.object(analysis, "ANALYSIS_CONCURRENCY", 2), \
             patch.object(analysis.asyncio, "gather", observe_drain):
            task = asyncio.create_task(self.run_job(fetch, total_fens_to_process=2))
            try:
                await asyncio.wait_for(engine_entered.wait(), 2)
                task.cancel("external cancellation")
                await asyncio.wait_for(close_entered.wait(), 2)
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(workers[0]), 2)
                await asyncio.wait_for(draining.wait(), 2)
                await checkpoint()
                self.assertFalse(close_interrupted.is_set(), "sibling close was cancelled again")
                self.assertFalse(task.done(), "coordinator finished before sibling cleanup")

                if cancel_again:
                    task.cancel("another external cancellation")
                    await checkpoint()
                    self.assertFalse(close_interrupted.is_set())
                    self.assertFalse(task.done())

                release_close.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
                self.assertTrue(task.cancelled())
                self.assertTrue(close_finished.is_set())
            finally:
                release_close.set()
                if not task.done():
                    task.cancel()
                await real_gather(task, return_exceptions=True)
                await real_gather(*workers, return_exceptions=True)

        for session in sessions:
            session.rollback.assert_awaited_once()
            session.close.assert_awaited_once()
            session.commit.assert_not_awaited()
        self.assertEqual(self.phases(), ["queued"])

    async def test_external_cancellation_drains_two_staggered_closes(self):
        for rollback_fails in (False, True):
            with self.subTest(rollback_fails=rollback_fails):
                await self._assert_staggered_cancellation_cleanup(
                    rollback_fails=rollback_fails, cancel_again=False)

    async def test_repeated_external_cancellation_does_not_interrupt_drain(self):
        for rollback_fails in (False, True):
            with self.subTest(rollback_fails=rollback_fails):
                await self._assert_staggered_cancellation_cleanup(
                    rollback_fails=rollback_fails, cancel_again=True)

    async def _assert_cleanup_already_underway(self, *, owner, trigger, close_fails, step="close"):
        self.progress.reset_mock()
        self.engine.reset_mock()
        self.stage.reset_mock()
        cleanup_entered, release_cleanup, close_finished = (asyncio.Event() for _ in range(3))
        cleanup_interrupted, fail_sibling, draining = (asyncio.Event() for _ in range(3))
        cleanup_finished = asyncio.Event()
        session = session_for()
        selection_failure = RuntimeError("selection failed before handoff")
        sibling_failure = RuntimeError("sibling acquisition failed")
        engine_failure = analysis.EngineServiceRequestError("original engine request failure")
        committed = owner == "worker" and step == "close"
        self.engine.side_effect = engine_failure if step == "rollback" else None
        workers = []
        cancellation_messages = []
        real_gather = asyncio.gather

        def observe_drain(*tasks, **kwargs):
            result = real_gather(*tasks, **kwargs)
            if kwargs.get("return_exceptions"):
                asyncio.get_running_loop().call_soon(draining.set)
            return result

        async def checkpoint():
            ready = asyncio.Event()
            asyncio.get_running_loop().call_soon(ready.set)
            await ready.wait()

        async def suspend_cleanup():
            cleanup_entered.set()
            try:
                await release_cleanup.wait()
            except asyncio.CancelledError:
                cleanup_interrupted.set()
                raise
            cleanup_finished.set()

        async def close():
            if step == "close":
                await suspend_cleanup()
            close_finished.set()
            if close_fails:
                raise OSError("secondary close failure")

        async def rollback():
            await suspend_cleanup()
            raise OSError("rollback failed after completing its wait")

        async def fetch(limit):
            workers.append(asyncio.current_task())
            if len(workers) == 1:
                await fail_sibling.wait()
                raise sibling_failure
            if owner == "selector":
                return await analysis_selection.get_fens_for_analysis(limit)
            return session, ["fen"]

        async def run_observed_job():
            try:
                return await self.run_job(fetch, total_fens_to_process=2)
            except asyncio.CancelledError as error:
                # Python 3.10 drops the message when wait_for retrieves a cancelled task.
                cancellation_messages.append(error.args)
                raise

        session.close.side_effect = close
        if owner == "selector":
            session.begin.side_effect = selection_failure
            session.rollback.side_effect = OSError("rollback failed before close")
        elif step == "rollback":
            session.rollback.side_effect = rollback

        with patch.object(analysis, "ANALYSIS_CONCURRENCY", 2), \
             patch.object(analysis_selection, "AsyncDBSession", return_value=session), \
             patch.object(analysis.asyncio, "gather", observe_drain):
            task = asyncio.create_task(run_observed_job())
            try:
                # The first cancellation must arrive after cleanup has begun.
                await asyncio.wait_for(cleanup_entered.wait(), 2)
                if trigger == "external":
                    task.cancel("original external cancellation")
                else:
                    fail_sibling.set()
                await asyncio.wait_for(draining.wait(), 2)
                await checkpoint()
                self.assertFalse(cleanup_interrupted.is_set(), "already-running cleanup was interrupted")
                self.assertFalse(task.done(), "coordinator returned before close finished")
                self.assertNotIn("failed", self.phases())
                self.assertNotIn("complete", self.phases())

                for _ in range(2):
                    task.cancel("repeated cancellation during cleanup")
                    await checkpoint()
                    self.assertFalse(cleanup_interrupted.is_set())
                    self.assertFalse(task.done())

                release_cleanup.set()
                expected = asyncio.CancelledError if trigger == "external" else RuntimeError
                with self.assertRaises(expected) as caught:
                    await asyncio.wait_for(task, 2)
                if trigger == "external":
                    self.assertEqual(cancellation_messages, [("original external cancellation",)])
                    self.assertTrue(task.cancelled())
                    self.assertNotIn("failed", self.phases())
                else:
                    self.assertIs(caught.exception, sibling_failure)
                    self.assertEqual(self.phases()[-1], "failed")
                    self.assertEqual(self.progress.await_args.kwargs["processed"], int(committed))
                self.assertTrue(cleanup_finished.is_set())
                self.assertTrue(close_finished.is_set())
                self.assertFalse(cleanup_interrupted.is_set())
                if owner == "selector":
                    self.assertIs(workers[1].exception(), selection_failure)
                elif step == "rollback":
                    self.assertIs(workers[1].exception(), engine_failure)
            finally:
                release_cleanup.set()
                if not task.done():
                    task.cancel()
                await real_gather(task, *workers, return_exceptions=True)

        session.close.assert_awaited_once()
        self.sleep.assert_not_awaited()
        if committed:
            session.commit.assert_awaited_once()
            session.rollback.assert_not_awaited()
            self.stage.assert_awaited_once()
        else:
            session.rollback.assert_awaited_once()
            session.commit.assert_not_awaited()
            self.stage.assert_not_awaited()
            if owner == "selector":
                self.engine.assert_not_awaited()

    async def test_external_cancellation_waits_for_worker_close_already_underway(self):
        for close_fails in (False, True):
            with self.subTest(close_fails=close_fails):
                await self._assert_cleanup_already_underway(
                    owner="worker", trigger="external", close_fails=close_fails)

    async def test_sibling_failure_waits_for_worker_close_already_underway(self):
        for close_fails in (False, True):
            with self.subTest(close_fails=close_fails):
                await self._assert_cleanup_already_underway(
                    owner="worker", trigger="sibling", close_fails=close_fails)

    async def test_external_cancellation_waits_for_selector_close_after_rollback_failure(self):
        for close_fails in (False, True):
            with self.subTest(close_fails=close_fails):
                await self._assert_cleanup_already_underway(
                    owner="selector", trigger="external", close_fails=close_fails)

    async def test_sibling_failure_waits_for_selector_close_after_rollback_failure(self):
        for close_fails in (False, True):
            with self.subTest(close_fails=close_fails):
                await self._assert_cleanup_already_underway(
                    owner="selector", trigger="sibling", close_fails=close_fails)

    async def test_external_cancellation_waits_for_worker_rollback_already_underway(self):
        for close_fails in (False, True):
            with self.subTest(close_fails=close_fails):
                await self._assert_cleanup_already_underway(
                    owner="worker", trigger="external", step="rollback", close_fails=close_fails)

    async def test_sibling_failure_waits_for_worker_rollback_already_underway(self):
        for close_fails in (False, True):
            with self.subTest(close_fails=close_fails):
                await self._assert_cleanup_already_underway(
                    owner="worker", trigger="sibling", step="rollback", close_fails=close_fails)

    async def test_processing_failures_propagate_without_reselection(self):
        for kind in ("no_result", "invalid_result", "write_failure"):
            with self.subTest(kind=kind):
                self.sleep.reset_mock()
                self.progress.reset_mock()
                first, second = session_for(), session_for()
                fetch = AsyncMock(side_effect=[(first, ["fen"]), (second, ["fen"])])
                self.engine.side_effect = ([None, engine_result()] if kind == "no_result" else
                                           [[{"is_valid": False}], engine_result()] if kind == "invalid_result" else None)
                self.stage.side_effect = [RuntimeError("write failed"), None] if kind == "write_failure" else None
                with self.assertRaises(RuntimeError):
                    await self.run_job(fetch, total_fens_to_process=1)
                fetch.assert_awaited_once_with(1)
                first.rollback.assert_awaited_once()
                first.close.assert_awaited_once()
                first.commit.assert_not_awaited()
                second.commit.assert_not_awaited()
                second.close.assert_not_awaited()
                self.sleep.assert_not_awaited()
                self.assertEqual(self.phases()[-1], "failed")
                self.assertNotIn("complete", self.phases())

    async def test_later_loop_failure_preserves_previous_pass_and_stops_further_cooldowns(self):
        committed, failed, empty = session_for(["fen"]), session_for(), session_for()
        failure = RuntimeError("second pass unavailable")
        failed.begin.side_effect = failure
        with patch.object(analysis_selection, "AsyncDBSession", side_effect=[committed, failed, empty]):
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

    async def test_entry_points_keep_one_attempt_for_all_engine_batches_and_progress(self):
        for name, _selector, _args, entry in self.entry_points():
            with self.subTest(entry=name):
                self.reset.reset_mock()
                self.progress.reset_mock()
                self.engine.reset_mock()
                attempt = f"attempt-{name}"
                self.reset.return_value = attempt
                with patch.object(analysis_selection, "AsyncDBSession", side_effect=lambda: session_for(["fen"])):
                    await entry()
                self.reset.assert_awaited_once_with(self.ctx, self.ctx["job_id"])
                self.assertGreater(self.engine.await_count, 1)
                for call in self.engine.await_args_list:
                    self.assertEqual(call.kwargs["progress_attempt_id"], attempt)
                for call in self.progress.await_args_list:
                    self.assertEqual(call.kwargs["attempt_id"], attempt)
                self.assertEqual(self.phases()[-1], "complete")

    async def test_redis_reset_and_reporting_errors_preserve_original_database_failure(self):
        failure = RuntimeError("database unavailable")
        self.ctx["redis"] = SimpleNamespace(eval=AsyncMock(side_effect=OSError("Redis unavailable")))
        with patch.object(analysis, "_reset_job_progress", self.real_reset), \
             patch.object(analysis, "_write_job_progress", self.real_progress):
            with self.assertRaises(RuntimeError) as caught:
                await self.run_job(AsyncMock(side_effect=failure))
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.ctx["redis"].eval.await_count, 3)
        self.engine.assert_not_awaited()
        self.stage.assert_not_awaited()

    async def test_parent_pass_requires_explicit_attempt_identity(self):
        fetch = AsyncMock()
        with self.assertRaisesRegex(ValueError, "parent attempt"):
            await self.run_job(fetch, reset_progress=False)
        fetch.assert_not_awaited()
        self.engine.assert_not_awaited()
        self.progress.assert_not_awaited()
