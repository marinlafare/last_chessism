"""Selection failure and session ownership contracts, without a live database."""
import asyncio
import inspect
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from chessism_api.database.postgresql import analysis_selection


def selection_session(fens=("fen-b", "fen-a"), eligible=("fen-a", "fen-b")):
    rows = Mock()
    rows.scalars.return_value.all.return_value = list(fens)
    recheck = Mock()
    recheck.all.return_value = list(eligible)
    return SimpleNamespace(
        begin=AsyncMock(), execute=AsyncMock(return_value=rows),
        scalars=AsyncMock(return_value=recheck), rollback=AsyncMock(),
        close=AsyncMock(), commit=AsyncMock(),
    )


SELECTORS = (
    (analysis_selection.get_fens_for_analysis, (2,)),
    (analysis_selection.get_player_fens_for_analysis, ("fixture", 2)),
    (analysis_selection.get_game_set_fens_for_analysis, ([33, None, 11, 33], 2)),
)


class AnalysisSelectionFailureTests(unittest.IsolatedAsyncioTestCase):
    def test_selection_contract_has_no_error_suppression_switch(self):
        for selector, _ in SELECTORS:
            with self.subTest(selector=selector.__name__):
                self.assertNotIn("raise_errors", inspect.signature(selector).parameters)

    async def test_begin_selection_and_fresh_snapshot_errors_propagate(self):
        for selector, args in SELECTORS:
            for stage in ("begin", "execute", "scalars"):
                with self.subTest(selector=selector.__name__, stage=stage):
                    session = selection_session()
                    failure = RuntimeError(f"database failure during {stage}")
                    getattr(session, stage).side_effect = failure
                    with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                        with self.assertRaises(RuntimeError) as caught:
                            await selector(*args)
                    self.assertIs(caught.exception, failure)
                    session.rollback.assert_awaited_once()
                    session.close.assert_awaited_once()
                    session.commit.assert_not_awaited()

    async def test_cleanup_failures_do_not_replace_the_selection_error(self):
        for selector, args in SELECTORS:
            for rollback_fails, close_fails in ((True, False), (False, True), (True, True)):
                with self.subTest(selector=selector.__name__, rollback=rollback_fails, close=close_fails):
                    session = selection_session()
                    failure = RuntimeError("original selection error")
                    session.execute.side_effect = failure
                    if rollback_fails:
                        session.rollback.side_effect = OSError("rollback failed")
                    if close_fails:
                        session.close.side_effect = OSError("close failed")
                    with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                        with self.assertRaises(RuntimeError) as caught:
                            await selector(*args)
                    self.assertIs(caught.exception, failure)
                    session.rollback.assert_awaited_once()
                    session.close.assert_awaited_once()

    async def test_cancellation_before_handoff_releases_sessions_even_if_cleanup_fails(self):
        for selector, args in SELECTORS:
            for stage in ("begin", "execute", "scalars"):
                for cleanup_fails in (False, True):
                    with self.subTest(selector=selector.__name__, stage=stage, cleanup_fails=cleanup_fails):
                        entered = asyncio.Event()
                        never = asyncio.Event()
                        session = selection_session()

                        async def blocked(*_args, **_kwargs):
                            entered.set()
                            await never.wait()

                        getattr(session, stage).side_effect = blocked
                        if cleanup_fails:
                            session.rollback.side_effect = OSError("rollback failed")
                            session.close.side_effect = OSError("close failed")
                        with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                            task = asyncio.create_task(selector(*args))
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

    async def test_empty_selection_or_empty_recheck_is_successful_and_releases_session(self):
        for selector, args in SELECTORS:
            for initial in ([], ["claimed-elsewhere"]):
                with self.subTest(selector=selector.__name__, initial=initial):
                    session = selection_session(initial, [])
                    with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                        self.assertEqual(await selector(*args), (None, None))
                    session.rollback.assert_awaited_once()
                    session.close.assert_awaited_once()
                    self.assertEqual(session.scalars.await_count, int(bool(initial)))

    async def test_empty_cleanup_failure_is_not_successful_exhaustion(self):
        for selector, args in SELECTORS:
            for stage in ("rollback", "close"):
                with self.subTest(selector=selector.__name__, stage=stage):
                    session = selection_session([], [])
                    failure = OSError(f"empty {stage} failed")
                    getattr(session, stage).side_effect = failure
                    with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                        with self.assertRaises(OSError) as caught:
                            await selector(*args)
                    self.assertIs(caught.exception, failure)
                    session.rollback.assert_awaited_once()
                    session.close.assert_awaited_once()

    async def _assert_repeated_cancellation_during_cleanup(self, step):
        for selector, args in SELECTORS:
            for selection_fails in (False, True):
                for cleanup_fails in (False, True):
                    with self.subTest(selector=selector.__name__, step=step,
                                      selection_fails=selection_fails, cleanup_fails=cleanup_fails):
                        entered, release, finished, interrupted = (asyncio.Event() for _ in range(4))
                        cancellation_messages = []
                        session = selection_session([], [])
                        failure = RuntimeError("original database selection failure")
                        if selection_fails:
                            session.begin.side_effect = failure

                        async def cleanup():
                            entered.set()
                            try:
                                await release.wait()
                            except asyncio.CancelledError:
                                interrupted.set()
                                raise
                            finished.set()
                            if cleanup_fails:
                                raise OSError("secondary cleanup failure")

                        getattr(session, step).side_effect = cleanup

                        async def run_observed_selection():
                            try:
                                return await selector(*args)
                            except asyncio.CancelledError as error:
                                # Observe before Python 3.10's cancelled-task message loss.
                                cancellation_messages.append(error.args)
                                raise

                        with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                            task = asyncio.create_task(run_observed_selection())
                            try:
                                await asyncio.wait_for(entered.wait(), 2)
                                for message in ("original cancellation", "repeated cancellation"):
                                    task.cancel(message)
                                    ready = asyncio.Event()
                                    asyncio.get_running_loop().call_soon(ready.set)
                                    await ready.wait()
                                    self.assertFalse(interrupted.is_set())
                                    self.assertFalse(task.done())
                                release.set()
                                expected = RuntimeError if selection_fails else asyncio.CancelledError
                                with self.assertRaises(expected) as caught:
                                    await asyncio.wait_for(task, 2)
                                if selection_fails:
                                    self.assertIs(caught.exception, failure)
                                else:
                                    self.assertEqual(cancellation_messages, [("original cancellation",)])
                                    self.assertTrue(task.cancelled())
                                self.assertTrue(finished.is_set())
                                self.assertFalse(interrupted.is_set())
                            finally:
                                release.set()
                                if not task.done():
                                    task.cancel()
                                await asyncio.gather(task, return_exceptions=True)
                        session.rollback.assert_awaited_once()
                        session.close.assert_awaited_once()
                        session.commit.assert_not_awaited()

    async def test_repeated_cancellation_finishes_selector_rollback_before_returning(self):
        await self._assert_repeated_cancellation_during_cleanup("rollback")

    async def test_repeated_cancellation_finishes_selector_close_before_returning(self):
        await self._assert_repeated_cancellation_during_cleanup("close")

    async def test_handoff_preserves_order_recheck_and_caller_transaction_ownership(self):
        for selector, args in SELECTORS:
            with self.subTest(selector=selector.__name__):
                session = selection_session()
                with patch.object(analysis_selection, "AsyncDBSession", return_value=session):
                    selected_session, fens = await selector(*args)
                self.assertIs(selected_session, session)
                self.assertEqual(fens, ["fen-b", "fen-a"])
                session.begin.assert_awaited_once()
                session.execute.assert_awaited_once()
                session.scalars.assert_awaited_once()
                self.assertEqual(session.scalars.await_args.args[1], {"fens": ["fen-b", "fen-a"]})
                session.rollback.assert_not_awaited()
                session.close.assert_not_awaited()
                session.commit.assert_not_awaited()
                if selector is analysis_selection.get_game_set_fens_for_analysis:
                    self.assertEqual(session.execute.await_args.args[1], {"game_links": [33, 11, 33], "limit": 2})

    async def test_empty_frozen_input_does_not_open_a_session(self):
        with patch.object(analysis_selection, "AsyncDBSession") as create:
            self.assertEqual(await analysis_selection.get_game_set_fens_for_analysis([], 10), (None, None))
        create.assert_not_called()
