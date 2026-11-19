from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from chessism_api.database.postgresql import summary_writes, home_summary as home_storage
from chessism_api.operations import mode_summary, global_summaries


class ExplicitSummaryFactoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_refresh_never_falls_back_to_global_database(self):
        session=AsyncMock()
        factory=MagicMock(return_value=session)
        connection=object()
        with patch.object(global_summaries,'AsyncDBSession',side_effect=AssertionError('Global DB used')), \
             patch.object(global_summaries,'driver_connection',AsyncMock(return_value=connection)) as driver, \
             patch.object(summary_writes,'refresh_game_counts',AsyncMock(return_value={})), \
             patch.object(summary_writes,'refresh_fen_summaries',AsyncMock(return_value=({},{}))) as counts, \
             patch.object(global_summaries,'refresh_pipeline_summary',AsyncMock(return_value={
                 'refreshed_at':datetime(2026,1,1,tzinfo=timezone.utc)})):
            await global_summaries.refresh_database_summary(session_factory=factory)
            await global_summaries.refresh_fen_pipeline_summary(session_factory=factory)
            await global_summaries.refresh_scored_position_summary(session_factory=factory)
        self.assertEqual([call.args for call in driver.await_args_list],[(session,)]*3)
        self.assertEqual([call.args for call in counts.await_args_list],[(connection,)]*2)
        self.assertEqual(session.begin.await_count,3)
        self.assertEqual(session.close.await_count,3)
        mode_session=AsyncMock()
        mode_factory=MagicMock(return_value=mode_session)
        with patch.object(mode_summary,'AsyncDBSession',side_effect=AssertionError('Global DB used')), \
             patch.object(mode_summary,'driver_connection',AsyncMock(return_value=object())), \
             patch.object(mode_summary,'refresh_modes',AsyncMock(return_value={'total':0})):
            await mode_summary.refresh_main_character_mode_summary(session_factory=mode_factory)
        self.assertEqual(session.commit.await_count+mode_session.commit.await_count,4)
        mode_session.begin.assert_awaited_once()
        mode_session.close.assert_awaited_once()

    async def test_unowned_summary_transactions_rejected(self):
        connection=MagicMock()
        connection.is_in_transaction.return_value=False
        connection.fetchval=AsyncMock()
        for operation in (summary_writes.refresh_game_counts,summary_writes.refresh_fen_summaries):
            with self.assertRaisesRegex(ValueError,'caller-owned'):
                await operation(connection)
        connection.fetchval.assert_not_awaited()
        session=MagicMock()
        session.in_transaction.return_value=False
        session.execute=AsyncMock()
        with self.assertRaisesRegex(ValueError,'caller-owned'):
            await home_storage.begin_refresh(session,1)
        session.execute.assert_not_awaited()
