import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock,MagicMock,patch

from scripts.run_db_refactor_corpus import verify_acceptance
from scripts.launch_db_refactor_http_backup import runner_command
from scripts.watch_db_refactor import render
from chessism_api.database.postgresql import summary_writes
from chessism_api.operations import mode_summary, global_summaries
from chessism_api.database.postgresql import home_summary as home_storage


class CorpusSafetyTests(unittest.TestCase):
    def test_failed_or_wrong_source_acceptance_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)
            status={'mode':'isolated_http_backup','state':'succeeded','source_workspace':'/accepted',
                'live_database_changed':False,'private_postgres_stopped':True,'source_unchanged':True,'completed_jobs':3}
            acceptance={'state':'succeeded','real_arq_jobs':3,'http_checks':18}
            (path/'status.json').write_text(json.dumps(status))
            (path/'acceptance.json').write_text(json.dumps(acceptance))
            verify_acceptance(path,Path('/accepted'))
            with self.assertRaises(ValueError): verify_acceptance(path,Path('/other'))
            status['state']='failed'
            (path/'status.json').write_text(json.dumps(status))
            with self.assertRaises(ValueError): verify_acceptance(path,Path('/accepted'))

    def test_launch_and_watch_explicitly_private(self):
        command=runner_command('corpus-finalization-001')
        self.assertEqual(command[command.index('--network')+1],'none')
        self.assertIn('/candidate/scripts/run_db_refactor_corpus.py',command)
        self.assertIn('/reports/http-backup-002',command)
        self.assertNotIn('/var/run/docker.sock',' '.join(command))
        output=render({'mode':'isolated_corpus_finalization','state':'running','steps_completed':4,
            'rollback_verified':True})
        self.assertIn('4 / 8',output)
        self.assertIn('rollback: verified',output)
        self.assertIn('Live database unchanged',output)


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
