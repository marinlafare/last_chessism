import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock,MagicMock,patch

from scripts.run_db_refactor_corpus import verify_acceptance
from scripts.launch_db_refactor_http_backup import runner_command
from scripts.watch_db_refactor import render
from chessism_api.database import ask_db
from chessism_api.operations.home_summary import refresh_home_summary


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
        result=MagicMock()
        result.mappings.return_value.first.return_value={}
        session=AsyncMock()
        session.execute.return_value=result
        context=MagicMock(__aenter__=AsyncMock(return_value=session),__aexit__=AsyncMock(return_value=False))
        factory=MagicMock(return_value=context)
        with patch.object(ask_db,'AsyncDBSession',side_effect=AssertionError('Global DB used')):
            await ask_db.refresh_database_summary(session_factory=factory)
            await ask_db.refresh_fen_pipeline_summary(session_factory=factory)
            await ask_db.refresh_scored_position_summary(session_factory=factory,database_summary={})
            await ask_db.refresh_main_character_mode_summary(session_factory=factory)
        self.assertEqual(session.commit.await_count,4)

    async def test_incomplete_factory_arguments_rejected(self):
        with self.assertRaises(ValueError):
            await ask_db.refresh_scored_position_summary(session_factory=MagicMock())
        with self.assertRaises(ValueError):
            await refresh_home_summary(transaction_managed=True)
