import asyncio
from pathlib import Path
import tempfile
import unittest

from scripts.run_db_refactor_release import check_source,clone_stopped_source
from scripts.watch_db_refactor import render


class ReleaseRehearsalTests(unittest.TestCase):
    def test_live_and_unaccepted_sources_refused_before_copy(self):
        for path in ('/var/lib/postgresql/data','/tmp/pgdata',
                     '/restore-test/db-refactor-baseline-original/pgdata'):
            with self.assertRaises(RuntimeError):
                check_source(Path(path))

    def test_copy_progress_is_explicitly_local(self):
        text=render({'mode':'isolated_canonical_release','state':'running',
            'phase':'copying_private_release_workspace','source_bytes':1024**3,'copy_bytes_done':512*1024**2})
        self.assertIn('50.0%',text)
        self.assertIn('Live database unchanged',text)
        self.assertIn('No API/worker deployment',text)


class CloneTests(unittest.IsolatedAsyncioTestCase):
    async def test_copy_is_independent_and_existing_target_refused(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name)
            source=root/'source';source.mkdir()
            (source/'data').write_text('original')
            target=root/'target'
            await clone_stopped_source(source,target)
            self.assertNotEqual((source/'data').stat().st_ino,(target/'data').stat().st_ino)
            (target/'data').write_text('changed')
            self.assertEqual((source/'data').read_text(),'original')
            with self.assertRaises(RuntimeError):
                await clone_stopped_source(source,target)
