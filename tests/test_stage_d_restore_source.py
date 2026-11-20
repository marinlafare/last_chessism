"""Safety gates for the isolated historical restore supervisor; no database access."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('stage_d_restore_source',
    Path(__file__).resolve().parents[1] / 'scripts/stage_d_restore_source.py')
restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(restore)


class SourceRestoreSafetyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='chessism-stage-d-guard-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for name in ['data', 'evidence', 'checkpoint']:
            (self.root / name).mkdir()
        for name, value in [('DATA', self.root / 'data'), ('EVIDENCE', self.root / 'evidence'),
                            ('CHECKPOINT', self.root / 'checkpoint'), ('PGDATA', self.root / 'data/pgdata')]:
            patcher = patch.object(restore, name, value); patcher.start(); self.addCleanup(patcher.stop)
        patcher = patch.object(restore.os, 'geteuid', return_value=999)
        patcher.start(); self.addCleanup(patcher.stop)
        patcher = patch.object(restore.shutil, 'disk_usage', return_value=SimpleNamespace(free=200 * restore.GIB))
        patcher.start(); self.addCleanup(patcher.stop)
        self.job = restore.Restore()

    def test_wrong_volume_marker_cannot_start_or_stop_an_existing_cluster(self):
        (restore.DATA / '.stage-d-owner').write_text('someone-else')
        restore.PGDATA.mkdir(); (restore.PGDATA / 'postmaster.pid').write_text('1')
        with patch.object(restore.subprocess, 'run') as command:
            with self.assertRaisesRegex(ValueError, 'ownership marker'):
                self.job.execute()
            self.job.stop_children()
            command.assert_not_called()

    def test_existing_owned_data_is_not_overwritten(self):
        (restore.DATA / '.stage-d-owner').write_text(restore.RUN_ID)
        restore.PGDATA.mkdir()
        with patch.object(self.job, 'command') as command:
            with self.assertRaisesRegex(ValueError, 'already exists'):
                self.job.execute()
            command.assert_not_called()

    def test_free_space_floor_is_enforced_before_work(self):
        with patch.object(restore.shutil, 'disk_usage', return_value=SimpleNamespace(free=79 * restore.GIB)):
            with self.assertRaisesRegex(RuntimeError, 'free-space floor'):
                self.job.check_budget()

    def test_time_and_database_size_limits_are_enforced(self):
        self.job.clock -= restore.MAX_SECONDS + 1
        with self.assertRaisesRegex(RuntimeError, 'Two-hour'):
            self.job.check_budget()
        self.job.clock += restore.MAX_SECONDS + 1
        self.job.database_bytes = restore.MAX_DATABASE_BYTES + 1
        with self.assertRaisesRegex(RuntimeError, '150 GiB'):
            self.job.check_budget()

    def test_stop_request_is_cooperative(self):
        (restore.EVIDENCE / 'STOP_RESTORE').touch()
        with self.assertRaisesRegex(RuntimeError, 'Graceful stop requested'):
            self.job.check_budget()

    def test_unexpected_checkpoint_identifier_is_rejected(self):
        with self.assertRaises(ValueError):
            restore.identifier('public; DROP DATABASE chessism')
        self.assertEqual(restore.identifier('chessism_archive'), '"chessism_archive"')

    def test_checksum_mismatch_refuses_restore_and_keeps_checkpoint_unchanged(self):
        archive = restore.CHECKPOINT / 'database.dump'
        archive.write_bytes(b'synthetic archive')
        metadata = {'checkpoint_id': restore.CHECKPOINT_ID, 'archive_sha256': restore.ARCHIVE_SHA256,
                    'archive_bytes': archive.stat().st_size}
        verification = {'checkpoint_id': restore.CHECKPOINT_ID, 'restore_tested': True,
                        'validation': {'archive_sha256': restore.ARCHIVE_SHA256,
                                       'schema_matches': True, 'table_counts_match': True}}
        (restore.CHECKPOINT / 'manifest.json').write_text(json.dumps(metadata))
        (restore.CHECKPOINT / 'verification.json').write_text(json.dumps(verification))
        for name in ['schema.sql', 'archive-contents.txt']:
            (restore.CHECKPOINT / name).write_text('synthetic')
        before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in restore.CHECKPOINT.iterdir()}
        with self.assertRaisesRegex(ValueError, 'SHA256 mismatch'):
            self.job.verify_checkpoint()
        self.assertEqual(before, {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in restore.CHECKPOINT.iterdir()})
