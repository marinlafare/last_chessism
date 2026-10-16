import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from chessism_api.operations import backup_volume_usage as usage


class BackupVolumeUsageTests(unittest.TestCase):
    def test_only_explicit_reserved_sizes_are_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            status = root / 'backup-status.json'
            status.write_text(json.dumps({'total_bytes': 900, 'database_bytes': 1000,
                                          'protected_bytes': 800, 'free_floor_bytes': 300}))
            self.assertIsNone(usage._reservation(root))
            status.write_text(json.dumps({'storage': {'quota_bytes': '200000000000'}}))
            self.assertEqual(usage._reservation(root), 200000000000)
            (root / 'backup-allocation.json').write_text('{"reserved_bytes":400}')
            self.assertEqual(usage._reservation(root), 400)

    def test_metadata_is_small_regular_json_and_does_not_follow_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / 'secret'
            target.write_text('{"quota_bytes":200}')
            (root / 'backup-policy.json').symlink_to(target)
            self.assertIsNone(usage._reservation(root))
            status = root / 'backup-status.json'
            for value in ('false', '[200]', 'not-json', '{"quota_bytes":-1}',
                          '{"quota_bytes":true}', '{"quota_bytes":"infinity"}', ' ' * 65537):
                status.write_text(value)
                self.assertIsNone(usage._reservation(root))

    def test_measurement_counts_hardlinks_once_and_ignores_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ('chessism', 'timeshift', 'other-app', 'lost+found'):
                (root / name).mkdir()
            (root / 'outside').symlink_to('/tmp', target_is_directory=True)
            original = root / 'timeshift/first-snapshot'
            original.write_bytes(b'x' * 16384)
            os.link(original, root / 'timeshift/second-snapshot')
            (root / 'other-app/backup-allocation.json').write_text('{"quota_bytes":1000000}')
            identity = (root.stat().st_dev, root.stat().st_ino)
            with patch.object(usage, '_check_volume', return_value=identity):
                result = usage.measure_volume(root, 'uuid', 'chessism', 200000000000)
            rows = {row['id']: row for row in result['allocations']}
            self.assertEqual(set(rows), {'chessism', 'timeshift', 'other-app'})
            self.assertEqual(rows['chessism']['reserved_bytes'], 200000000000)
            self.assertEqual(rows['other-app']['reserved_bytes'], 1000000)
            self.assertIsNone(rows['timeshift']['reserved_bytes'])
            expected = original.stat().st_blocks * 512 + (root / 'timeshift').stat().st_blocks * 512
            self.assertEqual(rows['timeshift']['used_bytes'], expected)

    def test_unmounted_volume_is_not_scanned(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(usage.subprocess, 'run') as run:
            with self.assertRaises(OSError):
                usage.measure_volume(Path(temporary), 'uuid', 'chessism', 200)
            run.assert_not_called()

    def test_cache_returns_immediately_and_only_starts_one_scan(self):
        key = ('/test-backup-volume', 'uuid', 'chessism', 200)
        with patch.object(usage, '_states', {}), patch.object(usage.threading, 'Thread') as thread:
            first = usage.volume_usage(Path(key[0]), *key[1:])
            second = usage.volume_usage(Path(key[0]), *key[1:])
            self.assertTrue(first['refreshing'])
            self.assertTrue(second['refreshing'])
            thread.assert_called_once()
            thread.return_value.start.assert_called_once()
            with patch.object(usage, 'measure_volume', return_value={'allocations': [], 'measured_at': 'now'}):
                usage._refresh(key)
            third = usage.volume_usage(Path(key[0]), *key[1:])
            self.assertFalse(third['refreshing'])
            self.assertEqual(third['measured_at'], 'now')
            thread.assert_called_once()

    def test_failed_refresh_retains_previous_data_and_throttles_retries(self):
        key = ('/test-backup-volume', 'uuid', 'chessism', 200)
        state = {'snapshot': {'allocations': [{'id': 'old'}], 'measured_at': 'yesterday'},
                 'refreshing': True, 'checked_at': None}
        with patch.object(usage, '_states', {key: state}), patch.object(usage, 'measure_volume',
                side_effect=subprocess.TimeoutExpired('sensitive command', 30)):
            usage._refresh(key)
            result = usage.volume_usage(Path(key[0]), *key[1:])
        self.assertFalse(result['refreshing'])
        self.assertTrue(result['stale'])
        self.assertEqual(result['measured_at'], 'yesterday')
        self.assertNotIn('sensitive', result['error'])


if __name__ == '__main__':
    unittest.main()
