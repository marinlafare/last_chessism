"""Restore socket bounds/privacy: no database or cloud connections."""
import os
from pathlib import Path
import stat
import unittest
from unittest.mock import patch

from chessism_api.operations import database_restore_tests as restore


class RestoreSocketTests(unittest.TestCase):
    def test_private_short_unique_directories_ignore_tmpdir(self):
        with patch.dict(os.environ, {'TMPDIR': '/deep/' + 'nested/' * 30}):
            first = restore._create_restore_socket_directory()
            try:
                second = restore._create_restore_socket_directory()
                try:
                    self.assertNotEqual(first, second)
                    for directory in (first, second):
                        self.assertEqual(directory.parent, Path('/tmp'))
                        self.assertFalse(directory.is_symlink())
                        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
                        options = restore._postgres_start_options(directory)
                        self.assertIn("listen_addresses=''", options)
                        self.assertIn('unix_socket_permissions=0700', options)
                        self.assertIn(str(directory), options)
                finally:
                    second.rmdir()
            finally:
                first.rmdir()

    def test_full_socket_filename_must_fit_107_bytes(self):
        directory = Path('/' + 'a' * 91)
        self.assertEqual(len(os.fsencode(directory / '.s.PGSQL.55432')), 107)
        restore._postgres_start_options(directory)
        with self.assertRaisesRegex(restore.RestoreTestUnavailableError, '107-byte'):
            restore._postgres_start_options(Path(str(directory) + 'b'))

    def test_socket_limit_is_bytes_not_characters(self):
        directory = Path('/' + 'é' * 50)
        self.assertLess(len(str(directory / '.s.PGSQL.55432')), 107)
        with self.assertRaisesRegex(restore.RestoreTestUnavailableError, '107-byte'):
            restore._postgres_start_options(directory)
