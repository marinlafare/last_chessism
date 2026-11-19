"""Two phases around a harness-owned pg_dump/pg_restore on private PostgreSQL."""
import json
import os
from pathlib import Path
import unittest
import asyncpg
from scripts.init_canonical_db import isolated_target
from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.canonical import backup
from tests.canonical_schema_fixture import seed

SOURCE='chessism_canonical_dump_source'
TARGET='chessism_canonical_dump_restored'


def settings():
    return dict(host=isolated_target(os.environ['CHESSISM_CANONICAL_TEST_SOCKET'],'chessism_canonical_test'),port=55432,user='test')


def evidence():
    path=Path(os.environ['CHESSISM_BACKUP_TEST_EVIDENCE'])
    if not str(path).startswith('/tmp/chessism-canonical-a3-'):
        raise ValueError('Explicit disposable evidence root required')
    return path


class Prepare(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_synthetic_snapshot(self):
        admin=await asyncpg.connect(database='chessism_canonical_test',**settings())
        try:await admin.execute('CREATE DATABASE '+SOURCE)
        finally:await admin.close()
        conn=await asyncpg.connect(database=SOURCE,**settings())
        try:
            await canonical.initialize(conn);await seed(conn)
            expected=await backup.describe(conn)
            (evidence()/'dump-manifest.json').write_text(json.dumps(expected))
            (evidence()/'source-catalog.json').write_text(json.dumps(await canonical.catalog_rows(conn),indent=2,sort_keys=True))
            self.assertEqual(await canonical.validate(conn),canonical.VERSION)
        finally:await conn.close()


class Restored(unittest.IsolatedAsyncioTestCase):
    async def test_actual_dump_restores_catalog_rows_and_sequences(self):
        conn=await asyncpg.connect(database=TARGET,**settings(),server_settings={'default_transaction_read_only':'on'})
        try:
            expected=json.loads((evidence()/'dump-manifest.json').read_text())
            import difflib
            source=(evidence()/'source-catalog.json').read_text()
            actual=json.dumps(await canonical.catalog_rows(conn),indent=2,sort_keys=True)
            (evidence()/'restored-catalog.json').write_text(actual)
            (evidence()/'catalog-diff.txt').write_text(''.join(difflib.unified_diff(source.splitlines(True),actual.splitlines(True))))
            result=await backup.validate_restored(conn,expected)
            self.assertEqual(result['schema_version'],canonical.VERSION)
            self.assertEqual(await conn.fetchval('SELECT count(*) FROM game'),5)
            self.assertEqual(await conn.fetchval('SELECT count(*) FROM account'),2)
            self.assertEqual(await conn.fetchval('SELECT count(*) FROM fen'),5)
            self.assertEqual(await canonical.validate(conn),canonical.VERSION)
        finally:await conn.close()
