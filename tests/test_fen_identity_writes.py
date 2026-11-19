import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.postgresql.fen_identity import schema_version, require_numeric_fen_schema


class IdentityTests(unittest.IsolatedAsyncioTestCase):


    async def test_startup_refuses_legacy_and_partial_schema_without_writing(self):
        session = AsyncMock()
        for columns in ([{'table_name': 'fen_continuation', 'column_name': 'fen_fen'}],
                        [{'table_name': 'fen', 'column_name': 'fen_id', 'not_null': True, 'identity_kind': 'd', 'data_type': 'bigint'}]):
            session.execute.return_value = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: columns))
            with self.assertRaisesRegex(RuntimeError, 'explicit migration'):
                await require_numeric_fen_schema(session)
            self.assertTrue(str(session.execute.await_args.args[0]).lstrip().startswith('SELECT'))
        self.assertEqual(schema_version([]), 'empty')


if __name__ == '__main__': unittest.main()
