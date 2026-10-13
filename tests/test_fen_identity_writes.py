import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.fen_identity import schema_version, require_numeric_fen_schema
from chessism_api.operations.fen_results import resolve_fen_ids, replace_fen_continuations


class IdentityTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolver_is_bounded_not_one_query_per_fen(self):
        session = AsyncMock()
        session.execute.side_effect = [SimpleNamespace(all=lambda i=i: [(f'fen-{j}', j+1) for j in range(i, min(i+1000, 2005))])
                                       for i in (0, 1000, 2000)]
        result = await resolve_fen_ids(session, [f'fen-{i}' for i in range(2005)] + ['fen-0'])
        self.assertEqual(len(result), 2005)
        self.assertEqual(session.execute.await_count, 3)

    async def test_reuse_cloud_mapping_and_reject_missing_or_unrelated_keys(self):
        session = AsyncMock()
        rows = [{'fen_fen': 'one', 'rank': 2, 'move': 'a1a2', 'score': 10}]
        await replace_fen_continuations(session, ['one'], rows, fen_ids={'one': 17})
        self.assertEqual(session.execute.await_count, 2)  # DELETE + INSERT, no lookup.
        self.assertEqual(session.execute.await_args_list[1].args[1], [{'fen_id': 17, 'rank': 2, 'move': 'a1a2', 'score': 10}])
        for keys, mapping in [(['one'], {}), (['different'], {'different': 19})]:
            session.reset_mock()
            with self.assertRaises(ValueError):
                await replace_fen_continuations(session, keys, rows, fen_ids=mapping)
            session.execute.assert_not_awaited()

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
