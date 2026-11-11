import os
import json
import unittest
from unittest.mock import AsyncMock,MagicMock,patch
os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations import fen_explorer as explorer
from test_common_positions import E4,result


class FenExplorerTests(unittest.IsolatedAsyncioTestCase):
    def test_scopes_and_fen_validation(self):
        for scope in ('unique','repeated','scored','unscored',*(str(n) for n in range(2,11))):
            self.assertIsNone(explorer.validate_scope(scope))
        for value in ('0','1','11','1 OR TRUE'):
            with self.assertRaises(ValueError):explorer.validate_scope(value)
        self.assertEqual(explorer.validate_fen(E4),E4)
        for fen in ('bad fen',E4+' 0 1','not/a/board w KQ -'):
            with self.assertRaises(ValueError):explorer.validate_fen(fen)

    async def test_samples_are_bounded_and_never_order_entire_table_randomly(self):
        session=AsyncMock();session.in_transaction=MagicMock(return_value=True);session.execute.side_effect=[result()]*3+[result([{'fen':E4,'appearances':1}])]*3
        with patch.object(explorer,'AsyncDBSession',return_value=session):
            rows=await explorer.sample_positions('unique')
        self.assertEqual(len(rows),1)
        session.commit.assert_awaited_once();session.close.assert_awaited_once()
        self.assertEqual(session.execute.await_count,6)
        for call in session.execute.call_args_list[3:]:
            self.assertIn('TABLESAMPLE SYSTEM',str(call.args[0]));self.assertIn('LIMIT 120',str(call.args[0]))
            self.assertNotIn('ORDER BY random',str(call.args[0]));self.assertLessEqual(call.args[1]['percent'],.2)

    async def test_sample_identity_is_per_account_and_pages_are_cached(self):
        redis=AsyncMock();redis.get.return_value=None
        sample=[{'fen':f'fen{i}','appearances':1} for i in range(20)]
        with patch.object(explorer,'sample_positions',AsyncMock(return_value=sample)),patch.object(explorer,'describe_examples',AsyncMock(side_effect=lambda rows:rows)):
            data=await explorer.explore_positions(redis,'admin-a','unique')
        self.assertEqual(len(data['rows']),10);self.assertTrue(data['has_next'])
        self.assertIn('fen-explorer:admin-a:',redis.set.call_args_list[0].args[0])
        token=data['token'];redis.get.side_effect=[json.dumps({'scope':'unique','rows':sample}),json.dumps(sample[10:])]
        with patch.object(explorer,'describe_examples',AsyncMock()) as describe:
            data=await explorer.explore_positions(redis,'admin-a','unique',token,1)
            describe.assert_not_awaited();self.assertFalse(data['has_next'])
        redis.get.side_effect=None;redis.get.return_value=None
        with self.assertRaises(LookupError):await explorer.explore_positions(redis,'admin-b','unique',token,1)


if __name__=='__main__':unittest.main()
