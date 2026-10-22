"""Offline contracts for bounded game-FEN reservation queues."""
import os
import unittest
from unittest.mock import patch

os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test:test@localhost/test')
from chessism_api.database.relational_storage import RelationalStorage
from chessism_api.operations.cloud_analysis import game_fen_queue as queue


class GameFenQueueTests(unittest.IsolatedAsyncioTestCase):
    def test_identity_is_stable_order_sensitive_and_keeps_first_game_priorities(self):
        self.assertEqual(queue.selection_identity([1,2,1]),queue.selection_identity([1,'2',1,None]))
        self.assertNotEqual(queue.selection_identity([1,2])[1],queue.selection_identity([2,1])[1])
        self.assertEqual(len(queue.selection_identity([])[1]),64)

    def test_build_preserves_legacy_minima_and_text_tiebreak(self):
        compact=RelationalStorage('core','archive','reads','relational_v3')
        sql=queue.candidate_sql(compact)
        self.assertIn('MIN(selected.priority)',sql)
        self.assertIn('CASE WHEN gfa.is_white',sql)
        self.assertIn('ORDER BY c.game_priority, c.move_priority, f.fen',sql)
        self.assertIn('"core"."game_fen_association"',sql)
        self.assertIn("gfa.move_color = 'white'",queue.candidate_sql(None))
        self.assertNotIn('cloud_fen_claim',sql)  # Claims are checked live, not frozen.

    def test_page_is_bounded_before_fen_lookup_and_retains_skip_locked_rows(self):
        sql=queue.page_sql(None)
        self.assertIn('page AS MATERIALIZED',sql)
        self.assertLess(sql.index('LIMIT :limit'),sql.index('JOIN LATERAL'))
        self.assertIn('ordinal > :after',sql)
        self.assertIn('LEFT JOIN LATERAL',sql)
        self.assertIn('FOR UPDATE OF f SKIP LOCKED',sql)
        self.assertNotIn('game_fen_association',sql)

    async def test_invalid_batch_sizes_never_open_a_database_session(self):
        with patch.object(queue,'AsyncDBSession') as factory:
            for size in [0,-1,50001,True,1.5,None,'10']:
                with self.assertRaises(ValueError):
                    await queue.get_game_queue_fens('unused',[1],size)
            factory.assert_not_called()


if __name__=='__main__': unittest.main()
