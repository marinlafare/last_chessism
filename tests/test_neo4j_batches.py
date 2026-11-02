import unittest
from chessism_api.database.neo4j.sync_budget import BatchPolicy, MIB, memory_bytes, select_prefix
from chessism_api.database.neo4j.sync_graph import delta_rows, position_payload, player_payload, POSITION_FIELDS


def event(id, ids, offset=0):
    return {'id':id, 'ids':ids, 'offset_items':offset}


class BatchTests(unittest.TestCase):
    def test_neo4j_memory_sizes(self):
        self.assertEqual(memory_bytes('1.00GiB'),1024*MIB)
        self.assertEqual(memory_bytes('716.80MiB'),int(716.8*MIB))
        self.assertEqual(memory_bytes('0B'),0)
        with self.assertRaises(ValueError):memory_bytes('unlimited')

    def test_both_memory_limits_and_low_memory(self):
        policy=BatchPolicy(256*MIB)
        self.assertEqual(policy.capacity(1024*MIB),16384)
        self.assertLess(policy.capacity(256*MIB),16384)
        with self.assertRaises(MemoryError):policy.capacity(64*MIB)

    def test_many_games_fit_not_fixed_twenty_five(self):
        ids=list(range(1000))
        selected,updates,cost=select_prefix([event(1,ids)],dict.fromkeys(ids,40),23000,23000)
        self.assertEqual(len(selected),273)
        self.assertEqual(updates,[{'id':1,'offset':273,'done':False}])
        self.assertLessEqual(cost,23000)

    def test_cross_event_prefix_and_duplicate_accounting(self):
        selected,updates,cost=select_prefix([event(1,[1,2]),event(2,[2,3,4])],dict.fromkeys(range(1,5),10),72,100)
        self.assertEqual(selected,[1,2,3])
        self.assertEqual(cost,72)
        self.assertEqual(updates,[{'id':1,'offset':2,'done':True},{'id':2,'offset':2,'done':False}])

    def test_partial_replay_and_deleted_game(self):
        selected,updates,cost=select_prefix([event(1,[1,2,3],1)],{2:10},28,100)
        self.assertEqual(selected,[2,3])
        self.assertEqual(cost,28)
        self.assertTrue(updates[0]['done'])

    def test_one_long_game_obeys_memory_not_time_target(self):
        self.assertEqual(select_prefix([event(1,[1])],{1:100},50,300)[0],[1])
        with self.assertRaises(MemoryError):select_prefix([event(1,[1])],{1:100},50,100)

    def test_adapts_to_slow_or_failed_batches_and_retry_history(self):
        policy=BatchPolicy(256*MIB)
        policy.observe(10000,30)
        self.assertEqual(policy.adaptive_plies,5000)
        policy.observe(5000,2)
        self.assertEqual(policy.adaptive_plies,6250)
        policy.shrink(5000)
        self.assertEqual(policy.adaptive_plies,2500)
        self.assertLess(policy.limit(1024*MIB,attempts=4),2500)

    def test_unchanged_counter_deltas_are_not_rewritten(self):
        game={'white_id':1,'black_id':2,'steps':[{'key':'p','from_key':'root','move_key':'m','uci':'e2e4','san':'e4'}]}
        self.assertEqual(delta_rows([game],[game]),([],[],[]))

    def test_position_wire_rows_merge_counts_and_keep_old_only_nodes_separate(self):
        position = dict(zip(POSITION_FIELDS, ['chess:1', 1, 'test-fen', 'chess',
                                            'cp', 35, None, None, None, None]))
        rows, old_only = position_payload([position], [
            {'key':'chess:1','occurrences':3,'games':2},
            {'key':'chess:2','occurrences':-2,'games':-1},
        ])
        self.assertEqual(rows, [list(position.values()) + [3, 2]])
        self.assertEqual(old_only, [['chess:2', -2, -1]])
        self.assertEqual(position['score_cp'], 35)

    def test_unchanged_paths_still_carry_updated_evaluations_without_counter_deltas(self):
        position = {'key':'chess:1','fen_id':1,'score_kind':'mate','mate_in':2,
                    'mate_for_white':False}
        rows, old_only = position_payload([position], [])
        self.assertEqual(rows[0][4:8], ['mate', None, 2, False])
        self.assertEqual(rows[0][-2:], [0, 0])
        self.assertEqual(old_only, [])

    def test_player_wire_rows_share_lookup_but_preserve_colors_and_negative_deltas(self):
        rows = player_payload([
            {'player':1,'key':'p','color':'white','occurrences':2,'games':1},
            {'player':1,'key':'p','color':'black','occurrences':-1,'games':-1},
            {'player':2,'key':'q','color':'black','occurrences':1,'games':1},
        ])
        self.assertEqual(rows, [[1,[['p','white',2,1],['p','black',-1,-1]]],
                                [2,[['q','black',1,1]]]])


if __name__=='__main__':unittest.main()
