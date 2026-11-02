import copy
import asyncio
import os
import time
import unittest
import numpy as np
from chessism_api.database.neo4j import ply_map_compute as compute
from chessism_api.database.neo4j.ply_map_reader import highlight
from chessism_api.operations.cuda_summaries import ply_map_process


def game(gid, positions, *, last=None, white=1, black=2):
    return dict(id=gid,source_hash=f'hash-{gid}',white_id=white,black_id=black,
        white_name=f'player{white}',black_name=f'player{black}',played_at='2026-11-01 00:00:00+00',
        last_ply=len(positions) if last is None else last,
        path=[dict(ply=i+1,fen_id=fen,games=count,san=f'move{i+1}') for i,(fen,count) in enumerate(positions)])


def fixture():
    return [game(1,[(10,3),(11,1),(20,2),(21,1)]),
            game(2,[(10,3),(12,1),(20,2),(22,1)],white=2,black=3),
            game(3,[(10,3),(30,1),(30,1)],white=1,black=3)]


class PlyMapTests(unittest.TestCase):
    def test_bulk_grouping_matches_independent_reference(self):
        data=np.asarray([[2,9],[0,2],[2,9],[1,4],[0,2],[2,8],[2,9]],dtype=np.int64)
        pairs,counts=compute.group_memberships(data,4,np)
        expected=np.unique(data,axis=0)
        np.testing.assert_array_equal(pairs,expected)
        np.testing.assert_array_equal(counts,[1,1,2,0])
        # No packed-key multiplication: retain ordinals near int64's limit.
        data=np.asarray([[1,2**63-1],[0,2**63-2],[1,2**63-1]],dtype=np.int64)
        np.testing.assert_array_equal(compute.group_memberships(data,3,np)[0],np.unique(data,axis=0))

    def test_progress_has_bounded_stages_and_exact_input_counts(self):
        events=[]
        compute.build(fixture(),np,events.append)
        self.assertEqual(events[0]['stage'],'compressing')
        self.assertEqual(events[-2]['stage'],'grouping')
        self.assertEqual(events[-1]['stage'],'layout')
        self.assertEqual(events[-2]['input_pairs'],events[-1]['input_pairs'])
        self.assertLessEqual(events[-1]['unique_pairs'],events[-1]['input_pairs'])

    def test_unique_runs_reconnect_and_bundles_keep_exact_counts(self):
        result=compute.build(fixture(),np)
        nodes={n['key']:n for n in result['nodes']}
        self.assertEqual(nodes['root']['games'],3)
        self.assertEqual(nodes['p:1:10']['games'],3)
        self.assertEqual(nodes['p:3:20']['games'],2)
        self.assertFalse(any(n.get('fen_key')=='chess:30' for n in nodes.values()))
        reconnect=next(e for e in result['connections'] if e['target']=='p:3:20')
        self.assertEqual((reconnect['kind'],reconnect['hidden'],reconnect['games']),('compressed',1,2))
        tail=nodes['t:p:3:20:ending']
        self.assertEqual((tail['kind'],tail['fen_key'],tail['games']),('ending',None,2))
        self.assertEqual(len(result['segments']),5)

    def test_same_fen_at_different_plies_is_not_one_visual_node(self):
        result=compute.build([game(1,[(10,2),(20,2),(10,2)])],np)
        self.assertTrue({'p:1:10','p:3:10'}.issubset({n['key'] for n in result['nodes']}))

    def test_display_boundary_is_not_an_ending(self):
        paths=[(i+1,1) for i in range(30)]
        result=compute.build([game(1,paths,last=80),game(2,paths,last=30)],np)
        self.assertEqual({n['kind'] for n in result['nodes']},{'root','ending','continuation'})
        self.assertEqual({e['reason'] for e in result['connections']},{'ending','continuation'})
        self.assertEqual({e['hidden'] for e in result['connections']},{30})

    def test_incomplete_paths_are_rejected(self):
        row=game(1,[(10,2)],last=20)
        with self.assertRaises(ValueError):compute.build([row],np)
        row=game(1,[(10,2)]);row['path'][0]['ply']=2
        with self.assertRaises(ValueError):compute.build([row],np)
        row=game(1,[(10,0)])
        with self.assertRaises(ValueError):compute.build([row],np)

    def test_zero_game_snapshot_and_empty_game(self):
        self.assertEqual(compute.build([],np)['nodes'][0]['games'],0)
        self.assertEqual(compute.build([game(1,[])],np)['nodes'][0]['games'],1)

    def test_layout_stable_and_selected_union_does_not_double_count(self):
        result=compute.build(fixture(),np)
        again=compute.build(list(reversed(fixture())),np)
        layout=lambda data:{n['key']:(n['x'],n['y']) for n in data['nodes']}
        self.assertEqual(layout(result),layout(again))
        selected=[dict(player_id=1,white_games=[0,2],black_games=[]),
                  dict(player_id=2,white_games=[1],black_games=[0])]
        rows=copy.deepcopy(result['nodes']);highlight(rows,selected)
        root=next(n for n in rows if n['key']=='root')
        self.assertEqual(root['players'],{'1':2,'2':2})
        self.assertEqual(root['selected_games'],3)
        self.assertEqual(layout(result),layout({'nodes':rows}))

    def test_cuda_agreement_when_explicitly_requested(self):
        if os.environ.get('PLY_MAP_TEST_CUDA')!='1':self.skipTest('CUDA test is opt-in')
        import cupy
        self.assertIn('4060',str(cupy.cuda.runtime.getDeviceProperties(0)['name']))
        self.assertEqual(compute.build(fixture(),np),compute.build(fixture(),cupy))


def slow_child(sender,games,budget):
    try:
        sender.send(('progress',dict(stage='test',pid=os.getpid())))
        time.sleep(60)
    finally:sender.close()


def error_child(sender,games,budget):
    sender.send(('error','synthetic child error'));sender.close()


def empty_child(sender,games,budget):
    sender.close()


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_kills_child_and_preserves_responsiveness(self):
        received=[]
        async def progress(event):received.append(event)
        beats=[]
        async def heartbeat():
            for _ in range(20):beats.append(time.monotonic());await asyncio.sleep(.05)
        ticker=asyncio.create_task(heartbeat())
        started=time.monotonic()
        with self.assertRaisesRegex(TimeoutError,'saved inputs retained'):
            await ply_map_process.execute([],1024,progress,timeout=1.5,_target=slow_child)
        await ticker
        self.assertGreaterEqual(len(beats),10)
        self.assertLess(time.monotonic()-started,7)
        self.assertTrue(received)
        with self.assertRaises(ProcessLookupError):os.kill(received[0]['pid'],0)

    async def test_cancellation_kills_child(self):
        event=asyncio.Event();received=[]
        async def progress(value):received.append(value);event.set()
        task=asyncio.create_task(ply_map_process.execute([],1024,progress,_target=slow_child))
        await asyncio.wait_for(event.wait(),5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        with self.assertRaises(ProcessLookupError):os.kill(received[0]['pid'],0)

    async def test_child_errors_and_early_exit_are_reported(self):
        async def progress(event):pass
        with self.assertRaisesRegex(RuntimeError,'synthetic child error'):
            await ply_map_process.execute([],1024,progress,_target=error_child)
        with self.assertRaisesRegex(RuntimeError,'exited without a result'):
            await ply_map_process.execute([],1024,progress,_target=empty_child)

    @unittest.skipUnless(os.environ.get('PLY_MAP_TEST_CUDA')=='1','CUDA test is opt-in')
    async def test_full_size_cuda_build_matches_reference(self):
        import cupy
        n=10219
        games=[game(i,[(ply,n) for ply in range(1,31)],last=60,white=1,black=2+i%20) for i in range(n)]
        expected=await asyncio.to_thread(compute.build,games,np)
        phases=[]
        async def progress(event):phases.append(event)
        started=time.monotonic()
        result,metrics=await ply_map_process.execute(games,2**30,progress,timeout=45)
        duration=time.monotonic()-started
        self.assertEqual(result,expected)
        self.assertEqual(metrics['input_pairs'],n*63)
        self.assertEqual(metrics['hardware'],'NVIDIA GeForce RTX 4060')
        self.assertEqual(metrics['implementation'],compute.IMPLEMENTATION)
        self.assertLess(duration,45)
        self.assertTrue(any(p['stage']=='grouping' for p in phases))
        print(f'FULL-SIZE CUDA PASS: {n} games, {metrics["input_pairs"]} memberships; grouping {metrics["grouping_seconds"]:.3f}s, whole child {duration:.3f}s',flush=True)
        # Different group sizes and duplicates, independent NumPy unique oracle.
        rng=np.random.default_rng(42)
        raw=np.column_stack((rng.integers(0,1000,250000),rng.integers(0,10219,250000))).astype(np.int64)
        pairs,counts=compute.group_memberships(raw,1000,cupy)
        reference=np.unique(raw,axis=0)
        np.testing.assert_array_equal(pairs,reference)
        np.testing.assert_array_equal(counts,np.bincount(reference[:,0],minlength=1000))
