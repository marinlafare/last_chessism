"""Offline durable-input, shared-upload and cancellation contracts."""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4
import numpy as np
from chessism_api.operations.cuda_summaries import input_snapshot as snap, snapshot_flow, device_inputs
from chessism_api.operations.cuda_summaries import shared_source, blunder_compute, accuracy_compute, score_compute, batch_budget
from chessism_api.operations.canonical_blunders import compute, registry
from tests.test_canonical_blunders import report


def fixture(start=1):
    games = [dict(game_link=start+i, total_positions=24, analyzed=True, mode='bullet',
        white_owner='alice', black_owner='bob', white_elo=800, black_elo=900,
        played_at=datetime(2025,1,1,tzinfo=timezone.utc), white_result=1, black_result=0,
        white_str_result='win', black_str_result='resigned', last_white=False) for i in range(3)]
    games[1]['analyzed'] = False  # A real subset, not just an all-selected fast path.
    raw = np.zeros(72, shared_source.RAW)
    raw['game'] = np.repeat(np.arange(3),24); raw['ply'] = np.tile(np.arange(1,25),3)
    raw['score'] = np.tile([105,-9998,9996,0,100,-100],12); raw['source'] = 1
    raw['source'][3] = 2; raw['source'][4] = 0
    items = [dict(game_link=g['game_link'],revision=1,blunders=True) for g in games]
    owners = [dict(game_link=g['game_link'],player_name='alice',day='2025-01-01',is_white=True) for g in games]
    eligible, unsupported, transitions = shared_source.blunder_inputs(raw,games,items)
    _, config, _ = registry.freeze(report(),registry.ACK)
    return config,items,games,owners,raw,eligible,unsupported,transitions


class SnapshotTests(TestCase):
    def test_no_move_side_never_selected_for_blunder_inference(self):
        _,_,games,_,raw,_,_,_=fixture()
        game={**games[0],'total_positions':1,'white_owner':None,'black_owner':'bob'}
        items=[dict(game_link=game['game_link'],revision=1,blunders=True)]
        eligible,unsupported,rows=shared_source.blunder_inputs(raw[:1],[game],items)
        self.assertEqual(eligible,[]);self.assertEqual(len(rows),0)
        self.assertIsNone(unsupported[0]['black_owner'])
        self.assertEqual(game['black_owner'],'bob')
        # If an eligible White player did move, retain only White's valid side.
        game['white_owner']='alice'
        eligible,unsupported,rows=shared_source.blunder_inputs(raw[:1],[game],items)
        self.assertEqual(len(rows),1);self.assertEqual(eligible[0]['white_owner'],'alice')
        self.assertIsNone(eligible[0]['black_owner']);self.assertEqual(unsupported,[])
    def test_roundtrip_merge_and_corruption(self):
        run = str(uuid4())
        with TemporaryDirectory() as directory, patch.object(snap,'ROOT',Path(directory)), patch.object(snap,'FREE_FLOOR',0):
            a,b = fixture(),fixture(4)
            one,two = snap.save(run,a),snap.save(run,b)
            loaded = snap.load(run,[one,two])
            self.assertEqual(loaded[2],a[2]+b[2])
            self.assertEqual(loaded[5],a[5]+b[5])
            np.testing.assert_array_equal(loaded[4]['game'],np.repeat(np.arange(6),24))
            np.testing.assert_array_equal(loaded[7]['game'],np.repeat(np.arange(4),24))
            self.assertEqual(loaded[7][:48].tobytes(),a[7].tobytes())
            with self.assertRaisesRegex(ValueError,'artifact path'): snap.read(run,{**one,'name':'../escape.npz'})
            with (snap.folder(run)/one['name']).open('ab') as stream: stream.write(b'corrupt')
            with self.assertRaisesRegex(ValueError,'corrupt'): snap.read(run,one)

    def test_choose_whole_chunks(self):
        chunks = [dict(positions=72,games=3)]*3
        budget = batch_budget.BatchBudget(144*batch_budget.DEVICE_ROW+6*batch_budget.DEVICE_GAME,
            144*batch_budget.HOST_ROW+6*batch_budget.HOST_GAME)
        self.assertEqual(len(snap.choose(chunks,budget)),2)
        with self.assertRaisesRegex(RuntimeError,'no CPU fallback'):
            snap.choose(chunks,batch_budget.BatchBudget(1,1))

    def test_shared_device_arrays_preserve_all_outputs(self):
        config,_,games,owners,raw,eligible,_,transitions = fixture()
        device = device_inputs.upload(np,raw,games,require_cuda=False)
        expected,_ = compute.infer(np,transitions,eligible,config,require_cuda=False)
        actual,_ = blunder_compute.infer(np,transitions,eligible,config,require_cuda=False,device=device)
        self.assertEqual(actual,expected)
        self.assertEqual(score_compute.calculate(np,raw,games,owners,require_cuda=False,device=device),
                         score_compute.calculate(np,raw,games,owners,require_cuda=False))
        self.assertEqual(accuracy_compute.calculate(np,raw,games,require_cuda=False,device=device),
                         accuracy_compute.calculate(np,raw,games,require_cuda=False))


class CancellationTests(IsolatedAsyncioTestCase):
    async def test_cancel_terminates_only_owned_reader_and_closes_session(self):
        session = Mock(close=AsyncMock()); driver = Mock(); started = asyncio.Event()
        async def blocked(*args):
            started.set(); await asyncio.Event().wait()
        with patch.object(snapshot_flow.source,'prepare',new=AsyncMock()), \
                patch.object(snapshot_flow.score_runs,'pending_items',new=AsyncMock(return_value=[{'game_link':1}])), \
                patch.object(snapshot_flow,'driver_connection',new=AsyncMock(return_value=driver)), \
                patch.object(snapshot_flow.shared_source,'load',side_effect=blocked):
            task = asyncio.create_task(snapshot_flow.read_chunk(lambda:session,{},None))
            await started.wait(); task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        driver.terminate.assert_called_once(); session.close.assert_awaited_once()
