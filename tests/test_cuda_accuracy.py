"""Independent scalar oracle for the unchanged formula and six-band reduction."""
from collections import Counter
from datetime import datetime, timezone
import math
import statistics
import unittest
import numpy as np
from chessism_api.operations.cuda_summaries import accuracy_compute as algorithm, shared_source


def fixture():
    rng = np.random.default_rng(42); games = []; raw = []
    for index, count in enumerate((1, 2, 19, 20, 21, 39, 40, 79, 80, 81, 113, 147, 200)):
        game = {'game_link': index+1, 'total_positions': count, 'analyzed': True, 'mode': ('bullet','blitz','rapid')[index % 3],
            'white_owner': 'alice', 'black_owner': 'bob', 'played_at': datetime(2025, 1, 1, tzinfo=timezone.utc),
            'white_result': 1, 'black_result': 0, 'white_str_result': 'win', 'black_str_result': 'resigned'}
        games.append(game)
        scores = rng.integers(-1200, 1200, count).astype(float)
        if index == 0: scores[:] = 15
        if index == 1: scores[:] = (10000, -10000)
        for ply, score in enumerate(scores, 1):
            src = 2 if ply % 7 == 0 else 0 if ply % 11 == 0 else 1
            if ply % 13 == 0: score = 9998 if ply % 2 else -9996
            raw.append((index, ply, score, src))
    return np.array(raw, dtype=shared_source.RAW), games


def oracle(raw, game, index, white):
    selected = raw[raw['game'] == index]; sign = 1 if white else -1
    def wp(score):
        return 50+50*(2/(1+math.exp(-.00368208*max(-1000,min(1000,score))))-1)
    series = [wp(15*sign), *[wp(float(r['score'])*sign) for r in selected]]
    window = min(8, max(2, len(selected)//10)); accuracies = []; weights = []; losses = []; gains = []; cp_losses = []
    previous = 15.; before_kind = 0
    for i, r in enumerate(selected):
        score = float(r['score']); kind = 2 if r['source'] == 2 else 1 if abs(score) >= 9000 else 0
        if (int(r['ply']) % 2 == 1) == white:
            loss = max(0, series[i]-series[i+1]); losses.append(loss)
            accuracy = 100. if loss == 0 else max(0,min(100,103.1668100711649*math.exp(-.04354415386753951*loss)-3.166924740191411+1))
            accuracies.append(accuracy)
            left = max(0, i+2-window); segment = series[left:left+window]
            weights.append(max(.5,min(12,statistics.pstdev(segment))))
            if kind == before_kind == 0:
                delta = (score-previous)*sign; gains.append(max(0,delta)); cp_losses.append(max(0,-delta))
        previous, before_kind = score, kind
    n = len(accuracies)
    weighted = sum(a*w for a,w in zip(accuracies,weights))/sum(weights) if n else None
    harmonic = 0 if 0 in accuracies else n/sum(1/a for a in accuracies) if n else None
    return {'analyzed_player_moves': n, 'own_move_cp_gain': sum(gains), 'own_move_cp_loss': sum(cp_losses),
        'game_efficiency': (weighted+harmonic)/2 if n else None,
        'mean_win_percent_loss': statistics.mean(losses) if n else None,
        'median_win_percent_loss': statistics.median(losses) if n else None,
        'mate_for_positions': sum(r['source'] != 2 and abs(r['score']) >= 9000 and r['score']*sign > 0 for r in selected),
        'mate_against_positions': sum(r['source'] != 2 and abs(r['score']) >= 9000 and r['score']*sign < 0 for r in selected),
        'final_player_cp': previous*sign if before_kind == 0 else None}


def verify(xp, *, require_cuda):
    raw, games = fixture()
    records = algorithm.calculate(xp, raw, games, require_cuda=require_cuda)
    error = 0.
    for record in records:
        expected = oracle(raw, games[record['game_link']-1], record['game_link']-1, record['is_white'])
        for key, value in expected.items():
            actual = record['payload'][key]
            if value is None: assert actual is None, (key, actual)
            else:
                error = max(error, abs(actual-value))
                assert math.isclose(actual,value,abs_tol=1e-9,rel_tol=1e-11), (record['game_link'],key,actual,value)
    scores = [0.,50.,50.00001,60.,60.00001,70.,70.00001,80.,80.00001,90.,90.00001,100.]
    rows = [{'day': '2025-01-01', 'accuracy': value, 'mode_bit': bit, 'salience': .5} for value in scores for bit in (1,2,4)]
    rows.append({'day':'2025-01-02','accuracy':80.,'mode_bit':1,'salience':None})
    daily = algorithm.daily(xp, rows, require_cuda=require_cuda)
    for row in daily:
        selected = [r for r in rows if r['day'] == row['day'] and r['mode_bit'] & row['mode_mask']]
        counts = Counter(next((i for i,b in enumerate(algorithm.BANDS) if r['accuracy'] <= b), 5) for r in selected)
        expected = [counts[i] for i in range(6)]
        assert row['counts'] == expected and row['games'] == len(selected), row
        assert row['modal_bands'] == [i for i,c in enumerate(expected) if c == max(expected)], row
        assert row['daily_salience'] == (None if any(r['salience'] is None for r in selected) else .5*len(selected))
    return {'games':len(games),'positions':len(raw),'max_numeric_error':error,'daily_selections':len(daily)}


class AccuracyTests(unittest.TestCase):
    def test_independent_formula_and_daily_bands(self):
        verify(np,require_cuda=False)

    def test_no_cpu_fallback(self):
        with self.assertRaises(RuntimeError): algorithm.calculate(np,*fixture())

    def test_incomplete_sequence_rejected(self):
        raw,games = fixture()
        with self.assertRaises(ValueError): algorithm.calculate(np,raw[1:],games,require_cuda=False)


if __name__ == '__main__':
    import sys
    if '--cuda' in sys.argv:
        from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
        cp,device = cuda_backend(0)
        assert device['name'] == 'NVIDIA GeForce RTX 4060'
        cp.get_default_memory_pool().set_limit(size=1024**3)
        print('PASS real CUDA accuracy',device,verify(cp,require_cuda=True))
        if '--large' in sys.argv:
            import time
            cp.get_default_memory_pool().set_limit(size=3*1024**3)
            game_count,plies=16384,128
            raw=np.empty(game_count*plies,dtype=shared_source.RAW)
            raw['game']=np.repeat(np.arange(game_count),plies)
            raw['ply']=np.tile(np.arange(1,plies+1),game_count)
            raw['score']=np.tile(np.sin(np.arange(plies))*500,game_count);raw['source']=1
            template=fixture()[1][0]
            games=[{**template,'game_link':i+1,'total_positions':plies} for i in range(game_count)]
            started=time.monotonic();records=algorithm.calculate(cp,raw,games)
            assert len(records)==game_count*2
            for i,r in enumerate(records):
                assert r['payload']['analyzed_player_moves']==plies//2
                assert math.isclose(r['payload']['game_efficiency'],records[i%2]['payload']['game_efficiency'],abs_tol=1e-9)
            print('PASS bounded full-size CUDA accuracy:',len(raw),'positions;',len(games),'games;',round(time.monotonic()-started,3),'seconds')
    else: unittest.main()
