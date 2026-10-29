"""Synthetic numeric parity on the actual GPU; no database/network access."""
import numpy as np
from chessism_api.operations.cuda_summaries import score_compute, shared_source
from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
from chessism_api.operations.research_algorithms.summary_validation import reference, group_counts, group_mates


def main():
    cp, hardware = cuda_backend(0)
    assert hardware['name'] == 'NVIDIA GeForce RTX 4060', hardware
    cp.get_default_memory_pool().set_limit(size=1024**3)
    values = [-10001,-10000,-9998,-9000,-551,-550,-549,-450,-350,-250,-150,-50,0,
        50,150,250,350,450,549,550,551,9000,9996,9999,10000,10001,9998.5]
    games, owners, numerical = [], [], []
    reference_rows = []; reference_ids = []
    for i in range(30):
        mode = score_compute.MODES[i % 3]
        g = {'game_link': i, 'mode': mode, 'analyzed': i % 2 == 0, 'total_positions': len(values)*4,
             'white_result': (0., .5, 1.)[i % 3], 'black_result': (1., .5, 0.)[i % 3],
             'white_str_result': 'stalemate' if i % 3 == 1 else 'checkmate',
             'black_str_result': 'stalemate' if i % 3 == 1 else 'checkmate',
             'last_white': None if i % 3 == 0 else i % 3 == 1}
        games.append(g)
        for player, white in (('alice', True), ('bob', False)):
            owners.append({'game_link': i, 'player_name': player, 'day': '2025-01-02', 'is_white': white})
        for src in (0,1,2,3):
            for value in values:
                numerical.append((i,len(numerical)+1,value if src != 3 else np.nan,src))
                if src == 3: continue
                for owner, sign in (('alice',1),('bob',-1)):
                    kind = 'tablebase' if src == 2 else 'mate' if abs(value) >= 9000 else 'cp'
                    moves = 10000-abs(value)
                    moves = int(moves) if kind == 'mate' and src == 1 and 0 <= moves <= 1000 and int(moves) == moves else None
                    reference_rows.append({'player': owner,'mode': mode,'player_score': sign*value,'score_kind': kind,'mate_moves': moves})
                    reference_ids.append((0 if owner == 'alice' else 3) + i % 3)
    raw = np.array(numerical, dtype=shared_source.RAW)
    expected = score_compute.calculate(np, raw, games, owners, require_cuda=False)
    actual = score_compute.calculate(cp, raw, games, owners)
    assert actual == expected
    decisions = reference(reference_rows); ids = np.array(reference_ids)
    oracle = group_counts(np, decisions, ids, 6)
    mate_codes, mate_counts = group_mates(np, decisions, ids)
    for group in range(6):
        row = actual[('alice' if group < 3 else 'bob','2025-01-02')][score_compute.MODES[group % 3]]
        assert row['counts'] + [row['mate_positions'],row['tablebase_positions']] == oracle[group].tolist()
        assert row['unscored_positions'] == len(values)*10
        reported = {group*2004+(1002 if m['side']=='for' else 0)+(1001 if m['moves'] is None else m['moves']):m['positions'] for m in row['mates']}
        assert reported == {int(c):int(n) for c,n in zip(mate_codes,mate_counts) if c//2004 == group}
    # Cross-batch and cross-day reduction is integral, exact and GPU-only.
    parts = [(key,value) for _ in range(3) for key,value in actual.items()]
    assert score_compute.reduce(cp, parts) == score_compute.reduce(np, parts, require_cuda=False)
    for function,args in ((score_compute.calculate,(raw,games,owners)),(score_compute.reduce,(parts,))):
        try: function(np,*args)
        except RuntimeError: pass
        else: raise AssertionError('Production accepted CPU fallback')
    cp.cuda.get_current_stream().synchronize()
    from tests.test_cuda_blunder_summary import cuda_check
    cuda_check()
    print('PASS real CUDA:',hardware,'rows:',len(raw),'occurrences:',len(reference_rows),
          'exact independent bin/mate oracle, coverage, outcomes, reductions; CPU fallback rejected')


if __name__ == '__main__': main()
