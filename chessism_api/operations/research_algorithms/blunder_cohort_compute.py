"""Bounded full-history CUDA .15 summaries, with independent references."""
from collections import defaultdict
import time

import numpy as np

from . import blunder_fixed as old, blunder_v2_model as model
from .blunder_shadow_compute import arrays, context, counts, host, sync
from .blunder_threshold_compare import scalar15, band

FIELDS = ('old15', 'candidate20', 'candidate15')
CHANGES = ('restored_old', 'new_from_lowering', 'still_removed', 'added_vs_old')


def calculate(xp, rows, games, fits, check=lambda: None):
    values, size = context(games, fits), len(games)
    totals = {k: np.zeros(size, dtype=np.int64) for k in (*FIELDS, 'moves', 'unassessed', *CHANGES)}
    checks = dict.fromkeys(('old_cpu_move', 'old_sql_move', 'new_cpu_move', 'new_vector_move', 'new_per_game',
        'saved_game', 'saved_moves', 'threshold_monotonicity', 'mate_rules', 'assessment'), 0)
    timings = dict.fromkeys(('cpu_scalar_reference_seconds', 'cpu_vector_candidate_seconds',
        'gpu_candidate_with_transfers_seconds', 'gpu_reference_and_comparison_seconds'), 0.)
    cpu_counts = np.zeros(size, dtype=np.int64)
    for start_row in range(0, len(rows), 262144):
        batch = rows[start_row:start_row+262144]
        check(); ids = batch['game']
        if batch.dtype != old.DTYPE or not len(batch) or ids.min() < 0 or ids.max() >= size:
            raise ValueError('Invalid cohort transition batch.')
        start = time.perf_counter()
        args = [batch[k] for k in ('before', 'after', 'before_kind', 'after_kind')] + [values[k][ids] for k in ('u', 'd', 'a', 'b')]
        vector = model.decisions(np, *args, threshold=.15)
        cpu_counts += np.bincount(ids[vector == 1], minlength=size)
        timings['cpu_vector_candidate_seconds'] += time.perf_counter()-start
        start = time.perf_counter()
        cpu_old = old.reference(batch)
        cpu_new = np.array([scalar15(*items) for items in zip(*args)], dtype=np.int8)
        timings['cpu_scalar_reference_seconds'] += time.perf_counter()-start
        sync(xp); start = time.perf_counter()
        gpu = arrays(xp, batch, values); gpu_ids = xp.asarray(ids)
        new_flags = model.decisions(xp, *gpu, threshold=.15)
        totals['candidate15'] += host(xp, counts(xp, gpu_ids, new_flags == 1, size))
        candidate = host(xp, new_flags); sync(xp)
        timings['gpu_candidate_with_transfers_seconds'] += time.perf_counter()-start
        start = time.perf_counter()
        old_flags, high_flags = old.kernel(xp, *gpu[:4]), model.decisions(xp, *gpu, threshold=.20)
        for key, flags in (('old15', old_flags), ('candidate20', high_flags)):
            totals[key] += host(xp, counts(xp, gpu_ids, flags == 1, size))
        legacy, high = host(xp, old_flags), host(xp, high_flags)
        cp = (batch['before_kind'] == 0) & (batch['after_kind'] == 0)
        totals['moves'] += np.bincount(ids, minlength=size)
        totals['unassessed'] += np.bincount(ids[candidate == -1], minlength=size)
        masks = ((legacy == 1) & (high == 0) & (candidate == 1), (legacy == 0) & (high == 0) & (candidate == 1),
            (legacy == 1) & (candidate == 0), (legacy == 0) & (candidate == 1))
        for key, mask in zip(CHANGES, masks):
            totals[key] += np.bincount(ids[mask], minlength=size)
        for key, mask in (
            ('old_cpu_move', legacy != cpu_old), ('old_sql_move', (legacy == 1) != (batch['sql'] == 1)),
            ('new_cpu_move', candidate != cpu_new), ('new_vector_move', candidate != vector),
            ('threshold_monotonicity', (high == 1) & (candidate != 1)),
            ('mate_rules', ~cp & (candidate != high)), ('assessment', (candidate == -1) != (legacy == -1))):
            checks[key] += int(np.count_nonzero(mask))
        timings['gpu_reference_and_comparison_seconds'] += time.perf_counter()-start
    checks['new_per_game'] = int(np.count_nonzero(totals['candidate15'] != cpu_counts))
    checks['saved_game'] = int(np.count_nonzero(totals['old15'] != [g['saved_blunders'] for g in games]))
    checks['saved_moves'] = int(np.count_nonzero(totals['moves'] != [g['saved_moves'] for g in games]))
    records = [{'game': g['link'], 'player': g['player'], 'is_white': g['is_white'], 'mode': g['mode'],
        'played_at': g['played_at'], 'player_rating': g['player_rating'], 'opponent_rating': g['opponent_rating'],
        'input_sha256': g['input_sha256'], **{k: int(v[i]) for k, v in totals.items()}} for i, g in enumerate(games)]
    return {'status': 'passed' if not any(checks.values()) else 'failed', 'checks': checks, 'timings': timings, 'records': records}


def empty_group():
    return {'games': 0, 'assessed_games': 0, 'no_assessed_player_move_games': 0, 'zero_player_move_games': 0,
        'moves': 0, 'unassessed': 0, **dict.fromkeys((*FIELDS, *CHANGES), 0),
        'histograms': {k: [0]*7 for k in FIELDS}, 'saved_scope_histogram': [0]*7}


class Groups:
    def __init__(self):
        self.modes = {mode: empty_group() for mode in model.MODES}
        self.ratings = defaultdict(empty_group)

    def add(self, records):
        for r in records:
            for target in (self.modes[r['mode']], self.ratings[band(r['player_rating']), r['mode']]):
                assessed = r['moves'] > r['unassessed']
                target['games'] += 1; target['assessed_games'] += assessed
                target['no_assessed_player_move_games'] += not assessed
                target['zero_player_move_games'] += r['moves'] == 0
                for k in ('moves', 'unassessed', *FIELDS, *CHANGES):
                    target[k] += r[k]
                if r['moves'] > 0:
                    target['saved_scope_histogram'][min(6, r['old15'])] += 1
                if assessed:
                    for k in FIELDS:
                        target['histograms'][k][min(6, r[k])] += 1

    def report(self):
        return {'by_mode': self.modes,
            'rating_groups': [{'rating_band': b, 'mode': m, **v} for (b, m), v in sorted(self.ratings.items())]}


def add_groups(target, addition):
    for key, value in addition.items():
        if isinstance(value, dict):
            add_groups(target[key], value)
        elif isinstance(value, list):
            target[key] = [a+b for a, b in zip(target[key], value)]
        else:
            target[key] += value
