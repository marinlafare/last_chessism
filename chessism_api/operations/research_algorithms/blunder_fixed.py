"""Fixed Lichess-coefficient CUDA candidate. No training or production writes."""
import hashlib
import inspect
import json
import time

import numpy as np

from .cuda_runtime import cuda_backend

VERSION = 1
COEFFICIENT = .00368208
THRESHOLD = .15
MODES = ('bullet', 'blitz', 'rapid')
BATCH_ROWS = 32768
DTYPE = np.dtype([('game', '<i4'), ('link', '<i8'), ('ply', '<i4'),
                  ('before', '<f8'), ('after', '<f8'),
                  ('before_kind', 'i1'), ('after_kind', 'i1'), ('sql', 'i1')])
RULES = {'coefficient': COEFFICIENT, 'probability_drop': THRESHOLD,
         'cp_ceiling': 1000, 'initial_white_cp': 15,
         'perspective': 'mover', 'mate_rules': 'existing_700cp_v1',
         'tablebase': 'unassessed_not_a_blunder', 'rating_aware': False}


def kernel(xp, before, after, before_kind, after_kind):
    """-1 unassessed, 0 ordinary, 1 blunder; preserve the existing float64 rule."""
    valid = xp.isfinite(before) & xp.isfinite(after)
    valid &= (before_kind >= 0) & (before_kind < 2) & (after_kind >= 0) & (after_kind < 2)
    b = 2 / (1 + xp.exp(-COEFFICIENT * xp.clip(before, -1000, 1000))) - 1
    a = 2 / (1 + xp.exp(-COEFFICIENT * xp.clip(after, -1000, 1000))) - 1
    cp_blunder = (before_kind == 0) & (after_kind == 0) & (b - a >= 2 * THRESHOLD)
    mate_blunder = ((before_kind == 0) & (after_kind == 1) & (after < 0) & (before >= -700)) | (
        (before_kind == 1) & (before > 0) & (((after_kind == 1) & (after < 0)) | ((after_kind == 0) & (after <= 700))))
    return xp.where(valid, cp_blunder | mate_blunder, -1).astype(xp.int8)


def reference(batch):
    # Use the actual scalar production function, independently of CUDA arithmetic.
    from ..player_game_scores import (is_lichess_blunder, LICHESS_WINNING_CHANCE_MULTIPLIER,
                                     LICHESS_BLUNDER_THRESHOLD, LICHESS_CP_CEILING)
    if (abs(LICHESS_WINNING_CHANCE_MULTIPLIER), LICHESS_BLUNDER_THRESHOLD, LICHESS_CP_CEILING) != (COEFFICIENT, 2*THRESHOLD, 1000):
        raise ValueError('The production rule changed; create a new candidate version.')
    kinds = {0: 'cp', 1: 'mate'}
    return np.array([int(is_lichess_blunder(float(row['before']), float(row['after']),
                     kinds[int(row['before_kind'])], kinds[int(row['after_kind'])]))
        if int(row['before_kind']) in kinds and int(row['after_kind']) in kinds
        and np.isfinite(row['before']) and np.isfinite(row['after']) else -1 for row in batch], dtype=np.int8)


def read_batch(stream):
    raw = stream.read(BATCH_ROWS * DTYPE.itemsize)
    if len(raw) % DTYPE.itemsize:
        raise ValueError('Truncated numerical input.')
    return np.frombuffer(raw, dtype=DTYPE)


class Comparison:
    def __init__(self, games, *, device=0):
        self.games = games
        self.cp, self.hardware = cuda_backend(device)
        self.counts = {key: np.zeros(len(games), dtype=np.int64) for key in ('cpu', 'cuda', 'sql', 'moves')}
        self.rows = self.unassessed = self.cpu_mismatches = self.sql_mismatches = 0
        self.examples = []
        self.digest = hashlib.sha256(json.dumps(games, sort_keys=True, separators=(',', ':')).encode())
        self.timings = dict.fromkeys(('cpu_reference_seconds', 'host_to_device_seconds',
            'gpu_calculation_seconds', 'device_to_host_seconds'), 0.0)
        started = time.perf_counter()
        cp = self.cp
        warm = kernel(cp, cp.asarray([200.], dtype=cp.float64), cp.asarray([-200.], dtype=cp.float64),
                      cp.asarray([0], dtype=cp.int8), cp.asarray([0], dtype=cp.int8))
        cp.bincount(cp.asarray([0], dtype=cp.int32)[warm == 1], minlength=len(games))
        cp.cuda.get_current_stream().synchronize()
        self.timings['cuda_warmup_seconds'] = time.perf_counter() - started

    def consume(self, batch):
        if not 0 < len(batch) <= BATCH_ROWS or batch.dtype != DTYPE:
            raise ValueError('Invalid or oversized transition batch.')
        ids = batch['game']
        if ids.min() < 0 or ids.max() >= len(self.games):
            raise ValueError('Unknown game in numerical input.')
        self.digest.update(batch.tobytes())
        started = time.perf_counter()
        expected = reference(batch)
        self.counts['cpu'] += np.bincount(ids[expected == 1], minlength=len(self.games))
        self.timings['cpu_reference_seconds'] += time.perf_counter() - started
        cp = self.cp
        started = time.perf_counter()
        arrays = [cp.asarray(batch[key]) for key in ('before', 'after', 'before_kind', 'after_kind')]
        gpu_ids = cp.asarray(ids)
        cp.cuda.get_current_stream().synchronize()
        self.timings['host_to_device_seconds'] += time.perf_counter() - started
        started = time.perf_counter()
        actual = kernel(cp, *arrays)
        selected = gpu_ids[actual == 1]
        # CuPy/CUB cannot reduce an empty input in this runtime. A batch with
        # zero blunders is valid and must still produce all-zero game counts.
        counts = cp.bincount(selected, minlength=len(self.games)) if selected.size else cp.zeros(len(self.games), dtype=cp.int64)
        cp.cuda.get_current_stream().synchronize()
        self.timings['gpu_calculation_seconds'] += time.perf_counter() - started
        started = time.perf_counter()
        actual, counts = cp.asnumpy(actual), cp.asnumpy(counts)
        self.timings['device_to_host_seconds'] += time.perf_counter() - started
        self.counts['cuda'] += counts
        self.counts['sql'] += np.bincount(ids[batch['sql'] == 1], minlength=len(self.games))
        self.counts['moves'] += np.bincount(ids, minlength=len(self.games))
        mismatch = (actual != expected) | ((actual == 1) != (batch['sql'] == 1))
        self.cpu_mismatches += int(np.count_nonzero(actual != expected))
        self.sql_mismatches += int(np.count_nonzero((actual == 1) != (batch['sql'] == 1)))
        for index in np.flatnonzero(mismatch)[:max(0, 20-len(self.examples))]:
            row = batch[index]
            self.examples.append({'game': int(row['link']), 'ply': int(row['ply']),
                'cpu': int(expected[index]), 'cuda': int(actual[index]), 'sql': int(row['sql'])})
        self.rows += len(batch)
        self.unassessed += int(np.count_nonzero(actual == -1))

    def report(self):
        saved = np.array([g['saved_blunders'] for g in self.games], dtype=np.int64)
        moves = np.array([g['saved_moves'] for g in self.games], dtype=np.int64)
        checks = {'cpu_moves': self.cpu_mismatches, 'sql_moves': self.sql_mismatches,
            'cpu_games': int(np.count_nonzero(self.counts['cuda'] != self.counts['cpu'])),
            'sql_games': int(np.count_nonzero(self.counts['cuda'] != self.counts['sql'])),
            'saved_games': int(np.count_nonzero(self.counts['cuda'] != saved)),
            'saved_move_counts': int(np.count_nonzero(self.counts['moves'] != moves))}
        examples = [{'game': g['link'], 'saved': g['saved_blunders'], 'cuda': int(self.counts['cuda'][i]),
                     'saved_moves': g['saved_moves'], 'moves': int(self.counts['moves'][i])}
                    for i, g in enumerate(self.games) if self.counts['cuda'][i] != saved[i] or self.counts['moves'][i] != moves[i]][:20]
        groups = []
        for mode in MODES:
            mask = np.array([g['mode'] == mode for g in self.games], dtype=bool)
            groups.append({'mode': mode, 'games': int(mask.sum()),
                **{key: int(values[mask].sum()) for key, values in self.counts.items()},
                'saved': int(saved[mask].sum()),
                'histogram': np.bincount(np.minimum(6, self.counts['cuda'][mask]), minlength=7).tolist()})
        return {'format': 'chessism_fixed_blunder_validation_v1', 'algorithm_version': VERSION,
            'status': 'failed' if any(checks.values()) else 'passed', 'canonical': False,
            'production_changed': False, 'backend': 'cuda_cupy', 'precision': 'float64', 'rules': dict(RULES),
            'input_sha256': self.digest.hexdigest(), 'rows': self.rows, 'unassessed_moves': self.unassessed,
            'implementation_sha256': hashlib.sha256((inspect.getsource(kernel) + inspect.getsource(Comparison)).encode()).hexdigest(),
            'checks': checks, 'mismatch_examples': list(self.examples), 'game_mismatch_examples': examples,
            'groups': groups, 'hardware': self.hardware, 'timings': dict(self.timings)}
