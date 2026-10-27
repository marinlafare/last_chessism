"""Offline, bounded CPU/CUDA comparisons. Never queries or writes a database.

The CPU oracle is intentionally separate from the vectorized CUDA code.
This is validation infrastructure, NOT a canonical summary writer or a fitted
rating-aware model. CUDA failure is an error, never a silent CPU fallback.
"""
import hashlib
import inspect
import json
import math
import time

import numpy as np

VERSION = 2
MAX_ROWS = 100_000
BATCH_ROWS = 8192
MODES = ('bullet', 'blitz', 'rapid')
KINDS = {'cp': 0, 'mate': 1, 'tablebase': 2}
COUNTERS = 15  # 13 CP bins, mate, tablebase. There is no unscored bucket.


def validate_rows(rows):
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise ValueError(f'Provide 1–{MAX_ROWS:,} rows from one frozen input snapshot.')
    result = []
    identities = set()
    with_keys = any('row_key' in row for row in rows if isinstance(row, dict))
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('Each row must be an object.')
        for key in ('player', 'mode', 'player_score', 'score_kind', 'mate_moves'):
            if key not in row:
                raise ValueError(f'Missing required input column: {key}')
        if row['mode'] not in MODES:
            raise ValueError('Unsupported game mode.')
        if not isinstance(row['player'], str) or not 1 <= len(row['player']) <= 128:
            raise ValueError('Player identity must be a nonempty bounded string.')
        score, score_kind, moves = row['player_score'], row['score_kind'], row['mate_moves']
        if isinstance(score, bool) or not isinstance(score, (float, int)) or not math.isfinite(score):
            raise ValueError('Only scored rows with finite evaluations are accepted; zero is valid.')
        if not isinstance(score_kind, str) or score_kind not in KINDS:
            raise ValueError('Only cp, mate and tablebase score kinds are accepted.')
        if (score_kind == 'cp' and abs(score) >= 9000) or (score_kind == 'mate' and abs(score) < 9000):
            raise ValueError('Evaluation kind contradicts the stored score encoding.')
        if moves is not None and (isinstance(moves, bool) or not isinstance(moves, (float, int)) or
                not math.isfinite(moves) or not 0 <= moves <= 1000 or int(moves) != moves or
                score_kind != 'mate' or moves != 10000 - abs(score)):
            raise ValueError('Mate distance must match the encoded mate score, or be null when unknown.')
        # Occurrence identifiers are optional audit metadata, never GPU columns.
        # Identical numerical rows are legitimate repeated occurrences.
        if with_keys:
            identity = row.get('row_key')
            if not isinstance(identity, str) or not 1 <= len(identity) <= 256:
                raise ValueError('Supply a bounded row_key for every row or omit all row keys.')
            if identity in identities:
                raise ValueError('Duplicate occurrence identity; repeated FENs at different occurrences are allowed.')
            identities.add(identity)
        result.append({key: row[key] for key in ('player', 'mode', 'player_score', 'score_kind', 'mate_moves')})
    return result


def reference(rows):
    from ..player_score_distribution import bins
    boundaries = bins()
    result = []
    for row in rows:
        score, score_kind = row['player_score'], row['score_kind']
        category, distance, side = {'mate': 13, 'tablebase': 14}.get(score_kind, -1), -1, 0
        if score_kind == 'cp':
            for boundary in boundaries:
                low, high = boundary['lower'], boundary['upper']
                if (low is None or score > low or score == low and boundary['lower_inclusive']) and (
                    high is None or score < high or score == high and boundary['upper_inclusive']
                ):
                    category = boundary['id']
                    break
        elif score_kind == 'mate':
            moves = row['mate_moves']
            distance, side = -1 if moves is None else moves, 1 if score > 0 else -1
        result.append((category, distance, side))
    return np.asarray(result, dtype=np.int64)


def score_kernel(xp, scores, kinds, mate_moves):
    """Scored-only numeric input: kind 0=CP, 1=mate, 2=tablebase."""
    cp = xp.clip(scores, -551, 551)
    categories = xp.where(cp < -550, 0, xp.where(cp > 550, 12,
        xp.where((cp >= -50) & (cp <= 50), 6,
        xp.where(cp < -50, xp.floor((cp + 550) / 100) + 1, xp.ceil((cp - 50) / 100) + 6)))).astype(xp.int64)
    categories = xp.where(kinds == 1, 13, categories)
    categories = xp.where(kinds == 2, 14, categories)
    distance = xp.where(kinds == 1, mate_moves, -1).astype(xp.int64)
    side = xp.where(kinds == 1, xp.where(scores > 0, 1, -1), 0).astype(xp.int64)
    return xp.stack((categories, distance, side), axis=1)


def group_counts(xp, decisions, groups, group_count):
    return xp.bincount(groups * COUNTERS + decisions[:, 0], minlength=group_count * COUNTERS).reshape(group_count, COUNTERS)


def group_mates(xp, decisions, groups):
    selected = decisions[:, 0] == 13
    # 0..1000 = known distance; 1001 = unknown. Side 0=against, 1=for.
    distance = xp.where(decisions[:, 1] < 0, 1001, decisions[:, 1])
    encoded = groups * 2004 + (decisions[:, 2] > 0) * 1002 + distance
    return xp.unique(encoded[selected], return_counts=True)


def mate_records(counts, group):
    return [{'moves': None if code % 1002 == 1001 else code % 1002,
             'side': 'for' if code % 2004 >= 1002 else 'against', 'positions': count}
            for code, count in sorted(counts.items()) if code // 2004 == group]


from .cuda_runtime import cuda_backend


def compare_score_distribution(rows, *, device=0, batch_rows=BATCH_ROWS):
    if not isinstance(batch_rows, int) or not 1 <= batch_rows <= BATCH_ROWS:
        raise ValueError(f'Batch size must be 1–{BATCH_ROWS}.')
    identity_checked = isinstance(rows, list) and bool(rows) and all(isinstance(row, dict) and 'row_key' in row for row in rows)
    rows = validate_rows(rows)
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    groups = sorted({(row['player'], row['mode']) for row in rows})
    if len(groups) > 512:
        raise ValueError('Validation is bounded to 512 player/mode groups.')
    group_ids = {key: index for index, key in enumerate(groups)}
    ids = np.array([group_ids[row['player'], row['mode']] for row in rows], dtype=np.int64)
    scores = np.array([row['player_score'] for row in rows], dtype=np.float64)
    kinds = np.array([KINDS[row['score_kind']] for row in rows], dtype=np.int8)
    mates = np.array([-1 if row['mate_moves'] is None else row['mate_moves'] for row in rows], dtype=np.int64)
    reference(rows[:1])  # Load the reference modules outside the warm timer.
    started = time.perf_counter()
    expected = reference(rows)
    expected_counts = group_counts(np, expected, ids, len(groups))
    expected_mates = {int(code): int(count) for code, count in zip(*group_mates(np, expected, ids))}
    cpu_seconds = time.perf_counter() - started
    started = time.perf_counter()
    cp, hardware = cuda_backend(device)
    # Explicit warm-up: report context/compilation separately, not as a speedup.
    for size in {min(batch_rows, len(rows)), len(rows) % batch_rows or min(batch_rows, len(rows))}:
        warm = score_kernel(cp, cp.asarray(scores[:size]), cp.asarray(kinds[:size]), cp.asarray(mates[:size]))
        group_counts(cp, warm, cp.asarray(ids[:size]), len(groups))
        group_mates(cp, warm, cp.asarray(ids[:size]))
    cp.cuda.get_current_stream().synchronize()
    setup_seconds = time.perf_counter() - started
    actual, actual_counts, actual_mates = [], np.zeros_like(expected_counts), {}
    timings = {'host_to_device_seconds': 0.0, 'calculation_seconds': 0.0, 'device_to_host_seconds': 0.0}
    started = time.perf_counter()
    for start in range(0, len(rows), batch_rows):
        checkpoint = time.perf_counter()
        end = start + batch_rows
        gpu_scores, gpu_kinds, gpu_mates, gpu_ids = (cp.asarray(array[start:end]) for array in (scores, kinds, mates, ids))
        cp.cuda.get_current_stream().synchronize()
        timings['host_to_device_seconds'] += time.perf_counter() - checkpoint
        checkpoint = time.perf_counter()
        decisions = score_kernel(cp, gpu_scores, gpu_kinds, gpu_mates)
        counts = group_counts(cp, decisions, gpu_ids, len(groups))
        mate_codes, mate_counts = group_mates(cp, decisions, gpu_ids)
        cp.cuda.get_current_stream().synchronize()
        timings['calculation_seconds'] += time.perf_counter() - checkpoint
        checkpoint = time.perf_counter()
        actual.append(cp.asnumpy(decisions))
        actual_counts += cp.asnumpy(counts)
        for code, count in zip(cp.asnumpy(mate_codes), cp.asnumpy(mate_counts)):
            actual_mates[int(code)] = actual_mates.get(int(code), 0) + int(count)
        cp.cuda.get_current_stream().synchronize()
        timings['device_to_host_seconds'] += time.perf_counter() - checkpoint
    timings['total_warm_seconds'] = time.perf_counter() - started
    actual = np.concatenate(actual)
    mismatch = np.flatnonzero(np.any(expected != actual, axis=1))
    count_mismatch = int(np.count_nonzero(expected_counts != actual_counts))
    count_mismatch += sum(expected_mates.get(code, 0) != actual_mates.get(code, 0) for code in expected_mates.keys() | actual_mates.keys())
    return {
        'format': 'chessism_cuda_comparison_v2', 'algorithm': 'score-distribution', 'algorithm_version': VERSION,
        'status': 'passed' if not len(mismatch) and not count_mismatch else 'failed',
        'canonical': False, 'input_sha256': digest, 'rows': len(rows), 'batch_rows': batch_rows,
        'implementation_sha256': hashlib.sha256((inspect.getsource(score_kernel) + inspect.getsource(group_counts) + inspect.getsource(group_mates)).encode()).hexdigest(),
        'input_contract': 'score_distribution_v2', 'backend': 'cuda_cupy', 'precision': 'float64',
        'row_identity_checked': identity_checked, 'scored_only': True,
        'hardware': hardware, 'row_mismatches': len(mismatch), 'counter_mismatches': count_mismatch,
        'mismatch_examples': [{'row': int(i), 'expected': expected[i].tolist(), 'actual': actual[i].tolist()} for i in mismatch[:20]],
        'groups': [{'player': player, 'mode': mode, 'cpu': expected_counts[index].tolist(), 'cuda': actual_counts[index].tolist(),
                    'mates_cpu': mate_records(expected_mates, index), 'mates_cuda': mate_records(actual_mates, index)}
                   for index, (player, mode) in enumerate(groups)],
        'timings': {'cpu_reference_seconds': cpu_seconds, 'cuda_setup_seconds': setup_seconds, **timings},
        'notes': ['No database reads or writes. No summary replacement or canonical promotion.',
                  'Scored rows only; exact per-occurrence bin, mate-distance and side comparison; exact grouped counters.',
                  'Source provenance is normalized to score_kind and mate_moves before GPU execution.',
                  'Equal-valued rows remain separate occurrences; row keys are optional audit metadata.',
                  'Timings exclude database extraction and summary publication; this is not an end-to-end benchmark.',
                  'This CPU oracle favors independent correctness, not optimized CPU performance.'],
    }
