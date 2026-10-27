"""Bounded numerical score validation; no database access or summary writes.

The temporary stream is an internal transport for the five-column v2 matrix,
not a new matrix definition. Player is constant; mode/kind use fixed dictionaries.
Only one batch of inputs and per-row decisions is retained at a time.
"""
import hashlib
import inspect
import time

import numpy as np

from .summary_validation import (BATCH_ROWS, COUNTERS, MODES, cuda_backend,
    group_counts, group_mates, mate_records, score_kernel)

MAX_ROWS = 2_000_000
DTYPE = np.dtype([('mode', '<i8'), ('score', '<f8'), ('kind', '<i8'), ('mate', '<i8')])
ENCODING = 'score_distribution_le64_v1'
GPU_BATCH_SIZES = (8192, 65536, 262144)


def validate_batch_size(value):
    if type(value) is not int or value not in GPU_BATCH_SIZES:
        raise ValueError('Unsupported bounded GPU batch size.')
    return value


def pack(rows):
    """Reject corrupt inputs before spooling; no truncation or score coercion."""
    batch = np.fromiter((tuple(row) for row in rows), dtype=DTYPE, count=len(rows))
    scores, kinds, mates = batch['score'], batch['kind'], batch['mate']
    if (not np.isfinite(scores).all() or not np.isin(batch['mode'], range(3)).all()
            or not np.isin(kinds, range(3)).all()
            or np.any((kinds == 0) & (np.abs(scores) >= 9000))
            or np.any((kinds == 1) & (np.abs(scores) < 9000))
            or np.any((mates != -1) & ((kinds != 1) | (mates < 0) | (mates > 1000)
                | (mates != 10000 - np.abs(scores))))):
        raise ValueError('Invalid scored-only numerical input.')
    return batch


def cpu_reference(batch):
    # Independent interval definitions, not the CUDA floor/ceil expression.
    from ..player_score_distribution import bins
    scores, kinds = batch['score'], batch['kind']
    out = np.full((len(batch), 3), -1, dtype=np.int64)
    out[:, 2] = 0
    for boundary in bins():
        selected = kinds == 0
        low, high = boundary['lower'], boundary['upper']
        if low is not None:
            selected &= scores >= low if boundary['lower_inclusive'] else scores > low
        if high is not None:
            selected &= scores <= high if boundary['upper_inclusive'] else scores < high
        out[selected, 0] = boundary['id']
    mate = kinds == 1
    out[mate, 0] = 13
    out[kinds == 2, 0] = 14
    out[mate, 1] = batch['mate'][mate]
    out[mate, 2] = np.where(scores[mate] > 0, 1, -1)
    return out


class Comparison:
    """Bounded accumulator. CUDA selection occurs on every calling thread."""
    def __init__(self, player, device=0, batch_rows=BATCH_ROWS):
        self.batch_rows = validate_batch_size(batch_rows)
        self.player, self.device = player, device
        self.cp, self.hardware = cuda_backend(device)
        self.rows, self.row_mismatches, self.peak_rows = 0, 0, 0
        self.digest = hashlib.sha256((ENCODING + '\n' + player + '\n').encode())
        self.expected_counts = np.zeros((3, COUNTERS), dtype=np.int64)
        self.actual_counts = np.zeros_like(self.expected_counts)
        self.expected_mates, self.actual_mates = {}, {}
        self.examples, self.warmed = [], set()
        self.coverage = dict(zero_scores=0, known_mate_distances=0, unknown_mate_distances=0, tablebase_positions=0)
        self.timings = {key: 0.0 for key in ('cpu_reference_seconds', 'cuda_setup_seconds',
            'host_to_device_seconds', 'calculation_seconds', 'device_to_host_seconds', 'total_warm_seconds')}

    def consume(self, batch):
        if not 1 <= len(batch) <= self.batch_rows or self.rows + len(batch) > MAX_ROWS:
            raise ValueError('CUDA stream exceeded its bounded batch/row limit.')
        cp = self.cp
        cp.cuda.Device(self.device).use()
        self.digest.update(batch.tobytes())
        ids = np.ascontiguousarray(batch['mode'])
        scores = np.ascontiguousarray(batch['score'])
        kinds = np.ascontiguousarray(batch['kind'], dtype=np.int8)
        mates = np.ascontiguousarray(batch['mate'])
        checkpoint = time.perf_counter()
        expected = cpu_reference(batch)
        self.expected_counts += group_counts(np, expected, ids, 3)
        for code, count in zip(*group_mates(np, expected, ids)):
            self.expected_mates[int(code)] = self.expected_mates.get(int(code), 0) + int(count)
        self.timings['cpu_reference_seconds'] += time.perf_counter() - checkpoint
        if len(batch) not in self.warmed:
            checkpoint = time.perf_counter()
            warm = score_kernel(cp, cp.asarray(scores), cp.asarray(kinds), cp.asarray(mates))
            group_counts(cp, warm, cp.asarray(ids), 3)
            group_mates(cp, warm, cp.asarray(ids))
            cp.cuda.get_current_stream().synchronize()
            self.timings['cuda_setup_seconds'] += time.perf_counter() - checkpoint
            self.warmed.add(len(batch))
        started = checkpoint = time.perf_counter()
        gpu_scores, gpu_kinds, gpu_mates, gpu_ids = (cp.asarray(a) for a in (scores, kinds, mates, ids))
        cp.cuda.get_current_stream().synchronize()
        self.timings['host_to_device_seconds'] += time.perf_counter() - checkpoint
        checkpoint = time.perf_counter()
        decisions = score_kernel(cp, gpu_scores, gpu_kinds, gpu_mates)
        counts = group_counts(cp, decisions, gpu_ids, 3)
        mate_codes, mate_counts = group_mates(cp, decisions, gpu_ids)
        cp.cuda.get_current_stream().synchronize()
        self.timings['calculation_seconds'] += time.perf_counter() - checkpoint
        checkpoint = time.perf_counter()
        actual = cp.asnumpy(decisions)
        self.actual_counts += cp.asnumpy(counts)
        for code, count in zip(cp.asnumpy(mate_codes), cp.asnumpy(mate_counts)):
            self.actual_mates[int(code)] = self.actual_mates.get(int(code), 0) + int(count)
        cp.cuda.get_current_stream().synchronize()
        self.timings['device_to_host_seconds'] += time.perf_counter() - checkpoint
        self.timings['total_warm_seconds'] += time.perf_counter() - started
        mismatch = np.flatnonzero(np.any(expected != actual, axis=1))
        self.row_mismatches += len(mismatch)
        self.examples.extend({'row': self.rows + int(i), 'expected': expected[i].tolist(), 'actual': actual[i].tolist()}
            for i in mismatch[:20 - len(self.examples)])
        for key, selection in (
            ('zero_scores', scores == 0), ('known_mate_distances', (kinds == 1) & (mates >= 0)),
            ('unknown_mate_distances', (kinds == 1) & (mates < 0)), ('tablebase_positions', kinds == 2)):
            self.coverage[key] += int(np.count_nonzero(selection))
        self.rows += len(batch)
        self.peak_rows = max(self.peak_rows, len(batch))

    def report(self):
        count_mismatch = int(np.count_nonzero(self.expected_counts != self.actual_counts))
        count_mismatch += sum(self.expected_mates.get(code, 0) != self.actual_mates.get(code, 0)
            for code in self.expected_mates.keys() | self.actual_mates.keys())
        return {
            'format': 'chessism_cuda_comparison_v2', 'algorithm': 'score-distribution', 'algorithm_version': 2,
            'status': 'failed' if self.row_mismatches or count_mismatch else 'passed', 'canonical': False,
            'input_contract': 'score_distribution_v2', 'input_encoding': ENCODING,
            'input_sha256': self.digest.hexdigest(), 'rows': self.rows, 'batch_rows': self.batch_rows,
            'streaming_version': 1, 'peak_batch_rows': self.peak_rows,
            'implementation_sha256': hashlib.sha256((inspect.getsource(score_kernel)
                + inspect.getsource(group_counts) + inspect.getsource(group_mates)).encode()).hexdigest(),
            'backend': 'cuda_cupy', 'precision': 'float64', 'row_identity_checked': False, 'scored_only': True,
            'hardware': self.hardware, 'row_mismatches': self.row_mismatches, 'counter_mismatches': count_mismatch,
            'mismatch_examples': self.examples, 'coverage': self.coverage,
            'groups': [{'player': self.player, 'mode': mode, 'cpu': self.expected_counts[i].tolist(),
                'cuda': self.actual_counts[i].tolist(), 'mates_cpu': mate_records(self.expected_mates, i),
                'mates_cuda': mate_records(self.actual_mates, i)} for i, mode in enumerate(MODES)],
            'timings': self.timings,
            'notes': [
                'Frozen numeric batches from one read-only snapshot. No database transaction is held during GPU calculation.',
                'All scored occurrences are retained, including repeats; no occurrence deduplication or sorting is needed.',
                'Exact per-row comparisons and grouped CP, mate-distance/side and tablebase counters.',
                'Input hash identifies the encoded stream order, not a canonical order of the same source dataset.',
                'GPU timers exclude extraction and publication; full validation includes CPU/SQL cross-checks.',
                'Validation only. Chart summaries and the canonical algorithm are unchanged.'],
        }


def read_batch(stream, batch_rows=BATCH_ROWS):
    validate_batch_size(batch_rows)
    data = stream.read(batch_rows * DTYPE.itemsize)
    if len(data) % DTYPE.itemsize:
        raise ValueError('Truncated numerical input stream.')
    return np.frombuffer(data, dtype=DTYPE)
