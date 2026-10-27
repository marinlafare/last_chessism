"""Opt-in experiment: reuse ONE frozen input across bounded CPU/GPU passes.

No persistent cache, database queries, canonical promotion or summary writes.
Each candidate is checked per occurrence before timing its aggregate-only path.
Both timed backends include file reading, array preparation, hashing and reduction;
the GPU path additionally includes both transfers, but returns only small counts.
"""
import hashlib
from statistics import median
import time

import numpy as np

from ..matrix_constructor.artifact_files import file_operation
from .score_stream import (Comparison, COUNTERS, ENCODING, GPU_BATCH_SIZES, MAX_ROWS,
    MODES, cpu_reference, cuda_backend, group_counts, group_mates, mate_records,
    read_batch, score_kernel, validate_batch_size)

REPEATS = 3


def instructions():
    return {'version': 1, 'batch_sizes': list(GPU_BATCH_SIZES), 'repeats': REPEATS}


def signature(report, backend='cuda'):
    return [(row['player'], row['mode'], row[backend], row['mates_' + backend]) for row in report['groups']]


def summary_pass(stream, player, *, backend, batch_rows, device=0):
    """A measured candidate, never a replacement production writer/fallback."""
    validate_batch_size(batch_rows)
    if backend not in ('cuda_cupy', 'numpy_cpu'):
        raise ValueError('Unknown benchmark backend.')
    if backend == 'cuda_cupy':
        cp, _ = cuda_backend(device)
        cp.cuda.get_current_stream().synchronize()
    stream.seek(0)
    started = time.perf_counter()
    digest = hashlib.sha256((ENCODING + '\n' + player + '\n').encode())
    counts, mates, rows = np.zeros((3, COUNTERS), dtype=np.int64), {}, 0
    while True:
        batch = read_batch(stream, batch_rows)
        if not len(batch):
            break
        rows += len(batch)
        if rows > MAX_ROWS:
            raise ValueError('Input exceeds the bounded benchmark limit.')
        digest.update(batch.tobytes())
        ids = np.ascontiguousarray(batch['mode'])
        if backend == 'numpy_cpu':
            decisions = cpu_reference(batch)
            counts += group_counts(np, decisions, ids, 3)
            codes, amounts = group_mates(np, decisions, ids)
        else:
            scores = cp.asarray(np.ascontiguousarray(batch['score']))
            kinds = cp.asarray(np.ascontiguousarray(batch['kind'], dtype=np.int8))
            distances = cp.asarray(np.ascontiguousarray(batch['mate']))
            gpu_ids = cp.asarray(ids)
            decisions = score_kernel(cp, scores, kinds, distances)
            counts += cp.asnumpy(group_counts(cp, decisions, gpu_ids, 3))
            codes, amounts = (cp.asnumpy(value) for value in group_mates(cp, decisions, gpu_ids))
        for code, count in zip(codes, amounts):
            mates[int(code)] = mates.get(int(code), 0) + int(count)
    if backend == 'cuda_cupy':
        cp.cuda.get_current_stream().synchronize()
    seconds = time.perf_counter() - started
    return {'seconds': seconds, 'rows': rows, 'input_sha256': digest.hexdigest(),
        'groups': [(player, mode, counts[i].tolist(), mate_records(mates, i)) for i, mode in enumerate(MODES)]}


async def validate_candidate(stream, player, batch_rows, reporter):
    comparison = await file_operation(Comparison, player, batch_rows=batch_rows)
    stream.seek(0)
    while True:
        batch = await file_operation(read_batch, stream, batch_rows)
        if not len(batch):
            break
        await reporter.check()
        await file_operation(comparison.consume, batch)
    return comparison.report()


async def benchmark(stream, baseline, source, reporter):
    started = time.perf_counter()
    if baseline['status'] != 'passed':
        raise ValueError('The baseline must pass before benchmarking.')
    expected = signature(baseline)
    player = source['player']
    variants = []
    total = len(GPU_BATCH_SIZES) * (1 + 2 * REPEATS)
    completed = 0
    for batch_rows in GPU_BATCH_SIZES:
        await reporter.progress('benchmarking_batches', completed, total,
            f'Validating {batch_rows:,}-row GPU batches on the same frozen input.')
        report = baseline if batch_rows == baseline['batch_rows'] else await validate_candidate(stream, player, batch_rows, reporter)
        if (report['status'] != 'passed' or report['rows'] != baseline['rows']
                or report['input_sha256'] != baseline['input_sha256'] or signature(report) != expected):
            raise ValueError('Batch-size validation disagrees; no benchmark result was accepted.')
        variants.append({'batch_rows': batch_rows, 'validation': {
            'status': 'passed', 'rows': report['rows'], 'row_mismatches': report['row_mismatches'],
            'counter_mismatches': report['counter_mismatches'], 'input_sha256': report['input_sha256'],
            'peak_batch_rows': report['peak_batch_rows'],
            'cuda_setup_seconds': report['timings']['cuda_setup_seconds']},
            'cuda_cupy_seconds': [], 'numpy_cpu_seconds': []})
        completed += 1
    # Alternate backend and batch-size order, rather than always granting the
    # same candidate the first/cold read. All kernels were warmed above.
    order = []
    for repeat in range(REPEATS):
        for variant in (variants if repeat % 2 == 0 else list(reversed(variants))):
            backends = ('numpy_cpu', 'cuda_cupy') if repeat % 2 == 0 else ('cuda_cupy', 'numpy_cpu')
            for backend in backends:
                await reporter.check()
                await reporter.progress('benchmarking_batches', completed, total,
                    f'Pass {repeat + 1}/{REPEATS}: {backend}, {variant["batch_rows"]:,} rows per batch.')
                result = await file_operation(summary_pass, stream, player, backend=backend, batch_rows=variant['batch_rows'])
                await reporter.check()
                if (result['rows'] != baseline['rows'] or result['input_sha256'] != baseline['input_sha256']
                        or result['groups'] != expected):
                    raise ValueError('Timed summary pass disagrees with the frozen validation; no partial benchmark was accepted.')
                variant[backend + '_seconds'].append(result['seconds'])
                order.append({'repeat': repeat + 1, 'batch_rows': variant['batch_rows'], 'backend': backend})
                completed += 1
    for variant in variants:
        for backend in ('cuda_cupy', 'numpy_cpu'):
            variant[backend + '_median_seconds'] = median(variant[backend + '_seconds'])
    fastest = min(variants, key=lambda row: row['cuda_cupy_median_seconds'])
    return {'version': 1, 'status': 'passed', 'canonical': False, 'repeats': REPEATS,
        'input_reuse': 'within_run_only', 'database_extractions': 1, 'variants': variants,
        'measurement_order': order, 'fastest_measured_gpu_batch_rows': fastest['batch_rows'],
        'wall_seconds': time.perf_counter() - started,
        'notes': [
            'Every candidate passes per-occurrence and aggregate comparisons before timed aggregate-only passes.',
            'Both backends include temporary-file reads, numeric preparation, hashing and aggregation; GPU includes transfers.',
            'Median of three warm passes on one frozen input; not an ingestion/update benchmark.',
            'Fresh input still pays extraction once. Reuse here does not prove a persistent cache can be invalidated correctly.',
            'SQL timing is a separate fresh-source aggregation measured after extraction; it is cache/order-sensitive.',
            'No persistent cache, default batch change, live summary replacement or canonical promotion.'],
    }
