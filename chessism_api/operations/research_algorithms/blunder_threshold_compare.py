"""Frozen, paired .15/.20 threshold audit. Research output only; no refitting."""
import asyncio
from collections import defaultdict
from copy import deepcopy
import hashlib
import inspect
import json
import math
import time
import uuid

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_shadow_compute as compute, blunder_shadow_source as source
from . import blunder_fixed, blunder_v2_model as model
from .blunder_development_reuse import digest
from .blunder_shadow_job import calculate_file
from .blunder_v2_job import cancellation_check
from .cuda_runtime import cuda_backend
from .storage import ensure_capacity, workspace

OPERATION = 'rating_blunder_threshold_comparison_v1'
FORMAT = 'chessism_blunder_threshold_comparison_v1'
FIELDS = ('old15', 'baseline20', 'candidate20', 'candidate15')
BANDS = ('<1000', '1000–1499', '1500–1999', '2000–2499', '2500–2999', '3000+')


def band(rating):
    return BANDS[int(np.searchsorted([1000, 1500, 2000, 2500, 3000], rating, side='right'))]


def configuration(parent_id, row):
    r = (row or {}).get('result') or {}
    if ((row or {}).get('status') != 'complete' or r.get('format') != 'chessism_blunder_shadow_pilot_v1'
            or r.get('summary_version') != 2 or r.get('status') != 'passed'
            or r.get('canonical') is not False or r.get('production_changed') is not False
            or r.get('source', {}).get('games') != 6000 or any(r.get('checks', {'missing': 1}).values())):
        raise ValueError('Choose the completed, verified 6,000-game summary-v2 shadow pilot.')
    return {'operation': OPERATION, 'implementation_version': 1, 'parent_run_id': str(uuid.UUID(str(parent_id))),
        'parent_report_sha256': digest(r), 'model_id': r['model_id'], 'fits_sha256': digest(r['fits']),
        'thresholds': [.15, .20], 'k0': .00368208, 'max_rows': source.MAX_POSITIONS,
        'columns': ['before', 'after', 'player_rating', 'opponent_rating', 'mode'],
        'canonical': False, 'refitted': False, 'independent_calibration_test': False}


def scalar15(before, after, bk, ak, u, d, a, b):
    """Independent scalar .15 oracle; unchanged scalar mate/unsupported rules."""
    if bk != 0 or ak != 0 or not all(math.isfinite(v) for v in (before, after, u, d)):
        return model.scalar(before, after, bk, ak, u, d, a, b)
    def expected(cp):
        z = model.K0 * max(-1000., min(1000., cp)) * math.exp(a*u) + b*d
        return .5 + .5*math.tanh(z/2)
    return int(expected(before)-expected(after) >= .15)


def aggregate(records, *, by_rating=False):
    groups = defaultdict(list)
    for r in records:
        keys = [(band(r['player_rating']), r['mode'])] if by_rating else [(r['player'], r['mode']), ('*', r['mode'])]
        for key in keys:
            groups[key].append(r)
    result = []
    for (identity, mode), rows in sorted(groups.items()):
        assessed = [r for r in rows if r['moves'] > r['unassessed']]
        result.append({('rating_band' if by_rating else 'player'): identity, 'mode': mode,
            'games': len(rows), 'assessed_games': len(assessed), 'moves': sum(r['moves'] for r in rows),
            'unassessed': sum(r['unassessed'] for r in rows),
            **{k: sum(r[k] for r in rows) for k in FIELDS},
            'histograms': {k: [sum(min(6, r[k]) == n for r in assessed) for n in range(7)] for k in FIELDS},
            'changes': {k: sum(r['changes'][k] for r in rows) for k in rows[0]['changes']}})
    return result


def calculate(xp, rows, games, fits, previous, check=lambda: None):
    values = compute.context(games, fits)
    records = deepcopy(previous)
    counts = np.zeros(len(games), dtype=np.int64)
    oracle_counts = counts.copy()
    categories = ('restored_old', 'new_from_lowering', 'still_removed', 'added_vs_old', 'mate_control', 'near_15')
    changes = {k: counts.copy() for k in categories}
    checks = dict.fromkeys(('threshold15_cpu_move_mismatches', 'threshold15_per_game_mismatches',
        'threshold_monotonicity_violations', 'mate_rule_mismatches', 'assessment_mismatches'), 0)
    candidates = defaultdict(list)
    highlighted = None
    for batch in model.chunks(rows):
        check()
        ids = batch['game']
        args = [batch[k] for k in ('before', 'after', 'before_kind', 'after_kind')] + [values[k][ids] for k in ('u', 'd', 'a', 'b')]
        oracle = np.array([scalar15(*items) for items in zip(*args)], dtype=np.int8)
        gpu = compute.arrays(xp, batch, values)
        flag15 = model.decisions(xp, *gpu, threshold=.15)
        counts += compute.host(xp, compute.counts(xp, xp.asarray(ids), flag15 == 1, len(games)))
        oracle_counts += np.bincount(ids[oracle == 1], minlength=len(games))
        low = compute.host(xp, flag15)
        high = compute.host(xp, model.decisions(xp, *gpu, threshold=.20))
        old = compute.host(xp, blunder_fixed.kernel(xp, *gpu[:4]))
        cp = (batch['before_kind'] == 0) & (batch['after_kind'] == 0)
        checks['threshold15_cpu_move_mismatches'] += int(np.count_nonzero(low != oracle))
        checks['threshold_monotonicity_violations'] += int(np.count_nonzero((high == 1) & (low != 1)))
        checks['mate_rule_mismatches'] += int(np.count_nonzero(~cp & (low != high)))
        checks['assessment_mismatches'] += int(np.count_nonzero((low == -1) != (high == -1)))
        loss = model.expected(np, np.where(cp, args[0], 0.), *args[4:])-model.expected(np, np.where(cp, args[1], 0.), *args[4:])
        masks = ((old == 1) & (high == 0) & (low == 1), (old == 0) & (high == 0) & (low == 1),
            (old == 1) & (low == 0), (old == 0) & (low == 1), ~cp & (low == 1), cp & (np.abs(loss-.15) <= .002))
        for category, mask in zip(categories, masks):
            changes[category] += np.bincount(ids[mask], minlength=len(games))
            for i in np.flatnonzero(mask):
                g = games[ids[i]]
                key = (category, g['mode'], band(g['player_rating']))
                rank = hashlib.sha256(f"threshold-audit-v1:{g['link']}:{int(batch['ply'][i])}".encode()).hexdigest()
                bucket = candidates[key]
                if len(bucket) >= 4 and rank >= bucket[-1][0] and (g['link'], int(batch['ply'][i])) != (136512236098, 7):
                    continue
                item = {'category': category, 'game': g['link'], 'ply': int(batch['ply'][i]),
                    'player': g['player'], 'mode': g['mode'], 'player_rating': g['player_rating'],
                    'opponent_rating': g['opponent_rating'], 'rating_band': key[2],
                    'before': float(args[0][i]), 'after': float(args[1][i]), 'before_kind': int(args[2][i]),
                    'after_kind': int(args[3][i]), 'candidate_loss': float(loss[i]) if cp[i] else None,
                    'old15': int(old[i]), 'candidate20': int(high[i]), 'candidate15': int(low[i])}
                if (g['link'], item['ply']) == (136512236098, 7):
                    highlighted = item
                bucket.append((rank, item)); bucket.sort(key=lambda pair: pair[0]); del bucket[4:]
    checks['threshold15_per_game_mismatches'] = int(np.count_nonzero(counts != oracle_counts))
    for i, row in enumerate(records):
        row['candidate15'] = int(counts[i])
        row['changes'] = {k: int(v[i]) for k, v in changes.items()}
    # Small illustrative audit, not a prevalence estimate: prioritize low-rated
    # bullet, then other bullet and both other modes. One move per game.
    examples, seen = [], set()
    if highlighted:
        examples.append(highlighted); seen.add(highlighted['game'])
    for partition, budget in (('low_bullet', 18), ('other_bullet', 9), ('other_modes', 9)):
        buckets = []
        for key in sorted(candidates):
            mode, rating_band = key[1:]
            part = 'low_bullet' if mode == 'bullet' and rating_band in BANDS[:2] else 'other_bullet' if mode == 'bullet' else 'other_modes'
            if part == partition:
                buckets.append(list(candidates[key]))
        used = int(partition == 'low_bullet' and highlighted is not None)
        while used < budget:
            added = 0
            for bucket in buckets:
                while bucket and bucket[0][1]['game'] in seen:
                    bucket.pop(0)
                if bucket and used < budget:
                    item = bucket.pop(0)[1]; examples.append(item); seen.add(item['game']); used += 1; added += 1
            if not added:
                break
    return {'status': 'passed' if not any(checks.values()) else 'failed', 'checks': checks,
        'records': records, 'groups': aggregate(records), 'rating_groups': aggregate(records, by_rating=True),
        'examples': examples, 'checked_moves': len(rows)}


async def execute(config, reporter):
    await reporter.check(); started = time.perf_counter(); ensure_capacity(config)
    if model.K0 != config['k0'] or model.THRESHOLD != .20:
        raise ValueError('Frozen model constants changed; this is not a threshold-only comparison.')
    xp, hardware = await file_operation(cuda_backend, 0)
    check = cancellation_check(reporter, asyncio.get_running_loop())
    with workspace(reporter.run_id) as folder:
        path = folder/'threshold-transitions.bin'
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='30s'"))
            await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
            await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
            if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
                raise ValueError('Wait for the active cloud job before reading the comparison sample.')
            row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'), {'id': config['parent_run_id']})).mappings().one_or_none()
            if config != configuration(config['parent_run_id'], row):
                raise ValueError('Frozen parent report or comparison instructions changed.')
            parent = row['result']
            await reporter.progress('selecting_threshold_sample', detail='Verifying the exact previous sample, source scores and frozen model.')
            games = await source.select_games(session, parent['source']['players'], reporter)
            with path.open('xb') as stream:
                moves, timings = await source.extract(session, games, reporter, stream)
            if digest(games) != parent['source']['sha256']:
                raise ValueError('The source snapshot changed; do not claim an identical-sample comparison.')
            baseline = await calculate_file(xp, path, games, parent['fits'], check)
            if baseline['status'] != 'passed' or baseline['records'] != parent['records']:
                raise ValueError('The previous three-rule results did not reproduce exactly.')
            await reporter.progress('comparing_thresholds', detail='Same frozen model, .15 versus .20; CPU oracle and unchanged-mate checks.')
            rows = np.memmap(path, dtype=blunder_fixed.DTYPE, mode='r')
            try:
                result = await file_operation(calculate, xp, rows, games, parent['fits'], baseline['records'], check)
            finally:
                rows._mmap.close()
            if result['status'] != 'passed':
                raise ValueError('Threshold reference check failed: '+json.dumps(result['checks']))
            await source.attach_examples(session, result['examples'])
            if not all(r['legal_transition_verified'] for r in result['examples']):
                raise ValueError('An audit example failed legal board-transition verification.')
            snapshot = str(await session.scalar(text('SELECT transaction_timestamp()')))
    result.update(format=FORMAT, summary_version=2, canonical=False, production_changed=False,
        promotion_ready=False, refitted=False, independent_calibration_test=False,
        config=config, model_id=parent['model_id'], fits=parent['fits'], original_quality_gates=parent['original_quality_gates'],
        source={**parent['source'], 'taken_at': snapshot}, included_rows=moves, hardware=hardware,
        backend='cuda_cupy', precision='float64', parent_reference_checks=baseline['checks'],
        timings={**timings, 'total_compute_seconds': time.perf_counter()-started},
        implementation_sha256=hashlib.sha256(''.join(inspect.getsource(m) for m in
            (inspect.getmodule(execute), compute, source, model, blunder_fixed)).encode()).hexdigest(),
        notes=['Research comparison only; no live summaries or canonical thresholds changed.',
            'The 6,000 unique games, per-game input hashes, frozen fits and all prior reference counts match the previous pilot.',
            'Rating bands use the selected player rating at game time, not current profile rating; only six sampled players are represented.',
            'Changed-move categories overlap: new_from_lowering is part of added_vs_old. They must not be summed as exclusive groups.',
            'At most 36 distinct-game examples: 18 low-rated bullet (<1500), 9 other bullet and 9 blitz/rapid. Fixed-hash illustrative selection; shortages are not padded.',
            'The previously discussed f4 example is deliberately included, not randomly sampled. Boards/arithmetic are verified, not engine reanalysis or human quality labels.',
            'Zero/wholly unassessed games remain excluded from every histogram. Forced-mate rules and unsupported transitions are unchanged.',
            'Lowering a decision threshold does not change expected-score predictions or resolve calibration/coverage failures. No refitting or independent validation is claimed.'])
    json.dumps(result, allow_nan=False)
    return result
