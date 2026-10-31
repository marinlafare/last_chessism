"""Compare fixed weaker rating adjustments; research only, no live publication."""
import asyncio
import hashlib
import inspect
import json
import math
import time
import uuid
from collections import Counter

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_strength_source as source, blunder_strength_compute as compute
from . import blunder_cohort_source as cohort, blunder_shadow_source as extraction
from . import blunder_development_reuse as history, blunder_v2_model as model, blunder_fixed
from .blunder_v2_job import cancellation_check
from .cuda_runtime import cuda_backend
from .storage import ensure_capacity, workspace

OPERATION = source.OPERATION


def configuration(parent_id, row, policy):
    r = (row or {}).get('result') or {}
    if ((row or {}).get('status') != 'complete' or r.get('format') != 'chessism_blunder_cohort_report_v1'
            or r.get('status') != 'passed' or r.get('canonical') is not False or r.get('production_changed') is not False
            or r.get('config', {}).get('threshold') != .15 or r.get('stale_players') or r.get('cohort_membership_changed')
            or not r.get('players') or len(r['players']) != len(r.get('inventory', []))
            or not r.get('checks') or any(r['checks'].values())):
        raise ValueError('A completed, fresh, verified all-player .15 shadow report is required.')
    fits = r.get('fits', [])
    if [f.get('mode') for f in fits] != list(model.MODES) or any(
            not all(isinstance(f.get(k), (int, float)) and math.isfinite(f[k]) for k in ('a', 'b')) for f in fits):
        raise ValueError('All frozen mode coefficients are required.')
    if policy.get('version') != history.VERSION or not policy.get('references'):
        raise ValueError('Verified historical development/test exclusions are required.')
    return {'operation': OPERATION, 'implementation_version': 1, 'backend': 'cuda_cupy',
        'parent_run_id': str(uuid.UUID(str(parent_id))), 'parent_report_sha256': history.digest(r),
        'model_id': r['model_id'], 'fits_sha256': history.digest(fits), 'sampling_policy': policy,
        'k0': .00368208, 'threshold': .15, 'candidates': compute.candidates(),
        'max_games': source.MAX_GAMES, 'max_rows': source.MAX_POSITIONS,
        'columns': ['before', 'after', 'player_rating', 'opponent_rating', 'mode'],
        'canonical': False, 'refitted': False, 'independent_calibration_test': False}


def verify_revisions(parent, current):
    if {p['player_name']: p['revision'] for p in current} != {p['player_name']: p['revision'] for p in parent['inventory']}:
        raise ValueError('Player data changed since the cohort; do not claim identical frozen references.')


async def execute(config, reporter):
    started = time.perf_counter(); await reporter.check(); ensure_capacity(config)
    if model.K0 != config['k0'] or config['threshold'] != .15:
        raise ValueError('Frozen coefficient/threshold changed.')
    xp, hardware = await file_operation(cuda_backend, 0)
    check = cancellation_check(reporter, asyncio.get_running_loop())
    async with AsyncDBSession() as session:
        await cohort.prepare(session)
        policy = await history.reference_plan(session)
        row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
            {'id': config['parent_run_id']})).mappings().one_or_none()
        if config != configuration(config['parent_run_id'], row, policy):
            raise ValueError('Frozen parent or source-history instructions changed.')
        parent = row['result']; verify_revisions(parent, await cohort.profiles(session))
        _, excluded, proof = await history.history(session, policy, reporter)
        excluded |= await source.exploration_ids(session)
    pool = source.Pool(excluded, policy['test_from']); t = time.perf_counter()
    for i, player in enumerate(parent['players']):
        await reporter.progress('selecting_strength_sample', i, len(parent['players']),
            'Reading saved game metadata only; protected research games and later test dates excluded.')
        await file_operation(source.read_artifact, player, parent, pool, check)
    selected, selection = await file_operation(pool.choose)
    if not selected: raise ValueError('No unexposed development games remain within the sample constraints.')
    inspected = sorted(r['game'] for r in selected)
    frozen = {'selection': selection, 'inspected_game_ids': inspected,
        'inspected_game_ids_sha256': history.digest(inspected), 'verified_history': proof,
        'excluded_prior_games': len(excluded), 'prior_game_overlap': 0, 'test_from': policy['test_from'],
        'selection_seed': source.SEED, 'independent_calibration_test': False}
    # Reserve before accessing outcomes/calibration, including failed/cancelled runs.
    await source.reserve(frozen, reporter)
    selection_seconds = time.perf_counter()-t
    with workspace(reporter.run_id) as folder:
        path = folder/'strength-input.bin'
        async with AsyncDBSession() as session:
            await cohort.prepare(session); verify_revisions(parent, await cohort.profiles(session))
            games, inspected_again, dropped = await source.load_metadata(session, selected, reporter)
            if inspected_again != inspected: raise ValueError('Selected identity reservation changed.')
            actual_cells = Counter((source.band(g['player_rating']), g['mode']) for g in games)
            frozen['selection']['final_cells'] = [{'rating_band': b, 'mode': m, 'target': source.PER_GROUP,
                'selected': actual_cells[b, m]} for b in source.BANDS for m in cohort.MODES]
            frozen.update(dropped_after_selection=dropped, selected_games=len(games),
                selected_players=sorted({g['player'] for g in games}),
                positions=sum(g['total_positions'] for g in games),
                selected_game_ids=[g['link'] for g in games], taken_at=str(await session.scalar(text('SELECT transaction_timestamp()'))))
            await reporter.progress('loading_shared_strength_input', detail=f'{len(games):,} unique games, mixed-player blocks; load once for all eight candidates.')
            with path.open('xb') as stream:
                moves, extraction_timings = await extraction.extract(session, games, reporter, stream)
            frozen['input_sha256'] = history.digest(games)
        await reporter.progress('comparing_rating_strengths', 0, moves,
            'RTX 4060: eight candidates together, fixed coefficient and .15 threshold; independent CPU checks.')
        rows = np.memmap(path, dtype=blunder_fixed.DTYPE, mode='r')
        try:
            result = await file_operation(compute.calculate, xp, rows, games, parent['fits'], check)
        finally:
            rows._mmap.close()
        if result['status'] != 'passed':
            raise ValueError('Strength comparison numerical mismatch: '+json.dumps(result['checks']))
        async with AsyncDBSession() as session:
            await cohort.prepare(session); verify_revisions(parent, await cohort.profiles(session))
            await extraction.attach_examples(session, result['examples'])
        if not all(r['legal_transition_verified'] for r in result['examples']):
            raise ValueError('A changed-move example failed board-transition validation.')
    result.update(format=source.FORMAT, config=config, canonical=False, production_changed=False,
        promotion_ready=False, selected_candidate=None, refitted=False, independent_calibration_test=False,
        source=frozen, included_rows=moves, hardware=hardware, backend='cuda_cupy', precision='float64',
        model_id=parent['model_id'], original_quality_gates=parent['original_quality_gates'],
        frozen_fits=[{k:f[k] for k in ('mode', 'a', 'b')} for f in parent['fits']],
        implementation_sha256=hashlib.sha256(''.join(inspect.getsource(m) for m in
            (inspect.getmodule(execute), source, compute, model, extraction)).encode()).hexdigest(),
        notes=[
            'Exploratory sensitivity comparison, not a new independent test or an automatically selected model. Live summaries are unchanged.',
            'The Lichess base coefficient 0.00368208, .15 decision threshold, CP clipping and explicit mate/unsupported rules are fixed. Only a and b are scaled.',
            'Outcome-blind fixed-hash sample, up to 250 unique games per historical rating/mode cell and 50 per owner/cell; rating gaps are interleaved. Shortfalls are not padded.',
            'Previous research games, later reserved test dates, and deterministic held-out players on either side are excluded. Inspected identities are durably protected from future fresh-test reuse.',
            'All eight candidates receive the same mixed-player inputs in one broadcast CUDA calculation per batch. Extraction happens once, not once per player or candidate.',
            'Calibration predicts expected game points after the selected player moves, with equal total weight per game with a CP observation. Draws are .5, not wins. This is not identical to earlier all-ply calibration.',
            'Balanced diagnostic cells are not population-weighted estimates. Sparse groups, shared players and positions limit confidence; descriptive ECE does not establish model quality.',
            'Changed-move examples have legally verified source boards, not new engine searches or human ground-truth quality labels. A fresh independent evaluation is still needed after choosing a candidate.',
            'CPU comparisons and legacy SQL checks are validation overhead; this run does not benchmark automatic ingestion performance.'])
    result['timings'].update(extraction_timings, selection_seconds=selection_seconds,
        total_compute_seconds=time.perf_counter()-started)
    json.dumps(result, allow_nan=False)
    return result
