"""Versioned maximum-development research; never publishes live summaries."""
import asyncio
from copy import deepcopy
import hashlib
import inspect
import json
import math
import time
import uuid

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_full_source as source, blunder_full_compute as compute
from . import blunder_full_diagnostics as diagnostics
from . import blunder_development_reuse as history, blunder_cohort_source as cohort
from . import blunder_v2_model as model, blunder_refinement as refinement
from .cuda_runtime import cuda_backend
from .storage import ensure_capacity, workspace

OPERATION = source.OPERATION


def configuration(parent_id, row, policy, roles):
    result = (row or {}).get('result') or {}
    if ((row or {}).get('status') != 'complete' or result.get('format') != 'chessism_blunder_strength_comparison_v1'
            or result.get('status') != 'passed' or result.get('production_changed') is not False
            or not result.get('checks') or any(result['checks'].values())):
        raise ValueError('A completed, numerically verified rating-strength comparison is required.')
    fits = deepcopy(result['frozen_fits'])
    if ([f['mode'] for f in fits] != list(model.MODES)
            or any(not all(isinstance(f.get(k), (int, float)) and math.isfinite(f[k]) for k in ('a', 'b')) for f in fits)):
        raise ValueError('All frozen comparison modes are required.')
    fits[0]['a'] *= .5
    return {'operation': OPERATION, 'implementation_version': 2, 'parent_run_id': str(uuid.UUID(str(parent_id))),
        'parent_report_sha256': history.digest(result), 'sampling_policy': policy,
        'history_sha256': history.digest(roles), 'backend': 'cuda_cupy', 'device_name': 'NVIDIA GeForce RTX 4060',
        'k0': .00368208, 'threshold': .15, 'mate_rules': 'existing_700cp_v1',
        'frozen_reference_fits': fits, 'training_selection': 'full_pool_cp_first_both_player_10_percent_cap_v2',
        'development_diagnostics': 'rapid_drift_and_bullet_changed_moves_v1',
        'refinement': refinement.policy(), 'max_rows': source.MAX_ROWS,
        'columns': ['cp', 'average_rating', 'rating_gap', 'outcome', 'game_weight'],
        'final_test_policy': 'not_loaded_or_evaluated', 'canonical': False}


def cancellation(reporter, loop):
    updated = 0.; started = time.monotonic()
    def check():
        nonlocal updated
        now = time.monotonic()
        if now-started > 6*3600-120:
            raise TimeoutError('Large development run reached its six-hour safety budget.')
        if now-updated >= 1:
            asyncio.run_coroutine_threadsafe(reporter.check(), loop).result(timeout=30)
            updated = now
    return check


async def execute(config, reporter):
    started = time.monotonic(); await reporter.check(); ensure_capacity(config)
    loop = asyncio.get_running_loop(); check = cancellation(reporter, loop)
    xp, hardware = await file_operation(cuda_backend, 0)
    if hardware['name'] != config['device_name']:
        raise ValueError('This run requires the RTX 4060; no alternate GPU or CPU fallback.')
    free, total = xp.cuda.runtime.memGetInfo()
    memory = compute.memory_plan(free, total)
    xp.get_default_memory_pool().set_limit(size=memory['pool_limit_bytes'])
    await reporter.progress('checking_large_gpu_budget', detail=f"Dedicated RTX 4060: {memory['pool_limit_bytes']/compute.GIB:.2f} GiB pool; {memory['runtime_reserve_bytes']/compute.GIB:.2f} GiB CUDA/runtime headroom. No final-test data loaded.")
    try:
        with workspace(reporter.run_id) as folder:
            async with AsyncDBSession() as session:
                await cohort.prepare(session)
                policy = await history.reference_plan(session)
                roles, _, _ = await history.history(session, policy, reporter)
                row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'),
                    {'id': config['parent_run_id']})).mappings().one_or_none()
                if config != configuration(config['parent_run_id'], row, policy, roles):
                    raise ValueError('Frozen source/model instructions changed.')
                original_gates = row['result']['original_quality_gates']
                inventory = await cohort.profiles(session)
                selected, frozen = await source.select(session, config, reporter)
                # Persist before accessing development outcomes or score values.
                await source.reserve(frozen, reporter)
                await source.extract(session, selected, folder, frozen, reporter)
            # Release the read-only DB snapshot before local compaction/fitting.
            await reporter.progress('balancing_cp_eligible_games', detail='Removing games without usable CP scores, then balancing both players. No database reread.')
            groups, diagnostic_games = await file_operation(source.finalize_inputs, selected, folder, frozen, config['sampling_policy'], check)
            del selected
            extraction_seconds = time.monotonic()-started
            fits = []
            for mode, baseline in zip(model.MODES, config['frozen_reference_fits']):
                def progress(done, total, detail):
                    asyncio.run_coroutine_threadsafe(reporter.progress('fitting_full_'+mode, done, total, detail), loop).result(timeout=30)
                await reporter.progress('fitting_full_'+mode, 0, len(refinement.LAMBDAS),
                    f'{mode}: full balanced development pool, CUDA fitting; final tests withheld.')
                fits.append(await file_operation(compute.fit_mode, xp, folder, groups, mode, baseline,
                                                memory['pool_limit_bytes'], check, progress))
            def diagnostic_progress(done, total, detail):
                asyncio.run_coroutine_threadsafe(reporter.progress('development_diagnostics', done, total, detail), loop).result(timeout=30)
            diagnostic_report = await file_operation(diagnostics.run, xp, folder, diagnostic_games, fits,
                memory['pool_limit_bytes'], check, diagnostic_progress)
            async with AsyncDBSession() as session:
                # A cloud/import job may have started after our source snapshot.
                # Only read small revision markers; never stop it or discard a
                # valid frozen development result merely because refreshes began.
                await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
                await session.execute(text("SET LOCAL statement_timeout='10s'"))
                current = {p['player_name']: await cohort.current_revision(session, p['player_name']) for p in inventory}
            revisions_match = current == {p['player_name']: p['revision'] for p in inventory}
        numerical = all(f.get('numerical', {}).get('max_prediction_error', 1) <= 1e-10 for f in fits)
        selected_ok = all(f.get('selection', {}).get('status') == 'passed' for f in fits)
        gates = {key: 'not_run' for key in ('overall_calibration', 'overall_improvement', 'subgroups', 'coverage',
            'held_out_players', 'unseen_positions', 'game_phases', 'player_cluster_uncertainty',
            'representative_population_audit', 'production_performance', 'production_recovery')}
        gates.update(development_prediction_numerical='passed' if numerical else 'not_passed',
            calibration_aware_selection='passed' if selected_ok else 'not_passed',
            player_balance='passed' if all(g['player_balance_passed'] for g in groups) else 'not_passed',
            historical_partition_safety='passed', current_source_revision='passed' if revisions_match else 'stale')
        gates['development_bullet_decision_numerical'] = 'passed' if diagnostic_report.get('bullet', {}).get('status') == 'passed' else 'not_passed'
        implementation = hashlib.sha256(''.join(inspect.getsource(m) for m in
            (inspect.getmodule(execute), source, compute, diagnostics, model, refinement)).encode()).hexdigest()
        result = {'format': source.FORMAT, 'status': 'development_only', 'canonical': False,
            'production_changed': False, 'promotion_ready': False, 'backend': 'cuda_cupy', 'precision': 'float64',
            'config': config, 'source': frozen, 'hardware': hardware, 'gpu_memory': memory, 'fits': fits,
            'development_diagnostics': diagnostic_report,
            'gates': gates, 'original_quality_gates': original_gates, 'included_rows': frozen['cp_rows'],
            'implementation_sha256': implementation,
            'model_id': history.digest({'config': config, 'input': frozen['input_sha256'], 'fits': fits}),
            'timings': {'extraction_seconds': extraction_seconds, 'total_compute_seconds': time.monotonic()-started},
            'notes': ['New development-only candidate, not a replacement of the frozen bullet-only half-strength reference or live summaries.',
                'All eligible development games staged once; games without CP are removed before applying the 10% cap for either player. All inspected identities stay reserved. Deterministic feasible selection, not a guarantee of maximum subset size.',
                'RTX 4060 inputs contain multiple players together. GPU memory sizes each calculation batch, not the total dataset; oversized inputs stream in bounded chunks.',
                'Equal total weight per game, all Stockfish CP plies, original formula and coefficient. Regularization/strength selection uses validation only. Threshold .15 and mate exceptions are unchanged.',
                'No final-test or held-player outcomes loaded. More training cannot fill the previously observed missing test groups.',
                'Rapid drift and bullet changed-move diagnostics reuse staged inputs. They do not tune parameters or relax gates; exact contemporary Lichess rules have not been substituted.',
                'Numerical gate here checks CP predictions against an independent scalar sample; full move-decision, subgroup, population, recovery and performance approval still require separate validation.',
                'Historical role reservations survive failure/cancellation. Original research gates remain visible; none are silently relaxed.']}
        json.dumps(result, allow_nan=False)
        return result
    finally:
        xp.get_default_memory_pool().free_all_blocks()
        xp.get_default_memory_pool().set_limit(size=256*1024**2)
