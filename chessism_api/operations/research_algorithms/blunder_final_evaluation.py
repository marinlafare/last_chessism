"""One-shot held-out evaluation of an immutable, already fitted candidate.

The only exception is permission to evaluate despite rapid's development
calibration/coverage limitations. No failed quality gate becomes a pass, no
parameters are fitted, and no production summaries are changed.
"""
import asyncio
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
from . import blunder_development_reuse as reuse, blunder_balanced_source as balanced_source
from . import blunder_v2_source as source, blunder_v2_model as model
from . import blunder_v2_validation as validation, blunder_refinement_audit as audit
from .blunder_v2_job import configuration as training_configuration, cancellation_check
from .cuda_runtime import cuda_backend
from .storage import ensure_capacity, workspace

OPERATION = 'frozen_rating_blunder_final_evaluation_v1'
RESERVATION_FORMAT = 'chessism_blunder_final_evaluation_reservation_v1'


def policy():
    return {'version': 'rapid_provisional_evaluation_v1', 'exception_modes': ['rapid'],
        'acknowledged_limitations': ['development_calibration', 'sparse_test_coverage'],
        'authorization': 'evaluate_frozen_candidate_not_publish_live',
        'parameters': 'immutable_parent_fits_no_refitting',
        'test': 'original_reserved_test_and_player_holdout_once',
        'quality_gates': 'unchanged_failures_remain_visible',
        'canonical': False}


def validate_parent(row):
    if row is None or row['status'] != 'complete':
        raise ValueError('A completed development candidate is required.')
    report = row['result'] or {}
    config = row['config']
    if (config.get('operation') != 'smooth_rating_blunder_v2'
            or config.get('sampling', {}).get('version') != reuse.VERSION
            or config != training_configuration(config.get('games_per_group'), config.get('sampling'), config.get('refinement'))
            or report.get('config') != config or report.get('format') != reuse.REPORT_FORMAT
            or report.get('status') != 'development_only'
            or report.get('refinement', {}).get('test_evaluated') is not False
            or report.get('canonical') is not False or report.get('production_changed') is not False):
        raise ValueError('Only an unevaluated, role-verified development candidate is supported.')
    if any(report.get('gates', {}).get(k) != 'passed' for k in ('numerical', 'mathematical', 'search', 'historical_partition_safety')):
        raise ValueError('Numerical, mathematical, search and source-safety checks must already pass.')
    fits = report.get('fits', [])
    if [f.get('mode') for f in fits] != list(model.MODES):
        raise ValueError('The frozen fit must contain all three modes in order.')
    for fit in fits:
        if (fit.get('status') != 'fitted' or fit.get('search_boundary') is not False
                or not all(isinstance(fit.get(k), (int, float)) and math.isfinite(fit[k]) for k in ('a', 'b'))
                or not -1 <= fit['a'] <= 1 or not 0 <= fit['b'] <= 4):
            raise ValueError('Invalid or unfitted frozen parameters.')
        if fit['mode'] != 'rapid' and fit.get('selection', {}).get('status') != 'passed':
            raise ValueError('This exception applies to rapid only; bullet and blitz must qualify.')
    groups = reuse.split_ids(report['source'])
    roles, _ = reuse.verified_manifest(report['source'])
    if not groups['0'] <= roles['training'] or not groups['1'] <= roles['validation'] or not (groups['2'] | groups['3']) <= roles['protected']:
        raise ValueError('Frozen partition roles disagree.')
    if not groups['2'] or not groups['3']:
        raise ValueError('The candidate has no reserved primary or held-player test.')
    return report


def configuration(parent_id, row):
    parent_id = str(uuid.UUID(str(parent_id)))
    report = validate_parent(row)
    return {'operation': OPERATION, 'implementation_version': 1, 'backend': 'cuda_cupy',
        'parent_run_id': parent_id, 'parent_report_sha256': reuse.digest(report),
        'model_id': report['model_id'], 'input_sha256': report['source']['sha256'],
        'fits_sha256': reuse.digest(report['fits']), 'policy': policy(),
        'max_rows': source.MAX_ROWS, 'columns': list(row['config']['columns']), 'canonical': False}


async def load_parent(session, config):
    row = (await session.execute(text('SELECT status,config,result FROM algorithm_run WHERE id=:id'),
        {'id': config['parent_run_id']})).mappings().one_or_none()
    if config != configuration(config['parent_run_id'], row):
        raise ValueError('Frozen candidate or evaluation instructions changed.')
    return row['result']


def verify_replay(parent, metadata, frozen):
    expected = parent['source']
    actual_ids = {str(s): [g['link'] for g in metadata if g['split'] == s] for s in range(4)}
    for key in ('sha256', 'positions', 'selected_games', 'split', 'selection_sql_sha256',
                'seen_game_ids_sha256', 'development_roles_sha256'):
        if frozen.get(key) != expected.get(key):
            raise ValueError(f'Frozen input changed ({key}); no test predictions were evaluated.')
    if actual_ids != expected['game_ids_by_split']:
        raise ValueError('Frozen test identities changed; no test predictions were evaluated.')


async def freeze(config, reporter, stream, *, session_factory=None):
    started = time.perf_counter()
    async with (session_factory or AsyncDBSession)() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='60s'"))
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            raise ValueError('Wait for the active cloud workflow before final evaluation.')
        parent = await load_parent(session, config)
        # Historical instructions, not a new sample. Full metadata + position
        # bytes and partition identities must match before any test prediction.
        metadata, split, frozen, query = await reuse.select(session, parent['config'], reporter, verify_current_history=False)
        if sum(g['total_positions'] for g in metadata) > source.MAX_ROWS:
            raise ValueError('Frozen replay exceeds its three-million-row bound.')
        metadata = await balanced_source.retain_cp_balance(session, metadata, parent['config'], frozen, reporter)
        counts, total, sha = await source.read_positions(session, metadata, reporter, stream, phase='verifying_frozen_final_input')
        frozen.update(sha256=sha, positions=total, selected_games=len(metadata), split=split,
            selection_sql_sha256=hashlib.sha256(query.encode()).hexdigest())
        verify_replay(parent, metadata, frozen)
    output = deepcopy(parent['source'])
    output['extraction_seconds'] = time.perf_counter()-started
    output['replay_verified'] = True
    return parent, metadata, source.game_arrays(metadata, counts), output


async def claim(config, parent, reporter, *, session_factory=None):
    """Durably mark tests consumed before metrics, including failed attempts."""
    await reporter.check()
    async with (session_factory or AsyncDBSession)() as session, session.begin():
        # Lock the parent to serialize attempts; its contents remain unchanged.
        await session.execute(text('SELECT id FROM algorithm_run WHERE id=:id FOR UPDATE'), {'id': config['parent_run_id']})
        await load_parent(session, config)
        consumed = await session.scalar(text('''SELECT EXISTS(SELECT 1 FROM algorithm_run
            WHERE config->>'operation'=:operation AND config->>'parent_run_id'=:parent
              AND result->'final_evaluation'->>'test_started'='true')'''),
            {'operation': OPERATION, 'parent': config['parent_run_id']})
        if consumed:
            raise ValueError('This reserved test has already been consumed; inspect its saved evaluation rather than rerun it.')
        payload = {'format': RESERVATION_FORMAT, 'canonical': False, 'production_changed': False,
            'source': parent['source'], 'final_evaluation': {**config, 'test_started': True, 'test_completed': False}}
        saved = await session.scalar(text('''UPDATE algorithm_run SET result=CAST(:result AS jsonb)
            WHERE id=:id AND status='running' AND NOT cancel_requested RETURNING id'''),
            {'id': reporter.run_id, 'result': json.dumps(payload, allow_nan=False)})
        if saved is None:
            from .repository import RunCancelled
            raise RunCancelled('Cancelled before final evaluation; test predictions were not calculated.')


def complete_report(report, parent, config, frozen, hardware, implementation):
    report.update(format=reuse.REPORT_FORMAT, algorithm='rating-aware-blunders-v2',
        status='frozen_final_evaluation', canonical=False, production_changed=False, promotion_ready=False,
        backend='cuda_cupy', precision='float64', model_id=parent['model_id'],
        implementation_sha256=implementation, config=config, hardware=hardware,
        source=frozen, fits=deepcopy(parent['fits']), included_rows=frozen['positions'])
    report['refinement'] = {**deepcopy(parent['refinement']), 'test_evaluated': True}
    report['final_evaluation'] = {**deepcopy(config), 'test_started': True, 'test_completed': True,
        'source_verified': True, 'refitted': False,
        'parent_implementation_sha256': parent['implementation_sha256']}
    for key in ('calibration_aware_selection', 'player_balance', 'historical_partition_safety'):
        report['gates'][key] = parent['gates'][key]
    report['gates']['representative_population_audit'] = 'not_run'
    report['notes'] = [
        'Frozen final evaluation only. Formula, selected parameters, 0.00368208 coefficient, 0.20 threshold and mate rules unchanged. No refitting.',
        'Rapid development calibration and sparse test coverage were provisionally acknowledged before evaluating test outcomes. This permits evaluation, not automatic publication.',
        'Original calibration, coverage and numerical gates remain unchanged. An exception is not a passed gate; bullet and blitz received no exception.',
        'Exact source fingerprint and saved partition identities verified. Original parent report is unchanged. Test consumption is saved before metrics and survives failure/cancellation.',
        'Development diagnostics were copied from the parent; independent evidence is in the test and held-player results.',
        'Balanced cohort, not a population-weighted estimate. Sparse rapid data and repeated players limit precision; expected game points are not win probabilities.',
        'No live blunder, accuracy, salience or summary writer changes. Population, performance and recovery checks still block automatic promotion.']
    return report


async def execute(config, reporter):
    await reporter.check()
    started = time.perf_counter()
    await reporter.progress('checking_gpu', detail='Frozen final evaluation; no refitting and no live summary changes.')
    xp, hardware = await file_operation(cuda_backend, 0)
    ensure_capacity(config)
    check = cancellation_check(reporter, asyncio.get_running_loop())
    with workspace(reporter.run_id) as folder:
        path = folder / 'transitions.bin'
        with path.open('xb') as stream:
            parent, metadata, games, frozen = await asyncio.wait_for(freeze(config, reporter, stream), timeout=600)
        await claim(config, parent, reporter)
        rows = np.memmap(path, dtype=model.DTYPE, mode='r')
        try:
            accumulator = await file_operation(validation.Validation, xp, rows, games, metadata, parent['fits'], (2, 3))
            for index, block in enumerate(model.chunks(rows)):
                await reporter.check()
                await file_operation(accumulator.consume, block)
                await reporter.progress('evaluating_frozen_test', min((index+1)*model.BATCH, len(rows)), len(rows),
                    'Untouched test and held-out players only; frozen parameters, CPU/CUDA agreement and unchanged gates.')
            await reporter.progress('evaluating_final_gates', detail='Calibration, subgroup and repeated-player uncertainty; no tuning.')
            report = await file_operation(accumulator.finish, check)
            report['player_cluster_uncertainty'] = {name: await file_operation(audit.player_intervals,
                xp, accumulator.metrics[0], metadata, games['split'] == split, check)
                for name, split in (('test', 2), ('held_out_players', 3))}
            report['gates']['player_cluster_uncertainty'] = 'passed' if all(
                v['status'] == 'estimated' for v in report['player_cluster_uncertainty'].values()) else 'insufficient_data'
            del accumulator
        finally:
            rows._mmap.close()
    await reporter.check()
    from . import blunder_balanced_sampling, blunder_v2_calibration
    modules = (inspect.getmodule(execute), model, source, validation, audit, reuse,
               balanced_source, blunder_balanced_sampling, blunder_v2_calibration)
    implementation = hashlib.sha256(''.join(inspect.getsource(m) for m in modules).encode()).hexdigest()
    complete_report(report, parent, config, frozen, hardware, implementation)
    report['timings'].update(extraction_seconds=frozen['extraction_seconds'], fitting_seconds=0.,
        total_compute_seconds=time.perf_counter()-started)
    json.dumps(report, allow_nan=False)
    return report
