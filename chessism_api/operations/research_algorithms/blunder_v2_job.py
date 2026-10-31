"""Versioned candidate workflow. Only AlgorithmRun reports may be published."""
import asyncio
import hashlib
import inspect
import json
import time

import numpy as np

from ..matrix_constructor.artifact_files import file_operation
from ..matrix_constructor.defaults import default_formula
from . import blunder_v2_model as model, blunder_v2_source as source, blunder_v2_validation as validation
from . import blunder_v2_calibration as calibration
from .cuda_runtime import cuda_backend
from .storage import ensure_capacity, workspace

OPERATION = 'smooth_rating_blunder_v2'


def configuration(games_per_group=1000, sampling=None, refinement=None):
    if type(games_per_group) is not int or not 100 <= games_per_group <= 3000:
        raise ValueError('Choose 100–3,000 games per rating/mode group.')
    config = {'operation': OPERATION, 'implementation_version': 2, 'backend': 'cuda_cupy',
        'games_per_group': games_per_group, 'max_rows': source.MAX_ROWS,
        'columns': ['before', 'after', 'player_rating', 'opponent_rating', 'mode'],
        'matrix': default_formula('blunder-moves')['config'], 'canonical': False,
        'model': {'formula': 'sigmoid(k0*clip(cp,-1000,1000)*exp(a_mode*u)+b_mode*d)',
            'k0': model.K0, 'rating_anchor': 2300, 'rating_scale': 400, 'threshold': model.THRESHOLD,
            'a_bounds': [-1., 1.], 'b_bounds': [0., 4.], 'regularization_grid': list(model.LAMBDAS),
            'clock_aware': False, 'complexity_aware': False, 'mate_rules': 'existing_700cp_v1'},
        'validation': {'minimum_games': list(validation.MIN_SUPPORT), 'bootstrap_repeats': validation.BOOTSTRAPS,
            'prediction_tolerance': 1e-10, 'decision_mismatches': 0, 'overall_ece': .03,
            'group_ece': .05, 'maximum_relative_mse_regression': .02,
            'split': 'chronological_70_15_15_plus_disjoint_player_holdout'}}
    if sampling is not None:
        from .blunder_balanced_sampling import plan
        if sampling.get('version') == 'development_reuse_v1':
            from .blunder_development_reuse import plan
            if refinement is None:
                raise ValueError('Development reuse requires the calibration refinement experiment.')
        if sampling!=plan(sampling.get('references')):
            raise ValueError('Unsupported balanced sampling instructions.')
        config['sampling']=sampling
        config['validation']['split']='frozen_chronology_disjoint_players_verified_prior_game_exclusion'
    if refinement is not None:
        from .blunder_refinement import policy
        if sampling is None or refinement != policy():
            raise ValueError('Refinement requires verified balanced sampling and fixed selection rules.')
        config['implementation_version'] = 3
        config['refinement'] = refinement
        config['model']['regularization_grid'] = refinement['regularization_grid']
        if sampling['version'] == 'development_reuse_v1':
            config['implementation_version'] = 4
    return config


def cancellation_check(reporter, loop):
    updated = 0.
    deadline = time.monotonic() + 1800
    def check():
        nonlocal updated
        if time.monotonic() >= deadline:
            raise TimeoutError('Candidate fitting/validation exceeded the 30-minute research budget.')
        if time.monotonic()-updated >= 1:
            asyncio.run_coroutine_threadsafe(reporter.check(), loop).result(timeout=30)
            updated = time.monotonic()
    return check


async def execute(config, reporter):
    if config != configuration(config.get('games_per_group'),config.get('sampling'),config.get('refinement')):
        raise ValueError('Unsupported or modified smooth rating-aware instructions.')
    await reporter.check()
    await reporter.progress('checking_gpu', detail='CUDA candidate v2; no CPU fallback and no live summary changes.')
    started = time.perf_counter()
    xp, hardware = await file_operation(cuda_backend, 0)
    ensure_capacity(config)
    check = cancellation_check(reporter, asyncio.get_running_loop())
    with workspace(reporter.run_id) as folder:
        path = folder / 'transitions.bin'
        with path.open('xb') as stream:
            metadata, games, frozen = await asyncio.wait_for(source.freeze(config, reporter, stream), timeout=600)
        if config.get('sampling', {}).get('version') == 'development_reuse_v1':
            from . import blunder_development_reuse
            await blunder_development_reuse.checkpoint(frozen, reporter)
        rows = np.memmap(path, dtype=model.DTYPE, mode='r')
        try:
            fitting_started = time.perf_counter()
            fits = []
            fitter = model.fit_mode
            if config.get('refinement'):
                from . import blunder_refinement, blunder_refinement_audit
                fitter = blunder_refinement.fit_mode
            for index, mode in enumerate(model.MODES):
                await reporter.progress('fitting_smooth_rating_model', index, 3,
                    f'Fitting {mode}: fixed baseline; validation selects calibration and prediction tradeoffs.' if config.get('refinement') else
                    f'Fitting {mode}: fixed baseline, two rating parameters; validation selects regularization.')
                fits.append(await file_operation(fitter, xp, rows, games, index, check))
            fitting = time.perf_counter()-fitting_started
            evaluate_test = not config.get('refinement') or all(f.get('selection', {}).get('status') == 'passed' for f in fits)
            accumulator = await file_operation(validation.Validation, xp, rows, games, metadata, fits,
                (1, 2, 3) if evaluate_test else (1,))
            for index, block in enumerate(model.chunks(rows)):
                await reporter.check()
                await file_operation(accumulator.consume, block)
                await reporter.progress('checking_v2_candidate', min((index+1)*model.BATCH, len(rows)), len(rows),
                    'Checking CPU/CUDA decisions; comparing old15, baseline20 and rating-aware20.' if evaluate_test else
                    'Validation selection did not pass; checking development data only. Final test withheld.')
            await reporter.progress('evaluating_v2_gates', detail='Whole-game confidence intervals, calibration, subgroup and held-out-player checks.')
            report = await file_operation(accumulator.finish, check)
            if config.get('refinement'):
                report['refinement'] = {'policy': config['refinement'],
                    'selection_status': 'passed' if evaluate_test else 'not_passed',
                    'test_evaluated': evaluate_test,
                    'development': await file_operation(blunder_refinement_audit.development, accumulator)}
                report['gates']['calibration_aware_selection'] = 'passed' if evaluate_test else 'not_passed'
                report['gates']['representative_population_audit'] = 'not_run'
                if evaluate_test:
                    report['player_cluster_uncertainty'] = {name: await file_operation(
                        blunder_refinement_audit.player_intervals, xp, accumulator.metrics[0], metadata,
                        games['split'] == split, check) for name, split in (('test', 2), ('held_out_players', 3))}
                    report['gates']['player_cluster_uncertainty'] = 'passed' if all(
                        r['status'] == 'estimated' for r in report['player_cluster_uncertainty'].values()) else 'insufficient_data'
                else:
                    report['test'] = report['held_out_players'] = {'status': 'not_run', 'reason': 'Validation selection did not pass; test outcomes were not evaluated.'}
                    report['blunder_comparison'] = []
                    report['disagreement_examples'] = []
                    report['strata'] = []
                    report['audits'] = {}
                    report['calibration_cross_groups'] = []
                    for key in ('overall_improvement', 'overall_calibration', 'held_out_players', 'subgroups',
                                'coverage', 'unseen_positions', 'game_phases', 'player_cluster_uncertainty'):
                        report['gates'][key] = 'not_run'
            del accumulator
        finally:
            rows._mmap.close()
    await reporter.check()
    modules=[model, source, validation, calibration, inspect.getmodule(execute)]
    if config.get('refinement'):
        modules.extend([blunder_refinement, blunder_refinement_audit])
    if config.get('sampling'):
        from . import blunder_balanced_source, blunder_balanced_sampling
        modules.extend([blunder_balanced_source,blunder_balanced_sampling])
        balance=frozen['sampling_audit']['cp_concentration']
        report['gates']['player_balance']='passed' if all(r['status']=='passed' for r in balance) else 'not_passed'
        if config['sampling']['version'] == 'development_reuse_v1':
            modules.append(blunder_development_reuse)
            report['gates']['historical_partition_safety'] = 'passed' if all(
                frozen['sampling_audit'][key] == 0 for key in ('protected_game_overlap', 'test_prior_overlap', 'role_violations')) else 'failed'
        else:
            report['gates']['prior_cohort_overlap']='passed' if frozen['sampling_audit']['prior_game_overlap']==0 else 'failed'
    implementation = hashlib.sha256(''.join(inspect.getsource(module) for module in modules).encode()).hexdigest()
    identity = hashlib.sha256(json.dumps({'config': config, 'input': frozen['sha256'],
        'implementation': implementation, 'fits': fits}, sort_keys=True).encode()).hexdigest()
    report.update(format='chessism_rating_blunder_candidate_v2', algorithm='rating-aware-blunders-v2',
        status='candidate' if report['cpu_cuda_passed'] else 'insufficient_data' if not report['checked_moves'] else 'numerical_mismatch',
        canonical=False, production_changed=False, backend='cuda_cupy', precision='float64',
        model_id=identity, implementation_sha256=implementation, config=config, hardware=hardware,
        source=frozen, fits=fits, included_rows=frozen['positions'])
    if config.get('refinement') and not evaluate_test:
        report['status'] = 'development_only'
    report['timings'].update(extraction_seconds=frozen['extraction_seconds'], fitting_seconds=fitting,
        total_compute_seconds=time.perf_counter()-started)
    report['notes'] = [
        'Candidate only. No live blunder, accuracy, salience, ingestion or canonical writer changes.',
        'Expected game points, not probability of winning; historical ratings and mode only. No clock or complexity inputs.',
        'k0 remains fixed; exp(a*u) changes effective evaluation sensitivity. a is fitted, not assumed positive; b is nonnegative.',
        'CP loss cutoff 0.20; existing forced-mate exceptions retained. +105 CP to forced mate against is a blunder.',
        'Unique standard-chess games; all scored CP plies fitted with equal total weight per game. Outcomes are training labels only.',
        'Timeouts and larger rating gaps included and audited. Missing ratings/dates, invalid results, mate/tablebase/unknown CP provenance do not become fitted CP labels.',
        'Chronological partitions never split timestamp ties. Any held-out player is excluded from training and validation; only their later games are tested.',
        'Sparse groups remain unvalidated. Candidate counts are research outputs, not approved rating-aware summaries.',
        'Aggregate metrics describe the stratified sampled cohort, not a population-weighted estimate for all games.',
        'Whole-game bootstrap does not fully account for dependence between different games of the same player; player-held-out and unseen-position audits are separate checks.',
        'Calibration compares both models on identical CP rows with equal total game weight. Threshold changes do not affect calibration.',
        'Calibration intervals are paired whole-game percentile bootstrap estimates for fixed fitted parameters; they are exploratory, not bias-corrected or simultaneous subgroup guarantees. Sparse bins and shared players limit precision.',
        'Five-bin calibration is a sensitivity check only; ten-bin calibration and the original acceptance thresholds remain unchanged. Cross-groups are exploratory, not new promotion gates.',
        'No complete production performance or recovery test was run. Promotion remains disabled even if statistical gates pass.']
    if config.get('sampling'):
        report['notes'].extend(frozen['sampling_audit']['limitations'])
        report['notes'].append('Balanced sampling: both players count toward a 10% mode/partition cap; fixed chronological boundaries. Role-locked development reuse is audited separately from fresh test isolation. Game identities are retained; position arrays remain temporary.' if config['sampling']['version'] == 'development_reuse_v1' else
            'Balanced sampling v1: both players count toward a 10% mode/partition cap; fixed chronological boundaries and verified exclusion of previously evaluated v2 cohorts. Game identities are retained for future exclusion, but position arrays remain temporary.')
    if config.get('refinement'):
        report['notes'].extend([
            'Refinement v1: 7 training regularizers × 4 rating strengths; validation only selects. k0, CP clipping, 0.20 threshold and mate rules are unchanged.',
            'Selection requires validation ECE <=0.03 and at least 1% baseline MSE improvement while retaining half the best valid candidate gain. Lowest eligible MSE wins. No qualifying candidate means final test is withheld, not relaxed gates.',
            'Development diagnostics are selected-on data, not acceptance evidence. Reused training/validation games stay in their roles; all known games remain excluded from future final tests. Historical test reservations and ambiguous roles are blocked everywhere. Source roles are saved before fitting and survive failures/cancellation.' if config.get('sampling', {}).get('version') == 'development_reuse_v1' else
            'Development diagnostics are selected-on data, not acceptance evidence. All inspected source identities remain excluded from future cohorts, even when the final test is withheld.',
            'Player-connected component intervals keep both players together; insufficient independent components are reported honestly. Whole-game intervals alone do not establish independent-player precision.',
            'Representative-population validation remains not run and blocks promotion. The balanced cohort alone does not establish deployment-wide calibration.'])
    json.dumps(report, allow_nan=False)
    return report
