"""Predeclared calibration-aware selection; never reads test outcomes.

The equation, k0 and inference rules are unchanged. CUDA reduces training losses
and validation sufficient statistics; the CPU controls the bounded search.
"""
import numpy as np

from . import blunder_v2_model as model

VERSION = 'calibration_refinement_v1'
LAMBDAS = (0., .0001, .001, .01, .03, .1, 1.)
STRENGTHS = (.25, .5, .75, 1.)


def policy():
    return {'version': VERSION, 'regularization_grid': list(LAMBDAS),
        'rating_strength_grid': list(STRENGTHS), 'validation_ece_limit': .03,
        'minimum_relative_mse_improvement': .01, 'retain_best_mse_gain_fraction': .5,
        'selection': 'lowest_mse_among_calibration_and_predictive_eligible_candidates',
        'test_policy': 'evaluate_once_only_if_all_modes_pass_validation_selection',
        'player_uncertainty': 'paired_player_connected_component_bootstrap',
        'population_audit': 'required_before_promotion_not_run'}


def validation_scores(xp, rows, games, mode, parameters, check=lambda: None):
    """One total weight per validation game, separate fixed bins per candidate.

    No test/held-out outcomes enter either the predictions or the reductions.
    Bounded to 29 simultaneous candidates and BATCH observations on device.
    """
    if not 1 <= len(parameters) <= len(LAMBDAS)*len(STRENGTHS)+1:
        raise ValueError('Refinement candidate budget exceeded.')
    params = xp.asarray(parameters, dtype=xp.float64)
    losses = xp.zeros(len(params), dtype=xp.float64)
    residuals = xp.zeros((len(params), 10), dtype=xp.float64)
    weight = 0.
    for block in model.chunks(rows):
        check()
        ids = block['game']
        keep = (games['mode'][ids] == mode) & (games['split'][ids] == 1) & (block['ak'] == 0)
        ids, scores = ids[keep], block['after'][keep]
        if not len(ids):
            continue
        weights = xp.asarray(1. / games['cp_count'][ids])
        predictions = model.expected(xp, xp.asarray(scores)[:, None],
            xp.asarray(games['u'][ids])[:, None], xp.asarray(games['d'][ids])[:, None],
            params[:, 0], params[:, 1])
        errors = predictions-xp.asarray(games['outcome'][ids])[:, None]
        losses += xp.sum(weights[:, None]*errors**2, axis=0)
        bins = xp.minimum(9, (predictions*10).astype(xp.int32))
        for index in range(len(params)):
            xp.add.at(residuals[index], bins[:, index], weights*errors[:, index])
        weight += float(xp.sum(weights))
    if not weight:
        raise ValueError('No validation CP observations for refinement.')
    ece = xp.sum(xp.abs(residuals), axis=1)/weight
    return [{'mse': float(loss/weight), 'ece': float(error)} for loss, error in zip(losses, ece)]


def select(trials, baseline):
    """All rules frozen before fitting; failed candidates remain diagnostic only."""
    usable = [i for i, t in enumerate(trials) if t['converged'] and not t['search_boundary']
              and all(np.isfinite(t[k]) for k in ('mse', 'ece', 'a', 'b'))]
    if not usable or not np.isfinite(baseline['mse']) or baseline['mse'] <= 0:
        return None, {'status': 'not_passed', 'reason': 'No valid converged candidate or baseline.'}
    rules = policy()
    best_gain = max(0., baseline['mse']-min(trials[i]['mse'] for i in usable))
    required_gain = max(rules['minimum_relative_mse_improvement']*baseline['mse'],
                        rules['retain_best_mse_gain_fraction']*best_gain)
    for t in trials:
        t['predictive_eligible'] = baseline['mse']-t['mse'] >= required_gain
        t['calibration_eligible'] = t['ece'] <= rules['validation_ece_limit']
    predictive = [i for i in usable if trials[i]['predictive_eligible']]
    eligible = [i for i in predictive if trials[i]['calibration_eligible']]
    def tie(i):
        t = trials[i]
        return (-t['regularization'], t['rating_strength'], i)
    if eligible:
        selected = min(eligible, key=lambda i: (trials[i]['mse'], trials[i]['ece'], tie(i)))
    else:
        selected = min(predictive or usable, key=lambda i: (trials[i]['ece'], trials[i]['mse'], tie(i)))
    return selected, {'status': 'passed' if eligible else 'not_passed', 'baseline': baseline,
        'required_mse_gain': required_gain, 'best_candidate_mse_gain': best_gain,
        'eligible_candidates': len(eligible), 'selected_trial': selected,
        'reason': 'Calibration and predictive requirements satisfied.' if eligible else
            'No candidate satisfies both requirements; selected parameters are diagnostic only. Final test withheld.'}


def fit_mode(xp, rows, games, mode, check=lambda: None):
    counts = [int(np.count_nonzero((games['mode'] == mode) & (games['split'] == split) &
                                  (games['cp_count'] > 0))) for split in range(4)]
    result = {'mode': model.MODES[mode], 'games_by_split': counts}
    # Only development support is a fitting prerequisite. Test support is audited
    # later; neither test outcomes nor test size selects a parameter candidate.
    if any(counts[i] < model.MIN_GAMES[i] for i in (0, 1)):
        return {**result, 'status': 'insufficient_data', 'selection': {'status': 'insufficient_data'}}
    objective = model.Objective(xp, rows, games, mode, 0, check)
    trials = []
    for value in LAMBDAS:
        check()
        fitted = model.fit(objective, value)
        for strength in STRENGTHS:
            trials.append({**fitted, 'unshrunk_a': fitted['a'], 'unshrunk_b': fitted['b'],
                'a': strength*fitted['a'], 'b': strength*fitted['b'], 'rating_strength': strength})
    metrics = validation_scores(xp, rows, games, mode,
        [[0., 0.]]+[[t['a'], t['b']] for t in trials], check)
    for trial, metric in zip(trials, metrics[1:]):
        trial.update(metric)
    index, selection = select(trials, metrics[0])
    if index is None:
        return {**result, 'status': 'not_converged', 'selection': selection, 'refinement_trials': trials}
    chosen = trials[index]
    return {**result, **chosen, 'status': 'fitted', 'validation_mse': chosen['mse'],
        'validation_ece': chosen['ece'], 'selection': selection, 'refinement_trials': trials}
