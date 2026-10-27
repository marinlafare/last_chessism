"""Paired calibration diagnostics; no model fitting or promotion decisions.

Each input row is one game's normalized sufficient statistics. Reusing the
same whole-game resamples for both models preserves their paired comparison.
The percentile intervals describe this cohort with fixed fitted parameters;
they are not bias-corrected or independent-player/population guarantees.
"""
import numpy as np

METRIC_COLUMNS = 63  # count, two squared losses, 2 * (10 weights/predictions/outcomes)


def statistics(xp, normalized, offset, bins=10):
    values = normalized[:, offset:offset+30].reshape(len(normalized), 3, bins, 10//bins)
    return xp.sum(values, axis=3)


def describe(xp, values):
    totals = xp.sum(values, axis=0)
    weights, predictions, outcomes = totals
    residuals = predictions-outcomes
    count = len(values)
    bins = []
    for i in range(len(weights)):
        weight = float(weights[i])
        if weight <= 0:
            continue
        bins.append({'bin': i, 'game_weight': weight,
            'games_contributing': int(xp.count_nonzero(values[:, 0, i])),
            'predicted': float(predictions[i]/weight), 'observed': float(outcomes[i]/weight),
            'signed_gap': float(residuals[i]/weight),
            'ece_contribution': float(xp.abs(residuals[i])/count)})
    return float(xp.sum(xp.abs(residuals))/count), bins


def paired_intervals(xp, candidate, baseline, repeats, check):
    rng = np.random.default_rng(20261028)
    # Only residuals are needed: bin weights cancel in weighted absolute error.
    residuals = xp.stack((candidate[:, 1]-candidate[:, 2], baseline[:, 1]-baseline[:, 2]), axis=1)
    samples = []
    for start in range(0, repeats, 8):
        check()
        ids = xp.asarray(rng.integers(0, len(residuals), size=(min(8, repeats-start), len(residuals))))
        samples.append(xp.sum(xp.abs(xp.mean(residuals[ids], axis=1)), axis=2))
    values = xp.concatenate(samples)
    columns = xp.column_stack((values, values[:, 0]-values[:, 1]))
    bounds = xp.percentile(columns, xp.asarray([2.5, 97.5]), axis=0)
    return {key: [float(bounds[0, i]), float(bounds[1, i])] for i, key in enumerate(
        ('ece_ci95', 'baseline_ece_ci95', 'ece_change_ci95'))}


def compare(xp, normalized, repeats, check=lambda: None):
    candidate, baseline = (statistics(xp, normalized, start) for start in (2, 32))
    ece, bins = describe(xp, candidate)
    base_ece, base_bins = describe(xp, baseline)
    coarse = [describe(xp, statistics(xp, normalized, start, bins=5))[0] for start in (2, 32)]
    result = {'ece': ece, 'calibration': bins, 'baseline_ece': base_ece,
        'baseline_calibration': base_bins, 'ece_change': ece-base_ece,
        'bin_sensitivity': {'bins_5': {'ece': coarse[0], 'baseline_ece': coarse[1]},
                            'bins_10': {'ece': ece, 'baseline_ece': base_ece}},
        'calibration_uncertainty': {'method': 'paired_whole_game_percentile_bootstrap',
            'repeats': repeats, 'fixed_fitted_parameters': True,
            'status': 'estimated' if len(normalized) >= 20 else 'insufficient_data'}}
    if len(normalized) >= 20:
        result['calibration_uncertainty'].update(paired_intervals(xp, candidate, baseline, repeats, check))
    return result


def player_dependence(metadata, selected):
    """Identity-only audit: neither model gains any new player-name features."""
    games = [g for g, keep in zip(metadata, selected) if keep]
    if not games or any(not g.get('white_name') or not g.get('black_name') for g in games):
        return {'status': 'unavailable'}
    counts = {}
    for game in games:
        for name in {game['white_name'].strip().lower(), game['black_name'].strip().lower()}:
            counts[name] = counts.get(name, 0)+1
    largest = max(counts.values())
    return {'games': len(games), 'unique_players': len(counts),
        'most_games_involving_one_player': largest, 'largest_player_game_share': largest/len(games),
        'status': 'descriptive_only', 'independent_player_interval': False}
