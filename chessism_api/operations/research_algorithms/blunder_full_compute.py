"""Memory-sized RTX fitting: resident inputs where possible, streaming otherwise."""
import math
import time

import numpy as np

from . import blunder_v2_model as model, blunder_refinement as refinement

GIB = 1024**3


def memory_plan(free, total):
    reserve = 512*1024**2
    limit = min(total-reserve, free-reserve)
    if limit < 512*1024**2:
        raise ValueError('Not enough free GPU memory above the CUDA runtime reserve; no CPU fallback.')
    return {'free_bytes_at_start': free, 'total_bytes': total, 'runtime_reserve_bytes': reserve,
            'pool_limit_bytes': limit}


class Input:
    def __init__(self, xp, path, expected_rows, budget, check):
        self.xp, self.check, self.budget = xp, check, budget
        if path.stat().st_size != expected_rows*40 or not expected_rows:
            raise ValueError('Missing or truncated compact CUDA input.')
        self.host = np.memmap(path, mode='r', dtype='<f8', shape=(expected_rows, 5))
        self.device = None
        if hasattr(xp, 'cuda') and self.host.nbytes <= budget//2:
            try:
                self.device = xp.empty(self.host.shape, dtype=xp.float64)
                for start in range(0, len(self.host), 262144):
                    check(); self.device[start:start+262144] = xp.asarray(self.host[start:start+262144])
                xp.cuda.get_current_stream().synchronize()
            except xp.cuda.memory.OutOfMemoryError:
                self.device = None; xp.get_default_memory_pool().free_all_blocks()
        self.batch = self.batch_size(32)

    def batch_size(self, candidates):
        used = self.xp.get_default_memory_pool().used_bytes() if hasattr(self.xp, 'cuda') else 0
        available = self.budget-used-128*1024**2
        # Conservative allowance for simultaneous float64 temporaries, indices,
        # transfers and reductions. Rounded to chunks; allocation failures shrink.
        size = max(32768, available//(candidates*96+256))
        return min(4_194_304, size//32768*32768)

    def blocks(self):
        for start in range(0, len(self.host), self.batch):
            self.check()
            yield self.device[start:start+self.batch] if self.device is not None else self.xp.asarray(self.host[start:start+self.batch])

    def metrics(self, parameters, *, calibration=False):
        xp = self.xp
        if not 1 <= len(parameters) <= 32 or not np.isfinite(parameters).all():
            raise ValueError('Invalid fitting parameters.')
        while True:
            try:
                return self._metrics(parameters, calibration)
            except Exception as error:
                if not hasattr(xp, 'cuda') or not isinstance(error, xp.cuda.memory.OutOfMemoryError) or self.batch <= 32768:
                    raise
                self.batch = max(32768, self.batch//2//32768*32768)
                xp.get_default_memory_pool().free_all_blocks()

    def _metrics(self, parameters, calibration):
        xp = self.xp
        params = xp.asarray(parameters, dtype=xp.float64)
        losses = xp.zeros(len(params), dtype=xp.float64)
        residuals = xp.zeros((len(params), 10), dtype=xp.float64) if calibration else None
        weight = xp.asarray(0., dtype=xp.float64)
        for block in self.blocks():
            p = model.expected(xp, block[:, :1], block[:, 1:2], block[:, 2:3], params[:, 0], params[:, 1])
            errors = p-block[:, 3:4]
            losses += xp.sum(block[:, 4:5]*errors**2, axis=0)
            weight += xp.sum(block[:, 4])
            if calibration:
                bins = xp.minimum(9, (p*10).astype(xp.int32))
                for index in range(len(params)):
                    xp.add.at(residuals[index], bins[:, index], block[:, 4]*errors[:, index])
        if float(weight) <= 0:
            raise ValueError('No training/validation weight.')
        mse = np.array([float(v/weight) for v in losses])
        if not calibration:
            return mse
        ece = xp.sum(xp.abs(residuals), axis=1)/weight
        return [{'mse': float(m), 'ece': float(e)} for m, e in zip(mse, ece)]

    def close(self):
        self.device = None
        self.host._mmap.close()
        if hasattr(self.xp, 'cuda'):
            self.xp.get_default_memory_pool().free_all_blocks()


def oracle_check(xp, data, parameters):
    sample = np.asarray(data.host[np.linspace(0, len(data.host)-1, min(4096, len(data.host)), dtype=int)])
    errors = []
    for a, b in parameters:
        cpu = np.array([.5+.5*math.tanh((model.K0*max(-1000., min(1000., cp))*math.exp(a*u)+b*d)/2)
                        for cp, u, d, _, _ in sample])
        gpu = model.expected(xp, xp.asarray(sample[:, 0]), xp.asarray(sample[:, 1]), xp.asarray(sample[:, 2]), a, b)
        errors.append(float(xp.max(xp.abs(gpu-xp.asarray(cpu)))))
    maximum = max(errors, default=0.)
    if maximum > 1e-10:
        raise ValueError('Independent scalar/CUDA prediction mismatch.')
    return {'sampled_cp_observations': len(sample), 'max_prediction_error': maximum}


class CachedObjective:
    """Reuse exact losses for repeated search points across regularizers."""
    def __init__(self, data, notify=lambda count: None):
        self.data, self.notify, self.cache = data, notify, {}
        self.calculated = 0; self.updated = 0.

    def __call__(self, parameters):
        keys = [tuple(map(float, row)) for row in parameters]
        missing = list(dict.fromkeys(key for key in keys if key not in self.cache))
        if missing:
            losses = self.data.metrics(missing)
            self.calculated += len(missing)
            self.cache.update(zip(missing, losses))
        result = np.array([self.cache[k] for k in keys])
        if len(self.cache) > 20000:
            self.cache.clear()
        if time.monotonic()-self.updated >= 3:
            self.notify(self.calculated); self.updated = time.monotonic()
        return result


def fit_mode(xp, folder, groups, mode, frozen_fit, budget, check, progress):
    records = [next(g for g in groups if g['split'] == s and g['mode'] == mode) for s in (0, 1)]
    if any(g['games'] < minimum for g, minimum in zip(records, (500, 100))):
        return {'mode': mode, 'status': 'insufficient_data', 'selection': {'status': 'insufficient_data'}, 'groups': records}
    train = valid = None; started = time.monotonic()
    try:
        train = Input(xp, folder/f'0-{mode}.bin', records[0]['cp_rows'], budget, check)
        current_trial = 0
        objective = CachedObjective(train, lambda count: progress(current_trial, len(refinement.LAMBDAS),
            f'{mode}: {records[0]["games"]:,} training games; {count:,} parameter evaluations on CUDA. Batch up to {train.batch:,} CP observations.'))
        trials = []
        for index, regularization in enumerate(refinement.LAMBDAS):
            current_trial = index
            progress(index, len(refinement.LAMBDAS), f'{mode}: fitting all {records[0]["games"]:,} selected games on CUDA; batches up to {train.batch:,} CP observations.')
            fitted = model.fit(objective, regularization)
            for strength in refinement.STRENGTHS:
                trials.append({**fitted, 'unshrunk_a': fitted['a'], 'unshrunk_b': fitted['b'],
                    'a': strength*fitted['a'], 'b': strength*fitted['b'], 'rating_strength': strength})
        resident = train.device is not None; batch = train.batch
        parameters = [[0., 0.], [frozen_fit['a'], frozen_fit['b']]] + [[t['a'], t['b']] for t in trials]
        numerical = oracle_check(xp, train, parameters)
        train.close(); train = None
        valid = Input(xp, folder/f'1-{mode}.bin', records[1]['cp_rows'], budget, check)
        progress(7, 7, f'{mode}: CUDA validation of fixed candidates; final test remains untouched.')
        metrics = valid.metrics(parameters, calibration=True)
        for trial, metric in zip(trials, metrics[2:]): trial.update(metric)
        index, selection = refinement.select(trials, metrics[0])
        chosen = trials[index] if index is not None else {}
        return {'mode': mode, 'status': 'fitted' if index is not None else 'not_converged',
            **chosen, 'selection': selection, 'refinement_trials': trials, 'groups': records,
            'frozen_comparison': {**frozen_fit, **metrics[1]}, 'baseline': metrics[0],
            'numerical': numerical, 'training_resident_on_gpu': resident,
            'training_batch_rows': batch, 'validation_batch_rows': valid.batch,
            'distinct_training_parameter_evaluations': objective.calculated,
            'seconds': time.monotonic()-started}
    finally:
        if train is not None: train.close()
        if valid is not None: valid.close()
