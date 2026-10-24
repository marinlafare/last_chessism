"""Typed, bounded working frames and pure vectorized step operations."""

from dataclasses import dataclass

import numpy as np

from .builder_schema import MAX_GROUPS
from .expressions import evaluate, missing


@dataclass
class Frame:
    columns: dict
    schema: dict
    row_keys: np.ndarray

    @property
    def rows(self):
        return len(self.row_keys)

    def take(self, indices):
        return Frame({key: value[indices] for key, value in self.columns.items()}, dict(self.schema), self.row_keys[indices])


def cell(value):
    if value is None:
        return None
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return None
    return value.item() if isinstance(value, np.generic) else value


def preview(frame, limit=8, columns=None):
    selected = columns or list(frame.schema)
    return {'columns': selected, 'types': [frame.schema[key] for key in selected],
            'rows': [[cell(frame.columns[key][i]) for key in selected] for i in range(min(limit, frame.rows))],
            'row_keys': [str(key) for key in frame.row_keys[:limit]], 'total_rows': frame.rows,
            'truncated': frame.rows > limit}


def prepare(frame, policy):
    masks = {key: missing(value) for key, value in frame.columns.items()}
    affected = np.logical_or.reduce(list(masks.values()))
    counts = {key: int(mask.sum()) for key, mask in masks.items()}
    if policy == 'drop_rows':
        return frame.take(~affected), counts, int(affected.sum()), 0
    if policy == 'column_mean':
        columns = dict(frame.columns)
        imputed = np.zeros(frame.rows, dtype=bool)
        for key, kind in frame.schema.items():
            if kind == 'number' and masks[key].any():
                valid = columns[key][~masks[key]]
                if not len(valid):
                    raise ValueError(f'Cannot impute {key}: no finite values.')
                columns[key] = np.where(masks[key], valid.mean(), columns[key])
                imputed |= masks[key]
        return Frame(columns, frame.schema, frame.row_keys), counts, 0, int(imputed.sum())
    return frame, counts, 0, 0


def group_codes(frame, group_by):
    if not group_by:
        return np.zeros(frame.rows, dtype=int), 1, {}
    codes, labels = [], []
    for key in group_by:
        values = frame.columns[key]
        valid = ~missing(values)
        unique, inverse = np.unique(values[valid], return_inverse=True)
        encoded = np.full(frame.rows, -1, dtype=np.int64)
        encoded[valid] = inverse
        codes.append(encoded)
        labels.append(unique)
    groups, inverse = np.unique(np.column_stack(codes), axis=0, return_inverse=True)
    if len(groups) > MAX_GROUPS:
        raise ValueError(f'Too many groups; limit {MAX_GROUPS:,}. Use wider bins or narrow the source.')
    columns = {}
    for index, key in enumerate(group_by):
        values = [labels[index][code] if code >= 0 else None for code in groups[:, index]]
        columns[key] = np.asarray(values, dtype=float if frame.schema[key] == 'number' else object)
    return inverse, len(groups), columns


def aggregate(frame, step, schema):
    inverse, size, columns = group_codes(frame, step['group_by'])
    for metric in step['metrics']:
        name, method = metric['name'], metric['op']
        if method == 'count':
            columns[name] = np.bincount(inverse, minlength=size).astype(float)
            continue
        values = frame.columns[metric['column']]
        valid = np.isfinite(values)
        if method == 'weighted_mean':
            weights = frame.columns[metric['weight']]
            if (weights[np.isfinite(weights)] < 0).any():
                raise ValueError('Weighted means require nonnegative weights.')
            valid &= np.isfinite(weights)
            totals = np.bincount(inverse[valid], weights=weights[valid], minlength=size)
            numerators = np.bincount(inverse[valid], weights=values[valid] * weights[valid], minlength=size)
            columns[name] = np.divide(numerators, totals, out=np.full(size, np.nan), where=totals > 0)
            continue
        ids, finite = inverse[valid], values[valid]
        counts = np.bincount(ids, minlength=size)
        total = np.bincount(ids, weights=finite, minlength=size)
        mean = np.divide(total, counts, out=np.full(size, np.nan), where=counts > 0)
        if method in ('min', 'max'):
            result = np.full(size, np.inf if method == 'min' else -np.inf)
            (np.minimum if method == 'min' else np.maximum).at(result, ids, finite)
        elif method == 'std':
            squared = np.bincount(ids, weights=(finite - mean[ids]) ** 2, minlength=size)
            result = np.sqrt(np.divide(squared, counts, out=np.full(size, np.nan), where=counts > 0))
        else:
            result = mean if method == 'mean' else total
        result[counts == 0] = np.nan
        columns[name] = result
    return Frame(columns, schema, np.asarray([f'group:{i}' for i in range(size)], dtype=object))


def apply_step(frame, step, schema, invalid):
    op = step['op']
    if op == 'filter':
        return frame.take(evaluate(step['expression'], frame.columns, frame.schema, frame.rows))
    if op == 'calculate':
        columns = dict(frame.columns)
        columns[step['column']] = evaluate(step['expression'], frame.columns, frame.schema, frame.rows, invalid=invalid)
        return Frame(columns, schema, frame.row_keys)
    if op == 'transform':
        columns = dict(frame.columns)
        for key in step['columns']:
            values = columns[key]
            good = np.isfinite(values)
            finite = values[good]
            if not len(finite):
                continue
            center = finite.mean() if step['method'] == 'standardize' else finite.min()
            scale = finite.std() if step['method'] == 'standardize' else finite.max() - finite.min()
            columns[key] = np.where(good, (values - center) / scale if scale else 0., np.nan)
        return Frame(columns, schema, frame.row_keys)
    if op == 'select':
        return Frame({key: frame.columns[key] for key in step['columns']}, schema, frame.row_keys)
    if op == 'sort':
        values = frame.columns[step['column']]
        valid = np.flatnonzero(~missing(values))
        order = np.argsort(-values[valid] if step['descending'] and values.dtype.kind in 'fi' else values[valid], kind='stable')
        if step['descending'] and values.dtype.kind not in 'fi':
            order = order[::-1]
        return frame.take(np.concatenate((valid[order], np.flatnonzero(missing(values)))))
    if op == 'aggregate':
        return aggregate(frame, step, schema)
    if op == 'correlate':
        names = step['columns']
        values = np.column_stack([frame.columns[key] for key in names])
        values = values[np.isfinite(values).all(axis=1)]
        correlations = np.full((len(names), len(names)), np.nan)
        if len(values) >= 2:
            with np.errstate(all='ignore'):
                correlations = np.corrcoef(values, rowvar=False)
        columns = {'variable': np.asarray(names, dtype=object)}
        columns.update({key: correlations[:, i] for i, key in enumerate(names)})
        return Frame(columns, schema, np.asarray([f'correlation:{key}' for key in names], dtype=object))
    raise ValueError('Unknown step operation.')
