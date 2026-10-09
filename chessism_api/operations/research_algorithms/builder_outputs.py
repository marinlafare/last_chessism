"""Bounded, JSON-safe outputs; calculations are independent of visualization."""

import numpy as np

from .builder_frames import cell, preview
from .expressions import missing


def render_output(frame, output, seed):
    result = dict(output)
    kind = output['type']
    result['total_rows'] = frame.rows
    if kind == 'table':
        result.update(preview(frame, limit=100, columns=output['columns']))
    elif kind == 'scalar':
        if frame.rows != 1:
            raise ValueError('Scalar output requires exactly one row. Add an ungrouped aggregation first.')
        result['value'] = cell(frame.columns[output['column']][0])
    elif kind == 'heatmap':
        names = [key for key in frame.schema if key != 'variable']
        result.update(columns=names, correlations=[[cell(frame.columns[key][i]) for key in names] for i in range(frame.rows)])
    else:
        x, y = frame.columns[output['x']], frame.columns[output['y']]
        indices = np.flatnonzero(~missing(x) & ~missing(y))
        result['valid_points'] = len(indices)
        limit = 1000 if kind == 'scatter' else 200
        if kind == 'scatter' and len(indices) > limit:
            indices = np.sort(np.random.default_rng(seed).choice(indices, limit, replace=False))
            result['sampled'] = True
        else:
            if kind == 'line':
                indices = indices[np.argsort(x[indices], kind='stable')]
            result['truncated'] = len(indices) > limit
            indices = indices[:limit]
        result.update(units='step values', points=[{'row_key': str(frame.row_keys[i]), 'x': cell(x[i]), 'y': cell(y[i])} for i in indices])
    return result
