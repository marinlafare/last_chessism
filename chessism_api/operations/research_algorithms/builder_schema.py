"""Versioned step graph validation; no database or numerical execution."""

from copy import deepcopy

from ..matrix_constructor.catalog import ROW_TYPES
from ..matrix_constructor.definitions import definition_config
from .expressions import FUNCTIONS, IDENTIFIER, parse_expression

BUILDER_VERSION = 2
BUILDER_MAX_ROWS = 100_000
MAX_STEPS = 20
MAX_OUTPUTS = 6
MAX_FRAME_COLUMNS = 64
MAX_GROUPS = 10_000
SAMPLE_ROWS = 500
OPERATIONS = ['filter', 'calculate', 'transform', 'aggregate', 'correlate', 'select', 'sort']
AGGREGATIONS = ['count', 'sum', 'mean', 'weighted_mean', 'min', 'max', 'std']


def identifier(value, label='Name'):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value) or value in FUNCTIONS:
        raise ValueError(f'{label} must start with a letter, contain only letters/numbers/underscores, and be at most 48 characters; function names are reserved.')
    return value


def keys(value, schema, *, numeric=False, minimum=1):
    if not isinstance(value, list) or any(not isinstance(key, str) for key in value):
        raise ValueError('Columns must be a list of column names.')
    selected = list(dict.fromkeys(value))
    if not minimum <= len(selected) <= MAX_FRAME_COLUMNS:
        raise ValueError(f'Select {minimum}–{MAX_FRAME_COLUMNS} columns.')
    if any(key not in schema or (numeric and schema[key] != 'number') for key in selected):
        raise ValueError('Selected columns are missing or have incompatible types.')
    return selected


def normalize_builder(request, matrix):
    recipe = definition_config(matrix)
    available = set(recipe['feature_columns'] + recipe['label_columns'])
    row_type = ROW_TYPES[recipe['row_type']].columns_by_key
    schema = {key: row_type[key].data_type for key in available}
    columns = keys(request.get('columns', []), schema)
    if len(columns) > 12:
        raise ValueError('Use at most 12 input columns.')
    schemas = {'input': {key: schema[key] for key in columns}}
    steps = request.get('steps', [])
    if not isinstance(steps, list) or not 1 <= len(steps) <= MAX_STEPS:
        raise ValueError(f'Add 1–{MAX_STEPS} calculation steps.')
    normalized = []
    for raw in steps:
        if not isinstance(raw, dict):
            raise ValueError('Each step must be an object.')
        name = identifier(raw.get('id'), 'Step ID')
        if name in schemas:
            raise ValueError(f'Duplicate/reserved step ID: {name}')
        source = raw.get('input', 'input')
        if source not in schemas:
            raise ValueError(f'{name}: input must refer to an earlier step or input; forward references/cycles are not allowed.')
        current = dict(schemas[source])
        op = raw.get('op')
        if op not in OPERATIONS:
            raise ValueError(f'{name}: unsupported step operation.')
        step = {'id': name, 'name': str(raw.get('name') or name)[:100], 'input': source, 'op': op}
        try:
            if op in ('filter', 'calculate'):
                expression = raw.get('expression', '')
                _, kind = parse_expression(expression, current)
                if op == 'filter' and kind != 'boolean':
                    raise ValueError('A filter must return a boolean condition.')
                step['expression'] = expression
                if op == 'calculate':
                    column = identifier(raw.get('column'), 'Calculated column')
                    if column in current:
                        raise ValueError('Use a new column name; formulas do not overwrite source columns.')
                    current[column] = kind
                    step['column'] = column
            elif op == 'transform':
                step['columns'] = keys(raw.get('columns'), current, numeric=True)
                step['method'] = raw.get('method')
                if step['method'] not in ('standardize', 'normalize'):
                    raise ValueError('Choose standardize or normalize.')
            elif op == 'aggregate':
                group_by = keys(raw.get('group_by', []), current, minimum=0)
                metrics = raw.get('metrics', [])
                if not isinstance(metrics, list) or not 1 <= len(metrics) <= 48:
                    raise ValueError('Add 1–48 aggregation metrics.')
                result_schema = {key: current[key] for key in group_by}
                clean_metrics = []
                for metric in metrics:
                    target = identifier(metric.get('name'), 'Metric name')
                    method = metric.get('op')
                    if target in result_schema or method not in AGGREGATIONS:
                        raise ValueError('Metric names must be distinct and aggregation methods supported.')
                    item = {'name': target, 'op': method}
                    if method != 'count':
                        item['column'] = keys([metric.get('column')], current, numeric=True)[0]
                    if method == 'weighted_mean':
                        item['weight'] = keys([metric.get('weight')], current, numeric=True)[0]
                    clean_metrics.append(item)
                    result_schema[target] = 'number'
                step.update(group_by=group_by, metrics=clean_metrics)
                current = result_schema
            elif op == 'correlate':
                selected = keys(raw.get('columns'), current, numeric=True, minimum=2)
                if len(selected) > 12 or 'variable' in selected:
                    raise ValueError('Correlations support up to 12 columns; variable is reserved for row labels.')
                step['columns'] = selected
                current = {'variable': 'category', **{key: 'number' for key in selected}}
            elif op == 'select':
                step['columns'] = keys(raw.get('columns'), current)
                current = {key: current[key] for key in step['columns']}
            elif op == 'sort':
                step['column'] = keys([raw.get('column')], current)[0]
                step['descending'] = bool(raw.get('descending', False))
            if len(current) > MAX_FRAME_COLUMNS:
                raise ValueError(f'A step may produce at most {MAX_FRAME_COLUMNS} columns.')
        except (ValueError, TypeError, AttributeError) as error:
            raise ValueError(f'{name}: {error}') from error
        schemas[name] = current
        normalized.append(step)

    outputs = request.get('outputs', [])
    if not isinstance(outputs, list) or not 1 <= len(outputs) <= MAX_OUTPUTS:
        raise ValueError(f'Choose 1–{MAX_OUTPUTS} outputs.')
    clean_outputs = []
    for raw in outputs:
        source, kind = raw.get('input'), raw.get('type')
        if source not in schemas or kind not in ('table', 'scatter', 'line', 'bar', 'heatmap', 'scalar'):
            raise ValueError('Each output needs a valid input and visualization type.')
        current = schemas[source]
        output = {'input': source, 'type': kind, 'name': str(raw.get('name') or kind)[:100]}
        if kind in ('scatter', 'line', 'bar'):
            output['x'] = keys([raw.get('x')], current, numeric=kind == 'scatter')[0]
            output['y'] = keys([raw.get('y')], current, numeric=True)[0]
        elif kind == 'scalar':
            output['column'] = keys([raw.get('column')], current, numeric=True)[0]
        elif kind == 'heatmap':
            if not any(step['id'] == source and step['op'] == 'correlate' for step in normalized):
                raise ValueError('A heatmap must use a correlation step.')
        else:
            output['columns'] = keys(raw.get('columns') or list(current), current)
        clean_outputs.append(output)
    missing = request.get('missing', 'keep')
    invalid = request.get('invalid_values', 'null')
    if missing not in ('keep', 'drop_rows', 'column_mean') or invalid not in ('null', 'error'):
        raise ValueError('Unsupported missing/invalid-value policy.')
    maximum = int(request.get('max_rows', 100000))
    seed = int(request.get('seed', 42))
    if not 1 <= maximum <= BUILDER_MAX_ROWS or not 0 <= seed < 2**32:
        raise ValueError(f'Builder runs support 1–{BUILDER_MAX_ROWS:,} source rows and a 32-bit nonnegative seed.')
    return {
        'operation': 'pipeline', 'implementation_version': BUILDER_VERSION, 'backend': 'numpy_cpu',
        'matrix': deepcopy(recipe), 'columns': columns, 'steps': normalized, 'outputs': clean_outputs,
        'missing': missing, 'invalid_values': invalid, 'max_rows': min(maximum, recipe['filters']['max_rows']),
        'seed': seed, 'schemas': schemas,
    }
