"""Small typed expression interpreter. Never compiles or evaluates Python code."""

import ast
import operator
import re

import numpy as np

IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,47}$")
FUNCTIONS = {
    'abs': 1, 'sqrt': 1, 'log': 1, 'exp': 1, 'floor': 1, 'ceil': 1,
    'clip': 3, 'safe_divide': 2, 'where': 3, 'is_missing': 1,
    'fill_missing': 2, 'utc_day': 1, 'utc_month': 1, 'utc_year': 1,
}
BINARY = {ast.Add: np.add, ast.Sub: np.subtract, ast.Mult: np.multiply,
          ast.Div: np.divide, ast.Mod: np.remainder, ast.Pow: np.power}
COMPARE = {ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt,
           ast.LtE: operator.le, ast.Gt: operator.gt, ast.GtE: operator.ge}


def missing(values):
    values = np.asarray(values)
    return ~np.isfinite(values) if values.dtype.kind in 'fiu' else values == None  # noqa: E711


def parse_expression(expression, schema):
    if not isinstance(expression, str) or not 1 <= len(expression) <= 500:
        raise ValueError('Formulas must contain 1–500 characters.')
    try:
        tree = ast.parse(expression, mode='eval').body
    except (SyntaxError, RecursionError) as error:
        raise ValueError('Invalid formula syntax.') from error
    if sum(1 for _ in ast.walk(tree)) > 128:
        raise ValueError('Formula is too complex (maximum 128 syntax nodes).')

    def check(node, depth=0):
        if depth > 24:
            raise ValueError('Formula nesting is too deep.')
        child = lambda value: check(value, depth + 1)
        if isinstance(node, ast.Name):
            if node.id not in schema:
                raise ValueError(f'Unknown column: {node.id}')
            return schema[node.id]
        if isinstance(node, ast.Constant):
            value = node.value
            if type(value) is bool:
                return 'boolean'
            if type(value) in (int, float) and abs(value) <= 1e12 and np.isfinite(value):
                return 'number'
            if isinstance(value, str) and len(value) <= 256:
                return 'category'
            raise ValueError('Only finite numeric, boolean and short text constants are allowed.')
        if isinstance(node, ast.BinOp) and type(node.op) in BINARY:
            if child(node.left) != 'number' or child(node.right) != 'number':
                raise ValueError('Arithmetic requires numeric columns.')
            return 'number'
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub, ast.Not)):
            expected = 'boolean' if isinstance(node.op, ast.Not) else 'number'
            if child(node.operand) != expected:
                raise ValueError(f'This unary operation requires {expected} values.')
            return expected
        if isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
            if any(child(item) != 'boolean' for item in node.values):
                raise ValueError('and/or require boolean conditions.')
            return 'boolean'
        if isinstance(node, ast.Compare):
            kinds = [child(item) for item in [node.left, *node.comparators]]
            if len(set(kinds)) != 1 or any(type(op) not in COMPARE for op in node.ops):
                raise ValueError('Comparisons require compatible types and supported operators.')
            return 'boolean'
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            if name not in FUNCTIONS or node.keywords or len(node.args) != FUNCTIONS.get(name):
                raise ValueError(f'Unsupported function or argument count: {name}')
            types = [child(arg) for arg in node.args]
            if name == 'is_missing':
                return 'boolean'
            if name == 'fill_missing':
                if types[0] != types[1]:
                    raise ValueError('fill_missing arguments must have the same type.')
                return types[0]
            if name == 'where':
                if types[0] != 'boolean' or types[1] != types[2]:
                    raise ValueError('where needs a condition and two values of the same type.')
                return types[1]
            if any(kind != 'number' for kind in types):
                raise ValueError(f'{name} requires numeric arguments.')
            return 'category' if name.startswith('utc_') else 'number'
        raise ValueError('Only column formulas are allowed; no attributes, indexing, imports, loops or arbitrary code.')

    return tree, check(tree)


def evaluate(expression, columns, schema, rows, *, invalid='null'):
    tree, kind = parse_expression(expression, schema)

    def run(node):
        if isinstance(node, ast.Name):
            return columns[node.id]
        if isinstance(node, ast.Constant):
            return np.asarray(node.value, dtype=float if type(node.value) in (int, float) else None)
        if isinstance(node, ast.BinOp):
            return BINARY[type(node.op)](run(node.left), run(node.right))
        if isinstance(node, ast.UnaryOp):
            value = run(node.operand)
            return np.logical_not(value) if isinstance(node.op, ast.Not) else -value if isinstance(node.op, ast.USub) else value
        if isinstance(node, ast.BoolOp):
            combine = np.logical_and if isinstance(node.op, ast.And) else np.logical_or
            value = run(node.values[0])
            for item in node.values[1:]:
                value = combine(value, run(item))
            return value
        if isinstance(node, ast.Compare):
            left, result = run(node.left), True
            for op, right_node in zip(node.ops, node.comparators):
                right = run(right_node)
                valid = ~(missing(left) | missing(right))
                # Mask missing object values before ordered comparisons.
                lhs = np.where(missing(left), '' if np.asarray(left).dtype.kind in 'OU' else 0, left)
                rhs = np.where(missing(right), '' if np.asarray(right).dtype.kind in 'OU' else 0, right)
                result = result & valid & COMPARE[type(op)](lhs, rhs)
                left = right
            return result
        name = node.func.id
        args = [run(arg) for arg in node.args]
        if name == 'is_missing':
            return missing(args[0])
        if name == 'fill_missing':
            return np.where(missing(args[0]), args[1], args[0])
        if name == 'where':
            return np.where(*args)
        if name == 'safe_divide':
            a, b = np.broadcast_arrays(*args)
            return np.divide(a, b, out=np.full(a.shape, np.nan), where=b != 0)
        if name.startswith('utc_'):
            values = np.broadcast_to(np.asarray(args[0], dtype=float), (rows,))
            valid = np.isfinite(values) & (values >= -62135596800) & (values <= 253402300799)
            unit = {'utc_day': 'D', 'utc_month': 'M', 'utc_year': 'Y'}[name]
            dates = np.where(valid, values, 0).astype('datetime64[s]').astype(f'datetime64[{unit}]').astype(str).astype(object)
            dates[~valid] = None
            return dates
        return {'abs': np.abs, 'sqrt': np.sqrt, 'log': np.log, 'exp': np.exp,
                'floor': np.floor, 'ceil': np.ceil, 'clip': np.clip}[name](*args)

    with np.errstate(all='ignore'):
        result = np.broadcast_to(np.asarray(run(tree)), (rows,)).copy()
    if kind == 'number':
        result = result.astype(float)
        bad = ~np.isfinite(result)
        if invalid == 'error' and bad.any():
            raise ValueError('Formula produced missing/non-finite values; use fill_missing/safe_divide or the N/A policy.')
        result[bad] = np.nan
    elif kind == 'category':
        result = result.astype(object)
    return result
