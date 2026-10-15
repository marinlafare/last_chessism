"""Default-deny PostgreSQL 15 AST policy. Never execute the caller's raw text.

Relations are versioned views; functions/operators/casts resolve only through
pg_catalog. This is one security layer, not a substitute for the reader role.
"""
from dataclasses import dataclass

from pglast import ast, parse_sql
from pglast.stream import RawStream

from .catalog import SOURCES

SCHEMA = 'exports_data_v1'
FUNCTIONS = frozenset('count sum avg min max stddev_pop stddev_samp var_pop var_samp '
    'abs round trunc ceil ceiling floor lower upper length char_length btrim ltrim rtrim '
    'replace substring date_trunc date_part row_number rank dense_rank percent_rank '
    'cume_dist ntile lag lead first_value last_value nth_value bool_and bool_or'.split())
TYPES = frozenset('bool int2 int4 int8 float4 float8 numeric text varchar date timestamp timestamptz interval'.split())
OPERATORS = frozenset(('+', '-', '*', '/', '%', '=', '<>', '!=', '<', '<=', '>', '>=', '||', '~~', '!~~', '~~*', '!~~*'))
NODES = frozenset('SelectStmt RangeVar RangeSubselect JoinExpr Alias ResTarget ColumnRef A_Star '
    'A_Const Integer Float String Boolean BoolExpr NullTest BooleanTest A_Expr TypeCast TypeName '
    'FuncCall ParamRef CoalesceExpr MinMaxExpr CaseExpr CaseWhen SortBy WindowDef SubLink RowExpr'.split())
PARAM_TYPES = {'int16': 'int2', 'int32': 'int4', 'int64': 'int8', 'float64': 'float8',
               'string': 'text', 'bool': 'bool', 'timestamp[us, UTC]': 'timestamptz'}


@dataclass(frozen=True)
class ValidatedSQL:
    sql: str
    relations: frozenset[str]


def names(parts):
    if not parts or any(not isinstance(part, ast.String) for part in parts):
        raise ValueError('Unsupported SQL identifier.')
    return tuple(part.sval for part in parts)


def qualified(parts, allowed, label):
    values = names(parts)
    if len(values) > 2 or (len(values) == 2 and values[0] != 'pg_catalog') or values[-1] not in allowed:
        raise ValueError(f'Unapproved SQL {label}.')
    return tuple(ast.String(sval=value) for value in ('pg_catalog', values[-1]))


def validate_sql(sql, parameter_types=()):
    if len(sql.encode()) > 32768:
        raise ValueError('SQL exceeds 32 KiB.')
    try:
        parsed = parse_sql(sql)
    except Exception:
        raise ValueError('Invalid PostgreSQL 15 query syntax.') from None
    if len(parsed) != 1 or not isinstance(parsed[0].stmt, ast.SelectStmt):
        raise ValueError('Exactly one SELECT query is permitted.')
    relations, parameters = set(), set()
    count = 0

    def visit(node, ctes=frozenset(), depth=0):
        nonlocal count
        if isinstance(node, (tuple, list)):
            return tuple(visit(item, ctes, depth+1) for item in node)
        if not isinstance(node, ast.Node):
            return node
        count += 1
        if count > 8000 or depth > 60:
            raise ValueError('SQL syntax tree exceeds its complexity limit.')
        if type(node).__name__ not in NODES:
            raise ValueError(f'Unsupported SQL syntax: {type(node).__name__}.')
        skip = set()
        if isinstance(node, ast.SelectStmt):
            if node.intoClause or node.lockingClause:
                raise ValueError('SELECT INTO and locking clauses are forbidden.')
            if node.withClause:
                clause = node.withClause
                if clause.recursive or len(clause.ctes) > 12:
                    raise ValueError('Recursive CTEs or more than 12 CTEs are not supported.')
                visible = set(ctes)
                for cte in clause.ctes:
                    if (not isinstance(cte.ctequery, ast.SelectStmt) or cte.search_clause or cte.cycle_clause):
                        raise ValueError('CTEs must contain plain read-only queries.')
                    cte.ctequery = visit(cte.ctequery, frozenset(visible), depth+1)
                    visible.add(cte.ctename)
                ctes = frozenset(visible)
                skip.add('withClause')
        elif isinstance(node, ast.RangeVar):
            if node.catalogname or node.relpersistence != 'p':
                raise ValueError('Cross-database and temporary relations are forbidden.')
            if node.schemaname is None and node.relname in ctes:
                pass
            elif node.schemaname == SCHEMA and node.relname in SOURCES:
                relations.add(node.relname)
            else:
                raise ValueError(f'Relations must be approved {SCHEMA} views or visible CTEs.')
        elif isinstance(node, ast.FuncCall):
            if node.func_variadic or node.agg_within_group:
                raise ValueError('Variadic and ordered-set function syntax is unsupported.')
            node.funcname = qualified(node.funcname, FUNCTIONS, 'function')
        elif isinstance(node, ast.A_Expr):
            # Reject ANY/ALL arrays and special operator forms not covered here.
            if node.kind.name not in ('AEXPR_OP', 'AEXPR_DISTINCT', 'AEXPR_NOT_DISTINCT',
                    'AEXPR_NULLIF', 'AEXPR_IN', 'AEXPR_LIKE', 'AEXPR_ILIKE',
                    'AEXPR_BETWEEN', 'AEXPR_NOT_BETWEEN', 'AEXPR_BETWEEN_SYM', 'AEXPR_NOT_BETWEEN_SYM'):
                raise ValueError('Unsupported SQL operator syntax.')
            if node.kind.name == 'AEXPR_OP':
                node.name = qualified(node.name, OPERATORS, 'operator')
            elif 'BETWEEN' not in node.kind.name:
                qualified(node.name, OPERATORS, 'operator')
            # IN/BETWEEN/LIKE/NULLIF are SQL grammar forms, not caller-selected
            # functions. Their built-in operators resolve via the fixed path.
        elif isinstance(node, ast.SubLink):
            if node.subLinkType.name not in ('EXISTS_SUBLINK','ALL_SUBLINK','ANY_SUBLINK','ROWCOMPARE_SUBLINK','EXPR_SUBLINK'):
                raise ValueError('Array-producing and unsupported subqueries are forbidden.')
            if node.operName:
                node.operName = qualified(node.operName, OPERATORS, 'subquery operator')
        elif isinstance(node, ast.TypeName):
            if node.setof or node.pct_type or node.arrayBounds:
                raise ValueError('Array, SETOF and %TYPE casts are unsupported.')
            node.names = qualified(node.names, TYPES, 'cast type')
        elif isinstance(node, ast.ColumnRef):
            if len(node.fields) > 2:
                raise ValueError('Use relation aliases for column references.')
        elif isinstance(node, ast.ParamRef):
            number = node.number
            if not 1 <= number <= len(parameter_types):
                raise ValueError('Every SQL parameter requires a corresponding typed value.')
            parameters.add(number)
            # Cast the bind node, not an interpolated value or string replacement.
            return ast.TypeCast(arg=node, typeName=ast.TypeName(names=(
                ast.String(sval='pg_catalog'), ast.String(sval=PARAM_TYPES[parameter_types[number-1]]))))
        elif isinstance(node, ast.SortBy) and node.useOp:
            raise ValueError('ORDER BY USING custom operators is unsupported.')
        for field in node:
            if field not in skip:
                setattr(node, field, visit(getattr(node, field), ctes, depth+1))
        return node

    query = visit(parsed[0].stmt)
    if parameters != set(range(1, len(parameter_types)+1)):
        raise ValueError('Supplied parameters must all be referenced.')
    return ValidatedSQL(RawStream()(query), frozenset(relations))
