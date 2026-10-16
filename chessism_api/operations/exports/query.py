"""Strict JSON query compiler. No caller identifiers enter SQL unvalidated."""
from datetime import datetime, timezone
import math
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import bindparam, text

from .catalog import SOURCES
from .config import MAX_COLUMNS, MAX_ROWS


class Order(BaseModel):
    model_config = ConfigDict(extra='forbid')
    field: str = Field(max_length=64)
    direction: Literal['asc', 'desc'] = 'asc'


class ExportSpec(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str = Field(default='Dataset export', min_length=1, max_length=100)
    source: str = Field(min_length=1, max_length=40)
    select: list[str] = Field(min_length=1, max_length=MAX_COLUMNS)
    where: dict[str, Any] | None = None
    order_by: list[Order] = Field(default_factory=list, max_length=8)
    limit: int = Field(default=100_000, ge=1, le=MAX_ROWS, strict=True)
    format: Literal['parquet'] = 'parquet'

    @model_validator(mode='after')
    def valid_query(self):
        if self.source not in SOURCES:
            raise ValueError('Unknown dataset source.')
        available = SOURCES[self.source].columns
        if len(set(self.select)) != len(self.select):
            raise ValueError('Selected columns must be unique.')
        if any(key not in available for key in self.select + [o.field for o in self.order_by]):
            raise ValueError('Unknown selected or ordering column.')
        if len({o.field for o in self.order_by}) != len(self.order_by):
            raise ValueError('Ordering columns must be unique.')
        # Validate recursively before any connection or export directory exists.
        filter_sql(self.where, available)
        return self


def typed_value(value, dtype):
    if value is None:
        raise ValueError('Use is_null/is_not_null for NULL comparisons.')
    if dtype.startswith('int'):
        bits = int(dtype[3:])
        if type(value) is not int or not -(2**(bits-1)) <= value < 2**(bits-1):
            raise ValueError(f'Filter requires an exact {dtype} integer.')
    elif dtype == 'float64':
        try:
            valid = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError('Filter requires a finite number.')
        value = float(value)
    elif dtype == 'bool':
        if type(value) is not bool:
            raise ValueError('Filter requires a boolean.')
    elif dtype == 'timestamp[us, UTC]':
        if not isinstance(value, str) or len(value) > 64:
            raise ValueError('Timestamp filters require ISO 8601 with a timezone.')
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if value.tzinfo is None:
            raise ValueError('Timestamp filters require an explicit timezone.')
        value = value.astimezone(timezone.utc)
    elif not isinstance(value, str) or len(value) > 1000:
        raise ValueError('Filter requires a string of at most 1000 characters.')
    return value


def filter_sql(condition, columns):
    params, expanding, used = {}, [], set()
    nodes = 0

    def visit(node, depth=0):
        nonlocal nodes
        nodes += 1
        if depth > 6 or nodes > 100 or not isinstance(node, dict):
            raise ValueError('Filters exceed the supported size or nesting depth.')
        for group, separator in (('all', ' AND '), ('any', ' OR ')):
            if group in node:
                children = node[group]
                if set(node) != {group} or not isinstance(children, list) or not 1 <= len(children) <= 50:
                    raise ValueError('Filter groups require 1–50 conditions and no extra keys.')
                return '(' + separator.join(visit(child, depth+1) for child in children) + ')'
        key, op = node.get('field'), node.get('op')
        if not isinstance(key, str) or key not in columns:
            raise ValueError('Unknown filter column.')
        if not isinstance(op, str):
            raise ValueError('Filter operator must be a string.')
        column = columns[key]; used.add(key)
        if op in ('is_null', 'is_not_null'):
            if set(node) != {'field', 'op'}:
                raise ValueError('NULL operators do not accept a value.')
            return f'({column.expression}) IS {"NOT " if op == "is_not_null" else ""}NULL'
        if set(node) != {'field', 'op', 'value'}:
            raise ValueError('Comparison filters require field, op and value only.')
        name = f'filter_{len(params)}'
        if op in ('in', 'not_in'):
            values = node['value']
            if not isinstance(values, list) or not 1 <= len(values) <= 1000:
                raise ValueError('Membership filters require 1–1000 values.')
            params[name] = [typed_value(value, column.dtype) for value in values]
            expanding.append(name)
            return f'({column.expression}) {"NOT " if op == "not_in" else ""}IN :{name}'
        comparisons = {'eq': '=', 'ne': '<>', 'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}
        if op not in comparisons or (column.dtype == 'bool' and op not in ('eq', 'ne')):
            raise ValueError('Unsupported comparison operator.')
        params[name] = typed_value(node['value'], column.dtype)
        return f'({column.expression}) {comparisons[op]} :{name}'

    sql = 'TRUE' if condition is None else visit(condition)
    return sql, params, expanding, used


def effective_order(spec):
    order = [o.model_dump() for o in spec.order_by]
    present = {o['field'] for o in order}
    # A tie-breaker is mandatory even when the caller omits identity columns.
    for key in SOURCES[spec.source].keys:
        if key not in present:
            order.append({'field': key, 'direction': 'desc' if key == 'is_white' else 'asc'})
    return order


def compile_query(spec, tables, limit=None):
    source = SOURCES[spec.source]
    where, params, expanding, filtered = filter_sql(spec.where, source.columns)
    ordering = effective_order(spec)
    used = set(spec.select) | filtered | {item['field'] for item in ordering}
    aliases = set(re.findall(r'\b([a-z]+)\.', ' '.join(source.columns[key].expression for key in used)))
    joins, added = [], set()

    def include(alias):
        if alias not in source.joins or alias in added:
            return
        template, dependencies = source.joins[alias]
        for dependency in dependencies:
            include(dependency)
        added.add(alias); joins.append(template.format(**tables))

    for alias in sorted(aliases):
        include(alias)
    projection = ', '.join(f'{source.columns[key].expression} AS "{key}"' for key in spec.select)
    order_sql = ', '.join(f'({source.columns[o["field"]].expression}) {o["direction"].upper()} NULLS LAST' for o in ordering)
    sql = f'''SELECT {projection} FROM {source.base.format(**tables)}
        {' '.join(joins)} WHERE ({source.predicate}) AND ({where})
        ORDER BY {order_sql} LIMIT :export_limit'''
    params['export_limit'] = limit if limit is not None else spec.limit + 1
    statement = text(sql)
    for name in expanding:
        statement = statement.bindparams(bindparam(name, expanding=True))
    return statement, params
