"""Lossless normalization of one PostgreSQL dump/restore deparser variation.

PostgreSQL can distribute a varchar[] -> text[] cast over constant array
elements when reparsing pg_dump's SQL. It does not change constraint/index
semantics. Do not ignore casts generally, strip SQL text, or ignore indexes.
"""
from copy import deepcopy
import json
import re

# Accept only ordinary string literals without backslashes and unbounded
# varchar. E-strings, expressions, NULLs and length-limited varchar are left
# unchanged (fail closed if PostgreSQL rewrites a different expression).
_ITEM = r"'(?:''|[^'\\])*'::character varying"
_ARRAY_CAST = re.compile(r"\(ARRAY\[(?P<items>" + _ITEM + r"(?:, " + _ITEM + r")*)\]\)::text\[\]")
_ITEM_RE = re.compile(_ITEM)
# Used only to prevent matching SQL-looking text inside strings/identifiers.
_QUOTED = re.compile(r"'(?:''|\\.|[^'\\])*'|\"(?:\"\"|[^\"])*\"")


def normalize_definition(sql: str) -> str:
    quoted = [match.span() for match in _QUOTED.finditer(sql)]

    def replace(match):
        if any(start <= match.start() < end for start, end in quoted):
            return match.group(0)
        items = _ITEM_RE.findall(match.group("items"))
        return "ARRAY[" + ", ".join(f"({item})::text" for item in items) + "]"

    return _ARRAY_CAST.sub(replace, sql)


def canonical_contract(contract: dict) -> dict:
    result = deepcopy(contract)
    for section, field in (("constraints", "definition"), ("indexes", "indexdef")):
        for row in result.get(section, []):
            row[field] = normalize_definition(row[field])
        # SQL definition text participates in the source's ORDER BY. Compare
        # canonical rows as multisets, retaining duplicates and every field.
        result[section] = sorted(result.get(section, []), key=lambda row: json.dumps(row, sort_keys=True))
    return result


def contract_differences(expected: dict, actual: dict) -> list[str]:
    left, right = canonical_contract(expected), canonical_contract(actual)
    return [key for key in sorted(left.keys() | right.keys()) if left.get(key) != right.get(key)]
