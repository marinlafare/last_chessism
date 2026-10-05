"""Versioned matrix recipes and bounded, non-materializing row-count checks."""

import json

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from .catalog import ROW_TYPES
from .config import normalize_matrix_config
from .queries import matrix_sql


DEFINITION_VERSION = 1
COUNT_SCAN_LIMIT = 10_000
QUERY_TIMEOUT_MS = 15_000


def definition_config(config: dict) -> dict:
    normalized = normalize_matrix_config(config)
    return {
        **normalized,
        "definition_version": DEFINITION_VERSION,
        "selection_order": ROW_TYPES[normalized["row_type"]].order_sql,
        "categorical_encoding": "zero-based dictionary when materialized",
        "missing_values": "separate uint8 mask when materialized",
    }


def definition_payload(definition) -> dict:
    return {
        "id": definition.id, "name": definition.name, "row_type": definition.row_type,
        "config": definition.config, "storage_kind": "definition", "status": "saved",
        "feature_count": len(definition.config["feature_columns"]),
        "label_count": len(definition.config["label_columns"]),
        "definition_bytes": len(json.dumps(definition.config, ensure_ascii=False).encode("utf-8")),
        "created_at": definition.created_at.isoformat() if definition.created_at else None,
    }


async def estimate_definition(config: dict) -> dict:
    normalized = definition_config(config)
    cap = normalized["filters"]["max_rows"]
    scan_limit = min(cap, COUNT_SCAN_LIMIT)
    count_sql, _, params = matrix_sql(normalized)
    params["count_limit"] = scan_limit + 1
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await session.execute(text(f"SET LOCAL statement_timeout = {QUERY_TIMEOUT_MS}"))
        count = int((await session.execute(text(count_sql), params)).scalar_one())
    lower_bound = count > scan_limit
    return {
        "matching_rows": count, "matching_rows_is_lower_bound": lower_bound,
        "selected_rows": min(cap, count),
        "selected_rows_is_lower_bound": lower_bound and count < cap,
        "row_limit": cap, "count_scan_limit": scan_limit,
        "definition_bytes": len(json.dumps(normalized, ensure_ascii=False).encode("utf-8")),
        "materialized": False,
    }
