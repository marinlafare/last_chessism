"""Bounded live samples of recipe data. No files, workers, counts, or dictionaries."""

from datetime import datetime, timezone
import math

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from .catalog import ROW_TYPES
from .definitions import QUERY_TIMEOUT_MS, definition_config
from .queries import matrix_sql


MAX_PREVIEW_ROWS = 100


def source_value(value):
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value) if abs(value) > 2 ** 53 - 1 else value
    value = float(value)
    return value if math.isfinite(value) else None


async def preview_definition(config: dict, *, limit: int = 50, role: str = "all") -> dict:
    if not 1 <= limit <= MAX_PREVIEW_ROWS or role not in {"all", "features", "labels"}:
        raise ValueError("Invalid live preview limit or column selection.")
    normalized = definition_config(config)
    row_type = ROW_TYPES[normalized["row_type"]]
    cap = normalized["filters"]["max_rows"]
    # Select a tiny ordered prefix BEFORE optional expensive enrichment joins.
    # There is intentionally no total-count query or unbounded OFFSET paging.
    _, select_sql, params = matrix_sql(normalized)
    sample_limit = min(limit, cap)
    params["max_rows"] = min(sample_limit + 1, cap)
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        await session.execute(text(f"SET LOCAL statement_timeout = {QUERY_TIMEOUT_MS}"))
        result = await session.execute(text(select_sql), params)
        records = list(result.mappings())
    columns = []
    for group, keys in (("features", normalized["feature_columns"]), ("labels", normalized["label_columns"])):
        if role in {"all", group}:
            for key in keys:
                column = row_type.columns_by_key[key]
                columns.append({"key": key, "role": group, "storage_dtype": column.numpy_dtype,
                                "encoding": "source_category" if column.data_type == "category" else "numeric"})
    rows = [[source_value(record[column["key"]]) for column in columns] for record in records[:sample_limit]]
    return {
        "preview_kind": "live", "materialized": False, "dimensions": 2,
        "shape": [len(rows), len(columns)], "offset": 0, "limit": limit,
        "total_rows": len(rows), "total_rows_is_sample": True,
        "more_matching_rows": len(records) > sample_limit, "row_limit": cap,
        "has_more": False, "slice_count": 1, "slice_index": 0,
        "columns": columns, "rows": rows,
        "row_keys": [str(record["row_key"]) for record in records[:sample_limit]],
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "selection_order": row_type.order_sql,
        "category_values": "source_labels_not_materialized_codes",
    }
