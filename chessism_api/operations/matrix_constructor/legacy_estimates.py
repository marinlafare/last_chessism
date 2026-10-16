"""Legacy materialization estimates; not used by definition-only requests."""

import shutil
from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from .catalog import ROW_TYPES, MatrixColumn
from .config import normalize_matrix_config
from .queries import matrix_sql
from .storage import ARTIFACT_ROOT, ARTIFACT_DISPLAY_ROOT, FREE_SPACE_FLOOR


DTYPE_BYTES = {"float32": 4, "int32": 4, "int64": 8, "uint8": 1}


def estimated_artifact_bytes(
    rows: int,
    selected_columns: int | list[MatrixColumn],
) -> int:
    """Estimate typed arrays, masks, row keys, dictionaries, and manifests."""
    if isinstance(selected_columns, int):
        value_bytes = max(1, selected_columns) * DTYPE_BYTES["float32"]
        column_count = max(1, selected_columns)
        dictionary_bytes = 0
    else:
        column_count = max(1, len(selected_columns))
        value_bytes = sum(DTYPE_BYTES[column.numpy_dtype] for column in selected_columns)
        # Conservatively allow one distinct string per row per categorical
        # column. Encoded arrays alone do not account for their dictionaries.
        dictionary_bytes = 128 * sum(column.data_type == "category" for column in selected_columns)
    # Each value column has a uint8 missing mask. The row-key allowance is
    # deliberately conservative because compressed string lengths vary.
    per_row = value_bytes + column_count + dictionary_bytes + 128
    return int(max(0, rows) * per_row * 1.12) + 65_536


def storage_status(required_bytes: int) -> dict[str, Any]:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(ARTIFACT_ROOT)
    available = max(0, usage.free - FREE_SPACE_FLOOR)
    return {
        "free_bytes": usage.free,
        "reserved_floor_bytes": FREE_SPACE_FLOOR,
        "available_for_matrix_bytes": available,
        "required_bytes": int(required_bytes),
        "safe_to_build": int(required_bytes) <= available,
        "artifact_root": str(ARTIFACT_DISPLAY_ROOT),
    }


async def estimate_matrix(config: dict[str, Any], *, session: Any | None = None) -> dict[str, Any]:
    safe_config = normalize_matrix_config(config)
    count_sql, _, params = matrix_sql(safe_config)

    async def _count(active_session: Any) -> int:
        result = await active_session.execute(text(count_sql), params)
        return int(result.scalar_one() or 0)

    if session is None:
        async with AsyncDBSession() as owned_session:
            total_rows = await _count(owned_session)
    else:
        total_rows = await _count(session)
    truncated = total_rows > safe_config["filters"]["max_rows"]
    selected_rows = min(total_rows, safe_config["filters"]["max_rows"])
    row_type = ROW_TYPES[safe_config["row_type"]]
    selected_columns = [
        row_type.columns_by_key[key]
        for key in [*safe_config["feature_columns"], *safe_config["label_columns"]]
    ]
    estimate_bytes = estimated_artifact_bytes(selected_rows, selected_columns)
    return {
        "config": safe_config,
        "matching_rows": total_rows,
        "matching_rows_is_lower_bound": truncated,
        "selected_rows": selected_rows,
        "truncated": truncated,
        "estimated_artifact_bytes": estimate_bytes,
        "storage": storage_status(estimate_bytes),
    }
