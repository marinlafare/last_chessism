"""Validation, SQL composition, and size estimates for matrix snapshots."""

from __future__ import annotations

import shutil
from datetime import date
from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession

from .catalog import ROW_TYPES, MatrixColumn, MatrixRowType
from .sql_plan import select_statements
from .storage import ARTIFACT_ROOT, ARTIFACT_DISPLAY_ROOT, FREE_SPACE_FLOOR


MAXIMUM_ROWS = 5_000_000
MAXIMUM_COLUMNS = 48
DEFAULT_ROWS = 100_000


def normalize_matrix_config(config: dict[str, Any]) -> dict[str, Any]:
    row_type_key = str(config.get("row_type") or "").strip().lower()
    row_type = ROW_TYPES.get(row_type_key)
    if row_type is None:
        raise ValueError("Unknown matrix row type.")

    available = row_type.columns_by_key
    features = list(dict.fromkeys(str(item) for item in config.get("feature_columns", [])))
    labels = list(dict.fromkeys(str(item) for item in config.get("label_columns", [])))
    unknown = [key for key in [*features, *labels] if key not in available]
    if unknown:
        raise ValueError(f"Unsupported columns for {row_type_key}: {', '.join(unknown)}")
    overlap = sorted(set(features) & set(labels))
    if overlap:
        raise ValueError(f"Columns cannot be both features and labels: {', '.join(overlap)}")
    if not features:
        raise ValueError("Select at least one feature column.")
    if len(features) + len(labels) > MAXIMUM_COLUMNS:
        raise ValueError(f"A matrix can contain at most {MAXIMUM_COLUMNS} selected columns.")
    invalid_labels = [key for key in labels if not available[key].label_allowed]
    if invalid_labels:
        raise ValueError(f"These columns cannot be labels: {', '.join(invalid_labels)}")

    filters = dict(config.get("filters") or {})
    players = sorted({str(item).strip().lower() for item in filters.get("players", []) if str(item).strip()})
    if len(players) > 100:
        raise ValueError("At most 100 players can be selected.")
    modes = [mode for mode in ("bullet", "blitz", "rapid") if mode in {
        str(item).strip().lower() for item in filters.get("modes", [])
    }]
    date_from = _iso_date(filters.get("date_from"))
    date_to = _iso_date(filters.get("date_to"))
    if date_from and date_to and date_from > date_to:
        raise ValueError("The start date must not be after the end date.")

    max_rows = max(1, min(MAXIMUM_ROWS, int(filters.get("max_rows") or DEFAULT_ROWS)))
    min_moves = max(0, min(10_000, int(filters.get("min_moves") or 0)))
    name = str(config.get("name") or f"{row_type.label} matrix").strip()[:100]
    if not name:
        name = f"{row_type.label} matrix"
    return {
        "version": 2,
        "name": name,
        "row_type": row_type_key,
        "feature_columns": features,
        "label_columns": labels,
        "filters": {
            "players": players,
            "modes": modes,
            "date_from": date_from,
            "date_to": date_to,
            "min_moves": min_moves,
            "analyzed_only": bool(filters.get("analyzed_only", False)),
            "max_rows": max_rows,
        },
    }


def _iso_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError as error:
        raise ValueError(f"Invalid ISO date: {value}") from error


def _position_scope_clause(filters: dict[str, Any], params: dict[str, Any]) -> str | None:
    scope_parts: list[str] = []
    if filters["players"]:
        scope_parts.append("gp.player_name = ANY(CAST(:players AS text[]))")
        params["players"] = filters["players"]
    if filters["modes"]:
        scope_parts.append("g.mode = ANY(CAST(:modes AS text[]))")
        params["modes"] = filters["modes"]
    if filters["date_from"]:
        scope_parts.append("g.played_at >= CAST(:date_from AS date)")
        params["date_from"] = filters["date_from"]
    if filters["date_to"]:
        scope_parts.append("g.played_at < CAST(:date_to AS date) + INTERVAL '1 day'")
        params["date_to"] = filters["date_to"]
    if filters["min_moves"]:
        scope_parts.append("g.n_moves >= :min_moves")
        params["min_moves"] = filters["min_moves"]
    if not scope_parts:
        return None
    return """EXISTS (
        SELECT 1
        FROM game_fen_association scope_gfa
        JOIN game g ON g.link = scope_gfa.game_link
        JOIN game_player gp ON gp.link = g.link
        WHERE scope_gfa.fen_fen = f.fen
          AND %s
    )""" % " AND ".join(scope_parts)


def _where_parts(row_type: MatrixRowType, config: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    filters = config["filters"]
    parts: list[str] = []
    params: dict[str, Any] = {
        "max_rows": int(filters["max_rows"]),
        # Estimation only needs to know whether the requested output cap is
        # reached. Avoid scanning tens of millions of rows just to display a
        # number that cannot change the resulting artifact size.
        "count_limit": int(filters["max_rows"]) + 1,
    }
    if row_type.key == "position":
        scope = _position_scope_clause(filters, params)
        if scope:
            parts.append(scope)
    else:
        if filters["players"] and row_type.player_filter_sql:
            parts.append(row_type.player_filter_sql)
            params["players"] = filters["players"]
        if filters["modes"] and row_type.mode_filter_sql:
            parts.append(row_type.mode_filter_sql)
            params["modes"] = filters["modes"]
        if filters["date_from"] and row_type.date_filter_sql:
            parts.append(f"{row_type.date_filter_sql} >= CAST(:date_from AS date)")
            params["date_from"] = filters["date_from"]
        if filters["date_to"] and row_type.date_filter_sql:
            parts.append(f"{row_type.date_filter_sql} < CAST(:date_to AS date) + INTERVAL '1 day'")
            params["date_to"] = filters["date_to"]
        if filters["min_moves"] and row_type.min_moves_filter_sql:
            parts.append(row_type.min_moves_filter_sql)
            params["min_moves"] = filters["min_moves"]
    if filters["analyzed_only"] and row_type.analyzed_filter_sql:
        parts.append(row_type.analyzed_filter_sql)
    if row_type.key == "move":
        parts.extend(("gp.color = side.move_color", "NULLIF(BTRIM(side.san), '') IS NOT NULL"))
    return parts, params


def matrix_sql(config: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    row_type = ROW_TYPES[config["row_type"]]
    selected = [*config["feature_columns"], *config["label_columns"]]
    expressions = [
        f'{row_type.columns_by_key[key].expression} AS "{key}"'
        for key in selected
    ]
    where_parts, params = _where_parts(row_type, config)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    count_sql, select_sql = select_statements(row_type, expressions, where_sql)
    return count_sql, select_sql, params


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
