"""Pure allowlist validation for saved definitions and legacy snapshots."""

from datetime import date
from typing import Any

from .catalog import ROW_TYPES


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
