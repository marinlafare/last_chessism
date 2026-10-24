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
    collection = config.get('collection', 'custom')
    if collection not in {'custom', 'foreign'}:
        raise ValueError('Unknown matrix formula collection.')
    template = config.get('definition_kind') == 'template'
    source = dict(config)
    if template:
        # Persist the shape, not a hidden default player, date range or row cap.
        source.pop('definition_kind')
        source['filters'] = {'max_rows': None}
        if source.get('row_type') == 'move_scores':
            source['move_scores'] = {'all_moves': True, 'score_perspective': 'player'}
    normalized = normalize_matrix_config(source)
    if template:
        normalized.pop('filters')
        normalized['definition_kind'] = 'template'
    return {
        **normalized,
        "collection": collection,
        "definition_version": DEFINITION_VERSION,
        "selection_order": ROW_TYPES[normalized["row_type"]].order_sql,
        "categorical_encoding": "zero-based dictionary when materialized",
        "missing_values": "separate uint8 mask when materialized",
    }


def resolve_definition(config: dict, parameters: dict | None = None) -> dict:
    """Bind a saved shape to one request without modifying the saved formula."""
    normalized = definition_config(config)
    if normalized.get('definition_kind') != 'template':
        if parameters is not None:
            raise ValueError('This older formula already contains its scope. Copy its columns into a reusable formula to supply runtime parameters.')
        return normalized
    if parameters is None:
        raise ValueError('Choose the player, games and move range in Matrices Collection before previewing this formula.')
    source = {key: value for key, value in normalized.items() if key != 'definition_kind'}
    source['filters'] = parameters.get('filters') or {}
    if normalized['row_type'] == 'move_scores':
        from .move_scores import normalize_options
        options = normalize_options(parameters.get('move_scores') or {'all_moves': True})
        source['move_scores'] = options
        if not options['all_moves']:
            source['feature_columns'] = [*source['feature_columns'], *(
                f'move_score_{move}' for move in range(options['first_move'], options['last_move'] + 1)
            )]
    elif parameters.get('move_scores') is not None:
        raise ValueError('Move range parameters apply only to Game move scores formulas.')
    return definition_config(source)


def definition_payload(definition) -> dict:
    return {
        "id": definition.id, "name": definition.name, "row_type": definition.row_type,
        "config": definition.config, "storage_kind": "definition", "status": "saved",
        "collection": definition.config.get('collection', 'custom'),
        "feature_count": len(definition.config["feature_columns"]),
        "label_count": len(definition.config["label_columns"]),
        "dynamic_move_columns": bool((definition.config.get("move_scores") or {}).get("all_moves")),
        "definition_bytes": len(json.dumps(definition.config, ensure_ascii=False).encode("utf-8")),
        "created_at": definition.created_at.isoformat() if definition.created_at else None,
    }


async def estimate_definition(config: dict) -> dict:
    normalized = resolve_definition(config)
    cap = normalized["filters"]["max_rows"]
    scan_limit = min(cap, COUNT_SCAN_LIMIT) if cap is not None else COUNT_SCAN_LIMIT
    if normalized['row_type'] == 'move_scores':
        from .move_scores import score_matrix_sql
        count_sql, _, params = score_matrix_sql(normalized, count_only=True)
    else:
        count_sql, _, params = matrix_sql(normalized)
    params["count_limit"] = scan_limit + 1
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await session.execute(text(f"SET LOCAL statement_timeout = {QUERY_TIMEOUT_MS}"))
        count = int((await session.execute(text(count_sql), params)).scalar_one())
    lower_bound = count > scan_limit
    return {
        "matching_rows": count, "matching_rows_is_lower_bound": lower_bound,
        "selected_rows": min(cap, count) if cap is not None else count,
        "selected_rows_is_lower_bound": lower_bound and (cap is None or count < cap),
        "row_limit": cap, "count_scan_limit": scan_limit,
        "definition_bytes": len(json.dumps(normalized, ensure_ascii=False).encode("utf-8")),
        "materialized": False,
    }
