"""Compose bounded, parameterized queries from validated matrix instructions."""

from typing import Any

from .catalog import ROW_TYPES, MatrixRowType
from .sql_plan import select_statements


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
