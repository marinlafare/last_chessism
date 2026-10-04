"""Plan only required joins, and enrich row-limited snapshots after selection.

Every identifier/expression comes from our server catalog, never browser SQL.
Keeping expensive FEN-frequency lookups outside the bounded scope avoids doing
millions of lookups to export a few thousand moves.
"""

from __future__ import annotations

import re

from .catalog import MatrixRowType


REFERENCE = re.compile(r"\b([a-z_][a-z_0-9]*)\.([a-z_][a-z_0-9]*)\b")
JOINS = {
    "gas": "LEFT JOIN game_analysis_summary gas ON gas.link = g.link",
    "gps": """LEFT JOIN game_player_engine_summary gps
        ON gps.game_link = gp.link AND gps.player_color = gp.color""",
    "gfa": """LEFT JOIN game_fen_association gfa
        ON gfa.game_link = m.link AND gfa.n_move = m.n_move
        AND gfa.move_color = side.move_color""",
    "f": "LEFT JOIN fen f ON f.fen = gfa.fen_fen",
    "pss": "LEFT JOIN player_salience_summary pss ON pss.player_name = gp.player_name",
    "gsl": """LEFT JOIN game_player_salience gsl
        ON gsl.game_link = gp.link AND gsl.player_color = gp.color
        AND gsl.player_name = gp.player_name AND pss.status = 'ready'""",
    "ppf": """LEFT JOIN player_position_frequency ppf
        ON ppf.player_name = gp.player_name AND ppf.player_color = gp.color
        AND ppf.fen_fen = gfa.fen_fen AND pss.status = 'ready'""",
    "salience_occurrence": """LEFT JOIN LATERAL (
        SELECT COUNT(*)::integer AS occurrence_index
        FROM game_fen_association earlier
        WHERE earlier.game_link = gfa.game_link AND earlier.fen_fen = gfa.fen_fen
          AND (earlier.n_move < gfa.n_move OR (
              earlier.n_move = gfa.n_move
              AND CASE WHEN earlier.move_color = 'white' THEN 1 ELSE 2 END
                  <= CASE WHEN gfa.move_color = 'white' THEN 1 ELSE 2 END
          ))
    ) salience_occurrence ON gfa.fen_fen IS NOT NULL""",
}
DEPENDENCIES = {
    "gas": (), "gps": (), "gfa": (), "f": ("gfa",), "pss": (),
    "gsl": ("pss",), "ppf": ("gfa", "pss"), "salience_occurrence": ("gfa",),
}
BASE_ALIASES = {
    "game": {"g"}, "game_player": {"gp"}, "player_period": {"gp"},
    "move": {"gp", "m", "side"}, "game_position": {"gp", "gfa"},
    "position": {"f"},
}


def required_joins(sql: str, present: set[str]) -> list[str]:
    ordered: list[str] = []
    def include(alias: str) -> None:
        if alias in present or alias in ordered or alias not in JOINS:
            return
        for dependency in DEPENDENCIES[alias]:
            include(dependency)
        ordered.append(alias)
    for alias, _ in REFERENCE.findall(sql):
        include(alias)
    return ordered


def select_statements(
    row_type: MatrixRowType, expressions: list[str], where_sql: str,
) -> tuple[str, str]:
    base_aliases = BASE_ALIASES[row_type.key]
    # Position scopes have their own EXISTS joins. They must not become outer
    # joins merely because the filter mentions their internal aliases.
    filter_joins = [] if row_type.key == "position" else required_joins(where_sql, base_aliases)
    base = f"FROM {row_type.from_sql}\n" + "\n".join(JOINS[key] for key in filter_joins)
    group = f"GROUP BY {row_type.group_by_sql}" if row_type.group_by_sql else ""
    count = f"SELECT COUNT(*)::bigint FROM (SELECT 1 {base} {where_sql} {group} LIMIT :count_limit) matrix_count"
    present = base_aliases | set(filter_joins)
    enrichment = required_joins(" ".join(expressions), present)
    select_columns = f"{row_type.row_key_expression} AS row_key, {', '.join(expressions)}"
    if group or not enrichment:
        joins = "\n".join(JOINS[key] for key in enrichment)
        return count, f"SELECT {select_columns} {base} {joins} {where_sql} {group} ORDER BY {row_type.order_sql} LIMIT :max_rows"

    joins = "\n".join(JOINS[key] for key in enrichment)
    references = sorted({
        (alias, column)
        for alias, column in REFERENCE.findall(select_columns + row_type.order_sql + joins)
        if alias in present
    })
    def scoped(sql: str) -> str:
        return REFERENCE.sub(
            lambda match: f'scope."{match[1]}__{match[2]}"' if match[1] in present else match[0],
            sql,
        )
    projection = ", ".join(f'{alias}.{column} AS "{alias}__{column}"' for alias, column in references)
    select = f"""
        WITH matrix_scope AS MATERIALIZED (
            SELECT {projection} {base} {where_sql}
            ORDER BY {row_type.order_sql} LIMIT :max_rows
        )
        SELECT {scoped(select_columns)} FROM matrix_scope scope
        {scoped(joins)} ORDER BY {scoped(row_type.order_sql)}
    """
    return count, select
