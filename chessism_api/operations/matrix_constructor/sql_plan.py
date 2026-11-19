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
    # Preserve saved recipes after removing the stored score aggregates. Only
    # selecting this column pays for it, after the bounded matrix scope.
    "score_stats": """LEFT JOIN LATERAL (
        SELECT AVG(ABS(position.score::bigint))::double precision AS avg_abs_score
        FROM public.game_fen_association score_association
        JOIN public.fen position ON position.fen_id=score_association.fen_id
        WHERE score_association.game_link=g.link
    ) score_stats ON TRUE""",
    "gas": "LEFT JOIN public.game_analysis_coverage gas ON gas.link = g.link",
    "gps": """LEFT JOIN public.canonical_blunder_engine gps
        ON gps.game_link = gp.game_link AND gps.is_white = gp.is_white""",
    "gfa": """LEFT JOIN public.game_fen_association gfa
        ON gfa.game_link = m.link AND gfa.n_move = m.n_move
        AND gfa.is_white = (side.move_color='white')""",
    "f": "LEFT JOIN public.fen f ON f.fen_id = gfa.fen_id",
    "pss": "LEFT JOIN public.player_salience_summary pss ON pss.player_id = gp.player_id",
    "gsl": """LEFT JOIN public.game_player_salience gsl
        ON gsl.game_link = gp.game_link AND gsl.is_white = gp.is_white AND pss.status = 'ready'""",
    "ppf": """LEFT JOIN public.player_position_frequency ppf
        ON ppf.player_id = gp.player_id AND ppf.is_white = gp.is_white
        AND ppf.fen_id = gfa.fen_id AND pss.status = 'ready'""",
    "salience_occurrence": """LEFT JOIN LATERAL (
        SELECT COUNT(*)::integer AS occurrence_index
        FROM public.game_fen_association earlier
        WHERE earlier.game_link = gfa.game_link AND earlier.fen_id = gfa.fen_id
          AND (earlier.n_move < gfa.n_move OR (
              earlier.n_move = gfa.n_move
              AND CASE WHEN earlier.is_white THEN 1 ELSE 2 END
                  <= CASE WHEN gfa.is_white THEN 1 ELSE 2 END
          ))
    ) salience_occurrence ON gfa.fen_id IS NOT NULL""",
}
DEPENDENCIES = {
    "score_stats": (),
    "gas": (), "gps": (), "gfa": (), "f": ("gfa",), "pss": (),
    "gsl": ("pss",), "ppf": ("gfa", "pss"), "salience_occurrence": ("gfa",),
}
BASE_ALIASES = {
    "game": {"g", "wp", "bp", "gm"},
    "game_player": {"gp", "g", "p", "opponent", "gm"},
    "player_period": {"gp", "g", "p", "opponent", "gm"},
    "move": {"gp", "g", "p", "opponent", "gm", "m", "side"},
    "game_position": {"gp", "g", "p", "opponent", "gm", "gfa"},
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
    select_columns = ", ".join([f"{row_type.row_key_expression} AS row_key", *expressions])
    if group or not enrichment:
        joins = "\n".join(JOINS[key] for key in enrichment)
        return count, f"SELECT {select_columns} {base} {joins} {where_sql} {group} ORDER BY {row_type.order_sql} LIMIT :max_rows"

    joins = "\n".join(JOINS[key] for key in enrichment)
    references = sorted({
        (alias, column)
        # Keep token boundaries: e.g. color_order + LEFT JOIN must not become
        # color_orderLEFT and hide the ordering column from the projection.
        for alias, column in REFERENCE.findall(' '.join((select_columns, row_type.order_sql, joins)))
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
