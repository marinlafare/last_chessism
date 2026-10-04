"""Shared temporary projections for exact, occurrence-based salience."""

from sqlalchemy import text


async def stage_game_positions(session, *, incremental: bool) -> None:
    """Aggregate ordered occurrences without writing an occurrence-sized table.

    Identifiers are selected internally, never supplied by a request. Keeping
    each occurrence in the window preserves intra-game repetition penalties.
    """
    corpus = "salience_new_games" if incremental else "salience_snapshot_games"
    stage = "salience_new_game_position" if incremental else "salience_game_position_stage"
    await session.execute(text(f"ANALYZE {corpus}"))
    await session.execute(text(f"""
        CREATE TEMP TABLE {stage} ON COMMIT DROP AS
        WITH ordered_occurrences AS (
            SELECT corpus.game_link, corpus.player_color, association.fen_fen,
                0.25 + 0.75 * LEAST((association.n_move * 2
                    - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END
                )::double precision / 16.0, 1.0) AS depth_weight,
                ROW_NUMBER() OVER (
                    PARTITION BY corpus.game_link, corpus.player_color, association.fen_fen
                    ORDER BY association.n_move,
                        CASE WHEN association.move_color = 'white' THEN 1 ELSE 2 END
                ) AS occurrence_index
            FROM {corpus} corpus
            CROSS JOIN LATERAL (
                SELECT fen_fen, n_move, move_color
                FROM game_fen_association
                WHERE game_link = corpus.game_link
                OFFSET 0
            ) association
        )
        SELECT game_link, player_color, fen_fen,
            COUNT(*)::integer AS occurrence_count,
            SUM(depth_weight)::double precision AS depth_weight_sum,
            SUM(depth_weight / occurrence_index)::double precision AS weighted_occurrence_mass
        FROM ordered_occurrences
        GROUP BY game_link, player_color, fen_fen
    """))
    await session.execute(text(f"""
        CREATE INDEX {stage}_frequency_idx ON {stage} (player_color, fen_fen)
    """))
    await session.execute(text(f"ANALYZE {stage}"))
