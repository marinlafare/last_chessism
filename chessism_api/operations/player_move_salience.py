"""Move-level views over the persistent player salience projection."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from chessism_api.database.postgresql.transactions import read_transaction

from chessism_api.database.postgresql.engine import AsyncDBSession


async def get_player_game_salience(player_name: str, game_id: int) -> dict[str, Any]:
    """Return every position occurrence and its contribution to game salience."""
    player = str(player_name or "").strip().lower()
    if not player:
        raise ValueError("A player name is required.")

    async with read_transaction(AsyncDBSession) as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        state = (await session.execute(text("""
            SELECT
                gp.game_link AS link,
                CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS player_color,
                g.played_at,
                mode.name AS mode,
                CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
                opponent.player_name AS opponent_name,
                summary.status,
                game_salience.salience,
                game_salience.position_occurrence_count,
                game_salience.unique_position_count,
                game_salience.repeated_position_count
            FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
            LEFT JOIN public.game_mode mode ON mode.id=g.mode_id
            JOIN public.player owner ON owner.id=gp.player_id
            JOIN public.player opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
            LEFT JOIN player_salience_summary summary
              ON summary.player_id = gp.player_id
            LEFT JOIN game_player_salience game_salience
              ON game_salience.game_link = gp.game_link
             AND game_salience.is_white = gp.is_white
            WHERE owner.player_name = :player AND g.is_standard AND g.n_moves>0
              AND gp.game_link = :game_id ORDER BY gp.is_white DESC
        """), {"player": player, "game_id": int(game_id)})).mappings().first()
        if state is None:
            raise LookupError(f"Game {game_id} does not belong to {player}.")
        if state["status"] != "ready" or state["salience"] is None:
            raise ValueError(f"Salience for {player} is not ready.")
        from .cuda_summaries.salience_store import published_version
        from .cuda_summaries.salience_compute import VERSION
        opening = await published_version(session,player) == VERSION

        result = await session.execute(text("""
            WITH ordered_occurrences AS (
                SELECT
                    association.n_move,
                    CASE WHEN association.is_white THEN 'white' ELSE 'black' END AS move_color,
                    association.fen_id,
                    (
                        association.n_move * 2
                        - CASE WHEN association.is_white THEN 1 ELSE 0 END
                    )::integer AS ply,
                    ROW_NUMBER() OVER (
                        PARTITION BY association.fen_id
                        ORDER BY
                            association.n_move,
                            CASE WHEN association.is_white THEN 1 ELSE 2 END
                    )::integer AS occurrence_index,
                    COUNT(*) OVER (
                        PARTITION BY association.fen_id
                    )::integer AS game_occurrence_count
                FROM game_fen_association association
                WHERE association.game_link = :game_id AND association.n_move>0
                  AND (NOT :opening OR association.n_move BETWEEN 1 AND 15)
            )
            SELECT
                occurrence.n_move,
                occurrence.ply,
                occurrence.move_color,
                CASE
                    WHEN occurrence.move_color = 'white' THEN moves.white_move
                    ELSE moves.black_move
                END AS move,
                position.fen AS fen_fen,
                COALESCE(frequency.games_with_position, 1)::bigint
                    AS games_with_position,
                COALESCE(
                    frequency.total_occurrences,
                    occurrence.game_occurrence_count
                )::bigint AS total_occurrences,
                occurrence.occurrence_index,
                (
                    1.0
                    / COALESCE(frequency.games_with_position, 1)
                    / occurrence.occurrence_index
                )::double precision AS position_salience,
                (
                    CASE WHEN :opening THEN 1.0 ELSE 0.25 + 0.75 * LEAST(
                        occurrence.ply::double precision / 16.0,
                        1.0
                    ) END
                )::double precision AS depth_weight,
                (
                    1.0
                    / COALESCE(frequency.games_with_position, 1)
                    / occurrence.occurrence_index
                    * (
                        CASE WHEN :opening THEN 1.0 ELSE 0.25 + 0.75 * LEAST(
                            occurrence.ply::double precision / 16.0,
                            1.0
                        ) END
                    )
                )::double precision AS weighted_move_salience
            FROM ordered_occurrences occurrence
            JOIN fen position ON position.fen_id = occurrence.fen_id
            LEFT JOIN player_position_frequency frequency
              ON frequency.player_id = (SELECT id FROM public.player WHERE player_name=:player)
             AND frequency.is_white = (:player_color='white')
             AND frequency.fen_id = occurrence.fen_id
            LEFT JOIN moves
              ON moves.link = :game_id
             AND moves.n_move = occurrence.n_move
            ORDER BY
                occurrence.n_move,
                CASE WHEN occurrence.move_color = 'white' THEN 1 ELSE 2 END
        """), {
            "player": player,
            "player_color": str(state["player_color"]),
            "game_id": int(game_id),
            "opening": opening,
        })
        rows = result.mappings().all()

    return {
        "player_name": player,
        "game": {
            "game_id": int(state["link"]),
            "player_color": str(state["player_color"]),
            "played_at": state["played_at"].isoformat() if state["played_at"] else None,
            "mode": str(state["mode"] or "unknown"),
            "result": float(state["result"]),
            "opponent": str(state["opponent_name"]),
            "salience": round(float(state["salience"]), 10),
            "position_occurrence_count": int(state["position_occurrence_count"]),
            "unique_position_count": int(state["unique_position_count"]),
            "repeated_position_count": int(state["repeated_position_count"]),
        },
        "formula": {
            "version": VERSION if opening else 'legacy_full_game_depth_weighted',
            "maximum_ply": 30 if opening else None,
            "position_salience": "1 / (corpus_games_with_position * occurrence_index)",
            "depth_weight": "1 (first 30 plies only)" if opening else "0.25 + 0.75 * min(ply / 16, 1)",
            "game_salience": "sum(position_salience * depth_weight) / sum(depth_weight)",
        },
        "moves": [
            {
                "move_number": int(row["n_move"]),
                "ply": int(row["ply"]),
                "move_color": str(row["move_color"]),
                "move": row["move"],
                "fen": str(row["fen_fen"]),
                "corpus_games_with_position": int(row["games_with_position"]),
                "corpus_total_occurrences": int(row["total_occurrences"]),
                "occurrence_index": int(row["occurrence_index"]),
                "position_salience": round(float(row["position_salience"]), 10),
                "depth_weight": round(float(row["depth_weight"]), 10),
                "weighted_move_salience": round(float(row["weighted_move_salience"]), 10),
            }
            for row in rows
        ],
    }
