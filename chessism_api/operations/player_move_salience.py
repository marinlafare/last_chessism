"""Move-level views over the persistent player salience projection."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession


async def get_player_game_salience(player_name: str, game_id: int) -> dict[str, Any]:
    """Return every position occurrence and its contribution to game salience."""
    player = str(player_name or "").strip().lower()
    if not player:
        raise ValueError("A player name is required.")

    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        state = (await session.execute(text("""
            SELECT
                gp.link,
                gp.color AS player_color,
                gp.played_at,
                gp.mode,
                gp.result,
                gp.opponent_name,
                summary.status,
                game_salience.salience,
                game_salience.position_occurrence_count,
                game_salience.unique_position_count,
                game_salience.repeated_position_count
            FROM game_player gp
            LEFT JOIN player_salience_summary summary
              ON summary.player_name = gp.player_name
            LEFT JOIN game_player_salience game_salience
              ON game_salience.game_link = gp.link
             AND game_salience.player_color = gp.color
             AND game_salience.player_name = gp.player_name
            WHERE gp.player_name = :player
              AND gp.link = :game_id
        """), {"player": player, "game_id": int(game_id)})).mappings().first()
        if state is None:
            raise LookupError(f"Game {game_id} does not belong to {player}.")
        if state["status"] != "ready" or state["salience"] is None:
            raise ValueError(f"Salience for {player} is not ready.")

        result = await session.execute(text("""
            WITH ordered_occurrences AS (
                SELECT
                    association.n_move,
                    association.move_color,
                    association.fen_fen,
                    (
                        association.n_move * 2
                        - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END
                    )::integer AS ply,
                    ROW_NUMBER() OVER (
                        PARTITION BY association.fen_fen
                        ORDER BY
                            association.n_move,
                            CASE WHEN association.move_color = 'white' THEN 1 ELSE 2 END
                    )::integer AS occurrence_index,
                    COUNT(*) OVER (
                        PARTITION BY association.fen_fen
                    )::integer AS game_occurrence_count
                FROM game_fen_association association
                WHERE association.game_link = :game_id
            )
            SELECT
                occurrence.n_move,
                occurrence.ply,
                occurrence.move_color,
                CASE
                    WHEN occurrence.move_color = 'white' THEN moves.white_move
                    ELSE moves.black_move
                END AS move,
                occurrence.fen_fen,
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
                    0.25 + 0.75 * LEAST(
                        occurrence.ply::double precision / 16.0,
                        1.0
                    )
                )::double precision AS depth_weight,
                (
                    1.0
                    / COALESCE(frequency.games_with_position, 1)
                    / occurrence.occurrence_index
                    * (
                        0.25 + 0.75 * LEAST(
                            occurrence.ply::double precision / 16.0,
                            1.0
                        )
                    )
                )::double precision AS weighted_move_salience
            FROM ordered_occurrences occurrence
            LEFT JOIN player_position_frequency frequency
              ON frequency.player_name = :player
             AND frequency.player_color = :player_color
             AND frequency.fen_fen = occurrence.fen_fen
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
            "position_salience": "1 / (corpus_games_with_position * occurrence_index)",
            "depth_weight": "0.25 + 0.75 * min(ply / 16, 1)",
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
