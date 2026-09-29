"""Player-game Stockfish summary calculations shared by analytics endpoints."""

from __future__ import annotations

import math

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession


# Kept in sync with Lichess's current move-judgment implementation:
# https://github.com/lichess-org/lila/blob/master/modules/tree/src/main/Advice.scala
# https://github.com/lichess-org/scalachess/blob/master/core/src/main/scala/eval.scala
LICHESS_WINNING_CHANCE_MULTIPLIER = -0.00368208
LICHESS_BLUNDER_THRESHOLD = 0.30


def lichess_winning_chances(centipawns: float) -> float:
    """Return Lichess winning chances on its published -1 to +1 scale."""
    exponent = LICHESS_WINNING_CHANCE_MULTIPLIER * float(centipawns)
    if exponent >= 700:
        return -1.0
    if exponent <= -700:
        return 1.0
    return 2.0 / (1.0 + math.exp(exponent)) - 1.0


def is_lichess_blunder(
    previous_player_score: float,
    player_score: float,
    previous_kind: str = "cp",
    score_kind: str = "cp",
) -> bool:
    """Mirror Lichess's CP and forced-mate blunder judgments for the mover."""
    if previous_kind == "cp" and score_kind == "cp":
        loss = (
            lichess_winning_chances(previous_player_score)
            - lichess_winning_chances(player_score)
        )
        return loss >= LICHESS_BLUNDER_THRESHOLD
    if previous_kind == "cp" and score_kind == "mate" and player_score < 0:
        return previous_player_score >= -700
    if previous_kind == "mate" and previous_player_score > 0:
        if score_kind == "mate" and player_score < 0:
            return True
        if score_kind == "cp":
            return player_score <= 700
    return False


async def refresh_game_player_engine_summaries(
    game_links: tuple[int, ...] | None = None,
) -> dict[str, int]:
    """Build compact per-player scores for fully analyzed games."""
    links = sorted({int(link) for link in game_links or ()})
    params = {"all_games": not links, "game_links": links}
    delete_sql = text("""
        DELETE FROM game_player_engine_summary engine_summary
        WHERE (:all_games OR engine_summary.game_link = ANY(CAST(:game_links AS bigint[])))
          AND NOT EXISTS (
              SELECT 1
              FROM game_analysis_summary coverage
              WHERE coverage.link = engine_summary.game_link
                AND coverage.is_fully_analyzed
                AND coverage.total_positions > 0
          )
    """)
    refresh_sql = text("""
        INSERT INTO game_player_engine_summary (
            game_link, player_name, player_color, analyzed_player_moves,
            own_move_cp_gain, own_move_cp_loss, blunder_count,
            mate_for_positions, mate_against_positions,
            final_player_cp, result, end_by
        )
        WITH eligible AS MATERIALIZED (
            SELECT
                gp.link AS game_link,
                gp.player_name,
                gp.color AS player_color,
                CASE
                    WHEN gp.result = 1 THEN 'win'
                    WHEN gp.result = 0.5 THEN 'draw'
                    ELSE 'loss'
                END AS result,
                CASE
                    WHEN gp.result = 1 THEN
                        CASE WHEN gp.color = 'white' THEN game.black_str_result ELSE game.white_str_result END
                    ELSE
                        CASE WHEN gp.color = 'white' THEN game.white_str_result ELSE game.black_str_result END
                END AS raw_end_by
            FROM game_player gp
            JOIN game ON game.link = gp.link
            JOIN game_analysis_summary coverage ON coverage.link = gp.link
            WHERE coverage.is_fully_analyzed
              AND coverage.total_positions > 0
              AND (:all_games OR gp.link = ANY(CAST(:game_links AS bigint[])))
        ),
        normalized AS MATERIALIZED (
            SELECT
                eligible.*,
                CASE LOWER(COALESCE(raw_end_by, ''))
                    WHEN 'checkmated' THEN 'checkmate'
                    WHEN 'checkmate' THEN 'checkmate'
                    WHEN 'timeout' THEN 'timeout'
                    WHEN 'resigned' THEN 'resignation'
                    WHEN 'resignation' THEN 'resignation'
                    WHEN 'abandoned' THEN 'abandonment'
                    WHEN 'agreed' THEN 'agreement'
                    WHEN 'repetition' THEN 'repetition'
                    WHEN 'stalemate' THEN 'stalemate'
                    WHEN 'insufficient' THEN 'insufficient_material'
                    WHEN 'timevsinsufficient' THEN 'timeout_vs_insufficient_material'
                    WHEN '50move' THEN 'fifty_move_rule'
                    ELSE COALESCE(NULLIF(LOWER(raw_end_by), ''), 'unknown')
                END AS end_by
            FROM eligible
        ),
        scored AS MATERIALIZED (
            SELECT
                normalized.game_link,
                normalized.player_name,
                normalized.player_color,
                normalized.result,
                normalized.end_by,
                association.n_move,
                association.move_color,
                fen.analysis_source,
                CASE
                    WHEN normalized.player_color = 'white' THEN fen.score
                    ELSE -fen.score
                END AS player_score,
                CASE
                    WHEN fen.analysis_source = 'tablebase' THEN 'tablebase'
                    WHEN ABS(fen.score) >= 9000 THEN 'mate'
                    ELSE 'cp'
                END AS score_kind
            FROM normalized
            JOIN game_fen_association association
              ON association.game_link = normalized.game_link
            JOIN fen ON fen.fen = association.fen_fen
            WHERE fen.score IS NOT NULL
        ),
        sequenced AS MATERIALIZED (
            SELECT
                scored.*,
                LAG(player_score) OVER game_order AS previous_player_score,
                LAG(score_kind) OVER game_order AS previous_score_kind,
                ROW_NUMBER() OVER reverse_game_order AS reverse_rank
            FROM scored
            WINDOW game_order AS (
                PARTITION BY game_link, player_color
                ORDER BY n_move, CASE WHEN move_color = 'white' THEN 0 ELSE 1 END
            ),
            reverse_game_order AS (
                PARTITION BY game_link, player_color
                ORDER BY n_move DESC, CASE WHEN move_color = 'black' THEN 0 ELSE 1 END
            )
        ),
        prepared AS MATERIALIZED (
            SELECT
                sequenced.*,
                CASE
                    WHEN previous_player_score IS NOT NULL THEN previous_player_score
                    WHEN player_color = 'white' THEN 15.0
                    ELSE -15.0
                END AS effective_previous_player_score,
                COALESCE(previous_score_kind, 'cp') AS effective_previous_score_kind
            FROM sequenced
        ),
        measured AS MATERIALIZED (
            SELECT
                prepared.*,
                CASE
                    WHEN score_kind = 'cp' AND effective_previous_score_kind = 'cp'
                    THEN player_score - effective_previous_player_score
                END AS cp_change,
                CASE
                    WHEN score_kind = 'cp' AND effective_previous_score_kind = 'cp'
                    THEN
                        (2.0 / (1.0 + EXP(
                            -0.00368208 * effective_previous_player_score
                        )) - 1.0)
                        -
                        (2.0 / (1.0 + EXP(
                            -0.00368208 * player_score
                        )) - 1.0)
                END AS winning_chance_loss
            FROM prepared
        )
        SELECT
            game_link,
            player_name,
            player_color,
            COUNT(*) FILTER (WHERE move_color = player_color)::int AS analyzed_player_moves,
            COALESCE(SUM(cp_change) FILTER (
                WHERE move_color = player_color AND cp_change > 0
            ), 0)::double precision AS own_move_cp_gain,
            COALESCE(SUM(-cp_change) FILTER (
                WHERE move_color = player_color AND cp_change < 0
            ), 0)::double precision AS own_move_cp_loss,
            COUNT(*) FILTER (
                WHERE move_color = player_color
                  AND (
                    (
                        effective_previous_score_kind = 'cp'
                        AND score_kind = 'cp'
                        AND winning_chance_loss >= 0.30
                    )
                    OR (
                        effective_previous_score_kind = 'cp'
                        AND score_kind = 'mate'
                        AND player_score < 0
                        AND effective_previous_player_score >= -700
                    )
                    OR (
                        effective_previous_score_kind = 'mate'
                        AND effective_previous_player_score > 0
                        AND score_kind = 'cp'
                        AND player_score <= 700
                    )
                    OR (
                        effective_previous_score_kind = 'mate'
                        AND effective_previous_player_score > 0
                        AND score_kind = 'mate'
                        AND player_score < 0
                    )
                  )
            )::int AS blunder_count,
            COUNT(*) FILTER (
                WHERE score_kind = 'mate' AND player_score > 0
            )::int AS mate_for_positions,
            COUNT(*) FILTER (
                WHERE score_kind = 'mate' AND player_score < 0
            )::int AS mate_against_positions,
            MAX(player_score) FILTER (
                WHERE reverse_rank = 1 AND score_kind = 'cp'
            )::double precision AS final_player_cp,
            result,
            end_by
        FROM measured
        GROUP BY game_link, player_name, player_color, result, end_by
        ON CONFLICT (game_link, player_color) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            analyzed_player_moves = EXCLUDED.analyzed_player_moves,
            own_move_cp_gain = EXCLUDED.own_move_cp_gain,
            own_move_cp_loss = EXCLUDED.own_move_cp_loss,
            blunder_count = EXCLUDED.blunder_count,
            mate_for_positions = EXCLUDED.mate_for_positions,
            mate_against_positions = EXCLUDED.mate_against_positions,
            final_player_cp = EXCLUDED.final_player_cp,
            result = EXCLUDED.result,
            end_by = EXCLUDED.end_by
        RETURNING game_link
    """)
    async with AsyncDBSession() as session:
        try:
            await session.execute(delete_sql, params)
            result = await session.execute(refresh_sql, params)
            rows = result.mappings().all()
            await session.commit()
        except Exception:
            await session.rollback()
            raise
    return {
        "game_players": len(rows),
        "games": len({int(row["game_link"]) for row in rows}),
    }
