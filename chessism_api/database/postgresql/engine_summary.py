"""Canonical per-game/side accuracy projections in a caller-owned transaction."""
from sqlalchemy import text

from chessism_api.accuracy import (
    LICHESS_WINNING_CHANCE_MULTIPLIER, LICHESS_CP_CEILING,
    LICHESS_ACCURACY_SCALE, LICHESS_ACCURACY_DECAY, LICHESS_ACCURACY_OFFSET,
    LICHESS_ACCURACY_UNCERTAINTY_BONUS, lichess_win_percent,
)

FIELDS = ('analyzed_player_moves', 'own_move_cp_gain', 'own_move_cp_loss',
          'game_efficiency', 'mean_win_percent_loss', 'median_win_percent_loss',
          'mate_for_positions', 'mate_against_positions', 'final_player_cp', 'result', 'end_by')

# The original window endpoints, opening prior, mate/tablebase classification and
# harmonic/weighted average are preserved; only canonical input/key mapping changes.
VALUES_SQL = f"""
WITH eligible AS MATERIALIZED (
    SELECT
        gp.game_link,
        gp.is_white,
        CASE
            WHEN (CASE WHEN gp.is_white THEN game.white_result ELSE game.black_result END) = 1 THEN 'win'
            WHEN (CASE WHEN gp.is_white THEN game.white_result ELSE game.black_result END) = 0.5 THEN 'draw'
            ELSE 'loss'
        END AS result,
        CASE
            WHEN (CASE WHEN gp.is_white THEN game.white_result ELSE game.black_result END) = 1 THEN
                CASE WHEN gp.is_white THEN game.black_str_result ELSE game.white_str_result END
            ELSE
                CASE WHEN gp.is_white THEN game.white_str_result ELSE game.black_str_result END
        END AS raw_end_by
    FROM public.game_player gp
    JOIN public.game ON game.link = gp.game_link
    JOIN public.game_analysis_coverage coverage ON coverage.link = gp.game_link
    WHERE game.is_standard AND game.n_moves > 0
      AND coverage.is_fully_analyzed
      AND coverage.total_positions > 0
      AND (:all_games OR gp.game_link = ANY(CAST(:game_links AS bigint[])))
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
        normalized.is_white,
        normalized.result,
        normalized.end_by,
        association.n_move,
        association.is_white AS move_is_white,
        fen.analysis_source,
        CASE
            WHEN normalized.is_white THEN fen.score::bigint
            ELSE -fen.score::bigint
        END AS player_score,
        CASE
            WHEN fen.analysis_source = 'tablebase' THEN 'tablebase'
            WHEN ABS(fen.score::bigint) >= 9000 THEN 'mate'
            ELSE 'cp'
        END AS score_kind
    FROM normalized
    JOIN public.game_fen_association association
      ON association.game_link = normalized.game_link
    JOIN public.fen ON fen.fen_id = association.fen_id
    WHERE fen.score IS NOT NULL
),
sequenced AS MATERIALIZED (
    SELECT
        scored.*,
        LAG(player_score) OVER game_order AS previous_player_score,
        LAG(score_kind) OVER game_order AS previous_score_kind,
        ROW_NUMBER() OVER game_order AS ply_index,
        COUNT(*) OVER game_partition AS total_plies,
        ROW_NUMBER() OVER reverse_game_order AS reverse_rank
    FROM scored
    WINDOW game_order AS (
        PARTITION BY game_link, is_white
        ORDER BY n_move, CASE WHEN move_is_white THEN 0 ELSE 1 END
    ),
    game_partition AS (
        PARTITION BY game_link, is_white
    ),
    reverse_game_order AS (
        PARTITION BY game_link, is_white
        ORDER BY n_move DESC, CASE WHEN move_is_white THEN 1 ELSE 0 END
    )
),
prepared AS MATERIALIZED (
    SELECT
        sequenced.*,
        CASE
            WHEN previous_player_score IS NOT NULL THEN previous_player_score
            WHEN is_white THEN 15.0
            ELSE -15.0
        END AS effective_previous_player_score,
        COALESCE(previous_score_kind, 'cp') AS effective_previous_score_kind,
        GREATEST(
            -{LICHESS_CP_CEILING},
            LEAST({LICHESS_CP_CEILING}, player_score)
        ) AS bounded_player_score,
        GREATEST(
            -{LICHESS_CP_CEILING},
            LEAST(
                {LICHESS_CP_CEILING},
                CASE
                    WHEN previous_player_score IS NOT NULL THEN previous_player_score
                    WHEN is_white THEN 15.0
                    ELSE -15.0
                END
            )
        ) AS bounded_previous_player_score
    FROM sequenced
),
evaluated AS MATERIALIZED (
    SELECT
        prepared.*,
        CASE
            WHEN score_kind = 'cp' AND effective_previous_score_kind = 'cp'
            THEN player_score - effective_previous_player_score
        END AS cp_change,
        50.0 + 50.0 * (
            2.0 / (1.0 + EXP(
                {LICHESS_WINNING_CHANCE_MULTIPLIER} * bounded_player_score
            )) - 1.0
        ) AS win_percent,
        50.0 + 50.0 * (
            2.0 / (1.0 + EXP(
                {LICHESS_WINNING_CHANCE_MULTIPLIER} * bounded_previous_player_score
            )) - 1.0
        ) AS previous_win_percent
    FROM prepared
),
game_series AS MATERIALIZED (
    SELECT
        game_link,
        is_white,
        ARRAY_PREPEND(
            CASE
                WHEN is_white THEN {lichess_win_percent(15.0)}
                ELSE {lichess_win_percent(-15.0)}
            END,
            ARRAY_AGG(win_percent ORDER BY ply_index)
        ) AS win_percents,
        LEAST(8, GREATEST(2, MAX(total_plies) / 10))::int AS window_size
    FROM evaluated
    GROUP BY game_link, is_white
),
measured AS MATERIALIZED (
    SELECT
        evaluated.*,
        GREATEST(
            0.0,
            previous_win_percent - win_percent
        ) AS win_percent_loss,
        CASE
            WHEN win_percent >= previous_win_percent THEN 100.0
            ELSE GREATEST(
                0.0,
                LEAST(
                    100.0,
                    {LICHESS_ACCURACY_SCALE} * EXP(
                        -{LICHESS_ACCURACY_DECAY}
                        * (previous_win_percent - win_percent)
                    )
                    + {LICHESS_ACCURACY_OFFSET}
                    + {LICHESS_ACCURACY_UNCERTAINTY_BONUS}
                )
            )
        END AS move_accuracy,
        volatility.weight AS volatility_weight
    FROM evaluated
    JOIN game_series
      ON game_series.game_link = evaluated.game_link
     AND game_series.is_white = evaluated.is_white
    CROSS JOIN LATERAL (
        SELECT GREATEST(
            0.5,
            LEAST(12.0, STDDEV_POP(sample.win_percent))
        ) AS weight
        FROM UNNEST(
            game_series.win_percents[
                GREATEST(
                    1,
                    evaluated.ply_index - game_series.window_size + 2
                ):
                GREATEST(
                    1,
                    evaluated.ply_index - game_series.window_size + 2
                ) + game_series.window_size - 1
            ]
        ) AS sample(win_percent)
    ) AS volatility
),
aggregated AS MATERIALIZED (
    SELECT
        game_link,
        is_white,
        COUNT(*) FILTER (WHERE move_is_white = is_white)::int
            AS analyzed_player_moves,
        COALESCE(SUM(cp_change) FILTER (
            WHERE move_is_white = is_white AND cp_change > 0
        ), 0)::double precision AS own_move_cp_gain,
        COALESCE(SUM(-cp_change) FILTER (
            WHERE move_is_white = is_white AND cp_change < 0
        ), 0)::double precision AS own_move_cp_loss,
        AVG(win_percent_loss) FILTER (
            WHERE move_is_white = is_white
        )::double precision AS mean_win_percent_loss,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY win_percent_loss
        ) FILTER (
            WHERE move_is_white = is_white
        )::double precision AS median_win_percent_loss,
        (
            SUM(move_accuracy * volatility_weight) FILTER (
                WHERE move_is_white = is_white
            )
            / NULLIF(
                SUM(volatility_weight) FILTER (
                    WHERE move_is_white = is_white
                ),
                0
            )
        )::double precision AS volatility_weighted_accuracy,
        CASE
            WHEN COUNT(*) FILTER (WHERE move_is_white = is_white) = 0
                THEN NULL
            WHEN COUNT(*) FILTER (
                WHERE move_is_white = is_white AND move_accuracy = 0
            ) > 0
                THEN 0.0
            ELSE
                COUNT(*) FILTER (WHERE move_is_white = is_white)::double precision
                / NULLIF(
                    SUM(1.0 / NULLIF(move_accuracy, 0)) FILTER (
                        WHERE move_is_white = is_white
                    ),
                    0
                )
        END AS harmonic_accuracy,
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
    GROUP BY game_link, is_white, result, end_by
)
SELECT
    game_link,
    is_white,
    analyzed_player_moves,
    own_move_cp_gain,
    own_move_cp_loss,
    (
        volatility_weighted_accuracy + harmonic_accuracy
    ) / 2.0 AS game_efficiency,
    mean_win_percent_loss,
    median_win_percent_loss,
    mate_for_positions,
    mate_against_positions,
    final_player_cp,
    result,
    end_by
FROM aggregated

ORDER BY game_link, is_white
"""


async def refresh_engine_summaries(session, links=None):
    if not session.in_transaction():
        raise ValueError('Engine summaries require a caller-owned transaction')
    if links is not None and not links:
        return {'game_players': 0, 'games': 0}
    # Activation takes an exclusive lock on this singleton. A CPU refresh that
    # already started must finish before activation can commit.
    manual = await session.scalar(text(
        'SELECT manual FROM public.cuda_summary_control WHERE id FOR SHARE'))
    if manual is None:
        raise RuntimeError('Required cuda_summary_control is missing')
    if manual:
        return {'game_players': 0, 'games': 0, 'deferred': True}
    params = {'all_games': links is None, 'game_links': list(links or ())}
    await session.execute(text("""SELECT link FROM public.game
        WHERE (:all_games OR link=ANY(CAST(:game_links AS bigint[])))
        ORDER BY link FOR UPDATE"""), params)
    # Read again after ordered locks: concurrent refreshes cannot publish an
    # older snapshot over a newer committed projection. Deletions are absolute.
    await session.execute(text("""DELETE FROM public.game_player_engine_summary s
        WHERE (:all_games OR s.game_link=ANY(CAST(:game_links AS bigint[])))
          AND NOT EXISTS (SELECT 1 FROM public.game_analysis_coverage c
            JOIN public.game g ON g.link=c.link
            WHERE c.link=s.game_link AND c.is_fully_analyzed AND c.total_positions>0
              AND g.is_standard AND g.n_moves>0)"""), params)
    result = await session.execute(text(f"""
        INSERT INTO public.game_player_engine_summary(game_link,is_white,{','.join(FIELDS)})
        {VALUES_SQL}
        ON CONFLICT(game_link,is_white) DO UPDATE SET
        {','.join(field+'=EXCLUDED.'+field for field in FIELDS)}
        RETURNING game_link
    """), params)
    rows = result.scalars().all()
    return {'game_players': len(rows), 'games': len(set(rows))}
