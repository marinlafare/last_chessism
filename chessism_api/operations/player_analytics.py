import json
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession


PLAYER_ANALYTICS_CACHE_VERSION = "v1"
PLAYER_ANALYTICS_CACHE_SECONDS = 300
VALID_MODES = {"all", "bullet", "blitz", "rapid"}


def normalize_player_analytics_filters(
    player_name: str,
    mode: str,
    date_from: date | None,
    date_to: date | None,
) -> dict[str, Any]:
    normalized_player = str(player_name or "").strip().lower()
    normalized_mode = str(mode or "all").strip().lower()
    if not normalized_player:
        raise ValueError("A player name is required.")
    if normalized_mode not in VALID_MODES:
        raise ValueError("Mode must be all, bullet, blitz or rapid.")
    if date_from and date_to and date_from > date_to:
        raise ValueError("Start date must not be after end date.")
    return {
        "player": normalized_player,
        "mode": normalized_mode,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
    }


def player_analytics_cache_key(kind: str, filters: dict[str, Any]) -> str:
    return ":".join([
        "chessism",
        "player_analytics",
        PLAYER_ANALYTICS_CACHE_VERSION,
        kind,
        filters["player"],
        filters["mode"],
        filters["date_from"] or "first",
        filters["date_to"] or "latest",
    ])


def _decode_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return {}


async def _run_payload_query(query: str, filters: dict[str, Any]) -> dict[str, Any]:
    async with AsyncDBSession() as session:
        result = await session.execute(text(query), filters)
        payload = _decode_payload(result.scalar())
    payload["player_name"] = filters["player"]
    payload["filters"] = {
        "mode": filters["mode"],
        "date_from": filters["date_from"],
        "date_to": filters["date_to"],
    }
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    return payload


ENGINE_INSIGHTS_SQL = """
WITH filtered_games AS MATERIALIZED (
    SELECT
        gp.link,
        gp.color,
        gp.result,
        gp.rating,
        gp.opponent_rating,
        gp.mode,
        gp.played_at
    FROM game_player gp
    WHERE gp.player_name = :player
      AND (:mode = 'all' OR gp.mode = :mode)
      AND (CAST(:date_from AS date) IS NULL OR gp.played_at >= CAST(:date_from AS date))
      AND (CAST(:date_to AS date) IS NULL OR gp.played_at < CAST(:date_to AS date) + INTERVAL '1 day')
),
analyzed_candidates AS MATERIALIZED (
    SELECT
        filtered.*,
        ROW_NUMBER() OVER (ORDER BY filtered.played_at NULLS FIRST, filtered.link) AS sample_rank,
        COUNT(*) OVER () AS analyzed_game_count
    FROM filtered_games filtered
    JOIN game_analysis_summary summary ON summary.link = filtered.link
    WHERE summary.is_fully_analyzed
      AND summary.total_positions > 0
),
eligible_games AS MATERIALIZED (
    SELECT
        link,
        color,
        result,
        rating,
        opponent_rating,
        mode,
        played_at
    FROM analyzed_candidates
    WHERE sample_rank = 1
       OR ((sample_rank - 1) * 2000 / analyzed_game_count)
          > ((sample_rank - 2) * 2000 / analyzed_game_count)
    ORDER BY sample_rank
    LIMIT 2000
),
sequenced AS MATERIALIZED (
    SELECT
        eligible.link,
        eligible.color,
        eligible.result,
        eligible.mode,
        gfa.n_move,
        gfa.move_color,
        (gfa.n_move * 2 - CASE WHEN gfa.move_color = 'white' THEN 1 ELSE 0 END) AS ply,
        f.score,
        f.wdl_win,
        f.wdl_draw,
        f.wdl_loss,
        COALESCE(
            f.piece_count,
            char_length(translate(split_part(f.fen, ' ', 1), '12345678/', ''))
        ) AS piece_count,
        f.analysis_source,
        LAG(f.score) OVER game_order AS previous_score,
        LAG(f.wdl_win) OVER game_order AS previous_wdl_win,
        LAG(f.wdl_draw) OVER game_order AS previous_wdl_draw,
        LAG(f.wdl_loss) OVER game_order AS previous_wdl_loss,
        LAG(f.analysis_source) OVER game_order AS previous_analysis_source
    FROM eligible_games eligible
    JOIN game_fen_association gfa ON gfa.game_link = eligible.link
    JOIN fen f ON f.fen = gfa.fen_fen
    WINDOW game_order AS (
        PARTITION BY eligible.link
        ORDER BY gfa.n_move, CASE WHEN gfa.move_color = 'white' THEN 0 ELSE 1 END
    )
),
evaluated AS MATERIALIZED (
    SELECT
        sequenced.*,
        CASE
            WHEN color = 'white' AND wdl_win IS NOT NULL
                THEN (wdl_win + 0.5 * wdl_draw) / 1000.0
            WHEN color = 'black' AND wdl_loss IS NOT NULL
                THEN (wdl_loss + 0.5 * wdl_draw) / 1000.0
        END AS player_expectation,
        CASE
            WHEN color = 'white' AND previous_wdl_win IS NOT NULL
                THEN (previous_wdl_win + 0.5 * previous_wdl_draw) / 1000.0
            WHEN color = 'black' AND previous_wdl_loss IS NOT NULL
                THEN (previous_wdl_loss + 0.5 * previous_wdl_draw) / 1000.0
        END AS previous_player_expectation
    FROM sequenced
),
player_moves AS MATERIALIZED (
    SELECT
        evaluated.*,
        LEAST(1000.0, GREATEST(
            0.0,
            CASE
                WHEN color = 'white' THEN previous_score - score
                ELSE score - previous_score
            END
        )) AS cp_loss,
        GREATEST(
            0.0,
            COALESCE(previous_player_expectation - player_expectation, 0.0)
        ) AS expectation_loss,
        CASE
            WHEN ply <= 20 THEN 'Opening'
            WHEN piece_count <= 10 THEN 'Endgame'
            ELSE 'Middlegame'
        END AS phase
    FROM evaluated
    WHERE move_color = color
      AND previous_score IS NOT NULL
),
coverage AS (
    SELECT
        COUNT(*)::bigint AS total_games,
        COUNT(*) FILTER (WHERE summary.is_fully_analyzed)::bigint AS analyzed_games,
        COALESCE(SUM(summary.total_positions), 0)::bigint AS positions,
        COALESCE(SUM(summary.analyzed_positions), 0)::bigint AS analyzed_positions,
        (SELECT COUNT(*) FROM eligible_games)::bigint AS sampled_games
    FROM filtered_games filtered
    LEFT JOIN game_analysis_summary summary ON summary.link = filtered.link
),
phase_quality AS (
    SELECT
        phase,
        CASE phase WHEN 'Opening' THEN 1 WHEN 'Middlegame' THEN 2 ELSE 3 END AS phase_order,
        COUNT(*)::bigint AS moves,
        ROUND(AVG(cp_loss)::numeric, 1) AS average_cp_loss,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cp_loss)::numeric, 1) AS median_cp_loss,
        ROUND((100.0 * AVG((cp_loss >= 200)::int))::numeric, 2) AS blunder_rate,
        ROUND((100.0 * AVG((cp_loss >= 100 AND cp_loss < 200)::int))::numeric, 2) AS mistake_rate,
        ROUND((100.0 * AVG((cp_loss >= 50 AND cp_loss < 100)::int))::numeric, 2) AS inaccuracy_rate,
        ROUND((100.0 * AVG(expectation_loss))::numeric, 2) AS expected_score_loss,
        ROUND((1.96 * STDDEV_SAMP(cp_loss) / NULLIF(SQRT(COUNT(*)), 0))::numeric, 1) AS margin_of_error
    FROM player_moves
    GROUP BY phase
),
game_expectations AS (
    SELECT
        link,
        MAX(result) AS result,
        MAX(player_expectation) AS best_expectation,
        MIN(player_expectation) AS worst_expectation
    FROM evaluated
    WHERE player_expectation IS NOT NULL
    GROUP BY link
),
conversion AS (
    SELECT
        CASE
            WHEN best_expectation >= 0.90 THEN 'Winning'
            WHEN best_expectation >= 0.70 THEN 'Clear edge'
            ELSE 'Small edge'
        END AS advantage,
        CASE
            WHEN best_expectation >= 0.90 THEN 3
            WHEN best_expectation >= 0.70 THEN 2
            ELSE 1
        END AS advantage_order,
        COUNT(*)::bigint AS games,
        COUNT(*) FILTER (WHERE result = 1)::bigint AS wins,
        COUNT(*) FILTER (WHERE result = 0.5)::bigint AS draws,
        COUNT(*) FILTER (WHERE result = 0)::bigint AS losses,
        ROUND((100.0 * AVG((result = 1)::int))::numeric, 2) AS conversion_rate
    FROM game_expectations
    WHERE best_expectation >= 0.55
    GROUP BY advantage, advantage_order
),
resilience AS (
    SELECT
        COUNT(*) FILTER (WHERE best_expectation >= 0.90)::bigint AS winning_opportunities,
        COUNT(*) FILTER (WHERE best_expectation >= 0.90 AND result < 1)::bigint AS thrown_games,
        COUNT(*) FILTER (WHERE worst_expectation <= 0.10)::bigint AS losing_positions,
        COUNT(*) FILTER (WHERE worst_expectation <= 0.10 AND result > 0)::bigint AS comebacks
    FROM game_expectations
),
clock_samples AS MATERIALIZED (
    SELECT
        player_moves.cp_loss,
        CASE WHEN player_moves.color = 'white' THEN moves.white_reaction_time ELSE moves.black_reaction_time END AS reaction_time,
        CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END AS time_left,
        CASE
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 10 THEN '0–10s'
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 30 THEN '10–30s'
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 60 THEN '30–60s'
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 180 THEN '1–3m'
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 600 THEN '3–10m'
            ELSE '10m+'
        END AS clock_bucket,
        CASE
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 10 THEN 1
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 30 THEN 2
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 60 THEN 3
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 180 THEN 4
            WHEN (CASE WHEN player_moves.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END) < 600 THEN 5
            ELSE 6
        END AS clock_order
    FROM player_moves
    JOIN moves ON moves.link = player_moves.link AND moves.n_move = player_moves.n_move
),
time_pressure AS (
    SELECT
        clock_bucket,
        clock_order,
        COUNT(*)::bigint AS moves,
        ROUND(AVG(cp_loss)::numeric, 1) AS average_cp_loss,
        ROUND((100.0 * AVG((cp_loss >= 200)::int))::numeric, 2) AS blunder_rate,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY reaction_time)::numeric, 2) AS median_reaction_seconds
    FROM clock_samples
    WHERE time_left >= 0 AND reaction_time >= 0
    GROUP BY clock_bucket, clock_order
),
endgame_precision AS (
    SELECT
        piece_count,
        COUNT(*)::bigint AS moves,
        COUNT(*) FILTER (WHERE expectation_loss <= 0.0001)::bigint AS precise_moves,
        ROUND((100.0 * AVG((expectation_loss <= 0.0001)::int))::numeric, 2) AS precision_rate,
        ROUND((100.0 * AVG(expectation_loss))::numeric, 2) AS expected_score_loss
    FROM player_moves
    WHERE piece_count <= 7
      AND analysis_source = 'tablebase'
      AND previous_analysis_source = 'tablebase'
      AND player_expectation IS NOT NULL
      AND previous_player_expectation IS NOT NULL
    GROUP BY piece_count
)
SELECT jsonb_build_object(
    'coverage', (SELECT jsonb_build_object(
        'total_games', total_games,
        'analyzed_games', analyzed_games,
        'sampled_games', sampled_games,
        'positions', positions,
        'analyzed_positions', analyzed_positions,
        'move_samples', (SELECT COUNT(*) FROM player_moves)
    ) FROM coverage),
    'phase_quality', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'phase', phase,
        'moves', moves,
        'average_cp_loss', average_cp_loss,
        'median_cp_loss', median_cp_loss,
        'blunder_rate', blunder_rate,
        'mistake_rate', mistake_rate,
        'inaccuracy_rate', inaccuracy_rate,
        'expected_score_loss', expected_score_loss,
        'margin_of_error', margin_of_error
    ) ORDER BY phase_order) FROM phase_quality), '[]'::jsonb),
    'conversion', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'advantage', advantage,
        'games', games,
        'wins', wins,
        'draws', draws,
        'losses', losses,
        'conversion_rate', conversion_rate
    ) ORDER BY advantage_order) FROM conversion), '[]'::jsonb),
    'resilience', (SELECT to_jsonb(resilience) FROM resilience),
    'time_pressure', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'bucket', clock_bucket,
        'moves', moves,
        'average_cp_loss', average_cp_loss,
        'blunder_rate', blunder_rate,
        'median_reaction_seconds', median_reaction_seconds
    ) ORDER BY clock_order) FROM time_pressure), '[]'::jsonb),
    'endgame_precision', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'piece_count', piece_count,
        'moves', moves,
        'precise_moves', precise_moves,
        'precision_rate', precision_rate,
        'expected_score_loss', expected_score_loss
    ) ORDER BY piece_count DESC) FROM endgame_precision), '[]'::jsonb)
)
"""


PLAYING_PATTERNS_SQL = """
WITH filtered_games AS MATERIALIZED (
    SELECT
        gp.link,
        gp.color,
        gp.result,
        gp.rating,
        gp.opponent_rating,
        gp.mode,
        gp.played_at,
        gp.eco,
        gp.n_moves
    FROM game_player gp
    WHERE gp.player_name = :player
      AND (:mode = 'all' OR gp.mode = :mode)
      AND (CAST(:date_from AS date) IS NULL OR gp.played_at >= CAST(:date_from AS date))
      AND (CAST(:date_to AS date) IS NULL OR gp.played_at < CAST(:date_to AS date) + INTERVAL '1 day')
),
summary AS (
    SELECT
        COUNT(*)::bigint AS games,
        COUNT(*) FILTER (WHERE result = 1)::bigint AS wins,
        COUNT(*) FILTER (WHERE result = 0.5)::bigint AS draws,
        COUNT(*) FILTER (WHERE result = 0)::bigint AS losses,
        ROUND(AVG(rating)::numeric, 1) AS average_rating,
        ROUND(AVG(opponent_rating)::numeric, 1) AS average_opponent_rating,
        MIN(played_at) AS first_game,
        MAX(played_at) AS last_game
    FROM filtered_games
),
monthly_desc AS (
    SELECT
        date_trunc('month', played_at)::date AS month,
        COUNT(*)::bigint AS games,
        ROUND(AVG(rating)::numeric, 1) AS average_rating,
        ROUND((100.0 * AVG(result))::numeric, 2) AS score_rate,
        COUNT(*) FILTER (WHERE result = 1)::bigint AS wins,
        COUNT(*) FILTER (WHERE result = 0.5)::bigint AS draws,
        COUNT(*) FILTER (WHERE result = 0)::bigint AS losses
    FROM filtered_games
    WHERE played_at IS NOT NULL
    GROUP BY date_trunc('month', played_at)
    ORDER BY month DESC
    LIMIT 240
),
opening_repertoire AS (
    SELECT
        COALESCE(NULLIF(eco, ''), 'Unknown') AS eco,
        color,
        COUNT(*)::bigint AS games,
        ROUND((100.0 * AVG(result))::numeric, 2) AS score_rate,
        ROUND(AVG(opponent_rating)::numeric, 0) AS average_opponent_rating
    FROM filtered_games
    GROUP BY COALESCE(NULLIF(eco, ''), 'Unknown'), color
    ORDER BY games DESC, eco, color
    LIMIT 12
),
clock_game_candidates AS MATERIALIZED (
    SELECT
        link,
        color,
        ROW_NUMBER() OVER (ORDER BY played_at NULLS FIRST, link) AS sample_rank,
        COUNT(*) OVER () AS game_count
    FROM filtered_games
),
clock_games AS MATERIALIZED (
    SELECT link, color
    FROM clock_game_candidates
    WHERE sample_rank = 1
       OR ((sample_rank - 1) * 5000 / game_count)
          > ((sample_rank - 2) * 5000 / game_count)
    ORDER BY sample_rank
    LIMIT 5000
),
player_moves AS MATERIALIZED (
    SELECT
        sampled.link,
        sampled.color,
        moves.n_move,
        CASE WHEN sampled.color = 'white' THEN moves.white_reaction_time ELSE moves.black_reaction_time END AS reaction_time,
        CASE WHEN sampled.color = 'white' THEN moves.white_time_left ELSE moves.black_time_left END AS time_left,
        CASE WHEN sampled.color = 'white' THEN moves.white_move ELSE moves.black_move END AS move
    FROM clock_games sampled
    JOIN moves ON moves.link = sampled.link
),
clock_curve AS (
    SELECT
        LEAST(20, FLOOR((n_move - 1) / 5.0)::int) AS bucket_order,
        CASE
            WHEN n_move > 100 THEN '101+'
            ELSE CONCAT((FLOOR((n_move - 1) / 5.0)::int * 5) + 1, '–', (FLOOR((n_move - 1) / 5.0)::int * 5) + 5)
        END AS move_range,
        COUNT(*)::bigint AS moves,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY reaction_time)::numeric, 2) AS median_reaction_seconds,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_left)::numeric, 2) AS median_time_left
    FROM player_moves
    WHERE move IS NOT NULL AND move <> '--' AND reaction_time >= 0 AND time_left >= 0
    GROUP BY bucket_order, move_range
),
activity AS (
    SELECT
        EXTRACT(ISODOW FROM played_at)::int AS weekday,
        FLOOR(EXTRACT(HOUR FROM played_at) / 6)::int AS time_band,
        COUNT(*)::bigint AS games
    FROM filtered_games
    WHERE played_at IS NOT NULL
    GROUP BY weekday, time_band
),
opponent_distribution AS (
    SELECT
        FLOOR(opponent_rating / 200.0)::int * 200 AS rating_from,
        COUNT(*)::bigint AS games,
        ROUND((100.0 * AVG(result))::numeric, 2) AS score_rate
    FROM filtered_games
    WHERE opponent_rating > 0
    GROUP BY rating_from
    ORDER BY rating_from
),
length_distribution AS (
    SELECT
        LEAST(100, FLOOR(n_moves / 10.0)::int * 10) AS moves_from,
        COUNT(*)::bigint AS games,
        ROUND((100.0 * AVG(result))::numeric, 2) AS score_rate
    FROM filtered_games
    WHERE n_moves > 0
    GROUP BY moves_from
    ORDER BY moves_from
)
SELECT jsonb_build_object(
    'summary', (SELECT jsonb_build_object(
        'games', games,
        'wins', wins,
        'draws', draws,
        'losses', losses,
        'average_rating', average_rating,
        'average_opponent_rating', average_opponent_rating,
        'clock_sample_games', (SELECT COUNT(*) FROM clock_games),
        'first_game', first_game,
        'last_game', last_game
    ) FROM summary),
    'rating_results', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'month', month,
        'games', games,
        'average_rating', average_rating,
        'score_rate', score_rate,
        'wins', wins,
        'draws', draws,
        'losses', losses
    ) ORDER BY month) FROM monthly_desc), '[]'::jsonb),
    'opening_repertoire', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'eco', eco,
        'color', color,
        'games', games,
        'score_rate', score_rate,
        'average_opponent_rating', average_opponent_rating
    ) ORDER BY games DESC, eco, color) FROM opening_repertoire), '[]'::jsonb),
    'clock_curve', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'move_range', move_range,
        'moves', moves,
        'median_reaction_seconds', median_reaction_seconds,
        'median_time_left', median_time_left
    ) ORDER BY bucket_order) FROM clock_curve), '[]'::jsonb),
    'activity', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'weekday', weekday,
        'time_band', time_band,
        'games', games
    ) ORDER BY weekday, time_band) FROM activity), '[]'::jsonb),
    'opponent_distribution', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'rating_from', rating_from,
        'games', games,
        'score_rate', score_rate
    ) ORDER BY rating_from) FROM opponent_distribution), '[]'::jsonb),
    'length_distribution', COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'moves_from', moves_from,
        'games', games,
        'score_rate', score_rate
    ) ORDER BY moves_from) FROM length_distribution), '[]'::jsonb)
)
"""


async def get_player_engine_insights(filters: dict[str, Any]) -> dict[str, Any]:
    return await _run_payload_query(ENGINE_INSIGHTS_SQL, filters)


async def get_player_playing_patterns(filters: dict[str, Any]) -> dict[str, Any]:
    return await _run_payload_query(PLAYING_PATTERNS_SQL, filters)
