"""Read-only game-level salience filtering before daily aggregation."""
import math
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.player_hero_analytics import _analytics_scope
from chessism_api.operations.player_timezone import player_local_timestamp_sql

SALIENCE_JOIN = """
    JOIN game_player_salience salience
      ON salience.game_link = gp.link AND salience.player_color = gp.color
     AND salience.player_name = gp.player_name
"""
ELIGIBLE = """
    engine.player_name = gp.player_name
    AND engine.analyzed_player_moves > 10
    AND engine.game_efficiency IS NOT NULL
    AND EXISTS (SELECT 1 FROM player_salience_summary summary
                WHERE summary.player_name = gp.player_name AND summary.status = 'ready')
"""


def validate_range(minimum, maximum):
    if minimum is None and maximum is None:
        return False
    if (minimum is None or maximum is None
            or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                   for v in (minimum, maximum))
            or not 0 <= minimum <= maximum <= 1):
        raise ValueError('Provide both salience bounds with 0 <= minimum <= maximum <= 1.')
    return True


def range_sql(column):
    # Tiny tolerance covers floating-point interpolation at inclusive boundaries.
    return f"""(CAST(:salience_min AS double precision) IS NULL OR
        {column} BETWEEN CAST(:salience_min AS double precision) - 1e-12
                     AND CAST(:salience_max AS double precision) + 1e-12)"""


LEGACY_ELIGIBLE_SQL = f"""
    SELECT ({player_local_timestamp_sql('gp')})::date AS day,
           engine.game_efficiency AS accuracy, salience.salience
    FROM game_player gp
    JOIN game_player_engine_summary engine
      ON engine.game_link = gp.link AND engine.player_color = gp.color
    {SALIENCE_JOIN}
    WHERE gp.player_name = :player AND {ELIGIBLE}
      AND gp.mode = ANY(CAST(:selected_modes AS text[]))
      AND gp.played_at IS NOT NULL
      AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at >= CAST(:date_from_utc AS timestamptz))
      AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at < CAST(:date_to_utc AS timestamptz))
"""

DAILY_AGGREGATES_SQL = f"""
bounds AS (
    SELECT MIN(salience) AS minimum, MAX(salience) AS maximum FROM eligible
), marked AS (
    SELECT *, {range_sql('salience')} AS included FROM eligible
), days AS (
    SELECT day, COUNT(*) AS total_games,
           SUM(accuracy * salience) / NULLIF(SUM(salience), 0) AS baseline_accuracy,
           AVG(accuracy) AS baseline_unweighted, SUM(salience) AS baseline_salience,
           COUNT(*) FILTER (WHERE included) AS games,
           SUM(accuracy * salience) FILTER (WHERE included)
             / NULLIF(SUM(salience) FILTER (WHERE included), 0) AS weighted_accuracy,
           AVG(accuracy) FILTER (WHERE included) AS unweighted_accuracy,
           SUM(salience) FILTER (WHERE included) AS daily_salience
    FROM marked GROUP BY day
)
SELECT bounds.*, days.* FROM bounds LEFT JOIN days ON TRUE ORDER BY day
"""

DAILY_SQL = f"WITH eligible AS MATERIALIZED ({LEGACY_ELIGIBLE_SQL}), {DAILY_AGGREGATES_SQL}"


def daily_sql(session):
    layout = storage_for(session)
    if layout is None:
        return text(DAILY_SQL)
    # All three records identify the same player perspective by game_player_id.
    # Joining the compatibility views by names and decoded colors expands the
    # same ownership joins repeatedly and hides these indexed relationships.
    return text(f"""
        WITH eligible AS MATERIALIZED (
            SELECT ({player_local_timestamp_sql('g')})::date AS day,
                   engine.game_efficiency AS accuracy, salience.salience
            FROM {layout.table('game_player')} gp
            JOIN {layout.table('game')} g ON g.link = gp.game_link
            JOIN {layout.table('game_player_engine_summary')} engine ON engine.game_player_id = gp.id
            JOIN {layout.table('game_player_salience')} salience ON salience.game_player_id = gp.id
            WHERE gp.player_id = (SELECT id FROM {layout.table('player')} WHERE player_name = :player)
              AND engine.analyzed_player_moves > 10
              AND engine.game_efficiency IS NOT NULL
              AND EXISTS (SELECT 1 FROM {layout.table('player_salience_summary')} summary
                          WHERE summary.player_id = gp.player_id AND summary.status = 'ready')
              AND g.mode_id = ANY(ARRAY(SELECT id FROM {layout.table('game_mode')}
                                        WHERE name = ANY(CAST(:selected_modes AS text[]))))
              AND g.played_at IS NOT NULL
              AND (CAST(:date_from_utc AS timestamptz) IS NULL OR g.played_at >= CAST(:date_from_utc AS timestamptz))
              AND (CAST(:date_to_utc AS timestamptz) IS NULL OR g.played_at < CAST(:date_to_utc AS timestamptz))
        ), {DAILY_AGGREGATES_SQL}
    """)


async def daily_chart(player, modes, date_from=None, date_to=None, timezone_name=None,
                      salience_min=None, salience_max=None):
    filtered = validate_range(salience_min, salience_max)
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        scope = await _analytics_scope(session, player, 'all', date_from, date_to, timezone_name)
        summary = (await session.execute(text("""
            SELECT status, source_game_count, effective_game_count
            FROM player_salience_summary WHERE player_name = :player
        """), {'player': scope.player})).mappings().first()
        status = str(summary['status']) if summary else 'missing'
        if filtered and status != 'ready':
            raise ValueError('Game salience is not ready; refresh the chart before filtering.')
        rows = (await session.execute(daily_sql(session), {
            **scope.params(), 'selected_modes': list(modes),
            'salience_min': salience_min, 'salience_max': salience_max,
        })).mappings().all()

    baseline, points = [], []
    for row in rows:
        if row['day'] is None:
            continue
        baseline.append([row['day'].isoformat(), round(float(row['baseline_accuracy']), 2),
            round(float(row['baseline_unweighted']), 2), round(float(row['baseline_salience']), 6),
            int(row['total_games']), int(row['total_games'])])
        if row['games']:
            points.append([row['day'].isoformat(), round(float(row['weighted_accuracy']), 2),
                round(float(row['unweighted_accuracy']), 2), round(float(row['daily_salience']), 6),
                int(row['games']), int(row['total_games'])])
    payload = scope.response_base()
    payload['filters'].update(mode=','.join(modes), modes=list(modes),
        minimum_analyzed_player_moves_exclusive=10, salience_min=salience_min, salience_max=salience_max)
    return {**payload,
        'salience': {'status': status, 'metric': 'individual_game_salience',
            'source_game_count': int(summary['source_game_count'] or 0) if summary else 0,
            'effective_game_count': round(float(summary['effective_game_count'] or 0), 6) if summary else 0,
            'scale': {'minimum': rows[0]['minimum'], 'maximum': rows[0]['maximum']}},
        'columns': ['date_game_init', 'weighted_accuracy', 'unweighted_accuracy', 'daily_salience', 'games', 'total_games'],
        'points': points, 'baseline_points': baseline,
    }
