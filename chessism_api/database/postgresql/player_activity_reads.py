"""Canonical game-side reads for metadata charts and local-calendar activity."""
from sqlalchemy import text

from .player_catalog_reads import OPENING_KEY_SQL


SIDES = '''WITH sides AS NOT MATERIALIZED (
    SELECT g.link,g.played_at,g.eco,m.name AS mode,
        CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
        CASE WHEN gp.is_white THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
        CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END AS opponent_id
    FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
    LEFT JOIN public.game_mode m ON m.id=g.mode_id
    WHERE gp.player_id=(SELECT id FROM public.player WHERE player_name=:player)
        AND g.is_standard AND g.n_moves>0
)'''

DATE_WINDOW = '''(:mode='all' OR gp.mode=:mode)
    AND (CAST(:date_from_utc AS timestamptz) IS NULL OR gp.played_at>=CAST(:date_from_utc AS timestamptz))
    AND (CAST(:date_to_utc AS timestamptz) IS NULL OR gp.played_at<CAST(:date_to_utc AS timestamptz))'''

# The resolved zone pair includes the existing mean-country-zone convention.
LOCAL_TIME = '''(timezone(:timezone_left,gp.played_at)
    +(timezone(:timezone_right,gp.played_at)-timezone(:timezone_left,gp.played_at))/2)'''


async def execute(session, sql, params=None):
    if not session.in_transaction():
        raise ValueError('Activity reads require a caller-owned transaction')
    return await session.execute(text(sql), params or {})


async def read_limits(session):
    await execute(session, "SET LOCAL statement_timeout='5s'")
    await execute(session, "SET LOCAL lock_timeout='1s'")


async def activity(session, params):
    result = await execute(session, SIDES+f'''
        SELECT gp.mode,extract(isodow FROM {LOCAL_TIME})::int AS weekday,
            extract(hour FROM {LOCAL_TIME})::int AS hour,count(*)::bigint AS games,
            count(*) FILTER(WHERE gp.result=1)::bigint AS wins,
            count(*) FILTER(WHERE gp.result=0.5)::bigint AS draws,
            count(*) FILTER(WHERE gp.result=0)::bigint AS losses
        FROM sides gp WHERE {DATE_WINDOW}
        GROUP BY gp.mode,weekday,hour ORDER BY gp.mode,weekday,hour''', params)
    return result.mappings().all()


async def ratings(session, params):
    result = await execute(session, SIDES+f''', ranked AS (
        SELECT gp.mode,gp.rating,gp.link,gp.played_at,({LOCAL_TIME})::date AS local_date,
            row_number() OVER(PARTITION BY gp.mode,({LOCAL_TIME})::date
                ORDER BY gp.played_at DESC,gp.link DESC,gp.color ASC) AS day_rank
        FROM sides gp WHERE {DATE_WINDOW} AND gp.mode IN ('bullet','blitz','rapid')
    ) SELECT mode,rating,link,played_at,local_date FROM ranked WHERE day_rank=1
        ORDER BY mode,local_date''', params)
    return result.mappings().all()


async def day(session, params):
    result = await execute(session, SIDES+f'''
        SELECT gp.link,gp.mode,gp.rating,gp.result,gp.played_at FROM sides gp
        WHERE {DATE_WINDOW} ORDER BY gp.played_at,gp.link,gp.color DESC''', params)
    return result.mappings().all()


async def chart_games(session, scope_kind, params):
    filters = {
        'rating': 'gp.rating>0 AND gp.rating>=:rating_from AND gp.rating<:rating_to',
        'result': 'gp.result=:game_result',
        'opponent': 'gp.opponent_id=(SELECT id FROM public.player WHERE player_name=:opponent)',
        'opening': OPENING_KEY_SQL+'=:opening',
    }
    clause = filters[scope_kind]
    if params.get('color') is not None:
        clause += ' AND gp.color=:color'
    result = await execute(session, SIDES+f''', selected AS MATERIALIZED (
        SELECT gp.link AS game_link,gp.color AS player_color,gp.played_at,gp.mode,
            gp.rating,gp.opponent_rating,gp.result,gp.opponent_id,gp.played_at AS cursor_time
        FROM sides gp WHERE gp.mode=ANY(CAST(:modes AS text[])) AND ({clause})
            AND (CAST(:cursor_time AS timestamptz) IS NULL OR
                (gp.played_at,gp.link,gp.color)<(CAST(:cursor_time AS timestamptz),
                    CAST(:cursor_id AS bigint),CAST(:cursor_color AS text)))
        ORDER BY gp.played_at DESC,gp.link DESC,gp.color DESC LIMIT :limit
    ) SELECT selected.*,p.player_name AS opponent_name FROM selected
        JOIN public.player p ON p.id=selected.opponent_id
        ORDER BY selected.played_at DESC,selected.game_link DESC,selected.player_color DESC''', params)
    return result.mappings().all()
