"""Canonical player-perspective game statistics in caller-owned read snapshots."""
from sqlalchemy import text

from chessism_api.player_titles import OFFICIAL_PLAYER_TITLES


# A fixed projection for this feature, not a compatibility view/query rewriter.
# Both sides of self-play count; archived variants/zero-move metadata never did.
PLAYER_SIDES = '''WITH sides AS NOT MATERIALIZED (
    SELECT g.link,g.played_at,g.mode_id,gp.is_white,g.n_moves,g.eco,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
        CASE WHEN gp.is_white THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
        CASE WHEN gp.is_white THEN g.white_str_result ELSE g.black_str_result END AS end_by
    FROM public.player p JOIN public.game_player gp ON gp.player_id=p.id
    JOIN public.game g ON g.link=gp.game_link
    WHERE p.player_name=:player AND g.is_standard AND g.n_moves>0
)'''


def _require_transaction(session):
    if not session.in_transaction():
        raise ValueError('Player reads require a caller-owned transaction')


async def coverage(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''
        SELECT totals.*,latest.rating AS latest_rating,latest.mode AS latest_rating_mode,
            latest.played_at AS latest_rating_at
        FROM (SELECT count(*)::bigint AS total_games,
            count(*) FILTER (WHERE c.is_fully_analyzed)::bigint AS analyzed_games,
            coalesce(sum(c.total_positions),0)::bigint AS total_positions,
            coalesce(sum(c.analyzed_positions),0)::bigint AS analyzed_positions,
            coalesce(sum(c.unscored_positions),0)::bigint AS unscored_positions
            FROM sides s LEFT JOIN LATERAL (
                SELECT total_positions,analyzed_positions,unscored_positions,is_fully_analyzed
                FROM public.game_analysis_coverage WHERE link=s.link OFFSET 0
            ) c ON TRUE
        ) totals LEFT JOIN LATERAL (
            SELECT s.rating,m.name AS mode,s.played_at FROM sides s
            LEFT JOIN public.game_mode m ON m.id=s.mode_id
            ORDER BY s.played_at DESC,s.link DESC,s.is_white ASC LIMIT 1
        ) latest ON TRUE
    '''), {'player': player})
    return dict(result.mappings().one())


async def neighbors(session, player):
    _require_transaction(session)
    result = await session.execute(text('''SELECT
        coalesce((SELECT player_name FROM public.player
            WHERE joined>0 AND deleted_at IS NULL AND player_name<:player ORDER BY player_name DESC LIMIT 1),
            (SELECT player_name FROM public.player WHERE joined>0 AND deleted_at IS NULL ORDER BY player_name DESC LIMIT 1)) AS previous_player,
        coalesce((SELECT player_name FROM public.player
            WHERE joined>0 AND deleted_at IS NULL AND player_name>:player ORDER BY player_name ASC LIMIT 1),
            (SELECT player_name FROM public.player WHERE joined>0 AND deleted_at IS NULL ORDER BY player_name ASC LIMIT 1)) AS next_player
    '''), {'player': player})
    return dict(result.mappings().one())


async def navigation(session, same_group_as):
    _require_transaction(session)
    result = await session.execute(text('''SELECT p.player_name,p.avatar FROM public.player p
        WHERE p.joined>0 AND p.deleted_at IS NULL
          AND (CAST(:selected AS text) IS NULL OR
            (upper(btrim(coalesce(p.title,'')))=ANY(CAST(:titles AS text[]))) = (
                SELECT upper(btrim(coalesce(s.title,'')))=ANY(CAST(:titles AS text[]))
                FROM public.player s WHERE s.player_name=:selected AND s.deleted_at IS NULL))
        ORDER BY p.player_name
    '''), {'selected': same_group_as, 'titles': list(OFFICIAL_PLAYER_TITLES)})
    return [dict(row) for row in result.mappings()]


async def performance(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+''' SELECT count(*) AS total_games,
        sum(is_white::int) AS white_games,
        sum((is_white AND result=1)::int) AS white_wins,
        sum((is_white AND result=0)::int) AS white_losses,
        sum((is_white AND result=0.5)::int) AS white_draws,
        sum((NOT is_white)::int) AS black_games,
        sum((NOT is_white AND result=1)::int) AS black_wins,
        sum((NOT is_white AND result=0)::int) AS black_losses,
        sum((NOT is_white AND result=0.5)::int) AS black_draws
        FROM sides'''), {'player': player})
    return dict(result.mappings().one())


async def averages(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''
        SELECT avg(n_moves) AS avg_game_length,avg(opponent_rating) AS avg_opponent_rating FROM sides
    '''), {'player': player})
    return dict(result.mappings().one())


async def terminations(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''SELECT
        sum((end_by='checkmated')::int) AS checkmated,sum((end_by='resigned')::int) AS resigned,
        sum((end_by='timeout')::int) AS timeout,sum((end_by='abandoned')::int) AS abandoned
        FROM sides'''), {'player': player})
    return dict(result.mappings().one())


async def openings(session, player, limit):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''SELECT eco,count(*) AS total_games,
        sum((result=1)::int) AS wins,sum((result=0)::int) AS losses,sum((result=0.5)::int) AS draws
        FROM sides WHERE eco<>'no_eco' GROUP BY eco ORDER BY total_games DESC,eco ASC LIMIT :limit
    '''), {'player': player, 'limit': limit})
    return [dict(row) for row in result.mappings()]


async def game_count(session, player):
    _require_transaction(session)
    return int(await session.scalar(text(PLAYER_SIDES+'SELECT count(*) FROM sides'), {'player': player}))


async def games(session, player, *, mode=None, limit=None, offset=0):
    _require_transaction(session)
    query = PLAYER_SIDES+'''SELECT s.link,s.played_at,
        CASE WHEN s.is_white THEN 'white' ELSE 'black' END AS color,s.result AS player_score
        FROM sides s'''
    params = {'player': player}
    if mode is not None:
        query += ' JOIN public.game_mode m ON m.id=s.mode_id WHERE m.name=:mode'
        params['mode'] = mode
    query += ' ORDER BY s.played_at DESC,s.link DESC,s.is_white ASC'
    if limit is not None:
        query += ' LIMIT :limit OFFSET :offset'
        params.update(limit=limit, offset=offset)
    result = await session.execute(text(query), params)
    return [dict(row) for row in result.mappings()]


async def summary(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''SELECT sum((result=1)::int) AS wins,
        sum((result=0)::int) AS losses,sum((result=0.5)::int) AS draws,
        min(played_at) AS first_game,max(played_at) AS last_game,count(*) AS total_games FROM sides
    '''), {'player': player})
    return dict(result.mappings().one())


async def time_controls(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''
        SELECT m.name AS time_control,count(*) AS total FROM sides s
        LEFT JOIN public.game_mode m ON m.id=s.mode_id
        GROUP BY m.name ORDER BY total DESC,m.name ASC
    '''), {'player': player})
    return [dict(row) for row in result.mappings()]


async def mode_stats(session, player):
    _require_transaction(session)
    result = await session.execute(text(PLAYER_SIDES+'''SELECT m.name AS mode,
        count(*)::bigint AS n_games,count(*) FILTER (WHERE is_white)::bigint AS as_white,
        count(*) FILTER (WHERE NOT is_white)::bigint AS as_black,
        count(*) FILTER (WHERE result=1)::bigint AS wins,
        count(*) FILTER (WHERE result=0.5)::bigint AS draws,
        count(*) FILTER (WHERE result=0)::bigint AS losses,
        (array_agg(rating ORDER BY played_at ASC,link ASC,is_white ASC))[1] AS oldest_rating,
        (array_agg(rating ORDER BY played_at DESC,link DESC,is_white ASC))[1] AS newest_rating
        FROM sides s LEFT JOIN public.game_mode m ON m.id=s.mode_id GROUP BY m.name
        ORDER BY count(*) DESC,m.name ASC
    '''), {'player': player})
    return [dict(row) for row in result.mappings()]
