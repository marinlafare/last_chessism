"""Canonical game-only catalog/rating reads in a caller-owned snapshot."""
from sqlalchemy import text


# Shared with chart membership: grouping and drill-down must normalize alike.
OPENING_KEY_SQL = r"""COALESCE(NULLIF(REGEXP_REPLACE(BTRIM(COALESCE(gp.eco, '')),
    '^https?://(www\.)?chess\.com/openings/', '', 'i'), 'no_eco'), '')"""

SIDES = '''WITH sides AS NOT MATERIALIZED (
    SELECT g.link,g.played_at,g.eco,m.name AS mode,
        CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
        opponent.player_name AS opponent_name
    FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
    JOIN public.game_mode m ON m.id=g.mode_id
    JOIN public.player opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
    WHERE gp.player_id=:player_id AND g.is_standard AND g.n_moves>0
        AND m.name IN ('bullet','blitz','rapid')
)'''


async def execute(session, sql, **params):
    if not session.in_transaction():
        raise ValueError('Catalog reads require a caller-owned transaction')
    return await session.execute(text(sql), params)


async def read_limits(session):
    await execute(session, "SET LOCAL statement_timeout='8s'")
    await execute(session, "SET LOCAL lock_timeout='1s'")


async def active_profile(session, player):
    result = await execute(session, '''SELECT id,player_name,country,location,timezone,timezone_source
        FROM public.player WHERE player_name=:player AND deleted_at IS NULL AND joined>0''', player=player)
    return result.mappings().first()


async def rating_counts(session, player_id):
    result = await execute(session, '''SELECT m.name AS mode,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,count(*)::bigint AS games
        FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
        JOIN public.game_mode m ON m.id=g.mode_id
        WHERE gp.player_id=:player_id AND g.is_standard AND g.n_moves>0
            AND m.name IN ('bullet','blitz','rapid')
        GROUP BY mode,rating ORDER BY mode,rating''', player_id=player_id)
    return result.mappings().all()


async def catalog_page(session, *, player_id, kind, mode, color, min_games, sort_by, ratio_bin, search, limit, offset):
    if kind not in ('opponents','openings') or sort_by not in ('games','wins','draws','losses'):
        raise ValueError('Invalid catalog grouping or ordering')
    entry = 'gp.opponent_name' if kind == 'opponents' else OPENING_KEY_SQL
    label = ("coalesce(nullif(replace(entry_key,'-',' '),''),'Unknown opening')" if kind == 'openings'
             else "coalesce(nullif(entry_key,''),'Unknown opponent')")
    filtered = '' if ratio_bin is None else (f'WHERE {sort_by}*10>=:ratio_bin*games '
        f'AND (:ratio_bin=9 OR {sort_by}*10<(:ratio_bin+1)*games)')
    ordering = ('games DESC,entry_key' if sort_by == 'games' else
                f'{sort_by}::numeric/nullif(games,0) DESC,games DESC,entry_key')
    result = await execute(session, SIDES+f''', buckets AS MATERIALIZED (
        SELECT {entry} AS entry_key,count(*)::bigint AS games,
            count(*) FILTER(WHERE gp.result=1)::bigint AS wins,
            count(*) FILTER(WHERE gp.result=0.5)::bigint AS draws,
            count(*) FILTER(WHERE gp.result=0)::bigint AS losses
        FROM sides gp WHERE (:mode='all' OR gp.mode=:mode) AND (:color='all' OR gp.color=:color)
        GROUP BY {entry} HAVING count(*)>=:min_games
    ), ranked AS (
        SELECT *,row_number() OVER(ORDER BY {ordering}) AS rank FROM buckets {filtered}
    ), matching AS MATERIALIZED (
        SELECT * FROM ranked WHERE :search='' OR strpos(lower(entry_key),:search)>0
            OR strpos(lower({label}),:search)>0
    ), totals AS (
        SELECT count(*)::bigint AS groups,coalesce(sum(games),0)::bigint AS total_games FROM matching
    ) SELECT page.*,totals.groups,totals.total_games FROM totals LEFT JOIN LATERAL (
        SELECT * FROM matching ORDER BY rank LIMIT :limit OFFSET :offset
    ) page ON true ORDER BY page.rank''', player_id=player_id,mode=mode,color=color,min_games=min_games,
        ratio_bin=ratio_bin,search=search,limit=limit,offset=offset)
    return result.mappings().all()


async def latest_opponent_ratings(session, opponents, mode):
    modes = ['bullet','blitz','rapid'] if mode == 'all' else [mode]
    ratings = {name:dict.fromkeys(modes) for name in opponents}
    if not opponents:
        return ratings
    # Seek at most one candidate per side before resolving date/link/black ties.
    branches = []
    for side, white in (('white','true'),('black','false')):
        branches.append(f'''(SELECT g.{side}_elo AS rating,g.played_at,g.link,{white} AS is_white
            FROM public.game g WHERE g.{side}_player_id=p.id AND g.mode_id=m.id
                AND g.is_standard AND g.n_moves>0 ORDER BY g.played_at DESC,g.link DESC LIMIT 1)''')
    result = await execute(session, f'''SELECT p.player_name,m.name AS mode,latest.rating
        FROM public.player p CROSS JOIN public.game_mode m LEFT JOIN LATERAL (
            SELECT rating FROM ({' UNION ALL '.join(branches)}) candidates
            ORDER BY played_at DESC,link DESC,is_white ASC LIMIT 1
        ) latest ON true
        WHERE p.player_name=ANY(CAST(:opponents AS text[])) AND p.deleted_at IS NULL
            AND m.name=ANY(CAST(:modes AS text[]))''', opponents=opponents,modes=modes)
    for row in result.mappings():
        rating = row['rating']
        ratings[row['player_name']][row['mode']] = int(rating) if rating is not None and rating>0 else None
    return ratings
