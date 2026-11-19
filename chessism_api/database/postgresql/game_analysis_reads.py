"""Canonical coverage pages and analysis previews; callers own transactions."""
from sqlalchemy import text

from chessism_api.evaluations import TABLEBASE_MAX_PIECES


PLAYER_GAMES = '''FROM public.game g
    JOIN public.player owner ON owner.player_name=:player
        AND (g.white_player_id=owner.id OR g.black_player_id=owner.id)
    JOIN public.game_analysis_coverage coverage ON coverage.link=g.link
    WHERE g.is_standard AND g.n_moves>0 AND coverage.total_positions>0'''

POSITION_COUNTS_SQL = f'''
    SELECT COUNT(*)::bigint AS position_occurrences,
        COUNT(f.score)::bigint AS analyzed_occurrences,
        COUNT(*) FILTER (WHERE f.score IS NULL)::bigint AS unscored_occurrences,
        COUNT(DISTINCT gfa.fen_id)::bigint AS unique_fens,
        COUNT(DISTINCT gfa.fen_id) FILTER (WHERE f.score IS NOT NULL)::bigint AS analyzed_unique_fens,
        COUNT(DISTINCT gfa.fen_id) FILTER (WHERE f.score IS NULL)::bigint AS fens_to_analyze,
        COUNT(DISTINCT gfa.fen_id) FILTER (
            WHERE f.score IS NULL AND COALESCE(f.analysis_source,'')<>'tablebase_unavailable'
            AND COALESCE(f.piece_count,
                char_length(translate(split_part(f.fen,' ',1),'12345678/','')))
                BETWEEN 2 AND {TABLEBASE_MAX_PIECES}
        )::bigint AS tablebase_fens
    FROM public.game_fen_association gfa JOIN public.fen f ON f.fen_id=gfa.fen_id
    WHERE gfa.game_link=ANY(CAST(:game_links AS bigint[]))
'''


def _require_transaction(session):
    if not session.in_transaction():
        raise ValueError('Analysis reads require a caller-owned transaction')


def _player_games(player, date_from, date_to_exclusive):
    sql = PLAYER_GAMES
    params = {'player': player}
    if date_from is not None:
        sql += ' AND g.played_at>=:date_from'
        params['date_from'] = date_from
    if date_to_exclusive is not None:
        sql += ' AND g.played_at<:date_to_exclusive'
        params['date_to_exclusive'] = date_to_exclusive
    return sql, params


async def overview(session):
    _require_transaction(session)
    result = await session.execute(text('''SELECT COUNT(*)::bigint AS games_with_positions,
        COUNT(*) FILTER (WHERE is_fully_analyzed)::bigint AS fully_analyzed_games,
        COUNT(*) FILTER (WHERE NOT is_fully_analyzed)::bigint AS incomplete_games,
        COALESCE(SUM(total_positions),0)::bigint AS total_game_positions,
        COALESCE(SUM(analyzed_positions),0)::bigint AS analyzed_game_positions,
        COALESCE(SUM(unscored_positions),0)::bigint AS unscored_game_positions
        FROM public.game_analysis_coverage'''))
    return dict(result.mappings().one())


def page_sql(status):
    predicate = {'fully': 'gas.is_fully_analyzed', 'incomplete': 'NOT gas.is_fully_analyzed',
                 'all': 'TRUE'}[status]
    # Keep the normalized layout's ordering, without a whole-corpus score scan.
    order = {'fully': 'gas.total_positions DESC, gas.link DESC',
             'incomplete': 'gas.unscored_positions DESC, gas.total_positions DESC, gas.link DESC',
             'all': 'gas.is_fully_analyzed DESC, gas.total_positions DESC, gas.link DESC'}[status]
    count = f'SELECT count(*) FROM public.game_analysis_coverage gas WHERE {predicate}'
    rows = f'''WITH page AS MATERIALIZED (
        SELECT gas.* FROM public.game_analysis_coverage gas WHERE {predicate}
        ORDER BY {order} OFFSET :offset LIMIT :limit
    ) SELECT gas.*,white.player_name AS white,black.player_name AS black,
        mode.name AS mode,g.played_at,g.white_result,g.black_result,
        scores.avg_score,scores.avg_abs_score,scores.max_abs_score
        FROM page gas JOIN public.game g ON g.link=gas.link
        JOIN public.player white ON white.id=g.white_player_id
        JOIN public.player black ON black.id=g.black_player_id
        LEFT JOIN public.game_mode mode ON mode.id=g.mode_id
        LEFT JOIN LATERAL (
            SELECT AVG(f.score)::float8 AS avg_score,
                AVG(ABS(f.score))::float8 AS avg_abs_score,MAX(ABS(f.score))::float8 AS max_abs_score
            FROM public.game_fen_association a JOIN public.fen f ON f.fen_id=a.fen_id
            WHERE a.game_link=gas.link
        ) scores ON TRUE ORDER BY {order}'''
    return count, rows


async def coverage_page(session, status, page, page_size):
    _require_transaction(session)
    count, query = page_sql(status)
    total = int(await session.scalar(text(count)))
    total_pages = (total+page_size-1)//page_size
    if total_pages:
        page = min(page, total_pages)
    result = await session.execute(text(query), {'offset': (page-1)*page_size, 'limit': page_size})
    return {'page': page, 'total': total, 'total_pages': total_pages,
            'rows': [dict(row) for row in result.mappings()]}


async def player_scope(session, player, date_from=None, date_to_exclusive=None):
    _require_transaction(session)
    source, params = _player_games(player, date_from, date_to_exclusive)
    result = await session.execute(text(f'''SELECT COUNT(*)::bigint AS games_with_fens,
        COUNT(*) FILTER (WHERE coverage.is_fully_analyzed)::bigint AS complete_games,
        COUNT(*) FILTER (WHERE NOT coverage.is_fully_analyzed)::bigint AS incomplete_games,
        MIN(g.played_at) AS earliest_game,MAX(g.played_at) AS latest_game {source}'''), params)
    return dict(result.mappings().one())


async def incomplete_games(session, player, *, descending, limit=None,
                           date_from=None, date_to_exclusive=None):
    _require_transaction(session)
    source, params = _player_games(player, date_from, date_to_exclusive)
    direction = 'DESC' if descending else 'ASC'
    query = f'''SELECT g.link,g.played_at,
        coverage.total_positions,coverage.analyzed_positions,coverage.unscored_positions
        {source} AND NOT coverage.is_fully_analyzed
        ORDER BY g.played_at {direction},g.link {direction}'''
    if limit is not None:
        query += ' LIMIT :limit'
        params['limit'] = limit
    result = await session.execute(text(query), params)
    return [dict(row) for row in result.mappings()]


async def position_counts(session, links):
    _require_transaction(session)
    result = await session.execute(text(POSITION_COUNTS_SQL), {'game_links': list(links)})
    return dict(result.mappings().one())


async def game_record(session, link):
    """The existing game-information payload, derived from canonical relationships."""
    _require_transaction(session)
    result = await session.execute(text('''SELECT g.link,w.player_name AS white,b.player_name AS black,
        extract(year FROM g.played_at)::int AS year,extract(month FROM g.played_at)::int AS month,
        extract(day FROM g.played_at)::int AS day,extract(hour FROM g.played_at)::int AS hour,
        extract(minute FROM g.played_at)::int AS minute,extract(second FROM g.played_at)::int AS second,
        g.white_elo,g.black_elo,g.white_result,g.black_result,g.white_str_result,g.black_str_result,
        g.time_control,m.name AS mode,g.played_at,
        (g.white_elo::double precision+g.black_elo)/2.0 AS avg_elo,
        g.eco,g.time_elapsed,g.n_moves,g.fens_done,g.rules,g.initial_setup,g.fens_processing
        FROM public.game g JOIN public.player w ON w.id=g.white_player_id
        JOIN public.player b ON b.id=g.black_player_id
        LEFT JOIN public.game_mode m ON m.id=g.mode_id WHERE g.link=:link'''), {'link':link})
    return [dict(row) for row in result.mappings()]
