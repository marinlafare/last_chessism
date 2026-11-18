"""Canonical time-control analytics inside the caller's read snapshot."""
from sqlalchemy import text


def _scope(player):
    # Global statistics count games once; player statistics count owned sides.
    # Restricting side/color later also makes self-play move counts unambiguous.
    owner_columns = '' if player is None else ''',gp.is_white,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result'''
    owner_join = '' if player is None else '''JOIN public.game_player gp ON gp.game_link=g.link
        JOIN public.player p ON p.id=gp.player_id AND p.player_name=:player'''
    return f'''WITH games AS NOT MATERIALIZED (
        SELECT g.link,g.played_at,g.n_moves,g.time_elapsed,g.white_elo,g.black_elo,
            g.white_result,g.black_result,(g.white_elo::numeric+g.black_elo)/2 AS avg_elo
            {owner_columns}
        FROM public.game g JOIN public.game_mode mode ON mode.id=g.mode_id {owner_join}
        WHERE mode.name=:mode AND g.is_standard AND g.n_moves>0
    )'''


# Existing public range semantics: an incomplete pair does not filter ratings.
RATING_RANGE = '''(CAST(:min_rating AS integer) IS NULL
    OR CAST(:max_rating AS integer) IS NULL
    OR avg_elo BETWEEN :min_rating AND :max_rating)'''


async def _rows(session, query, params):
    if not session.in_transaction():
        raise ValueError('Time-control reads require a caller-owned transaction')
    result = await session.execute(text(query), params)
    return [dict(row) for row in result.mappings()]


async def rating_histogram(session, mode):
    return await _rows(session, _scope(None)+'''
        SELECT rating,count(*)::bigint AS appearances FROM games
        CROSS JOIN LATERAL (VALUES (white_elo),(black_elo)) side(rating)
        GROUP BY rating ORDER BY rating
    ''', {'mode': mode})


async def top_moves(session, *, mode, move_color, max_move, offset, limit,
                    player=None, min_rating=None, max_rating=None):
    side_filter = ('is_white=(:move_color=\'white\')' if player is not None else
                   '''(CAST(:min_rating AS integer) IS NULL OR CAST(:max_rating AS integer) IS NULL
                       OR (CASE WHEN :move_color='white' THEN white_elo ELSE black_elo END)
                          BETWEEN :min_rating AND :max_rating)''')
    return await _rows(session, _scope(player)+f''', clean_moves AS (
        SELECT m.n_move,CASE WHEN :move_color='black' THEN m.black_move ELSE m.white_move END AS move
        FROM games g JOIN public.moves m ON m.link=g.link
        WHERE m.n_move BETWEEN 1 AND :max_move AND {side_filter}
    ), ranked AS (
        SELECT n_move,move,count(*)::bigint AS times_played,
            row_number() OVER (PARTITION BY n_move ORDER BY count(*) DESC,move ASC) AS rank
        FROM clean_moves WHERE move NOT IN ('','--') GROUP BY n_move,move
    ) SELECT number AS move_number,coalesce(move,'-') AS move,
        coalesce(times_played,0) AS times_played
        FROM generate_series(1,:max_move) number
        LEFT JOIN ranked ON n_move=number AND rank=1
        ORDER BY number LIMIT :limit OFFSET :offset
    ''', dict(mode=mode,move_color=move_color,max_move=max_move,offset=offset,limit=limit,
              player=player,min_rating=min_rating,max_rating=max_rating))


def _openings(player):
    filters = 'result=:target_score' if player is not None else RATING_RANGE
    return _scope(player)+f''', openings AS (
        SELECT sequence.opening,count(*)::bigint AS times_played,
            round(avg(g.avg_elo))::integer AS mean_rating_for_this_opening
        FROM games g JOIN public.game_opening o ON o.link=g.link
        JOIN public.opening_sequence sequence ON sequence.id=o.opening_id
        WHERE o.n_moves=:n_moves AND sequence.opening<>'' AND {filters}
        GROUP BY sequence.opening
    )'''


async def opening_count(session, **params):
    rows = await _rows(session, _openings(params.get('player'))+'SELECT count(*) AS total FROM openings', params)
    return rows[0]['total']


async def opening_page(session, **params):
    return await _rows(session, _openings(params.get('player'))+'''
        SELECT * FROM openings ORDER BY times_played DESC,opening ASC LIMIT :limit OFFSET :offset
    ''', params)


async def results(session, **params):
    rows = await _rows(session, _scope(None)+f'''SELECT count(*)::bigint AS total_games,
        count(*) FILTER (WHERE white_result=1) AS white_wins,
        count(*) FILTER (WHERE white_result=0.5) AS white_draws,
        count(*) FILTER (WHERE white_result=0) AS white_losses,
        count(*) FILTER (WHERE black_result=1) AS black_wins,
        count(*) FILTER (WHERE black_result=0.5) AS black_draws,
        count(*) FILTER (WHERE black_result=0) AS black_losses
        FROM games WHERE {RATING_RANGE}''', params)
    return rows[0]


def _filtered_games(player):
    return _scope(player)+', filtered AS (SELECT * FROM games'+(
        ' WHERE '+RATING_RANGE if player is None else '')+')'


async def lengths(session, *, mode, player=None, min_rating=None, max_rating=None):
    params = dict(mode=mode,player=player,min_rating=min_rating,max_rating=max_rating)
    scope = _filtered_games(player)
    summary = await _rows(session, scope+'''SELECT count(*)::bigint AS total_games,
        round(avg(n_moves)::numeric,2) AS avg_n_moves,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY n_moves) AS median_n_moves,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY n_moves) AS p90_n_moves,
        round(avg(time_elapsed)::numeric,2) AS avg_time_elapsed_sec,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY time_elapsed) AS median_time_elapsed_sec,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY time_elapsed) AS p90_time_elapsed_sec
        FROM filtered''', params)
    moves = await _rows(session, scope+'''SELECT (floor(n_moves/10.0)*10)::int AS bucket_start,
        (floor(n_moves/10.0)*10+9)::int AS bucket_end,count(*)::bigint AS total FROM filtered
        GROUP BY bucket_start,bucket_end ORDER BY bucket_start''', params)
    if player is not None:
        elapsed = await _rows(session, scope+'''SELECT floor(time_elapsed/60.0)::int AS bucket_start_min,
            (floor(time_elapsed/60.0)+1)::int AS bucket_end_min,count(*)::bigint AS total
            FROM filtered GROUP BY bucket_start_min,bucket_end_min ORDER BY bucket_start_min''', params)
    else:
        elapsed = await _rows(session, scope+''', bucketed AS (
            SELECT width_bucket(time_elapsed/60.0,ARRAY[0,1,2,3,5,10,20,40]::float8[]) AS bucket_order
            FROM filtered
        ) SELECT (ARRAY['0-1','1-2','2-3','3-5','5-10','10-20','20-40','40+'])[bucket_order] AS bucket_label,
            bucket_order,count(*)::bigint AS total FROM bucketed GROUP BY bucket_order ORDER BY bucket_order
        ''', params)
    return summary[0], moves, elapsed


async def activity(session, *, mode, player=None, min_rating=None, max_rating=None):
    # One scan instead of three. UTC extraction is independent of server defaults.
    rows = await _rows(session, _filtered_games(player)+'''
        SELECT part,idx,count(*)::bigint AS total FROM filtered
        CROSS JOIN LATERAL (VALUES
            ('month',extract(month FROM played_at AT TIME ZONE 'UTC')::int),
            ('weekday',extract(dow FROM played_at AT TIME ZONE 'UTC')::int),
            ('hour',extract(hour FROM played_at AT TIME ZONE 'UTC')::int)) calendar(part,idx)
        GROUP BY part,idx ORDER BY part,idx
    ''', dict(mode=mode,player=player,min_rating=min_rating,max_rating=max_rating))
    return tuple([row for row in rows if row['part']==part] for part in ('month','weekday','hour'))


async def rating_history(session, player, mode):
    return await _rows(session, _scope(player)+'''
        SELECT rating,played_at FROM games ORDER BY played_at,link,is_white ASC
    ''', dict(player=player,mode=mode))
