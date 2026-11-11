"""Canonical FEN browsing queries; the application owns the read transaction."""
from sqlalchemy import text


def _require_transaction(session):
    if not session.in_transaction():
        raise ValueError('A caller-owned transaction is required')


async def top(session, limit, offset=0, *, unscored=False):
    _require_transaction(session)
    columns = 'fen,n_games,score' if unscored else 'fen,n_games,moves_counter,score'
    condition = 'WHERE score IS NULL' if unscored else ''
    rows = await session.execute(text(f'''SELECT {columns} FROM public.fen {condition}
        ORDER BY n_games DESC,fen ASC LIMIT :limit OFFSET :offset'''),
        {'limit':limit,'offset':offset})
    return [dict(row) for row in rows.mappings().all()]


async def repetition_sum(session, threshold):
    _require_transaction(session)
    return await session.scalar(text('SELECT SUM(n_games) FROM public.fen WHERE n_games>:threshold'),
                                {'threshold':threshold})


async def analysis_counts(session):
    _require_transaction(session)
    return dict((await session.execute(text('''SELECT n_positions AS total_fens,
        analyzed_fens,unscored_fens,nonzero_scored_fens FROM public.database_summary
        WHERE id=1'''))).mappings().one())


async def scored_summary(session):
    _require_transaction(session)
    return dict((await session.execute(text('SELECT * FROM public.scored_position_summary WHERE id=1')))
                .mappings().one())


SCORED_ORDERS = {
    'frequency':'n_games DESC, ABS(score::bigint) DESC, fen ASC',
    'impact':'(n_games::numeric * ABS(score::bigint)) DESC, n_games DESC, fen ASC',
    'evaluation':'ABS(score::bigint) DESC, n_games DESC, fen ASC',
}


async def scored_count(session, minimum):
    _require_transaction(session)
    return await session.scalar(text('''SELECT count(*) FROM public.fen
        WHERE score IS NOT NULL AND ABS(score::bigint)>=:minimum'''), {'minimum':minimum})


async def scored_page(session, sort, minimum, limit, offset):
    _require_transaction(session)
    rows = await session.execute(text(f'''SELECT fen,n_games,moves_counter,next_moves,score,
        wdl_win,wdl_draw,wdl_loss,split_part(fen,' ',2) AS side_to_move,
        n_games::numeric * ABS(score::bigint) AS impact_score
        FROM public.fen WHERE score IS NOT NULL AND ABS(score::bigint)>=:minimum
        ORDER BY {SCORED_ORDERS[sort]} LIMIT :limit OFFSET :offset'''),
        {'minimum':minimum,'limit':limit,'offset':offset})
    return rows.mappings().all()


async def sample(session, scope, percent, seed):
    _require_transaction(session)
    conditions = {'unique':'n_games=1','repeated':'n_games>1',
                  'scored':'score IS NOT NULL','unscored':'score IS NULL'}
    # Scope is validated by the operation; interpolation is only fixed SQL.
    condition = conditions[scope] if scope in conditions else 'n_games=:appearances'
    params = {'percent':percent,'seed':seed}
    if scope not in conditions: params['appearances'] = int(scope)
    rows = await session.execute(text(f'''SELECT fen,n_games AS appearances FROM public.fen
        TABLESAMPLE SYSTEM (:percent) REPEATABLE (:seed) WHERE {condition} LIMIT 120'''), params)
    return [dict(row) for row in rows.mappings().all()]


async def arrival(session, fen):
    _require_transaction(session)
    return (await session.execute(text('''SELECT a.game_link,a.n_move,
        CASE WHEN a.is_white THEN 'white' ELSE 'black' END AS move_color,
        CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END AS san
        FROM (SELECT game_link,n_move,is_white FROM public.game_fen_association
            WHERE fen_id=(SELECT fen_id FROM public.fen WHERE fen=:fen)
            ORDER BY game_link,n_move,is_white LIMIT 1) a
        LEFT JOIN public.moves m ON m.link=a.game_link AND m.n_move=a.n_move'''),
        {'fen':fen})).mappings().first()


async def example_moves(session, link, n_move):
    _require_transaction(session)
    return (await session.execute(text('''SELECT n_move,white_move,black_move FROM public.moves
        WHERE link=:link AND n_move<=:n_move ORDER BY n_move LIMIT 100'''),
        {'link':link,'n_move':n_move})).mappings().all()


async def game_ids(session, fen, after):
    _require_transaction(session)
    return (await session.execute(text('''SELECT DISTINCT game_link FROM public.game_fen_association
        WHERE fen_id=(SELECT fen_id FROM public.fen WHERE fen=:fen)
          AND game_link>:after ORDER BY game_link LIMIT 11'''),
        {'fen':fen,'after':after})).scalars().all()


async def games(session, ids):
    _require_transaction(session)
    return (await session.execute(text('''SELECT g.link,w.player_name AS white,b.player_name AS black,
        g.white_elo,g.black_elo,EXTRACT(YEAR FROM g.played_at)::integer AS year,
        EXTRACT(MONTH FROM g.played_at)::integer AS month,EXTRACT(DAY FROM g.played_at)::integer AS day,
        g.white_str_result,g.time_control FROM public.game g
        JOIN public.player w ON w.id=g.white_player_id JOIN public.player b ON b.id=g.black_player_id
        WHERE g.link=ANY(:ids) ORDER BY g.link'''), {'ids':ids})).mappings().all()


async def occurrences(session, fen, ids):
    _require_transaction(session)
    return (await session.execute(text('''SELECT game_link,n_move,
        CASE WHEN is_white THEN 'white' ELSE 'black' END AS move_color
        FROM public.game_fen_association WHERE fen_id=(SELECT fen_id FROM public.fen WHERE fen=:fen)
        AND game_link=ANY(:ids) ORDER BY game_link,n_move,is_white LIMIT 2001'''),
        {'fen':fen,'ids':ids})).mappings().all()
