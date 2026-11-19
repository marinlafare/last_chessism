"""Canonical FEN snapshot queries; operations own transactions and refresh policy."""
from sqlalchemy import text


MODAL_MOVE = '''
    SELECT a.n_move, CASE WHEN a.is_white THEN 'white' ELSE 'black' END AS move_color,
        CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END AS san,
        COUNT(*)::bigint AS label_occurrences, MIN(a.game_link) AS example_game_link
    FROM public.game_fen_association a
    JOIN public.moves m ON m.link=a.game_link AND m.n_move=a.n_move
    WHERE a.fen_id = :fen_id AND a.n_move>0
      AND NULLIF(BTRIM(CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END),'') IS NOT NULL
      AND CASE WHEN a.is_white THEN m.white_move ELSE m.black_move END <> '--'
    GROUP BY a.n_move, a.is_white, san
    ORDER BY label_occurrences DESC, a.n_move, move_color, san LIMIT 1
'''


async def execute(session, sql, params=None):
    if not session.in_transaction():
        raise ValueError('FEN snapshots require a caller-owned transaction')
    return await session.execute(text(sql), params or {})


async def source_stamp(session):
    return (await execute(session, 'SELECT refreshed_at FROM public.database_summary WHERE id=1')).scalar_one()


async def read_bins(session):
    rows = (await execute(session, '''SELECT m.refreshed_at,m.source_updated_at,m.attempted_at,m.last_error,
        d.refreshed_at AS current_source_at,b.appearances,b.total_fens,b.unscored_fens
        FROM public.fen_appearance_summary m JOIN public.database_summary d USING(id)
        LEFT JOIN public.fen_appearance_bin b ON TRUE WHERE m.id=1 ORDER BY b.appearances''')).mappings().all()
    if not rows:
        raise RuntimeError('Required FEN appearance singleton is missing')
    return rows


async def read_common(session):
    rows = (await execute(session, '''SELECT m.refreshed_at,m.source_updated_at,m.attempted_at,m.last_error,
        p.rank,p.fen,p.appearances,p.n_move,p.move_color,p.san,p.label_occurrences,p.example_line
        FROM public.fen_commonness_summary m LEFT JOIN public.fen_commonness_position p ON TRUE
        WHERE m.id=1 ORDER BY p.rank''')).mappings().all()
    if not rows:
        raise RuntimeError('Required common-position singleton is missing')
    return rows


async def top_positions(session, after=None):
    where = '' if after is None else '''AND n_games <= :appearances
        AND (n_games < :appearances OR (n_games = :appearances AND fen > :fen))'''
    return (await execute(session, f'''SELECT fen,fen_id,n_games AS appearances FROM public.fen
        WHERE n_games>1 {where} ORDER BY n_games DESC, fen LIMIT {10 if after is None else 11}''',
        after)).mappings().all()


async def modal_move(session, fen_id):
    return (await execute(session, MODAL_MOVE, {'fen_id': fen_id})).mappings().first()


async def example_moves(session, link, move):
    return (await execute(session, '''SELECT n_move,white_move,black_move FROM public.moves
        WHERE link=:link AND n_move<=:n_move ORDER BY n_move LIMIT 100''',
        {'link': link, 'n_move': move})).mappings().all()


async def read_limits(session, seconds=90):
    await execute(session, "SELECT set_config('statement_timeout',:timeout,true)", {'timeout': f'{seconds}s'})
    await execute(session, "SET LOCAL lock_timeout = '5s'")
    await execute(session, 'SET LOCAL max_parallel_workers_per_gather = 2')
    await execute(session, 'SET LOCAL jit = off')
    await execute(session, 'SET LOCAL plan_cache_mode = force_custom_plan')


async def try_lock(session, key):
    return (await execute(session, 'SELECT pg_try_advisory_xact_lock(:key)', {'key': key})).scalar_one()


def metadata_table(kind):
    return {'appearance': 'public.fen_appearance_summary', 'commonness': 'public.fen_commonness_summary'}[kind]


async def mark_attempt(session, kind):
    return (await execute(session, f'''UPDATE {metadata_table(kind)} SET attempted_at=clock_timestamp()
        WHERE id=1 RETURNING attempted_at''')).scalar_one()


async def mark_failure(session, kind, attempt, message):
    await execute(session, f'''UPDATE {metadata_table(kind)} SET last_error=:error
        WHERE id=1 AND attempted_at=:attempt AND (refreshed_at IS NULL OR refreshed_at<:attempt)''',
        {'attempt': attempt, 'error': message})


async def publish_stamp(session, kind, source):
    result = await execute(session, f'''UPDATE {metadata_table(kind)} SET source_updated_at=:source,
        refreshed_at=clock_timestamp(),last_error = NULL WHERE id=1''', {'source': source})
    if result.rowcount != 1:
        raise RuntimeError('Required FEN snapshot singleton is missing')


async def replace_bins(session):
    await execute(session, 'DELETE FROM public.fen_appearance_bin')
    await execute(session, '''INSERT INTO public.fen_appearance_bin(appearances,total_fens,unscored_fens)
        SELECT n_games,COUNT(*)::bigint,COUNT(*) FILTER (WHERE score IS NULL)::bigint
        FROM public.fen GROUP BY n_games''')


async def replace_common(session, positions):
    await execute(session, 'DELETE FROM public.fen_commonness_position')
    if positions:
        await execute(session, '''INSERT INTO public.fen_commonness_position
            (rank,fen,appearances,n_move,move_color,san,label_occurrences,example_game_link,example_line)
            VALUES(:rank,:fen,:appearances,:n_move,:move_color,:san,:label_occurrences,:example_game_link,:example_line)''', positions)
