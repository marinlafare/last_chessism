"""Bounded canonical FEN popup queries in a caller-owned read snapshot."""
from sqlalchemy import text


EXAMPLE_GAMES = '''SELECT gp.game_link,gp.is_white
    FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
    JOIN public.game_mode m ON m.id=g.mode_id
    WHERE gp.player_id=:player_id AND g.is_standard AND g.n_moves>0
        AND m.name=ANY(CAST(:modes AS text[]))
        AND (CAST(:color AS text) IS NULL OR gp.is_white=(:color='white'))
        AND (CAST(:after_game AS bigint) IS NULL
            OR (gp.game_link,gp.is_white)<(:after_game,CAST(:after_white AS boolean))
            OR (gp.game_link=:after_game AND gp.is_white=:after_white
                AND CAST(:after_move AS integer) IS NOT NULL))
    ORDER BY gp.game_link DESC,gp.is_white DESC LIMIT :game_limit'''

EXAMPLE_OCCURRENCES = '''WITH source AS MATERIALIZED (
    SELECT g.game_link,g.is_white,a.n_move,a.is_white AS ply_is_white,a.fen_id
    FROM unnest(CAST(:games AS bigint[]),CAST(:sides AS boolean[])) g(game_link,is_white)
    CROSS JOIN LATERAL (
        SELECT n_move,is_white,fen_id FROM public.game_fen_association
        WHERE game_link=g.game_link AND (
            CAST(:after_game AS bigint) IS NULL OR game_link<>:after_game
            OR g.is_white<>:after_white OR CAST(:after_move AS integer) IS NULL
            OR (n_move,is_white)<(:after_move,CAST(:after_ply AS boolean)))
        ORDER BY n_move DESC,is_white DESC LIMIT :scan_limit
    ) a
    ORDER BY g.game_link DESC,g.is_white DESC,a.n_move DESC,a.is_white DESC LIMIT :scan_limit
) SELECT s.*,f.score,f.analysis_source FROM source s JOIN public.fen f ON f.fen_id=s.fen_id
    ORDER BY s.game_link DESC,s.is_white DESC,s.n_move DESC,s.ply_is_white DESC'''


def require_transaction(session):
    if not session.in_transaction():
        raise ValueError('Position reads require a caller-owned transaction')


async def execute(session, sql, params=None):
    require_transaction(session)
    return await session.execute(text(sql), params or {})


async def read_limits(session, timeout):
    await execute(session, "SELECT set_config('statement_timeout',:timeout,true)", {'timeout': timeout})
    await execute(session, "SET LOCAL lock_timeout='1s'")
    await execute(session, "SET LOCAL work_mem='8MB'")
    await execute(session, 'SET LOCAL max_parallel_workers_per_gather=0')
    await execute(session, 'SET LOCAL jit=off')


async def owner_id(session, player):
    require_transaction(session)
    return await session.scalar(text('''SELECT id FROM public.player
        WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'''), {'player': player})


async def fen_frequency(session, fen):
    result = await execute(session, 'SELECT n_games FROM public.fen WHERE fen_id=:fen', {'fen': fen})
    return result.scalar_one_or_none()


async def mode_ids(session, modes):
    result = await execute(session, '''SELECT id FROM public.game_mode
        WHERE name=ANY(CAST(:modes AS text[])) ORDER BY id''', {'modes': modes})
    return result.scalars().all()


def position_games_sql(color, rare, mode_count):
    if color not in ('white', 'black') or not 1 <= mode_count <= 3:
        raise ValueError('Invalid position game query')
    side, other = ('white', 'black') if color == 'white' else ('black', 'white')
    arrival = '''WITH arrivals AS MATERIALIZED (
        SELECT DISTINCT game_link FROM public.game_fen_association WHERE fen_id=:fen),''' if rare else 'WITH'
    occurrence = 'g.link IN (SELECT game_link FROM arrivals)' if rare else '''EXISTS (
        SELECT 1 FROM public.game_fen_association a WHERE a.game_link=g.link AND a.fen_id=:fen OFFSET 0)'''
    branches = []
    for i in range(mode_count):
        branches.append(f'''(SELECT g.link,g.played_at,g.mode_id,g.{side}_elo AS rating,
            g.{other}_elo AS opponent_rating,g.{side}_result AS result,g.{other}_player_id AS opponent_id
            FROM public.game g WHERE g.{side}_player_id=:owner AND g.mode_id=:mode_{i}
                AND g.is_standard AND g.n_moves>0 AND {occurrence}
                AND (CAST(:after_time AS timestamptz) IS NULL OR
                    (g.played_at,g.link)<(CAST(:after_time AS timestamptz),CAST(:after_id AS bigint)))
            ORDER BY g.played_at DESC,g.link DESC LIMIT :limit)''')
    return f'''{arrival} selected AS MATERIALIZED (
        SELECT * FROM ({' UNION ALL '.join(branches)}) candidates
        ORDER BY played_at DESC,link DESC LIMIT :limit)
        SELECT s.*,p.player_name AS opponent_name,m.name AS mode FROM selected s
        JOIN public.player p ON p.id=s.opponent_id JOIN public.game_mode m ON m.id=s.mode_id
        ORDER BY s.played_at DESC,s.link DESC'''


async def position_games(session, color, rare, mode_count, params):
    return (await execute(session, position_games_sql(color, rare, mode_count), params)).mappings().all()


async def example_games(session, params):
    return (await execute(session, EXAMPLE_GAMES, params)).mappings().all()


async def example_occurrences(session, params):
    return (await execute(session, EXAMPLE_OCCURRENCES, params)).mappings().all()


async def boards(session, ids):
    result = await execute(session, 'SELECT fen_id,fen FROM public.fen WHERE fen_id=ANY(CAST(:ids AS bigint[]))', {'ids': ids})
    return dict(result.all())
