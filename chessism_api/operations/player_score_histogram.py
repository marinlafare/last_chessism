"""Bounded histogram reads: one game cursor and small position batches.

The caller keeps one repeatable-read transaction for the entire calculation.
Only game identities and the (at most) 48 aggregate counters enter Python.
"""
from sqlalchemy import text

BATCH_GAMES = 1000
BATCH_SELECTION = """SELECT * FROM unnest(
    CAST(:game_links AS bigint[]), CAST(:player_sides AS boolean[]),
    CAST(:game_modes AS text[])) AS batch(game_link, is_white, mode)"""


async def read_histogram_batches(session, games_query, positions_query, params, total_games, progress):
    query = text(positions_query + '''
        SELECT mode, score_kind, bin_id, COUNT(*)::bigint AS positions
        FROM classified GROUP BY mode, score_kind, bin_id''')
    stream = await session.stream(text(
        f'SELECT game_link, is_white, mode FROM ({games_query}) AS matching_games'), params)
    counts = {}
    completed = 0
    progress({'phase': 'calculating', 'completed_games': 0, 'total_games': total_games})
    try:
        async for games in stream.mappings().partitions(BATCH_GAMES):
            rows = (await session.execute(query, {
                'game_links': [game['game_link'] for game in games],
                'player_sides': [game['is_white'] for game in games],
                'game_modes': [game['mode'] for game in games],
            })).mappings().all()
            for row in rows:
                key = (row['mode'], row['score_kind'], row['bin_id'])
                counts[key] = counts.get(key, 0) + int(row['positions'])
            completed += len(games)
            progress({'phase': 'calculating', 'completed_games': completed, 'total_games': total_games})
    finally:
        await stream.close()
    if completed != total_games:
        raise RuntimeError('Game selection changed inside the histogram snapshot; refusing partial results.')
    return [
        {'mode': mode, 'score_kind': kind, 'bin_id': bin_id, 'positions': count}
        for (mode, kind, bin_id), count in counts.items()
    ]
