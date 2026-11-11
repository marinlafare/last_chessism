"""Player navigation and basic game statistics with one read snapshot per view."""
from contextlib import asynccontextmanager

from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import player_game_reads as reads
from chessism_api.database.postgresql.transactions import write_transaction


@asynccontextmanager
async def _snapshot(factory):
    # Shared setup for this feature's reads; the accepted cleanup handles errors
    # and repeated cancellation before the connection is returned to the pool.
    async with write_transaction(factory or AsyncDBSession) as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        yield session


async def get_player_fen_score_counts(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        row = await reads.coverage(session, player_name)
    stamp = row['latest_rating_at']
    return {'player_name': player_name,
        **{key: row[key] for key in ('total_games','analyzed_games','total_positions',
                                     'analyzed_positions','unscored_positions')},
        'total_fens': row['total_positions'], 'analyzed_fens': row['analyzed_positions'],
        'latest_rating': row['latest_rating'], 'latest_rating_mode': row['latest_rating_mode'],
        'latest_rating_at': stamp.isoformat() if stamp else None}


async def get_player_neighbors(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        return {'player_name': player_name, **await reads.neighbors(session, player_name)}


async def get_tracked_player_navigation(same_group_as=None, *, factory=None):
    selected = same_group_as.strip().lower() if same_group_as is not None else None
    async with _snapshot(factory) as session:
        return await reads.navigation(session, selected)


async def get_player_performance_summary(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        return await reads.performance(session, player_name)


async def get_player_game_averages(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        return await reads.averages(session, player_name)


async def get_player_termination_stats(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        return await reads.terminations(session, player_name)


async def get_player_top_openings(player_name, limit=5, *, factory=None):
    async with _snapshot(factory) as session:
        return await reads.openings(session, player_name, limit)


def _format_games(rows):
    labels = {1.0:'win', 0.5:'draw', 0.0:'loss'}
    return [{'link': row['link'], 'played_at': row['played_at'].strftime('%Y-%m-%d %H:%M:%S'),
             'color': row['color'], 'result': labels[row['player_score']]} for row in rows]


async def get_player_games_page(player_name, page=1, page_size=10, *, factory=None):
    page, page_size = max(1, page), max(1, min(100, page_size))
    async with _snapshot(factory) as session:
        total = await reads.game_count(session, player_name)
        rows = await reads.games(session, player_name, limit=page_size, offset=(page-1)*page_size)
    return {'player_name': player_name, 'page': page, 'page_size': page_size,
            'total': total, 'total_pages': (total+page_size-1)//page_size, 'games': _format_games(rows)}


async def get_player_game_summary(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        row = await reads.summary(session, player_name)
        modes = await reads.time_controls(session, player_name)
    return {'player_name': player_name, **{key:row[key] for key in ('wins','losses','draws','total_games')},
            'date_from': row['first_game'].isoformat() if row['first_game'] else None,
            'date_to': row['last_game'].isoformat() if row['last_game'] else None,
            'time_controls': modes}


async def get_player_mode_games(player_name, mode, *, factory=None):
    mode = (mode or '').strip().lower()
    rows = []
    if mode in {'bullet','blitz','rapid'}:
        async with _snapshot(factory) as session:
            rows = await reads.games(session, player_name, mode=mode)
    games = _format_games(rows)
    wins = sum(row['result']=='win' for row in games)
    draws = sum(row['result']=='draw' for row in games)
    losses = sum(row['result']=='loss' for row in games)
    total = len(games)
    return {'player_name': player_name, 'mode': mode, 'total_games': total,
            'wins': wins, 'losses': losses, 'draws': draws,
            'score_rate': round((wins+0.5*draws)/total,4) if total else 0.0,
            'date_from': games[-1]['played_at'] if games else None,
            'date_to': games[0]['played_at'] if games else None, 'games': games}


async def get_player_time_control_results(player_name, mode, *, factory=None):
    mode = (mode or '').strip().lower()
    if mode not in {'bullet','blitz','rapid'}:
        return {'mode':mode,'total_games':0,'wins':0,'losses':0,'draws':0,'score_rate':0.0}
    return await get_player_mode_games(player_name, mode, factory=factory)


async def get_player_modes_stats(player_name, *, factory=None):
    async with _snapshot(factory) as session:
        rows = await reads.mode_stats(session, player_name)
    by_mode = {}
    for row in rows:
        mode = row.pop('mode') or 'unknown'
        row['score_rate'] = round((row['wins']+0.5*row['draws'])/row['n_games'],4)
        by_mode[mode] = row
    return by_mode
