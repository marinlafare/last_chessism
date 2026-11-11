"""Application transaction for per-game/side accuracy projections."""
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.engine_summary import refresh_engine_summaries
from chessism_api.database.postgresql.transactions import write_transaction


async def refresh_game_player_engine_summaries(game_links=None, *, factory=None):
    links = None if game_links is None else tuple(sorted({int(link) for link in game_links}))
    if links == ():
        return {'game_players': 0, 'games': 0}
    async with write_transaction(factory or AsyncDBSession) as session:
        return await refresh_engine_summaries(session, links)
