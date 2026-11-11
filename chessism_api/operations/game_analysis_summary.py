"""Absolute coverage refresh followed by player accuracy publication."""
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations.player_game_scores import refresh_game_player_engine_summaries


async def refresh_game_analysis_summary(game_links=None, *, factory=None):
    links = None if game_links is None else tuple(sorted({
        int(link) for link in game_links if link is not None}))
    if links == ():
        return {'games': 0, 'scope': 'links'}
    factory = factory or AsyncDBSession
    async with write_transaction(factory) as session:
        rows = await refresh_coverage(session, links)
    # Coverage remains committed if accuracy fails. The durable queue only
    # acknowledges after both stages succeed, so a retry safely repeats them.
    await refresh_game_player_engine_summaries(links, factory=factory)
    return {'games': len(rows), 'scope': 'all' if links is None else 'links'}


async def increment_game_analysis_summary_for_scored_fens(scored_fens, *, factory=None):
    """Reconcile affected games absolutely; replay never increments coverage."""
    fens = sorted({str(row.get('fen') or '').strip() for row in scored_fens or ()} - {''})
    if not fens:
        return {'games': 0, 'fens': 0}
    factory = factory or AsyncDBSession
    async with write_transaction(factory) as session:
        rows = await refresh_coverage(session, fens=fens)
    completed = tuple(row['link'] for row in rows if row['is_fully_analyzed'])
    if completed:
        await refresh_game_player_engine_summaries(completed, factory=factory)
    return {'games': len(rows), 'fully_analyzed_games': len(completed), 'fens': len(fens)}
