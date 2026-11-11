"""Canonical analysis leases: successful exhaustion differs from acquisition failure.

The selector owns a newly opened session until handoff. A nonempty selection
returns its open transaction to the application, which owns publication/commit.
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .engine import AsyncDBSession
from .analysis_cleanup import finish_analysis_cleanup
from .fen_claims import locked_eligible_fens


# Frozen-game ordering is shared with the durable cloud reservation queue.
# Preserve the independent minima for game and move priority, including shared
# positions whose earliest occurrence is in a later-priority game.
GAME_SET_CANDIDATES_SQL = """WITH selected_games AS MATERIALIZED (
    SELECT link, priority FROM unnest(CAST(:game_links AS bigint[]))
    WITH ORDINALITY AS selected(link, priority)
), candidate_fens AS MATERIALIZED (
    SELECT gfa.fen_id, MIN(selected.priority) AS game_priority,
        MIN(gfa.n_move * 2 + CASE WHEN gfa.is_white THEN 0 ELSE 1 END) AS move_priority
    FROM selected_games selected
    JOIN public.game_fen_association gfa ON gfa.game_link=selected.link
    GROUP BY gfa.fen_id
) """


async def release_analysis_session(session: AsyncSession) -> None:
    """Release a selection that was not handed off, preserving rollback failures."""
    try:
        await finish_analysis_cleanup(session.rollback())
    except BaseException:
        try:
            await finish_analysis_cleanup(session.close())
        except BaseException as error:
            print(f"Failed to close analysis selection session: {error!r}", flush=True)
        raise
    await finish_analysis_cleanup(session.close())


async def _lease_fens(statement, parameters=None, *, timings=None):
    session = AsyncDBSession()
    try:
        await session.begin()
        fens = await locked_eligible_fens(session, statement, parameters, timings=timings)
    except BaseException:
        try:
            await release_analysis_session(session)
        except BaseException as error:
            print(f"Failed to release analysis selection session: {error!r}", flush=True)
        raise
    if not fens:
        await release_analysis_session(session)
        return None, None
    return session, fens


async def get_fens_for_analysis(limit: int, *, timings=None):
    statement = text("""SELECT f.fen FROM public.fen f
        WHERE f.score IS NULL AND NOT EXISTS (
            SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=f.fen)
        ORDER BY f.n_games DESC LIMIT :limit FOR UPDATE OF f SKIP LOCKED
    """).bindparams(limit=limit)
    return await _lease_fens(statement, timings=timings)


def _player_fens_for_analysis_stmt(player_name: str, limit: int):
    # FEN is the only row-producing relation: repeated occurrences and self-play
    # cannot consume LIMIT, and locks are skipped before filling the batch.
    return text("""SELECT f.fen FROM public.fen f
        WHERE f.score IS NULL AND EXISTS (SELECT 1
            FROM public.game_fen_association a JOIN public.game g ON g.link=a.game_link
            JOIN public.player p ON p.id=g.white_player_id OR p.id=g.black_player_id
            WHERE a.fen_id=f.fen_id AND p.player_name=:player_name
              AND g.is_standard AND g.n_moves>0)
          AND NOT EXISTS (SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=f.fen)
        ORDER BY f.n_games DESC LIMIT :limit FOR UPDATE OF f SKIP LOCKED
    """).bindparams(player_name=player_name, limit=limit)


async def get_player_fens_for_analysis(player_name: str, limit: int, *, timings=None):
    return await _lease_fens(_player_fens_for_analysis_stmt(player_name, limit), timings=timings)


async def get_game_set_fens_for_analysis(game_links: list[int], limit: int, *, timings=None):
    clean_links = [int(link) for link in game_links if link is not None]
    if not clean_links:
        return None, None
    statement = text(GAME_SET_CANDIDATES_SQL + """
        SELECT f.fen FROM candidate_fens candidate JOIN public.fen f USING(fen_id)
        WHERE f.score IS NULL AND NOT EXISTS (
            SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=f.fen)
        ORDER BY candidate.game_priority,candidate.move_priority,f.fen
        LIMIT :limit FOR UPDATE OF f SKIP LOCKED
    """)
    return await _lease_fens(statement, {'game_links':clean_links,'limit':max(1,int(limit))}, timings=timings)


async def count_game_set_fens_for_analysis(game_links: list[int]) -> int:
    clean_links = [int(link) for link in game_links if link is not None]
    if not clean_links:
        return 0
    async with AsyncDBSession() as session:
        result = await session.execute(text("""
            SELECT COUNT(DISTINCT gfa.fen_id)::bigint AS remaining
            FROM public.game_fen_association gfa
            JOIN public.fen f ON f.fen_id = gfa.fen_id
            WHERE gfa.game_link = ANY(CAST(:game_links AS BIGINT[]))
              AND f.score IS NULL;
        """), {"game_links": clean_links})
        return int(result.scalar() or 0)


async def get_game_set_analysis_completion(game_links: list[int]) -> dict[str, int]:
    clean_links = [int(link) for link in game_links if link is not None]
    if not clean_links:
        return {"selected_games": 0, "fully_analyzed_games": 0, "incomplete_games": 0}
    async with AsyncDBSession() as session:
        result = await session.execute(text("""
            SELECT
                COUNT(*)::bigint AS selected_games,
                COUNT(*) FILTER (WHERE is_fully_analyzed)::bigint AS fully_analyzed_games,
                COUNT(*) FILTER (WHERE NOT is_fully_analyzed)::bigint AS incomplete_games
            FROM public.game_analysis_coverage
            WHERE link = ANY(CAST(:game_links AS BIGINT[]));
        """), {"game_links": clean_links})
        row = result.mappings().first() or {}
    return {
        "selected_games": int(row.get("selected_games") or 0),
        "fully_analyzed_games": int(row.get("fully_analyzed_games") or 0),
        "incomplete_games": int(row.get("incomplete_games") or 0),
    }
