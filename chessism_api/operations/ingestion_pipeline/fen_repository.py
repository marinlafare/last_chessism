"""Canonical storage operations used by bounded FEN ingestion."""
from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import write_transaction


async def count_pending_games() -> int:
    """Read committed eligible work; a failed read must propagate."""
    async with AsyncDBSession() as session:
        return int(await session.scalar(text('''SELECT count(*) FROM public.game
            WHERE is_standard AND n_moves>0 AND NOT fens_done''')))


async def claim_games_needing_fens(session, batch_size: int) -> list[int]:
    result = await session.execute(text('''SELECT link FROM public.game
        WHERE is_standard AND n_moves>0 AND NOT fens_done AND NOT fens_processing
        ORDER BY link LIMIT :limit FOR UPDATE SKIP LOCKED'''),
        {'limit': max(1, int(batch_size))})
    return [int(link) for link in result.scalars()]


async def read_moves_for_games(session, game_links: list[int]) -> dict:
    if not game_links:
        return {}
    result = await session.execute(text('''SELECT link,n_move,white_move,black_move
        FROM public.moves WHERE link=ANY(CAST(:links AS bigint[])) ORDER BY link,n_move'''),
        {'links': game_links})
    moves = {}
    for row in result.mappings():
        moves.setdefault(int(row['link']), []).append(dict(row))
    return moves


async def set_game_fen_state(session, game_links, *, done=None, processing=None):
    if not game_links:
        return
    values = {}
    if done is not None:
        values['fens_done'] = done
    if processing is not None:
        values['fens_processing'] = processing
    if values:
        assignments = ','.join(f'{key}=:{key}' for key in values)
        await session.execute(text(f'UPDATE public.game SET {assignments} '
            'WHERE link=ANY(CAST(:links AS bigint[]))'), dict(values, links=list(game_links)))


async def release_fen_processing_claims(game_links=None) -> int:
    """Application recovery owns this transaction, after all known writers finish."""
    if game_links is not None and not game_links:
        return 0
    async with write_transaction(AsyncDBSession) as session:
        result = await session.execute(text('''UPDATE public.game SET fens_processing=false
            WHERE fens_processing AND (:all_games OR link=ANY(CAST(:links AS bigint[])))'''),
            {'all_games': game_links is None, 'links': list(game_links or ())})
        return int(result.rowcount or 0)


async def refresh_fen_occurrence_counts(fen_values: list[str]) -> None:
    """Explicit repair; normal ingestion increments only newly inserted links."""
    unique_fens = sorted({str(fen) for fen in fen_values if fen})
    if not unique_fens:
        return
    async with write_transaction(AsyncDBSession) as session:
        # Match the writer lock order, then count using a fresh snapshot.
        await session.execute(text('''SELECT fen FROM public.fen
            WHERE fen=ANY(CAST(:fens AS text[])) ORDER BY fen FOR NO KEY UPDATE'''),
            {'fens': unique_fens})
        for start in range(0, len(unique_fens), 5_000):
            await session.execute(text('''
                WITH counts AS (
                    SELECT f.fen_id,count(a.fen_id)::bigint AS occurrences
                    FROM public.fen f LEFT JOIN public.game_fen_association a USING(fen_id)
                    WHERE f.fen=ANY(CAST(:fens AS text[])) GROUP BY f.fen_id
                ) UPDATE public.fen f SET n_games=counts.occurrences
                  FROM counts WHERE f.fen_id=counts.fen_id
            '''), {'fens': unique_fens[start:start + 5_000]})
