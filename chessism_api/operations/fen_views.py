"""FEN browse responses from explicit read-only database snapshots."""
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import fen_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.scored_positions import summary_payload


async def get_top_fens(limit=20, *, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        return await reads.top(session, limit)


async def get_top_fens_unscored(limit=20, offset=0, *, factory=None):
    limit, offset = max(1, min(100, int(limit))), max(0, int(offset))
    async with read_transaction(factory or AsyncDBSession) as session:
        return await reads.top(session, limit, offset, unscored=True)


async def get_sum_n_games(threshold=10, *, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        total = await reads.repetition_sum(session, threshold)
    return int(total) if total is not None else None


async def get_fen_analysis_counts(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        return await reads.analysis_counts(session)


async def get_scored_positions_overview(*, factory=None):
    async with read_transaction(factory or AsyncDBSession) as session:
        return summary_payload(await reads.scored_summary(session))


async def get_scored_positions_page(sort='impact', min_abs_score=0, page=1, page_size=20, *, factory=None):
    sort = (sort or 'impact').strip().lower()
    if sort not in reads.SCORED_ORDERS: sort = 'impact'
    minimum = max(0, min(int(min_abs_score or 0), 10000))
    page_size = max(1, min(int(page_size or 20), 50))
    page = max(1, int(page or 1))
    async with read_transaction(factory or AsyncDBSession) as session:
        total = await reads.scored_count(session, minimum)
        total_pages = (total+page_size-1)//page_size
        if total_pages: page = min(page, total_pages)
        rows = await reads.scored_page(session, sort, minimum, page_size, (page-1)*page_size)
    return {'sort':sort, 'min_abs_score':minimum, 'page':page, 'page_size':page_size,
            'total':total, 'total_pages':total_pages, 'rows':[
        {'fen':str(row['fen']), 'n_games':int(row['n_games']),
         'moves_counter':str(row['moves_counter'] or ''), 'next_moves':str(row['next_moves'] or ''),
         'score':float(row['score']),
         **{key:float(row[key]) if row[key] is not None else None for key in ('wdl_win','wdl_draw','wdl_loss')},
         'side_to_move':str(row['side_to_move']), 'impact_score':float(row['impact_score'])}
        for row in rows]}
