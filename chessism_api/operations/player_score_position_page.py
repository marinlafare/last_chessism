"""Rank narrow occurrence identities before looking up scores or board text.

Outside the central interval a FEN can qualify from only one player's side:
white and black scores are opposites. Rank those sides independently. In the
central interval both sides qualify together, so combine their occurrences.
This preserves exact frequency ordering without joining every occurrence to
the large FEN table, or sorting full FEN strings. Only candidate score batches
and the final page of board text cross into Python.
"""
from sqlalchemy import text

from chessism_api.database.relational_storage import storage_for

SCORE_BATCH_SIZE = 64


def ranked_candidates_sql(session, games_query, *, central):
    layout = storage_for(session)
    association = layout.table('game_fen_association') if layout else 'game_fen_association'
    side_group = '' if central else ', g.is_white'
    return text(f"""WITH selected_games AS MATERIALIZED (
        SELECT game_link, is_white FROM ({games_query}) matching_games
    ), counts AS (
        SELECT a.fen_id, COUNT(*)::bigint AS appearances,
            BOOL_OR(g.is_white) AS has_white, BOOL_OR(NOT g.is_white) AS has_black
        FROM selected_games g JOIN {association} a ON a.game_link=g.game_link
        GROUP BY a.fen_id{side_group}
    )
    SELECT * FROM counts WHERE CAST(:after_count AS bigint) IS NULL
        OR appearances<:after_count OR (appearances=:after_count AND fen_id>:after_id)
    ORDER BY appearances DESC, fen_id ASC, has_white DESC""")


def matches_interval(score, interval):
    lower, upper = interval['lower'], interval['upper']
    if lower is not None and (score < lower or (score == lower and not interval['lower_inclusive'])):
        return False
    if upper is not None and (score > upper or (score == upper and not interval['upper_inclusive'])):
        return False
    return True


async def read_position_page(session, games_query, params, interval, page_size):
    layout = storage_for(session)
    fen = layout.table('fen') if layout else 'fen'
    score_query = text(f'SELECT fen_id, score, analysis_source FROM {fen} WHERE fen_id=ANY(CAST(:ids AS bigint[]))')
    stream = await session.stream(ranked_candidates_sql(session, games_query,
        central=interval['id'] == 6), params, execution_options={'yield_per': SCORE_BATCH_SIZE})
    selected = []
    try:
        async for batch in stream.mappings().partitions(SCORE_BATCH_SIZE):
            scores = {row['fen_id']: row for row in (await session.execute(score_query,
                {'ids': list({row['fen_id'] for row in batch})})).mappings()}
            for row in batch:
                value = scores.get(row['fen_id'])
                if value is None or value['score'] is None or value['analysis_source'] == 'tablebase':
                    continue
                score = value['score']
                if abs(score) >= 9000:
                    continue
                perspectives = ([score] if row['has_white'] else []) + ([-score] if row['has_black'] else [])
                if not perspectives or not matches_interval(perspectives[0], interval):
                    continue
                selected.append({'fen_id': str(row['fen_id']), 'appearances': int(row['appearances']),
                    'score_min': float(min(perspectives)), 'score_max': float(max(perspectives))})
                if len(selected) > page_size:
                    break
            if len(selected) > page_size:
                break
    finally:
        await stream.close()
    rows = selected[:page_size]
    if rows:
        boards = dict((await session.execute(text(
            f'SELECT fen_id, fen FROM {fen} WHERE fen_id=ANY(CAST(:ids AS bigint[]))'),
            {'ids': [int(row['fen_id']) for row in rows]})).all())
        for row in rows:
            row['fen'] = boards[int(row['fen_id'])]
    return rows, len(selected) > page_size
