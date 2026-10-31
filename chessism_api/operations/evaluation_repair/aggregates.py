"""Exact old-to-new deltas for already-scored, locked repair rows.

Mirrors the existing scored-position SQL boundaries, including its different
50-cp boundary for side counts. Does not rescan the position corpus.
"""
from sqlalchemy import text
from chessism_api.evaluations import integer

BINS = ('equal', 'small', 'clear', 'decisive', 'mate')
FIELDS = tuple(f'{b}_{suffix}' for b in BINS for suffix in ('positions', 'appearances', 'abs_score_sum')) + (
    'white_better', 'black_better', 'balanced', 'score_sum', 'abs_score_sum',
    'wdl_win_sum', 'wdl_draw_sum', 'wdl_loss_sum', 'wdl_positions', 'nonzero_scored_fens')


def contribution(row):
    integer(row['score'], 'score', -2147483648, 2147483647)
    integer(row['n_games'], 'appearances', 0, 2**63-1)
    score, appearances = int(row['score']), int(row['n_games'])
    magnitude = abs(score)
    bucket = next((name for name, upper in zip(BINS, (50, 150, 300, 9000, float('inf')))
                   if magnitude < upper))
    result = dict.fromkeys(FIELDS, 0)
    result.update({f'{bucket}_positions': 1, f'{bucket}_appearances': appearances,
                   f'{bucket}_abs_score_sum': magnitude, 'score_sum': score,
                   'abs_score_sum': magnitude, 'nonzero_scored_fens': int(score != 0),
                   'white_better': int(score > 50), 'black_better': int(score < -50),
                   'balanced': int(magnitude <= 50)})
    if all(row.get(f'wdl_{k}') is not None for k in ('win', 'draw', 'loss')):
        result.update({f'wdl_{k}_sum': row[f'wdl_{k}'] for k in ('win', 'draw', 'loss')})
        result['wdl_positions'] = 1
    return result


def delta(candidates):
    result = dict.fromkeys(FIELDS, 0)
    for candidate in candidates:
        before = contribution(candidate['before'])
        after = contribution({**candidate['replacement'], 'n_games': candidate['before']['n_games']})
        for field in FIELDS:
            result[field] += after[field]-before[field]
    return {field: value for field, value in result.items() if value}


async def apply(session, candidates):
    changes = delta(candidates)
    if not changes:
        return {'positions': {}, 'database': {}, 'before': {}, 'after': {}}
    # These rows are tiny cached projections. Lock in the normal summary order.
    before = {}
    for name in ('database_summary', 'scored_position_summary'):
        row = await session.scalar(text(f'SELECT to_jsonb(s) FROM public.{name} s WHERE id=1 FOR UPDATE NOWAIT'))
        if row is None:
            raise ValueError('Both existing global summaries are required for changed-value repair')
        before[name] = row
    db, positions = before['database_summary'], before['scored_position_summary']
    if (positions['analyzed_fens'] != db['analyzed_fens']
            or positions['nonzero_scored_fens'] != db['nonzero_scored_fens']):
        raise ValueError('Global summaries disagree; repair publication refused')
    for field, value in changes.items():
        if field != 'score_sum' and positions[field]+value < 0:
            raise ValueError('Repair delta would make a summary negative')
    nonzero = changes.get('nonzero_scored_fens', 0)
    if nonzero and min(db['scored_fens'], db['nonzero_scored_fens'])+nonzero < 0:
        raise ValueError('Repair delta would make a database counter negative')
    await session.execute(text('UPDATE public.scored_position_summary SET '+
        ','.join(f'{field}={field}+:{field}' for field in changes)+',refreshed_at=clock_timestamp() WHERE id=1'), changes)
    if nonzero:
        await session.execute(text('''UPDATE public.database_summary SET scored_fens=scored_fens+:n,
            nonzero_scored_fens=nonzero_scored_fens+:n,refreshed_at=clock_timestamp() WHERE id=1'''), {'n': nonzero})
    after = {name: await session.scalar(text(f'SELECT to_jsonb(s) FROM public.{name} s WHERE id=1'))
             for name in before}
    return {'positions': changes, 'database': {'scored_fens': nonzero, 'nonzero_scored_fens': nonzero} if nonzero else {},
            'before': before, 'after': after}
