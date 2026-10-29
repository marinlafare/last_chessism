"""Assessment coverage policy, separate from the immutable inference formula.

Canonical input validates contiguous plies starting at one. Assessed moves plus
the recorded unassessed plies therefore give the exact number of moves per side.
No FEN scan, source inference, or GPU recalculation is needed for this projection.
"""
from sqlalchemy import text
from .schema import table

POLICY = 'complete_player_moves_v1'


async def install_view(connection, layout):
    await connection.execute(text(f'''CREATE OR REPLACE VIEW {table(layout, 'assessment')} AS
        WITH totals AS (
            SELECT b.*, white_assessed + black_assessed +
                jsonb_array_length(flags->'unassessed') AS total_plies
            FROM {table(layout, 'game')} b
        ), coverage AS (
            SELECT totals.*,
                CASE WHEN status='assessed' THEN (total_plies+1)/2-white_assessed END AS white_unassessed,
                CASE WHEN status='assessed' THEN total_plies/2-black_assessed END AS black_unassessed
            FROM totals
        ) SELECT coverage.*,
            COALESCE(status='assessed' AND white_assessed>0 AND white_unassessed=0, false) AS white_complete,
            COALESCE(status='assessed' AND black_assessed>0 AND black_unassessed=0, false) AS black_complete
        FROM coverage'''))


def empty_mode():
    return {'counts': [0]*7, 'games_count': 0, 'partial_games': 0,
            'unassessed_games': 0, 'ineligible_games': 0,
            'assessed_moves': 0, 'unassessed_moves': 0, 'assessment_policy': POLICY}


def add_record(payloads, row):
    """Reference aggregation for isolated benchmark reports, not inference."""
    total = (row.get('white_assessed') or 0) + (row.get('black_assessed') or 0) + len(row['flags']['unassessed'])
    for side, moves in (('white', (total+1)//2), ('black', total//2)):
        owner = row.get(side+'_owner')
        if owner not in payloads or row['mode'] not in payloads[owner]:
            continue
        if row['status'] == 'assessed' and not moves:
            continue
        dest = payloads[owner][row['mode']]
        assessed = row.get(side+'_assessed') or 0
        dest['assessed_moves'] += assessed
        if row['status'] != 'assessed':
            dest['ineligible_games'] += 1
            continue
        unassessed = moves-assessed
        dest['unassessed_moves'] += unassessed
        if not assessed:
            dest['unassessed_games'] += 1
        elif unassessed:
            dest['partial_games'] += 1
        else:
            dest['counts'][min(6, row[side+'_count'])] += 1
            dest['games_count'] += 1
