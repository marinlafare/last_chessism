"""Assessment coverage policy, separate from the immutable inference formula.

Canonical input validates contiguous plies starting at one. Assessed moves plus
the recorded unassessed plies therefore give the exact number of moves per side.
No FEN scan, source inference, or GPU recalculation is needed for this projection.
"""

POLICY = 'complete_player_moves_v1'


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
