"""Bounded player/game score pivots for saved matrix recipes, not exports."""

MAX_MOVE = 500
MAX_COLUMNS = MAX_MOVE + 3
DEFAULT_LAST_MOVE = 40


def normalize_options(raw):
    options = dict(raw or {})
    all_moves = options.get('all_moves', False)
    if type(all_moves) is not bool:
        raise ValueError('all_moves must be a boolean.')
    first = 1 if all_moves else options.get('first_move', 1)
    last = None if all_moves else options.get('last_move', DEFAULT_LAST_MOVE)
    if not all_moves and (type(first) is not int or type(last) is not int or not 1 <= first <= last <= MAX_MOVE):
        raise ValueError(f'Choose a move range between 1 and {MAX_MOVE}, with first move <= last move.')
    perspective = options.get('score_perspective', 'player')
    if perspective not in ('player', 'white'):
        raise ValueError('Score perspective must be player or white.')
    return {'all_moves': all_moves, 'first_move': first, 'last_move': last, 'score_perspective': perspective}


def score_matrix_sql(config, *, count_only=False, sample_all_moves=False):
    """Limit qualifying player/game rows BEFORE pivoting their played moves.

    Uses the application's normalized read views. Moves and associations have
    unique game/move/side keys in the required compact storage contract.
    All caller values bind; identifiers come only from validated column names.
    """
    options = normalize_options(config.get('move_scores'))
    first, last = options['first_move'], options['last_move']
    if options['all_moves'] and not (count_only or sample_all_moves):
        raise ValueError('All-moves formulas support saving and live previews. Choose a fixed move range before running the existing fixed-column algorithms.')
    filters = config['filters']
    params = {'first_move': first, 'last_move': last,
              'max_rows': filters['max_rows'],
              'count_limit': filters['max_rows']+1 if filters['max_rows'] is not None else 10_001}
    if not options['all_moves']:
        params['expected_moves'] = last-first+1
    conditions = []
    score_eligibility = None
    for key, expression in (
        ('players', 'gp.player_name = ANY(CAST(:players AS text[]))'),
        ('modes', 'gp.mode = ANY(CAST(:modes AS text[]))'),
        ('date_from', 'gp.played_at >= CAST(:date_from AS date)'),
        ('date_to', "gp.played_at < CAST(:date_to AS date) + INTERVAL '1 day'"),
        ('min_moves', 'gp.n_moves >= :min_moves'),
    ):
        if filters[key]:
            conditions.append(expression)
            params[key] = filters[key]
    if filters['analyzed_only']:
        # Coverage is a cheap prefilter; inspect actual played moves too. This
        # avoids selecting a row with stale coverage or a hole in the range.
        required_range = '' if options['all_moves'] else '''AND COUNT(*) FILTER (
            WHERE side.color=gp.color AND m.n_move BETWEEN :first_move AND :last_move
        ) = :expected_moves'''
        conditions.append(
            '''EXISTS (SELECT 1 FROM game_analysis_summary coverage
                WHERE coverage.link=gp.link AND coverage.is_fully_analyzed
                  AND coverage.total_positions>0)'''
        )
        score_eligibility = f'''(SELECT BOOL_AND(f.score IS NOT NULL) {required_range}
                FROM moves m CROSS JOIN LATERAL (VALUES
                    ('white',m.white_move),('black',m.black_move)
                ) side(color,san)
                LEFT JOIN game_fen_association a ON a.game_link=m.link
                    AND a.n_move=m.n_move AND a.move_color=side.color
                LEFT JOIN fen f ON f.fen_id=a.fen_id
                WHERE m.link=gp.link AND NULLIF(BTRIM(side.san),'') IS NOT NULL
                  AND side.san<>'--')'''
    where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
    order = 'gp.played_at NULLS LAST, gp.link, gp.color'
    count_where = where + (f' AND {score_eligibility}' if score_eligibility else '')
    count = f'SELECT COUNT(*)::bigint FROM (SELECT 1 FROM game_player gp {count_where} LIMIT :count_limit) counted'
    if count_only:
        return count, '', params
    selected = [*config['feature_columns'], *config['label_columns']]
    score_keys = [key for key in selected if key.startswith('move_score_')]
    # Explicit projection rather than gp.* prevents unrelated summary joins.
    candidates = f'''SELECT gp.link,gp.player_name,gp.color,gp.result,gp.played_at
        FROM game_player gp {where}'''
    if score_eligibility:
        # Keep cheap filtering/sorting below the expensive move-score check.
        # OFFSET 0 is a pull-up barrier, NOT a candidate limit: keep scanning
        # in order until enough qualifying rows exist or the source is exhausted.
        # Without it PostgreSQL can check every game's moves before sorting,
        # even for a 50-row preview. Do not MATERIALIZE this unbounded stream.
        candidates = f'''SELECT gp.* FROM (
            {candidates} ORDER BY {order} OFFSET 0
        ) gp WHERE {score_eligibility}'''
    scope = f'''WITH selected AS MATERIALIZED (
        {candidates} ORDER BY {order} LIMIT :max_rows
    )'''
    identity = {
        'player': 'gp.player_name', 'game': 'gp.link',
        'result': """CASE WHEN g.white_result+g.black_result=1 THEN
            CASE gp.result WHEN 1 THEN 'w' WHEN 0.5 THEN 'd' WHEN 0 THEN 'l' END END""",
    }
    columns = ["CONCAT(gp.link,':',gp.color) AS row_key"]
    for key in selected:
        expression = identity.get(key, f'scores."{key}"')
        columns.append(f'{expression} AS "{key}"')
    joins = 'JOIN game g ON g.link=gp.link' if 'result' in selected else ''
    if score_keys or sample_all_moves:
        score = 'f.score' if options['score_perspective'] == 'white' else "CASE WHEN gp.color='white' THEN f.score ELSE -f.score END"
        if sample_all_moves:
            # This runs only AFTER selecting <=101 preview rows. Discover the
            # width in that sample, never by scanning the complete game scope.
            pivots = f'ARRAY_AGG(m.n_move ORDER BY m.n_move) AS move_numbers, ARRAY_AGG({score} ORDER BY m.n_move) AS move_values'
            columns.extend(('scores.move_numbers AS _move_numbers', 'scores.move_values AS _move_values'))
            move_range = ''
        else:
            pivots = ', '.join(f'MAX({score}) FILTER (WHERE m.n_move={int(key.removeprefix("move_score_"))}) AS "{key}"'
                               for key in score_keys)
            move_range = 'AND m.n_move BETWEEN :first_move AND :last_move'
        joins += f''' LEFT JOIN LATERAL (
            SELECT {pivots} FROM moves m
            LEFT JOIN game_fen_association a ON a.game_link=m.link
                AND a.n_move=m.n_move AND a.move_color=gp.color
            LEFT JOIN fen f ON f.fen_id=a.fen_id
            WHERE m.link=gp.link {move_range}
              AND NULLIF(BTRIM(CASE WHEN gp.color='white' THEN m.white_move ELSE m.black_move END),'') IS NOT NULL
              AND (CASE WHEN gp.color='white' THEN m.white_move ELSE m.black_move END)<>'--'
        ) scores ON TRUE'''
    query = f'{scope} SELECT {", ".join(columns)} FROM selected gp {joins} ORDER BY {order}'
    return count, query, params


def expand_sample_moves(records):
    """Expand only sampled games; never silently truncate moves or average duplicates."""
    maximum = 0
    expanded = []
    for record in records:
        row = dict(record)
        numbers, values = row.pop('_move_numbers') or [], row.pop('_move_values') or []
        if len(numbers) != len(set(numbers)):
            raise ValueError('Duplicate player/game/move records; refusing an ambiguous matrix.')
        for number, value in zip(numbers, values, strict=True):
            if number < 1 or number > MAX_MOVE:
                raise ValueError(f'The sample contains move {number}; previews support at most {MAX_MOVE} move columns. No moves were silently removed.')
            maximum = max(maximum, number)
            row[f'move_score_{number}'] = value
        expanded.append(row)
    keys = [f'move_score_{number}' for number in range(1, maximum+1)]
    for row in expanded:
        for key in keys:
            row.setdefault(key, None)
    return expanded, keys
