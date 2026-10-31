"""Frozen, read-only legacy workload for the game/player migration baseline.

This is a database baseline, not an HTTP benchmark or permission to migrate.
Full analytics SQL is read from the deployed image, not candidate ORM models.
"""
import ast
import hashlib
import json
import math
import re
import statistics
import time
from pathlib import Path


TABLES = (
    'game', 'player', 'game_player', 'moves', 'fen', 'game_fen_association',
    'player_position_frequency', 'game_player_salience', 'player_salience_summary',
    'player_salience_pending_game', 'game_opening', 'game_analysis_summary',
    'game_player_engine_summary',
)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str,
                                    separators=(',', ':')).encode()).hexdigest()


def literal_sql(source_root, symbol):
    file = Path(source_root) / 'chessism_api/operations/player_analytics.py'
    for node in ast.parse(file.read_text()).body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == symbol
                and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)):
            return node.value.value
    raise ValueError('Expected deployed analytics SQL literal is unavailable')


def bind_analytics(sql, player):
    names = ('player', 'mode', 'date_from', 'date_to')
    bound = re.sub(r'(?<!:):(player|mode|date_from|date_to)\b',
                   lambda match: '$' + str(names.index(match[1]) + 1), sql)
    return bound, [player, 'all', None, None]


def select_player_cohorts(rows):
    eligible = [dict(row) for row in rows if row['games'] >= 10]
    if len(eligible) < 3:
        raise RuntimeError('Need at least three player histories for the baseline')
    return [dict(eligible[index], cohort=label) for label, index in
            (('large', 0), ('medium', len(eligible) // 2), ('small', len(eligible) - 1))]


async def discover_inputs(connection):
    rows = await connection.fetch('''
        SELECT player_name, count(*)::bigint AS games FROM game_player
        GROUP BY player_name ORDER BY games DESC, player_name
    ''')
    players = select_player_cohorts(rows)
    for player in players:
        seed = await connection.fetchrow('''
            SELECT gp.link, gp.mode, g.year FROM game_player gp
            JOIN game g ON g.link=gp.link
            WHERE gp.player_name=$1 AND g.rules='chess' AND g.fens_done
            ORDER BY gp.played_at DESC NULLS LAST, gp.link DESC LIMIT 1
        ''', player['player_name'])
        if seed is None:
            raise RuntimeError('A selected player has no extracted standard game')
        player.update(dict(seed))
    opening = await connection.fetchrow('''
        SELECT opening, n_moves, mode FROM game_opening
        WHERE link=$1 ORDER BY n_moves LIMIT 1
    ''', players[0]['link'])
    if opening is None:
        raise RuntimeError('Selected baseline game has no opening prefix')
    return {'players': players, 'opening': dict(opening)}


def make_cases(inputs, source_root):
    cases = []

    def add(name, sql, args=(), source='representative legacy SQL'):
        cases.append({'name': name, 'sql': sql.strip().rstrip(';'),
                      'args': list(args), 'source': source})

    for player in inputs['players']:
        name, tag = player['player_name'], player['cohort']
        add(tag + '/recent_games', '''
            SELECT link,color,player_name,opponent_name,rating,opponent_rating,
                   result,mode,played_at,eco,n_moves,time_elapsed,avg_elo
            FROM game_player WHERE player_name=$1
            ORDER BY played_at DESC NULLS LAST,link DESC,color LIMIT 50
        ''', [name])
        add(tag + '/mode_games', '''
            SELECT link,color,rating,result,played_at FROM game_player
            WHERE player_name=$1 AND mode=$2
            ORDER BY played_at DESC NULLS LAST,link DESC,color LIMIT 50
        ''', [name, player['mode']])
        add(tag + '/calendar_filter', '''
            SELECT g.link,g.year,g.month,g.day,g.avg_elo,g.mode
            FROM game_player gp JOIN game g ON g.link=gp.link
            WHERE gp.player_name=$1 AND g.year=$2
            ORDER BY g.played_at DESC NULLS LAST,g.link DESC,gp.color LIMIT 100
        ''', [name, player['year']])
        add(tag + '/coverage', '''
            SELECT count(*) AS games,
                   count(*) FILTER(WHERE gas.is_fully_analyzed) AS complete,
                   sum(gas.total_positions) AS positions,
                   sum(gas.analyzed_positions) AS scored
            FROM game_player gp JOIN game_analysis_summary gas ON gas.link=gp.link
            WHERE gp.player_name=$1
        ''', [name])
        add(tag + '/salience', '''
            SELECT gp.link,gp.color,s.salience,s.weighted_numerator,s.depth_weight_sum,
                   s.position_occurrence_count,s.unique_position_count,s.repeated_position_count
            FROM game_player gp JOIN game_player_salience s
              ON s.game_link=gp.link AND s.player_color=gp.color
            WHERE gp.player_name=$1
            ORDER BY gp.played_at DESC NULLS LAST,gp.link DESC,gp.color LIMIT 100
        ''', [name])
        add(tag + '/position_frequencies', '''
            SELECT p.player_color,p.fen_fen,p.games_with_position,p.total_occurrences,f.score
            FROM player_position_frequency p JOIN fen f ON f.fen=p.fen_fen
            WHERE p.player_name=$1
            ORDER BY p.games_with_position DESC,p.player_color,p.fen_fen COLLATE "C" LIMIT 100
        ''', [name])
        add(tag + '/fen_selection', '''
            WITH selected AS MATERIALIZED (
                SELECT gp.link FROM game_player gp JOIN game g ON g.link=gp.link
                WHERE gp.player_name=$1 AND g.rules='chess' AND g.fens_done
                ORDER BY gp.played_at DESC NULLS LAST,gp.link DESC LIMIT 100
            )
            SELECT DISTINCT a.fen_fen COLLATE "C" AS fen FROM selected
            JOIN game_fen_association a ON a.game_link=selected.link
            JOIN fen f ON f.fen=a.fen_fen WHERE f.score IS NULL
            ORDER BY fen LIMIT 5000
        ''', [name])
        add(tag + '/game_positions', '''
            SELECT a.n_move,a.move_color,f.fen,f.score,f.next_moves
            FROM game_fen_association a JOIN fen f ON f.fen=a.fen_fen
            WHERE a.game_link=$1 ORDER BY a.n_move,a.move_color DESC,a.id
        ''', [player['link']])
        add(tag + '/moves', '''
            SELECT n_move,white_move,black_move,white_time_left,black_time_left,
                   white_reaction_time,black_reaction_time
            FROM moves WHERE link=$1 ORDER BY n_move
        ''', [player['link']])
        for symbol in ('ENGINE_INSIGHTS_SQL', 'PLAYING_PATTERNS_SQL'):
            sql, args = bind_analytics(literal_sql(source_root, symbol), name)
            add(tag + '/' + symbol.lower(), sql, args,
                'deployed player_analytics.py:' + symbol)

    opening = inputs['opening']
    add('opening/filter', '''
        SELECT link,n_moves,opening,mode,avg_elo,played_at FROM game_opening
        WHERE mode=$1 AND n_moves=$2 AND opening=$3
        ORDER BY played_at DESC NULLS LAST,link DESC LIMIT 100
    ''', [opening['mode'], opening['n_moves'], opening['opening']])
    add('opening/rating_filter', '''
        SELECT link,n_moves,opening,avg_elo FROM game_opening
        WHERE mode=$1 AND n_moves=$2 AND avg_elo BETWEEN 2200 AND 2600
        ORDER BY avg_elo DESC,link DESC LIMIT 100
    ''', [opening['mode'], opening['n_moves']])
    add('game/rating_filter', '''
        SELECT link,white,black,avg_elo FROM game
        WHERE mode='blitz' AND avg_elo BETWEEN 2200 AND 2600
        ORDER BY avg_elo DESC,link DESC LIMIT 100
    ''')
    add('coverage/incomplete_page', '''
        SELECT gas.link,gas.total_positions,gas.analyzed_positions,gas.unscored_positions,
               g.white,g.black,g.mode FROM game_analysis_summary gas
        JOIN game g ON g.link=gas.link
        WHERE NOT gas.is_fully_analyzed AND gas.total_positions>0
        ORDER BY gas.unscored_positions DESC,gas.total_positions DESC,gas.link DESC LIMIT 20
    ''')
    add('coverage/overview', '''
        SELECT count(*) FILTER(WHERE is_fully_analyzed AND total_positions>0) AS complete,
               count(*) FILTER(WHERE NOT is_fully_analyzed AND total_positions>0) AS incomplete
        FROM game_analysis_summary
    ''')
    return cases


def row_digest(rows):
    # Decode JSON results so insignificant JSON serialization differences do not
    # appear as correctness regressions. Outer query order remains significant.
    normalized = []
    for row in rows:
        values = {}
        for key, value in dict(row).items():
            if isinstance(value, str) and value[:1] in ('{', '['):
                try:
                    value = json.loads(value)
                except ValueError:
                    pass
            values[key] = value
        normalized.append(values)
    return digest(normalized)


async def measure_case(connection, case, repeats, publish):
    samples, expected = [], None
    result = {'name': case['name'], 'status': 'running', 'samples': samples}
    for attempt in range(repeats + 1):
        await publish(attempt + 1, repeats + 1)
        started = time.perf_counter()
        # Defense in depth: even deployed SELECTs may contain side-effecting
        # functions. PostgreSQL must enforce read-only for every measurement.
        async with connection.transaction(readonly=True):
            rows = await connection.fetch(case['sql'], *case['args'])
        elapsed = (time.perf_counter() - started) * 1000
        fingerprint = row_digest(rows)
        if expected is not None and fingerprint != expected:
            raise RuntimeError('A benchmark query returned unstable results')
        expected = fingerprint
        samples.append({'wall_ms': round(elapsed, 3), 'rows': len(rows)})
    async with connection.transaction(readonly=True):
        plan = await connection.fetchval('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + case['sql'],
                                         *case['args'])
    warm = [sample['wall_ms'] for sample in samples[1:]]
    result.update(status='succeeded', result_sha256=expected,
                  first_observation_ms=samples[0]['wall_ms'],
                  warm_median_ms=statistics.median(warm), warm_min_ms=min(warm),
                  warm_max_ms=max(warm),
                  sample_p95_ms=sorted(warm)[math.ceil(.95 * len(warm)) - 1],
                  plan=json.loads(plan) if isinstance(plan, str) else plan)
    return result
