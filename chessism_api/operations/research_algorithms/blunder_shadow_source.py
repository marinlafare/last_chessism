"""Bounded, outcome-blind pilot input. Source and saved CPU counts are read-only."""
import hashlib
import itertools
import json
import time
from collections import defaultdict, deque

from sqlalchemy import text

from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_validation_job as fixed_source
from .blunder_training import game_blocks

QUOTAS = {'bullet': 400, 'blitz': 400, 'rapid': 200}
MAX_POSITIONS = 1_000_000
POOL = 600


def candidate_sql(session):
    layout = storage_for(session)
    if layout:
        t = layout.table
        joins = f'''FROM {t('player')} p JOIN {t('game_player')} gp ON gp.player_id=p.id
            JOIN {t('game')} g ON g.link=gp.game_link
            JOIN {t('game_mode')} m ON m.id=g.mode_id
            JOIN {t('game_analysis_coverage')} c ON c.link=g.link
            LEFT JOIN {t('game_player_engine_summary')} e ON e.game_player_id=gp.id'''
        white, mode, standard = 'gp.is_white', 'm.name', 'TRUE'
    else:
        joins = '''FROM player p JOIN game_player gp USING(player_name) JOIN game g ON g.link=gp.link
            JOIN game_analysis_summary c ON c.link=g.link
            LEFT JOIN game_player_engine_summary e ON e.game_link=g.link AND e.player_color=gp.color'''
        white, mode = "gp.color='white'", 'gp.mode'
        standard = "g.rules='chess' AND (g.initial_setup IS NULL OR g.initial_setup IN ('','startpos','rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'))"
    return f'''WITH ranked AS (SELECT p.player_name AS player,g.link,{white} AS is_white,{mode} AS mode,
        g.played_at,c.total_positions,e.blunder_count AS saved_blunders,e.analyzed_player_moves AS saved_moves,
        CASE WHEN {white} THEN g.white_elo ELSE g.black_elo END AS player_rating,
        CASE WHEN {white} THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
        ROW_NUMBER() OVER(PARTITION BY {mode} ORDER BY hashtextextended(g.link::text,20261030),g.link) AS rank
        {joins} WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL AND {standard}
        AND {mode} IN ('bullet','blitz','rapid') AND c.is_fully_analyzed AND c.total_positions>0
        AND g.played_at IS NOT NULL AND g.white_elo BETWEEN 1 AND 4000 AND g.black_elo BETWEEN 1 AND 4000)
        SELECT * FROM ranked WHERE rank<=:pool ORDER BY mode,rank'''


def choose(pool, players):
    buckets = defaultdict(deque)
    identities = set()
    for game in pool:
        key = game['player'], game['link']
        if key in identities or game['player'] not in players or game['mode'] not in QUOTAS:
            raise ValueError('Invalid or duplicate pilot metadata.')
        identities.add(key)
        buckets[game['player'], game['mode']].append(game)
    selected, seen, counts = [], set(), defaultdict(int)
    # The first 500 are spread across players and modes, not one player's tail.
    while True:
        added = 0
        for mode in ('bullet', 'blitz', 'rapid', 'bullet', 'blitz'):
            for player in players:
                key = player, mode
                bucket = buckets[key]
                while bucket and bucket[0]['link'] in seen:
                    bucket.popleft()
                if bucket and counts[key] < QUOTAS[mode]:
                    game = bucket.popleft()
                    selected.append(game); seen.add(game['link']); counts[key] += 1; added += 1
        if not added:
            break
    if not selected or len(selected) > 6000 or sum(g['total_positions'] for g in selected) > MAX_POSITIONS:
        raise ValueError('Pilot source is empty or exceeds its bounded whole-game budget.')
    if any(g['saved_blunders'] is None or g['saved_moves'] is None for g in selected):
        raise ValueError('Saved CPU counts are missing; finish their existing refresh, no live repair is performed here.')
    return selected


async def select_games(session, players, reporter):
    pool = []
    for player in players:
        await reporter.check()
        rows = (await session.execute(text(candidate_sql(session)), {'player': player, 'pool': POOL})).mappings().all()
        if not rows:
            raise ValueError(f'No eligible analyzed games for complete player {player}.')
        if len(rows) > POOL*3:
            raise ValueError('Pilot candidate pool exceeded its bound.')
        pool.extend({**dict(r), 'played_at': r['played_at'].isoformat()} for r in rows)
    return choose(pool, players)


async def extract(session, games, reporter, stream, *, index_offset=0):
    timings = {'source_read_pack_seconds': 0., 'existing_sql_reference_seconds': 0.}
    indices = {g['link']: i+index_offset for i, g in enumerate(games)}
    if len(indices) != len(games):
        raise ValueError('Pilot games must be unique.')
    source_query = fixed_source.position_source(session) + ' ORDER BY game_link,ply LIMIT :row_limit'
    rows = 0
    for block in game_blocks(games):
        await reporter.check()
        params = {'links': [g['link'] for g in block], 'sides': [g['is_white'] for g in block],
                  'row_limit': sum(g['total_positions'] for g in block)+1}
        started = time.perf_counter()
        records = (await session.execute(text(source_query), params)).all()
        by_game = {key: [(r.ply, r.score, r.kind) for r in values]
            for key, values in itertools.groupby(records, lambda r: r.game_link)}
        timings['source_read_pack_seconds'] += time.perf_counter()-started
        started = time.perf_counter()
        baseline = (await session.execute(text(fixed_source.baseline_sql(session)), params)).all()
        flags = {(r.game_link, r.ply): r.blunder for r in baseline}
        if len(flags) != len(baseline):
            raise ValueError('Duplicate independent SQL reference move.')
        timings['existing_sql_reference_seconds'] += time.perf_counter()-started
        started = time.perf_counter()
        for game in block:
            packed = fixed_source.pack_game(indices[game['link']], game, by_game.pop(game['link'], []), flags)
            # Hash local identity independently of the position in a batch.
            stable = packed.copy(); stable['game'] = 0
            game['input_sha256'] = hashlib.sha256(json.dumps(
                {k: v for k, v in game.items() if k != 'input_sha256'}, sort_keys=True).encode()+stable.tobytes()).hexdigest()
            await file_operation(stream.write, packed.tobytes())
            rows += len(packed)
        if by_game or flags:
            raise ValueError('Unexpected source or reference rows.')
        timings['source_read_pack_seconds'] += time.perf_counter()-started
        await reporter.progress('extracting_shadow_input', rows, sum(g['saved_moves'] for g in games),
            'Same read-only snapshot for exact predecessors, game-time ratings and saved CPU counts.')
    return rows, timings


async def attach_examples(session, examples):
    """Small board/move audit, never a new engine analysis or quality label."""
    import chess
    layout = storage_for(session)
    t = layout.table if layout else lambda name: name
    white = 'a.is_white' if layout and layout.compact else "a.move_color='white'"
    keys = sorted({(r['game'], p) for r in examples for p in (r['ply']-1, r['ply']) if p > 0})
    if len(keys) > 150:
        raise ValueError('Example board audit exceeds its bound.')
    records = (await session.execute(text(f'''SELECT requested.link,requested.ply,f.fen
        FROM UNNEST(CAST(:links AS bigint[]),CAST(:plies AS int[])) requested(link,ply)
        JOIN {t('game_fen_association')} a ON a.game_link=requested.link
          AND a.n_move=(requested.ply+1)/2 AND ({white})=(requested.ply%2=1)
        JOIN {t('fen')} f ON f.fen_id=a.fen_id'''),
        {'links': [k[0] for k in keys], 'plies': [k[1] for k in keys]})).all()
    positions = {(r.link, r.ply): r.fen for r in records}
    if len(records) != len(keys) or len(positions) != len(keys):
        raise ValueError('Example board identities are missing or duplicated.')
    for row in examples:
        row['before_fen'] = positions[row['game'], row['ply']-1] if row['ply'] > 1 else chess.STARTING_FEN
        row['after_fen'] = positions[row['game'], row['ply']]
        before, after = [chess.Board(' '.join(row[k].split()[:4])+' 0 1') for k in ('before_fen', 'after_fen')]
        matching = []
        for move in before.legal_moves:
            following = before.copy(); following.push(move)
            if following.fen(en_passant='legal').split()[:4] == after.fen(en_passant='legal').split()[:4]:
                matching.append((move.uci(), before.san(move)))
        row['legal_transition_verified'] = before.is_valid() and after.is_valid() and len(matching) == 1
        row['uci'], row['san'] = matching[0] if len(matching) == 1 else (None, None)
