"""One-player fixed-rule validation; only an AlgorithmRun report is published.

Native bounded game batches; all source/SQL/saved references share one read-only
repeatable snapshot. Full eligible history or an explicit error, never sampling.
"""
import asyncio
import hashlib
import itertools
import tempfile
import time

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from ..matrix_constructor.defaults import default_formula
from ..matrix_constructor.definitions import resolve_definition
from ..player_score_distribution import normalize
from .blunder_fixed import VERSION, RULES, MODES, DTYPE, Comparison, read_batch, cuda_backend
from .blunder_training import game_blocks
from .storage import ensure_capacity, WORK_ROOT

OPERATION = 'fixed_blunder_cuda_validation'
MAX_ROWS = 2_000_000  # All plies, including opponents, before selecting own moves.
MAX_GAMES = 100_000


def configuration(player):
    player, _ = normalize(player)
    matrix = resolve_definition(default_formula('blunder-fixed')['config'],
        {'filters': {'players': [player], 'modes': list(MODES), 'max_rows': MAX_ROWS}})
    return {'operation': OPERATION, 'implementation_version': VERSION, 'backend': 'cuda_cupy',
        'player': player, 'rules': dict(RULES), 'matrix': matrix, 'columns': matrix['feature_columns'],
        'max_rows': MAX_ROWS, 'max_games': MAX_GAMES, 'canonical': False}


def selection_sql(session):
    layout = storage_for(session)
    if layout:
        return f'''SELECT g.link,gp.is_white,m.name AS mode,c.total_positions,
            e.blunder_count AS saved_blunders,e.analyzed_player_moves AS saved_moves
            FROM {layout.table('player')} p JOIN {layout.table('game_player')} gp ON gp.player_id=p.id
            JOIN {layout.table('game')} g ON g.link=gp.game_link
            JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
            JOIN {layout.table('game_analysis_coverage')} c ON c.link=g.link
            LEFT JOIN {layout.table('game_player_engine_summary')} e ON e.game_player_id=gp.id
            WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL
              AND m.name=ANY(CAST(:modes AS text[])) AND c.is_fully_analyzed AND c.total_positions>0
            ORDER BY g.link,gp.is_white LIMIT :game_limit'''
    return '''SELECT gp.link,gp.color='white' AS is_white,gp.mode,c.total_positions,
        e.blunder_count AS saved_blunders,e.analyzed_player_moves AS saved_moves
        FROM player p JOIN game_player gp USING(player_name)
        JOIN game_analysis_summary c ON c.link=gp.link
        LEFT JOIN game_player_engine_summary e ON e.game_link=gp.link AND e.player_color=gp.color
        WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL
          AND gp.mode=ANY(CAST(:modes AS text[])) AND c.is_fully_analyzed AND c.total_positions>0
        ORDER BY gp.link,gp.color LIMIT :game_limit'''


def position_source(session):
    layout = storage_for(session)
    a = layout.table('game_fen_association') if layout else 'game_fen_association'
    f = layout.table('fen') if layout else 'fen'
    white = 'a.is_white' if layout and layout.compact else "a.move_color='white'"
    # This version mirrors production/matrix kinds, including scored CP rows
    # with legacy provenance. Rating-fit source exclusions do not apply here.
    return f'''SELECT a.game_link,(a.n_move-1)*2+CASE WHEN {white} THEN 1 ELSE 2 END AS ply,
        f.score::float8 AS score,CASE WHEN f.score IS NULL THEN -1
        WHEN f.analysis_source='tablebase' THEN 2 WHEN ABS(f.score)>=9000 THEN 1 ELSE 0 END AS kind
        FROM {a} a LEFT JOIN {f} f ON f.fen_id=a.fen_id
        WHERE a.game_link=ANY(CAST(:links AS bigint[]))'''


def baseline_sql(session):
    # Independent SQL equivalent of the existing production blunder expression;
    # unlike CUDA it computes LAG and perspective directly in PostgreSQL.
    return f'''WITH raw AS ({position_source(session)}), oriented AS (
        SELECT raw.*,CASE WHEN s.is_white THEN score ELSE -score END AS player_score,s.is_white
        FROM raw JOIN UNNEST(CAST(:links AS bigint[]),CAST(:sides AS boolean[])) s(link,is_white)
        ON s.link=raw.game_link
    ), sequenced AS (
        SELECT *,LAG(player_score) OVER w AS previous_score,LAG(kind) OVER w AS previous_kind
        FROM oriented WINDOW w AS (PARTITION BY game_link ORDER BY ply)
    ), prepared AS (
        SELECT *,COALESCE(previous_score,CASE WHEN is_white THEN 15.0 ELSE -15.0 END) AS before,
            COALESCE(previous_kind,0) AS bk FROM sequenced
    ), measured AS (
        SELECT *,50.0+50.0*(2.0/(1.0+EXP(-0.00368208*GREATEST(-1000.0,LEAST(1000.0,before))))-1.0) AS bp,
            50.0+50.0*(2.0/(1.0+EXP(-0.00368208*GREATEST(-1000.0,LEAST(1000.0,player_score))))-1.0) AS ap
        FROM prepared
    ) SELECT game_link,ply,CASE WHEN
        (bk=0 AND kind=0 AND bp-ap>=15.0) OR
        (bk=0 AND kind=1 AND player_score<0 AND before>=-700) OR
        (bk=1 AND before>0 AND kind=0 AND player_score<=700) OR
        (bk=1 AND before>0 AND kind=1 AND player_score<0)
        THEN 1 ELSE 0 END AS blunder
    FROM measured WHERE (ply%2=1)=is_white ORDER BY game_link,ply LIMIT :row_limit'''


def pack_game(index, metadata, positions, sql_flags):
    if len(positions) != metadata['total_positions']:
        raise ValueError('Coverage disagrees with source positions; refusing partial validation.')
    rows, previous, previous_kind = [], 15., 0
    for expected_ply, (ply, score, kind) in enumerate(positions, 1):
        if ply != expected_ply or score is None or not np.isfinite(score) or kind not in (0, 1, 2):
            raise ValueError('Missing, duplicate, unordered or invalid scored ply; no predecessor is skipped.')
        if (ply % 2 == 1) == metadata['is_white']:
            key = (metadata['link'], ply)
            if key not in sql_flags:
                raise ValueError('SQL reference omitted a move.')
            sign = 1 if metadata['is_white'] else -1
            rows.append((index, metadata['link'], ply, sign*previous, sign*score,
                         previous_kind, kind, sql_flags.pop(key)))
        previous, previous_kind = score, kind
    return np.array(rows, dtype=DTYPE)


async def freeze_source(config, reporter, stream, *, session_factory=None):
    if config != configuration(config.get('player')):
        raise ValueError('Unsupported or modified fixed-blunder instructions.')
    factory = session_factory or AsyncDBSession
    timings = {'metadata_seconds': 0., 'extraction_seconds': 0., 'sql_reference_seconds': 0.}
    async with factory() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='30s'"))
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            raise ValueError('Wait for the active cloud workflow to finish before validation.')
        await reporter.progress('selecting_games', detail='Reading fully analyzed games and saved CPU counts; no source writes.')
        started = time.perf_counter()
        games = [dict(r) for r in (await session.execute(text(selection_sql(session)),
            {'player': config['player'], 'modes': list(MODES), 'game_limit': MAX_GAMES+1})).mappings()]
        if not games:
            raise ValueError('Choose a complete player profile with fully analyzed games.')
        if len(games) > MAX_GAMES or len({g['link'] for g in games}) != len(games):
            raise ValueError('Too many or duplicate games; no truncation is allowed.')
        total = sum(g['total_positions'] for g in games)
        if total > MAX_ROWS:
            raise ValueError(f'This whole-player test supports at most {MAX_ROWS:,} source positions; found {total:,}. Choose a smaller player.')
        if any(g['saved_blunders'] is None or g['saved_moves'] is None for g in games):
            raise ValueError('Saved CPU game summaries are missing. Finish their existing refresh before validation.')
        taken_at = (await session.scalar(text('SELECT transaction_timestamp()'))).isoformat()
        timings['metadata_seconds'] = time.perf_counter()-started
        indices = {g['link']: i for i, g in enumerate(games)}
        source_query = position_source(session) + ' ORDER BY game_link,ply LIMIT :row_limit'
        sql_query = baseline_sql(session)
        count = batches = 0
        for block in game_blocks(games):
            await reporter.check()
            params = {'links': [g['link'] for g in block], 'sides': [g['is_white'] for g in block],
                      'row_limit': sum(g['total_positions'] for g in block)+1}
            started = time.perf_counter()
            records = (await session.execute(text(source_query), params)).all()
            by_game = {key: [(r.ply, r.score, r.kind) for r in rows]
                       for key, rows in itertools.groupby(records, lambda r: r.game_link)}
            timings['extraction_seconds'] += time.perf_counter()-started
            started = time.perf_counter()
            baseline = (await session.execute(text(sql_query), params)).all()
            sql_flags = {(r.game_link, r.ply): r.blunder for r in baseline}
            if len(sql_flags) != len(baseline):
                raise ValueError('Duplicate SQL move identity.')
            timings['sql_reference_seconds'] += time.perf_counter()-started
            started = time.perf_counter()
            for game in block:
                packed = pack_game(indices[game['link']], game, by_game.pop(game['link'], []), sql_flags)
                await file_operation(stream.write, packed.tobytes())
                count += len(packed)
            if by_game or sql_flags:
                raise ValueError('Unexpected source or SQL rows.')
            timings['extraction_seconds'] += time.perf_counter()-started
            batches += 1
            await reporter.progress('freezing_input', count, sum(g['saved_moves'] for g in games),
                'Freezing own moves and independent SQL decisions in bounded whole-game batches.')
    return games, {'player': config['player'], 'scope': 'all_fully_analyzed_games_all_modes',
        'games': len(games), 'source_positions': total, 'rows': count, 'taken_at': taken_at,
        'truncated': False, 'game_batches': batches, 'matrix_default': 'blunder-fixed',
        'temporary_input_bytes': count*DTYPE.itemsize,
        'source_sql_sha256': hashlib.sha256(source_query.encode()).hexdigest(),
        'reference_sql_sha256': hashlib.sha256(sql_query.encode()).hexdigest()}, timings


async def execute(config, reporter):
    started = time.perf_counter()
    if config != configuration(config.get('player')):
        raise ValueError('Unsupported or modified fixed-blunder instructions.')
    await reporter.check()
    await reporter.progress('checking_gpu', detail='Checking CUDA; no CPU fallback and no coefficient training.')
    checkpoint = time.perf_counter()
    await file_operation(cuda_backend, 0)
    preflight = time.perf_counter()-checkpoint
    ensure_capacity(config)
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(dir=WORK_ROOT) as stream:
        games, source, timings = await asyncio.wait_for(freeze_source(config, reporter, stream), timeout=300)
        await reporter.check()
        comparison = await file_operation(Comparison, games)
        stream.seek(0)
        while True:
            await reporter.check()
            batch = await file_operation(read_batch, stream)
            if not len(batch):
                break
            await file_operation(comparison.consume, batch)
            await reporter.progress('comparing', comparison.rows, source['rows'],
                'Checking every move and accumulating per-game CPU/CUDA/SQL counts.')
        if comparison.rows != source['rows']:
            raise ValueError('Numerical input stream changed.')
        report = comparison.report()
    await reporter.check()
    report.update(source=source, input_rows=source['source_positions'], included_rows=source['rows'])
    report['timings'].update(timings, gpu_preflight_seconds=preflight, validation_wall_seconds=time.perf_counter()-started)
    report['notes'] = ['Fixed coefficient and existing 0.15 threshold; no rating model or 0.20 rule applied.',
        'All fully analyzed games for this player, all three modes. Whole games and immediate predecessors; no sampling.',
        'Tablebase transitions are unassessed and contribute no blunders, matching existing counts.',
        'CPU scalar, independent SQL and saved per-game counts must all agree; boundary mismatches are never hidden with a tolerance.',
        'Database extraction, transfer, calculation and reference timings are separate. No production publication was benchmarked; no end-to-end speedup is claimed.',
        'Only this research report is saved. Live summaries, coefficients and canonical algorithms remain unchanged.']
    return report
