"""Bounded, read-only training snapshot and CUDA candidate research job.

Only AlgorithmRun's compact report is persisted by the existing job publisher.
No game scores, player summaries, canonical definitions or cloud jobs are changed.
"""
import asyncio
import hashlib
import inspect
import itertools
import json
import tempfile
import time

import numpy as np
from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import research_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from ..matrix_constructor.artifact_files import file_operation
from .blunder_model import (Dataset, GROUPS, MIN_GAMES, TRANSITION, VERSION,
                            compare_transitions, fit_group, group_id)
from .cuda_runtime import cuda_backend
from .storage import WORK_ROOT, ensure_capacity

OPERATION = 'rating_aware_blunder_training'
MAX_ROWS = 3_000_000
BATCH_ROWS = 32768


def configuration(games_per_group=1000):
    if type(games_per_group) is not int or not 100 <= games_per_group <= 3000:
        raise ValueError('Choose 100–3,000 games per rating/mode group.')
    return {'operation': OPERATION, 'implementation_version': VERSION, 'backend': 'cuda_cupy',
        'games_per_group': games_per_group, 'max_rows': MAX_ROWS, 'columns': ['before', 'after', 'group'],
        'input_contract': 'blunder_moves_v2', 'rating_basis': 'average_game_rating',
        'max_rating_gap': 200, 'opening_plies_excluded_from_fit': 20,
        'minimum_games': dict(zip(('training', 'validation', 'test'), MIN_GAMES)),
        'split_seed': 20261026, 'sample_seed': 20261027, 'threshold_expected_points': .15,
        'canonical': False}


def game_blocks(games):
    block, positions = [], 0
    for game in games:
        count = int(game['total_positions'])
        if not 1 <= count <= BATCH_ROWS:
            raise ValueError('A game exceeds the bounded per-query position limit; no game was truncated.')
        if block and (positions + count > BATCH_ROWS or len(block) == 128):
            yield block
            block, positions = [], 0
        block.append(game)
        positions += count
    if block:
        yield block


def add_game(dataset, stream, metadata, positions):
    game = int(metadata['link'])
    group = group_id(metadata['mode'], metadata['white_elo'], metadata['black_elo'])
    split = dataset.add_game(game, group, float(metadata['white_result']), positions)
    rows, previous_ply, previous_score, previous_kind = [], 0, 15., 0
    for ply, score, kind in positions:
        if ply <= previous_ply:
            raise ValueError('Duplicate or unordered ply; refusing ambiguous move transitions.')
        if score is not None and not np.isfinite(score):
            raise ValueError('Non-finite evaluation in training source.')
        sign = 1 if ply % 2 else -1
        # A missing immediate predecessor is unsupported, never a skipped LAG.
        before = previous_score if ply == previous_ply + 1 else None
        before_kind = previous_kind if ply == previous_ply + 1 else -1
        rows.append((group, split, game, sign * before if before is not None else np.nan,
                     sign * score if score is not None else np.nan, before_kind, kind))
        previous_ply, previous_score, previous_kind = ply, score, kind
    stream.write(np.array(rows, dtype=TRANSITION).tobytes())


async def freeze_source(config, reporter, stream, *, session_factory=None):
    if config != configuration(config.get('games_per_group')):
        raise ValueError('Unsupported or modified rating-aware training instructions.')
    dataset = Dataset()
    started = time.perf_counter()
    factory = session_factory or AsyncDBSession
    async with read_transaction(factory) as session:
        await session.execute(text("SET LOCAL statement_timeout='60s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            raise ValueError('Wait for the active cloud workflow to finish before model training.')
        await reporter.progress('selecting_training_games', detail='Selecting a bounded, rating-stratified sample of whole games; source database is read-only.')
        query = reads.TRAINING_GAMES
        games = [dict(row) for row in (await session.execute(text(query), config)).mappings()]
        if not games:
            raise ValueError('No fully analyzed eligible games with valid ratings and outcomes.')
        expected = sum(int(game['total_positions']) for game in games)
        if expected > MAX_ROWS:
            raise ValueError(f'Selected games contain {expected:,} positions, above the {MAX_ROWS:,} research limit. Reduce games per group; no games were truncated.')
        taken_at = str(await session.scalar(text('SELECT transaction_timestamp()')))
        rows = 0
        digest = hashlib.sha256()
        processed = 0
        for selected in game_blocks(games):
            await reporter.check()
            block = {int(game['link']): game for game in selected}
            records = (await session.execute(text(reads.positions()),
                {'links': list(block), 'row_limit': BATCH_ROWS + 1})).all()
            if len(records) > BATCH_ROWS:
                raise ValueError('Source exceeded the bounded per-query position limit.')
            rows += len(records)
            if rows > MAX_ROWS:
                raise ValueError('Source exceeded the bounded training limit.')
            present = set()
            for game, moves in itertools.groupby(records, key=lambda row: int(row[0])):
                present.add(game)
                positions = [(int(row[1]), row[2], int(row[3])) for row in moves]
                if len(positions) != int(block[game]['total_positions']):
                    raise ValueError('Stored game coverage disagrees with source moves; training was not published.')
                # Hash labels/ratings and every frozen evaluation, not just the fit.
                digest.update(repr((block[game], positions)).encode())
                await file_operation(add_game, dataset, stream, block[game], positions)
            if present != block.keys():
                raise ValueError('A selected game has no source positions.')
            processed += len(block)
            await reporter.progress('freezing_training_input', processed, len(games),
                f'Prepared {rows:,} positions; preserving whole-game train/validation/test splits.')
        if rows != expected:
            raise ValueError('Source count disagrees with the bounded selection.')
    return dataset, {'taken_at': taken_at, 'sha256': digest.hexdigest(), 'selected_games': len(games),
        'fitting_games': int(dataset.games.sum()), 'games_without_eligible_cp': dataset.empty_games,
        'positions': rows, 'fitting_positions': int(dataset.positions.sum()),
        'sample': 'deterministic_stratified_whole_games', 'split': 'game_hash_70_15_15',
        'selection_sql_sha256': hashlib.sha256(query.encode()).hexdigest()}, time.perf_counter() - started


async def execute(config, reporter):
    started = time.perf_counter()
    await reporter.progress('checking_gpu', detail='Checking CUDA before reading source data; no CPU fallback.')
    _, hardware = await file_operation(cuda_backend, 0)
    ensure_capacity(config)
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(dir=WORK_ROOT) as stream:
        dataset, source, extraction = await asyncio.wait_for(freeze_source(config, reporter, stream), timeout=600)
        fits = []
        fit_started = time.perf_counter()
        for group, (rating, mode) in enumerate(GROUPS):
            await reporter.progress('fitting_rating_model', group, len(GROUPS), f'Fitting {mode}, {rating["label"]}; outcomes are training labels only.')
            fits.append(await file_operation(fit_group, dataset, group))
        fit_seconds = time.perf_counter() - fit_started
        stream.seek(0)
        changes = {}
        comparison_started = time.perf_counter()
        count = 0
        while data := stream.read(BATCH_ROWS * TRANSITION.itemsize):
            if len(data) % TRANSITION.itemsize:
                raise ValueError('Truncated frozen transition stream.')
            batch = np.frombuffer(data, dtype=TRANSITION)
            for record in await file_operation(compare_transitions, batch, fits):
                totals = changes.setdefault(record['group'], {})
                for key, value in record.items():
                    if key != 'group':
                        totals[key] = totals.get(key, 0) + value
            count += len(batch)
            await reporter.progress('checking_held_out_blunders', count, source['positions'],
                'Checking CPU/CUDA decisions and old/new blunder counts on test games only.')
        comparison_seconds = time.perf_counter() - comparison_started
    for model in fits:
        model['blunder_comparison'] = changes.get(model['group'], {})
    numerical_ok = all(model['status'] != 'numerical_mismatch' and
        model['blunder_comparison'].get('cpu_cuda_mismatches', 0) == 0 for model in fits)
    fitted = sum(model['status'] == 'fitted' for model in fits)
    from . import blunder_model
    implementation_hash = hashlib.sha256((inspect.getsource(blunder_model) + inspect.getsource(inspect.getmodule(execute))).encode()).hexdigest()
    model_id = hashlib.sha256(json.dumps({'config': config, 'source': source['sha256'],
        'implementation': implementation_hash,
        'coefficients': [[model['group'], model.get('coefficient')] for model in fits]}, sort_keys=True).encode()).hexdigest()
    return {'format': 'chessism_rating_blunder_candidate_v1', 'algorithm': 'rating-aware-blunders',
        'model_id': model_id, 'implementation_sha256': implementation_hash,
        'implementation_version': VERSION, 'status': 'numerical_mismatch' if not numerical_ok else 'candidate' if fitted else 'insufficient_data',
        'canonical': False, 'production_changed': False, 'backend': 'cuda_cupy', 'precision': 'float64',
        'hardware': hardware, 'config': config, 'source': source, 'fits': fits, 'fitted_groups': fitted,
        'included_rows': source['fitting_positions'], 'cpu_cuda_passed': numerical_ok and fitted > 0,
        'timings': {'extraction_seconds': extraction, 'fitting_and_reference_seconds': fit_seconds,
                    'held_out_comparison_seconds': comparison_seconds, 'total_compute_seconds': time.perf_counter() - started},
        'notes': ['Candidate only; live blunders, accuracy, salience and CPU score distributions are unchanged.',
            'Expected points (win=1, draw=0.5, loss=0), not a separately estimated probability of winning.',
            'One positive sigmoid slope per average-rating band and mode; rating gap at most 200. No coefficients borrowed for sparse groups.',
            'Equal total fitting weight per game; integer CP clipped at ±1000. First 20 plies, mate, tablebase and unrecognized sources excluded from fitting.',
            'Timeout/abandoned games excluded. This sample does not establish calibration for those endings or larger rating gaps.',
            'Whole-game 70/15/15 hash split; repeated positions and players can cross splits. This is not a player-held-out or temporal generalization test.',
            'Fitting uses training only. Validation and test scores never select coefficients. Reported count changes are descriptive, not an approval threshold.',
            '15-point CP drop threshold and forced-mate rules retained; tablebase or incomplete transitions unsupported. First White move retains the explicit +15 CP prior.',
            'No canonicalization until coverage, calibration, boundary discontinuities and per-game count changes receive further review.']}
