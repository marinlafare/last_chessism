"""Read-only, bounded, frozen input for smooth rating-aware research v2."""
import hashlib
import itertools
import json
import time

import numpy as np
from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import research_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from ..matrix_constructor.artifact_files import file_operation
from .blunder_training import game_blocks
from .blunder_v2_model import DTYPE, MODES

MAX_ROWS = 3_000_000


def held_player(name):
    return int.from_bytes(hashlib.sha256(('blunder-v2-player:' + name.strip().lower()).encode()).digest()[:8], 'big') % 5 == 0


def partitions(metadata):
    if len({g['link'] for g in metadata}) != len(metadata):
        raise ValueError('Duplicate game identities in training selection.')
    stamps = sorted(g['played_at'] for g in metadata)
    if not stamps:
        raise ValueError('No eligible scored games with valid historical ratings, dates and outcomes.')
    first, second = stamps[int(len(stamps)*.70)], stamps[int(len(stamps)*.85)]
    if first == second or first == stamps[0]:
        raise ValueError('Not enough distinct game dates for chronological train/validation/test partitions.')
    for game in metadata:
        stamp = game['played_at']
        split = 0 if stamp < first else 1 if stamp < second else 2
        held = held_player(game['white_name']) or held_player(game['black_name'])
        game['split'] = (3 if split == 2 else -1) if held else split
    return {'training_before': first.isoformat(), 'test_from': second.isoformat(),
            'held_out_players': 'SHA256(name) modulo 5 = 0; every game involving such a player excluded from fitting/selection'}


def pack(index, metadata, positions):
    if len(positions) != metadata['total_positions']:
        raise ValueError('Coverage disagrees with source positions.')
    rows, previous, previous_kind = [], 15., 0
    for expected_ply, (ply, score, fen, kind) in enumerate(positions, 1):
        if ply != expected_ply or score is None or not np.isfinite(score):
            raise ValueError('Missing, duplicate, unordered or nonfinite evaluation; no predecessor is skipped.')
        rows.append((index, ply, fen, previous, score, previous_kind, kind))
        previous, previous_kind = score, kind
    return np.array(rows, dtype=DTYPE)


def game_arrays(metadata, cp_counts):
    return {'mode': np.array([MODES.index(g['mode']) for g in metadata]),
            'split': np.array([g['split'] for g in metadata]), 'cp_count': np.asarray(cp_counts),
            'u': np.array([((g['white_elo']+g['black_elo'])/2-2300)/400 for g in metadata]),
            'd': np.array([(g['white_elo']-g['black_elo'])/400 for g in metadata]),
            'outcome': np.array([g['white_result'] for g in metadata])}


async def read_positions(session, metadata, reporter, stream=None, *, phase='freezing_v2_input'):
    """Same ordered hash for extraction and exact historical-cohort verification."""
    digest = hashlib.sha256(json.dumps(metadata, default=str, sort_keys=True).encode())
    indices = {g['link']: i for i, g in enumerate(metadata)}
    counts = np.zeros(len(metadata), dtype=np.int64)
    total, expected_total = 0, sum(g['total_positions'] for g in metadata)
    for block in game_blocks(metadata):
        await reporter.check()
        values = (await session.execute(text(reads.positions(fen_ids=True)),
            {'links': [g['link'] for g in block], 'row_limit': sum(g['total_positions'] for g in block)+1})).all()
        grouped = {link: [(r[1], r[2], r[3], r[4]) for r in records]
                   for link, records in itertools.groupby(values, lambda r: r[0])}
        for game in block:
            index = indices[game['link']]
            packed = pack(index, game, grouped.pop(game['link'], []))
            raw = packed.tobytes()
            digest.update(raw)
            if stream is not None:
                await file_operation(stream.write, raw)
            counts[index] = np.count_nonzero(packed['ak'] == 0)
            total += len(packed)
        if grouped:
            raise ValueError('Unexpected source game.')
        await reporter.progress(phase, total, expected_total,
            'Read-only whole-game input; checking source identity and exact predecessors.')
    return counts, total, digest.hexdigest()


async def freeze(config, reporter, stream, *, session_factory=None):
    started = time.perf_counter()
    async with read_transaction(session_factory or AsyncDBSession) as session:
        await session.execute(text("SET LOCAL statement_timeout='60s'"))
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
            raise ValueError('Wait for the active cloud workflow to finish before candidate training.')
        await reporter.progress('selecting_v2_games', detail='Bounded unique games, including timeouts and larger rating gaps; source is read-only.')
        audit = {}
        if config.get('sampling'):
            from .blunder_balanced_source import select_balanced
            if config['sampling']['version'] == 'development_reuse_v1':
                from .blunder_development_reuse import select as select_balanced
            metadata, split, audit, query = await select_balanced(session, config, reporter)
        else:
            query = reads.v2_selection()
            metadata = [dict(r) for r in (await session.execute(text(query), config)).mappings()]
            split = partitions(metadata)
        if sum(g['total_positions'] for g in metadata) > MAX_ROWS:
            raise ValueError('Selection exceeds 3 million source positions. Reduce games per group; no games are truncated.')
        if audit:
            from .blunder_balanced_source import retain_cp_balance
            metadata=await retain_cp_balance(session,metadata,config,audit,reporter)
        if not metadata:
            raise ValueError('No games meet the frozen sampling constraints; no rules were relaxed.')
        counts, total, digest = await read_positions(session, metadata, reporter, stream)
        if audit:
            from .blunder_balanced_sampling import concentration
            audit['sampling_audit']['cp_concentration'] = concentration([g for g,n in zip(metadata,counts) if n>0])
        taken = str(await session.scalar(text('SELECT transaction_timestamp()')))
    return metadata, game_arrays(metadata, counts), {'sha256': digest, 'taken_at': taken,
        'selected_games': len(metadata), 'positions': total, 'split': split,
        'excluded_held_player_early_games': sum(g['split'] == -1 for g in metadata),
        'selection_sql_sha256': hashlib.sha256(query.encode()).hexdigest(),
        'sampling': 'fixed-seed rating/mode-stratified whole games; excludes missing dates/ratings and invalid results',
        'extraction_seconds': time.perf_counter()-started, **audit}
