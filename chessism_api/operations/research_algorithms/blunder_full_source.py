"""Large development-only inputs; no test outcomes, bounded source queries."""
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import itertools
import json

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_balanced_sampling as balance, blunder_development_reuse as history
from . import blunder_v2_source as source, blunder_v2_model as model, blunder_cohort_source as cohort
from .blunder_training import game_blocks
from .repository import RunCancelled

OPERATION = 'full_development_rating_blunder_v1'
RESERVATION = 'chessism_blunder_full_development_reservation_v1'
FORMAT = 'chessism_blunder_full_development_v1'
MAX_GAMES = 1_000_000
MAX_ROWS = 50_000_000


async def additional_roles(session):
    rows = (await session.execute(text('''SELECT result FROM algorithm_run
        WHERE config->>'operation'=:operation AND result IS NOT NULL
        ORDER BY created_at,id LIMIT 101'''), {'operation': OPERATION})).scalars().all()
    if len(rows) > 100:
        raise ValueError('Full development history exceeds its bounded scan.')
    training, validation = set(), set()
    for result in rows:
        if result.get('format') not in (RESERVATION, FORMAT):
            raise ValueError('Unrecognized full development identity reservation.')
        frozen = result['source']; roles = frozen['reserved_roles']
        if set(roles) != {'training', 'validation'} or history.digest(roles) != frozen['reserved_roles_sha256']:
            raise ValueError('Full development identity checksum changed.')
        t, v = history.ids(roles['training']), history.ids(roles['validation'])
        if t & v or t & validation or v & training:
            raise ValueError('Full development historical role conflict.')
        training.update(t); validation.update(v)
    return training, validation


def choose_maximum_pool(metadata, policy, check=lambda: None):
    """No per-cell sample limit. Deterministic large feasible subset, not an
    assertion of globally optimal graph matching. Both players keep the 10% cap.
    """
    groups = defaultdict(list)
    for g in metadata:
        split = balance.partition(g, policy)
        if split in (0, 1):
            g['split'] = split
            groups[split, g['mode']].append(g)
    selected, audit = [], []
    for (split, mode), group in sorted(groups.items()):
        check()
        group.sort(key=lambda g: (g['sample_order'], g['link']))
        cap = len(group)//10
        for _ in range(128):
            counts = Counter(); kept = []
            for index, g in enumerate(group):
                if index % 5000 == 0: check()
                names = balance.names(g)
                if cap and all(counts[n] < cap for n in names):
                    kept.append(g); counts.update(names)
            new_cap = len(kept)//10
            if max(counts.values(), default=0) <= new_cap:
                break
            cap = new_cap
        else:
            raise ValueError('Player balance did not converge; no cap was relaxed.')
        selected.extend(kept)
        audit.append({'split': split, 'mode': mode, 'available_games': len(group),
            'selected_games': len(kept), 'largest_player_games': max(counts.values(), default=0),
            'largest_player_share': max(counts.values(), default=0)/len(kept) if kept else None})
    selected.sort(key=lambda g: g['link'])
    if len(selected) != len({g['link'] for g in selected}):
        raise ValueError('Duplicate development game.')
    return selected, audit


async def reserve(frozen, reporter, session_factory=AsyncDBSession):
    await reporter.check()
    payload = {'format': RESERVATION, 'source': frozen, 'canonical': False, 'production_changed': False}
    async with session_factory() as session, session.begin():
        saved = await session.scalar(text('''UPDATE algorithm_run SET result=CAST(:value AS jsonb)
            WHERE id=:id AND status='running' AND NOT cancel_requested RETURNING id'''),
            {'id': reporter.run_id, 'value': json.dumps(payload, allow_nan=False)})
        if saved is None:
            raise RunCancelled('Cancelled before development reservation; no outcomes loaded.')


async def select(session, config, reporter):
    roles, known, proof = await history.history(session, config['sampling_policy'], reporter)
    if history.digest(roles) != config['history_sha256']:
        raise ValueError('Research identity history changed; submit a fresh request.')
    policy = config['sampling_policy']
    query = source.eligible_sql(session, owners=True) + ''' SELECT link,mode,played_at,white_name,black_name,white_complete,black_complete,
        white_elo,black_elo,rating_band,total_positions,sample_order FROM eligible
        WHERE played_at<CAST(:test_from AS timestamptz) AND NOT(link=ANY(CAST(:excluded AS bigint[])))
          AND (NOT(link=ANY(CAST(:training AS bigint[]))) OR played_at<CAST(:training_before AS timestamptz))
          AND (NOT(link=ANY(CAST(:validation AS bigint[]))) OR played_at>=CAST(:training_before AS timestamptz))
        ORDER BY link LIMIT :bound'''
    params = {'test_from': datetime.fromisoformat(policy['test_from']),
        'training_before': datetime.fromisoformat(policy['training_before']),
        'excluded': roles['protected'], 'training': roles['training'], 'validation': roles['validation'],
        'bound': MAX_GAMES+1}
    await reporter.progress('selecting_full_development', detail='All eligible development metadata; no per-rating sampling cap. Test dates and historical test games excluded.')
    metadata = []; scanned = 0
    rows = await session.stream(text(query), params, execution_options={'yield_per': 5000})
    async for row in rows.mappings():
        scanned += 1
        if scanned > MAX_GAMES:
            raise ValueError('Development metadata exceeds one million games; no truncation.')
        if scanned % 5000 == 0:
            await reporter.check()
        g = dict(row)
        if balance.partition(g, policy) in (0, 1):
            metadata.append(g)
    # Reserve/stage the entire eligible pool. Balancing before CP eligibility is
    # known lets mate-only removals push retained players over the cap.
    selected = metadata
    counts = Counter()
    for g in selected:
        g['split'] = balance.partition(g, policy)
        counts[g['split'], g['mode']] += 1
    if not selected or sum(g['total_positions'] for g in selected) > MAX_ROWS:
        raise ValueError('Empty balanced pool or source exceeds fifty million positions; no truncation.')
    ids = {g['link'] for g in selected}
    if ids & set(roles['protected']) or any(g['split'] not in (0, 1) for g in selected):
        raise ValueError('Protected game or test date entered development.')
    reserved = {name: [g['link'] for g in selected if g['split'] == split]
                for split, name in enumerate(('training', 'validation'))}
    frozen = {'reserved_roles': reserved, 'reserved_roles_sha256': history.digest(reserved),
        'selected_games': len(selected), 'metadata_games_scanned': scanned,
        'source_positions': sum(g['total_positions'] for g in selected),
        'reserved_games': len(selected), 'selection': [
            {'split': s, 'mode': m, 'available_games': n, 'selected_games': None,
             'largest_player_share': None} for (s, m), n in sorted(counts.items())],
        'verified_history': proof, 'history_sha256': config['history_sha256'],
        'metadata_sha256': hashlib.sha256(json.dumps(selected, sort_keys=True, default=str).encode()).hexdigest(),
        'query_sha256': hashlib.sha256(query.encode()).hexdigest(), 'test_evaluated': False,
        'training_before': policy['training_before'], 'test_from': policy['test_from']}
    return selected, frozen


def compact(game, records):
    """Exact existing all-ply, White-perspective, equal-total-game CP objective."""
    packed = source.pack(0, game, records)
    scores = packed['after'][packed['ak'] == 0]
    values = np.empty((len(scores), 5), dtype='<f8')
    values[:, 0] = scores
    values[:, 1] = ((game['white_elo']+game['black_elo'])/2-2300)/400
    values[:, 2] = (game['white_elo']-game['black_elo'])/400
    values[:, 3] = game['white_result']
    if len(scores):
        values[:, 4] = 1./len(scores)
    return values, packed


def stage_block(block, outcomes, grouped, streams, moves, offsets, digest):
    buffers = defaultdict(list); move_buffers = []
    processed = 0
    for game in block:
        outcome = outcomes.get(game['link'])
        if outcome not in (0, .5, 1):
            raise ValueError('Missing or invalid development label.')
        game['white_result'] = float(outcome)
        values, packed = compact(game, grouped.pop(game['link'], []))
        digest.update(json.dumps(game, default=str, sort_keys=True).encode()); digest.update(packed.tobytes())
        key = game['split'], game['mode']
        game['cp_offset'], game['cp_count'] = offsets[key], len(values)
        offsets[key] += len(values); processed += len(packed)
        buffers[key].append(values.tobytes())
        if key == (1, 'bullet'):
            game['move_offset'] = offsets['moves']; offsets['moves'] += len(packed)
            move_buffers.append(packed.tobytes())
    if grouped:
        raise ValueError('Unexpected source game.')
    # One threaded write per source block, not a thread round-trip per game.
    for key, buffers_ in buffers.items(): streams[key].write(b''.join(buffers_))
    moves.write(b''.join(move_buffers))
    return processed


def finalize_inputs(selected, folder, frozen, policy, check=lambda: None):
    """Balance whole CP-eligible games, compact staged files without DB rereads."""
    eligible = [g for g in selected if g['cp_count']]
    kept, audit = choose_maximum_pool(eligible, policy, check)
    if not kept:
        raise ValueError('No CP-eligible balanced development games.')
    counts = Counter(); cp_games = Counter(); no_cp = Counter()
    cells = Counter(); players = defaultdict(Counter)
    for g in selected:
        if not g['cp_count']: no_cp[g['split'], g['mode']] += 1
    grouped = defaultdict(list)
    for g in kept: grouped[g['split'], g['mode']].append(g)
    digest = hashlib.sha256(); diagnostic_games = []
    for s in (0, 1):
        for m in model.MODES:
            check(); key = s, m; path = folder/f'staged-{s}-{m}.bin'
            raw = np.memmap(path, mode='r', dtype='<f8') if path.stat().st_size else None
            try:
                with (folder/f'{s}-{m}.bin').open('xb') as output:
                    for i, g in enumerate(grouped[key]):
                        if i % 2048 == 0: check()
                        start = g['cp_offset']*5; data = raw[start:start+g['cp_count']*5].tobytes()
                        if len(data) != g['cp_count']*40: raise ValueError('Truncated staged CP input.')
                        output.write(data); g['cp_offset'] = counts[key]
                        counts[key] += g['cp_count']; cp_games[key] += 1
                        cells[s, m, g['rating_band']] += 1; players[key].update(balance.names(g))
                        digest.update(json.dumps(g, default=str, sort_keys=True).encode()); digest.update(data)
                        if m == 'rapid' or key == (1, 'bullet'): diagnostic_games.append(g)
            finally:
                if raw is not None: raw._mmap.close()
            path.unlink()  # Only this run's temporary staging file.
    path = folder/'staged-bullet-moves.bin'
    raw = np.memmap(path, mode='r', dtype=model.DTYPE) if path.stat().st_size else None
    try:
        with (folder/'bullet-moves.bin').open('xb') as output:
            for i, g in enumerate(grouped[1, 'bullet']):
                if i % 2048 == 0: check()
                block = np.array(raw[g['move_offset']:g['move_offset']+g['total_positions']], copy=True)
                if len(block) != g['total_positions']: raise ValueError('Truncated staged move input.')
                block['game'] = i
                data = block.tobytes(); output.write(data); digest.update(data)
    finally:
        if raw is not None: raw._mmap.close()
    path.unlink()
    groups = [{'split': s, 'mode': m, 'games': cp_games[s, m], 'cp_rows': counts[s, m], 'no_cp_games': no_cp[s, m],
        'rating_bands': [cells[s, m, b] for b in range(9)],
        'largest_player_share': max(players[s, m].values(), default=0)/cp_games[s, m] if cp_games[s, m] else None,
        'player_balance_passed': bool(cp_games[s, m] and max(players[s, m].values())*10 <= cp_games[s, m])}
        for s in (0, 1) for m in model.MODES]
    frozen.update(selection=audit, selected_games=len(kept), cp_eligible_games=len(eligible),
        removed_by_player_cap=len(eligible)-len(kept), no_cp_games=sum(no_cp.values()),
        balancing_order='cp_eligibility_then_both_player_cap_v2',
        input_sha256=digest.hexdigest(), groups=groups, cp_rows=sum(counts.values()))
    return groups, diagnostic_games


async def extract(session, selected, folder, frozen, reporter):
    layout = storage_for(session); table = layout.table('game') if layout else 'game'
    streams = {(s, m): (folder/f'staged-{s}-{m}.bin').open('xb') for s in (0, 1) for m in model.MODES}
    moves = (folder/'staged-bullet-moves.bin').open('xb'); offsets = Counter()
    processed = 0; digest = hashlib.sha256()
    try:
        for block in game_blocks(selected):
            await reporter.check()
            links = [g['link'] for g in block]
            outcomes = dict((await session.execute(text(f'SELECT link,white_result FROM {table} WHERE link=ANY(CAST(:links AS bigint[]))'), {'links': links})).all())
            records = (await session.execute(text(source.positions_sql(session)),
                {'links': links, 'row_limit': sum(g['total_positions'] for g in block)+1})).all()
            grouped = {link: [(r[1], r[2], r[3], r[4]) for r in rows]
                       for link, rows in itertools.groupby(records, lambda r: r[0])}
            processed += await file_operation(stage_block, block, outcomes, grouped, streams, moves, offsets, digest)
            await reporter.progress('loading_full_development', processed, frozen['source_positions'],
                'Read-only whole-game queries; building mixed-player CUDA inputs once. Test outcomes remain untouched.')
    finally:
        for stream in streams.values(): stream.close()
        moves.close()
    frozen.update(staged_input_sha256=digest.hexdigest(), positions=processed,
                  source_snapshot=str(await session.scalar(text('SELECT transaction_timestamp()'))))
