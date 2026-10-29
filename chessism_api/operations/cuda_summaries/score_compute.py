"""CUDA histogram/coverage/ending reduction. NumPy is permitted only in tests.

The established scored-only kernel and bin boundaries are reused unchanged.
Unscored occurrences are coverage counters, never rows fed to that kernel.
"""
import numpy as np
from ..player_stockfish_summary import empty_modes
from ..research_algorithms.summary_validation import score_kernel, group_counts, group_mates, MODES

ENDINGS = ('games', 'checkmates_for', 'checkmates_against', 'stalemates_by_player',
           'stalemates_by_opponent', 'stalemates_unattributed', 'other_draws')
WIDTH = 24  # 13 CP bins, mate, tablebase, unscored, eligible games, seven endings.


def check_device(xp, require_cuda):
    if require_cuda and xp.__name__ != 'cupy':
        raise RuntimeError('CUDA score summaries require CuPy; CPU fallback is prohibited.')


def host(xp, value):
    return xp.asnumpy(value) if xp.__name__ == 'cupy' else np.asarray(value)


def bincount(xp, values, length):
    # CuPy versions with CUB enabled do not accept an empty bincount input.
    return xp.bincount(values, minlength=length) if values.size else xp.zeros(length, dtype=xp.int64)


def payloads(xp, counters, codes, mate_counts, keys):
    # Sum CP counters on the GPU too; host work only serializes compact results.
    counts = host(xp, counters); cp_counts = host(xp, counters[:, :13].sum(axis=1))
    codes, mate_counts = host(xp, codes), host(xp, mate_counts)
    output = {key: empty_modes() for key in keys}
    for index, key in enumerate(keys):
        for m, mode in enumerate(MODES):
            group = index * 3 + m; row = counts[group]; target = output[key][mode]
            target.update(counts=row[:13].tolist(), cp_positions=int(cp_counts[group]),
                mate_positions=int(row[13]), tablebase_positions=int(row[14]), unscored_positions=int(row[15]),
                eligible_games=int(row[16]), endings={k: int(row[17+i]) for i, k in enumerate(ENDINGS)} if row[17] else {})
    for code, count in zip(codes.tolist(), mate_counts.tolist()):
        group = code // 2004; key = keys[group // 3]; mode = MODES[group % 3]
        output[key][mode]['mates'].append({'moves': None if code % 1002 == 1001 else code % 1002,
            'side': 'for' if code % 2004 >= 1002 else 'against', 'positions': int(count)})
    return output


def calculate(xp, raw, games, owners, *, require_cuda=True, device=None):
    check_device(xp, require_cuda)
    keys = sorted({(o['player_name'], o['day']) for o in owners})
    if not keys:
        return {}
    if not games:
        return {key: empty_modes() for key in keys}
    groups = {key: i for i, key in enumerate(keys)}; game_ids = {g['game_link']: i for i, g in enumerate(games)}
    maps = np.full((2, len(games)), -1, dtype=np.int64)
    for owner in owners:
        index = game_ids.get(owner['game_link'])
        if index is not None:
            maps[int(owner['is_white']), index] = groups[owner['player_name'], owner['day']] * 3 + MODES.index(games[index]['mode'])
    counters = xp.zeros((len(keys)*3, WIDTH), dtype=xp.int64)
    arrays = device.columns if device is not None else {name:xp.asarray(raw[name]) for name in ('game','score','source')}
    raw_ids, scores, sources = (arrays[name] for name in ('game','score','source'))
    analyzed = xp.asarray([g['analyzed'] and g['total_positions'] > 0 for g in games], dtype=xp.bool_)
    last = xp.asarray([-1 if g['last_white'] is None else int(g['last_white']) for g in games])
    # String normalization is transport encoding; outcome decisions/reductions run on CUDA.
    def reason(value):
        return 1 if str(value or '').lower() in ('checkmate', 'checkmated') else 2 if str(value or '').lower() == 'stalemate' else 0
    reasons = xp.asarray([[reason(g['black_str_result']), reason(g['white_str_result'])] for g in games])
    results = xp.asarray([[float('nan') if g[k] is None else g[k] for k in ('black_result', 'white_result')] for g in games])
    all_codes, all_mates = [], []
    for white in (0, 1):
        mapping = xp.asarray(maps[white]); ids = mapping[raw_ids]
        present = ids >= 0; selected = present & (sources != 3)
        values = scores[selected] * (1 if white else -1); src = sources[selected]
        kinds = xp.where(src == 2, 2, xp.where(xp.abs(values) >= 9000, 1, 0))
        distance = 10000 - xp.abs(values)
        moves = xp.where((src == 1) & (kinds == 1) & (distance >= 0) & (distance <= 1000)
                         & (distance == xp.floor(distance)), distance, -1)
        decisions = score_kernel(xp, values, kinds, moves)
        if values.size:
            counters[:, :15] += group_counts(xp, decisions, ids[selected], len(keys)*3)
        codes, counts = group_mates(xp, decisions, ids[selected])
        all_codes.append(codes); all_mates.append(counts)
        counters[:, 15] += bincount(xp, ids[present & (sources == 3)], len(keys)*3)
        own = mapping >= 0; own_result = results[:, white]; draw = own_result == .5
        stalemate = (reasons[:, 0] == 2) | (reasons[:, 1] == 2)
        flags = (analyzed, xp.ones(len(games), dtype=xp.bool_),
            (own_result == 1) & (reasons[:, 1-white] == 1),
            (own_result == 0) & (reasons[:, white] == 1),
            draw & stalemate & (last == white), draw & stalemate & (last == 1-white),
            draw & stalemate & (last == -1), draw & ~stalemate)
        for column, mask in enumerate(flags, 16):
            counters[:, column] += bincount(xp, mapping[own & mask], len(keys)*3)
    codes, inverse = xp.unique(xp.concatenate(all_codes), return_inverse=True)
    counts = xp.zeros(len(codes), dtype=xp.uint64)
    xp.add.at(counts, inverse, xp.concatenate(all_mates).astype(xp.uint64))
    return payloads(xp, counters, codes, counts, keys)


def reduce(xp, entries, *, require_cuda=True):
    """Combine compact day/batch counters on CUDA, without rereading any FENs."""
    check_device(xp, require_cuda)
    keys = sorted({key for key, _ in entries}); ids = {key: i for i, key in enumerate(keys)}
    if not keys:
        return {}
    rows, destinations, codes, counts = [], [], [], []
    for key, payload in entries:
        for m, mode in enumerate(MODES):
            row = payload[mode]; group = ids[key]*3+m
            rows.append([*row['counts'], row['mate_positions'], row['tablebase_positions'], row['unscored_positions'],
                row['eligible_games'], *[row['endings'].get(k, 0) for k in ENDINGS]])
            destinations.append(group)
            for mate in row['mates']:
                codes.append(group*2004 + (1002 if mate['side'] == 'for' else 0) + (1001 if mate['moves'] is None else mate['moves']))
                counts.append(mate['positions'])
    counters = xp.zeros((len(keys)*3, WIDTH), dtype=xp.uint64)
    xp.add.at(counters, xp.asarray(destinations), xp.asarray(rows, dtype=xp.uint64))
    unique, inverse = xp.unique(xp.asarray(codes, dtype=xp.int64), return_inverse=True)
    mates = xp.zeros(len(unique), dtype=xp.uint64)
    xp.add.at(mates, inverse, xp.asarray(counts, dtype=xp.uint64))
    return payloads(xp, counters, unique, mates, keys)
