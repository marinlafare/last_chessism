"""Collapsed, layered view of real game paths, not walks over aggregate MOVE edges.

Path compression and deterministic layout are host-side structural work. Entity
membership deduplication/counting is one numeric CUDA operation in the manual
runner. NumPy is accepted only by the isolated reference tests.
"""
from collections import defaultdict
import time
import numpy as np

VERSION = 'collapsed_ply_map_v1'
MAX_PLY = 30
ROOT = 'root'
IMPLEMENTATION = 'bulk_lexsort_v2'


def group_memberships(raw, entity_count, xp):
    """One device lexsort plus an adjacent-row mask, never unique(axis=0).

    CuPy 13.6 implements axis-based unique with a Python comparison loop which
    synchronizes the GPU for individual rows. This is the same bulk grouping
    pattern used by the position-frequency stage. Full int64 keys are preserved
    without packed-key multiplication/overflow.
    """
    if not len(raw):
        return np.empty((0, 2), dtype=np.int64), np.zeros(entity_count, dtype=np.int64)
    data = xp.asarray(raw, dtype=xp.int64)
    data = data[xp.lexsort(xp.stack((data[:, 1], data[:, 0])))]
    different = xp.ones(len(data), dtype=xp.bool_)
    different[1:] = xp.any(data[1:] != data[:-1], axis=1)
    pairs = data[different]
    counts = xp.bincount(pairs[:, 0], minlength=entity_count)
    if xp.__name__ != 'numpy':
        pairs, counts = xp.asnumpy(pairs), xp.asnumpy(counts)
    return pairs, counts


def build(games, xp, progress=None):
    report = progress or (lambda event: None)
    report(dict(stage='compressing',games=0,total_games=len(games)))
    nodes, edges, segments, occurrences = {}, {}, [], []
    entities = {}

    def entity(key):
        return entities.setdefault(key, len(entities))

    def node(key, ply, kind, fen=None, label=None):
        if key not in nodes:
            nodes[key] = dict(key=key, ply=ply, kind=kind, fen_key=fen,
                              label=label or kind, entity=entity(key))
        return key

    def visit(key, ordinal):
        occurrences.append((entity(key), ordinal))

    def link(source, target, hidden, ordinal, start, end, reason, move=''):
        # Different hidden lengths remain separate connections with exact counts.
        key = f'e:{source}>{target}:{hidden}:{reason}'
        if key not in edges:
            edges[key] = dict(key=key, source=source, target=target,
                kind='compressed' if hidden else 'direct', hidden=hidden,
                reason=reason, label=move if not hidden else f'{hidden} hidden positions',
                entity=entity(key))
        visit(key, ordinal)
        if hidden or reason != 'shared':
            segments.append((key, ordinal, start, end, reason))

    node(ROOT, 0, 'root', 'chess:initial', 'Initial position')
    records = []
    for ordinal, game in enumerate(games):
        path = game['path']
        if [p['ply'] for p in path] != list(range(1, len(path) + 1)) or len(path) > MAX_PLY:
            raise ValueError(f"Incomplete or unordered imported path for game {game['id']}.")
        if game['last_ply'] < len(path) or len(path) != min(MAX_PLY, game['last_ply']):
            raise ValueError('Imported path coverage does not match its final ply.')
        if any(p['games'] < 1 for p in path):
            raise ValueError('A position is missing its distinct-game count.')
        if not game.get('source_hash'):
            raise ValueError('A graph game has no verified source hash.')
        records.append({k: v for k, v in game.items() if k != 'path'} | {'ordinal': ordinal})
        visit(ROOT, ordinal)
        anchor, anchor_ply = ROOT, 0
        for position in path:
            # Global full-game count, never a count within the displayed window
            # or within the currently highlighted players.
            if position['games'] == 1:
                continue
            ply = position['ply']
            target = node(f"p:{ply}:{position['fen_id']}", ply, 'position',
                          f"chess:{position['fen_id']}", position['san'])
            visit(target, ordinal)
            link(anchor, target, ply - anchor_ply - 1, ordinal,
                 anchor_ply + 1, ply - 1, 'shared', position['san'])
            anchor, anchor_ply = target, ply
        reason = 'ending' if game['last_ply'] <= MAX_PLY else 'continuation'
        end = len(path)
        target = node(f't:{anchor}:{reason}', anchor_ply + 1, reason,
                      label='Game endings' if reason == 'ending' else 'Beyond move 15')
        visit(target, ordinal)
        link(anchor, target, end - anchor_ply, ordinal,
             anchor_ply + 1, end, reason)
        if (ordinal + 1) % 500 == 0 or ordinal + 1 == len(games):
            report(dict(stage='compressing',games=ordinal + 1,total_games=len(games)))

    # One transfer/grouping pass for all captured games, not one kernel per game.
    raw = np.asarray(occurrences, dtype=np.int64).reshape(-1, 2)
    report(dict(stage='grouping',input_pairs=len(raw),entities=len(entities)))
    started = time.monotonic()
    pairs, counts = group_memberships(raw, len(entities), xp)
    report(dict(stage='layout',input_pairs=len(raw),unique_pairs=len(pairs),
                grouping_seconds=time.monotonic()-started))
    members = defaultdict(list)
    for e, ordinal in pairs.tolist():
        members[e].append(ordinal)
    for item in [*nodes.values(), *edges.values()]:
        item['games'] = int(counts[item['entity']])
        item['members'] = members[item.pop('entity')]

    # Layout is saved once. Player selection never recomputes coordinates.
    parents = defaultdict(list)
    for edge in edges.values():
        parents[edge['target']].append((edge['source'], edge['games']))
    layers = defaultdict(list)
    for item in nodes.values():
        layers[item['ply']].append(item)
    rank = {}
    for ply in sorted(layers):
        def order(item):
            incoming = parents[item['key']]
            weight = sum(n for _, n in incoming)
            center = sum(rank.get(k, 0) * n for k, n in incoming) / max(weight, 1)
            return (center, -item['games'], item['key'])
        layer = sorted(layers[ply], key=order)
        for index, item in enumerate(layer):
            item['x'], item['y'] = ply, index
            rank[item['key']] = index / max(1, len(layer) - 1)
    return dict(version=VERSION, max_ply=MAX_PLY, games=records,
                nodes=list(nodes.values()), connections=list(edges.values()), segments=segments)
