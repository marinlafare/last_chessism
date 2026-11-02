"""Bounded opening-map projection. No new graph writes or full-game path walks.

Counts come from actual game occurrences, never from composing aggregate MOVE
edges. Both-player slots use a union, so head-to-head games are counted once.
"""
from datetime import datetime, timezone
from hashlib import sha256
import json

GAME_LIMIT = 20000
NODE_LIMIT = 6000
EDGE_LIMIT = 20000
MAX_PLY = 30
SLOTS = ('all', 'a', 'b', 'both', 'a_white', 'a_black', 'b_white', 'b_black', 'both_white', 'both_black')


def conditions():
    aw, ab = 'g.white_name=$a', 'g.black_name=$a'
    bw, bb = 'g.white_name=$b', 'g.black_name=$b'
    a, b = f'({aw} OR {ab})', f'({bw} OR {bb})'
    return ('true', a, b, f'({a} OR {b})', aw, ab, bw, bb, f'({aw} OR {bw})', f'({ab} OR {bb})')


def sums(value):
    return '[' + ','.join(f'sum(CASE WHEN {condition} THEN {value} ELSE 0 END)' for condition in conditions()) + ']'


async def snapshot(tx, player_a, player_b):
    async def read(query, **kwargs):
        return await (await tx.run(query, a=player_a, b=player_b, **kwargs)).data()

    total = (await read('MATCH (g:Game) RETURN count(g) AS n'))[0]['n']
    selected = await read('MATCH (g:Game) WITH g ORDER BY g.id LIMIT $limit RETURN collect(g.id) AS ids', limit=GAME_LIMIT)
    ids = selected[0]['ids']
    coverage = (await read(f'MATCH (g:Game) WHERE g.id IN $ids RETURN {sums("1")} AS counts', ids=ids))[0]['counts']
    prefix = '''MATCH (g:Game) WHERE g.id IN $ids
        MATCH (g)-[v:AT_PLY]->(p:Position) WHERE v.ply>=1 AND v.ply<=$max_ply'''
    node_rows = await read(prefix + f'''
        WITH p,g,count(v) AS n,min(v.ply) AS first_ply
        RETURN p.key AS key,p.fen_id AS fen_id,min(first_ply) AS first_ply,
            {sums('n')} AS appearances,{sums('1')} AS games
        ORDER BY games[0] DESC,key LIMIT $limit''', ids=ids, max_ply=MAX_PLY, limit=NODE_LIMIT + 1)
    nodes = node_rows[:NODE_LIMIT]
    keys = {n['key'] for n in nodes} | {'chess:initial'}
    # Do not aggregate or transfer thousands of rare edges whose endpoints were
    # already excluded from this bounded snapshot.
    edge_rows = await read(prefix + f''' AND p.key IN $keys AND v.from_key IN $keys
        WITH v.move_key AS key,v.from_key AS source,p.key AS target,g,count(v) AS n,head(collect(v.san)) AS move
        RETURN key,source,target,head(collect(move)) AS move,{sums('n')} AS appearances,{sums('1')} AS games
        ORDER BY games[0] DESC,key LIMIT $limit''', ids=ids, keys=sorted(keys), max_ply=MAX_PLY, limit=EDGE_LIMIT + 1)
    edges = [e for e in edge_rows[:EDGE_LIMIT] if e['source'] in keys and e['target'] in keys and e['key']]
    if not any(n['key'] == 'chess:initial' for n in nodes):
        nodes.insert(0, {'key': 'chess:initial', 'fen_id': None, 'first_ply': 0,
                         'appearances': [0] * len(SLOTS), 'games': [0] * len(SLOTS)})
    result = {'nodes': nodes, 'edges': edges, 'slots': list(SLOTS), 'players': [player_a, player_b],
              'coverage': {'games': coverage, 'imported_games': total, 'sample_games': len(ids),
                           'game_limit': GAME_LIMIT, 'max_ply': MAX_PLY},
              'limits': {'games': total > GAME_LIMIT, 'nodes': len(node_rows) > NODE_LIMIT,
                         'edges': len(edge_rows) > EDGE_LIMIT,
                         'node_limit': NODE_LIMIT, 'edge_limit': EDGE_LIMIT}}
    # Includes counts, not just topology: a new import cannot reuse stale weights.
    result['revision'] = sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()[:24]
    result['prepared_at'] = datetime.now(timezone.utc).isoformat()
    return result
