"""Idempotent batched graph publication, with exact per-game counter deltas."""
from collections import Counter, defaultdict
from neo4j import unit_of_work
from .sync_source import ROOT, START
from .sync_metrics import TimedQueries


# The wire format is local to one publication, not a stored schema. Fixed-width
# rows avoid encoding the same property names for every occurrence. Keep fields
# explicit so transport metadata never becomes a node/relationship property.
POSITION_FIELDS = ('key', 'fen_id', 'fen', 'variant', 'score_kind', 'score_cp',
                   'mate_in', 'mate_for_white', 'tablebase_wdl', 'tablebase_dtz')
STEP_FIELDS = ('key', 'ply', 'from_key', 'uci', 'san', 'move_key')


def position_payload(positions, deltas):
    """Apply counters while positions are bound; old-only nodes keep their data."""
    pending = {row['key']: row for row in deltas}
    rows = []
    for position in positions:
        delta = pending.pop(position['key'], {})
        rows.append([position.get(field) for field in POSITION_FIELDS] +
                    [delta.get('occurrences', 0), delta.get('games', 0)])
    return rows, [[row['key'], row['occurrences'], row['games']]
                  for row in pending.values()]


def player_payload(deltas):
    """Resolve each player once, not once for every position they reached."""
    groups = defaultdict(list)
    for row in deltas:
        groups[row['player']].append([row[k] for k in ('key','color','occurrences','games')])
    return [[player, rows] for player, rows in groups.items()]


async def install(driver, database):
    statements = [
        "CREATE CONSTRAINT chessism_player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT chessism_game_id IF NOT EXISTS FOR (g:Game) REQUIRE g.id IS UNIQUE",
        "CREATE CONSTRAINT chessism_position_key IF NOT EXISTS FOR (p:Position) REQUIRE p.key IS UNIQUE",
        "CREATE CONSTRAINT chessism_position_id IF NOT EXISTS FOR (p:Position) REQUIRE p.fen_id IS UNIQUE",
        "CREATE CONSTRAINT chessism_move_key IF NOT EXISTS FOR ()-[m:MOVE]-() REQUIRE m.key IS UNIQUE",
        "CREATE INDEX chessism_position_score IF NOT EXISTS FOR (p:Position) ON (p.score_cp)",
        "CREATE INDEX chessism_position_mate IF NOT EXISTS FOR (p:Position) ON (p.mate_in)",
        "CREATE INDEX chessism_position_frequency IF NOT EXISTS FOR (p:Position) ON (p.occurrences)",
    ]
    async with driver.session(database=database) as session:
        for query in statements:
            await (await session.run(query)).consume()
        await (await session.run("CALL db.awaitIndexes(120)")).consume()


def contributions(games, sign=1):
    positions, players, moves = Counter(), Counter(), Counter()
    for game in games:
        counts = Counter(step['key'] for step in game['steps'])
        for key, count in counts.items():
            positions[(key, 'occurrences')] += sign * count
            positions[(key, 'games')] += sign
            for color, player in [('white',game['white_id']), ('black',game['black_id'])]:
                players[(player, key, color, 'occurrences')] += sign * count
                players[(player, key, color, 'games')] += sign
        counts = Counter((s['move_key'], s['from_key'], s['key'], s['uci'], s['san']) for s in game['steps'])
        for key, count in counts.items():
            moves[(*key, 'occurrences')] += sign * count
            moves[(*key, 'games')] += sign
    return positions, players, moves


def delta_rows(old, new):
    before, after = contributions(old, -1), contributions(new)
    # Counter addition drops nonpositive values, so use update instead.
    for destination, incoming in zip(before, after):
        destination.update(incoming)
    p, r, m = before
    return (
        [dict(key=k, occurrences=p[k,'occurrences'], games=p[k,'games']) for k in sorted({k for k,_ in p})
         if p[k,'occurrences'] or p[k,'games']],
        [dict(player=id, key=k, color=c, occurrences=r[id,k,c,'occurrences'], games=r[id,k,c,'games'])
         for id,k,c in sorted({key[:3] for key in r}) if r[id,k,c,'occurrences'] or r[id,k,c,'games']],
        [dict(move_key=id, source=a, target=b, uci=u, san=s, occurrences=m[id,a,b,u,s,'occurrences'], games=m[id,a,b,u,s,'games'])
         for id,a,b,u,s in sorted({key[:5] for key in m}) if m[id,a,b,u,s,'occurrences'] or m[id,a,b,u,s,'games']],
    )


@unit_of_work(timeout=90)
async def publish(tx, games, positions, deleted):
    queries = TimedQueries(tx)
    run = queries.run
    ids = [g['id'] for g in games] + deleted
    existing = await (await run('game_hashes', "MATCH (g:Game) WHERE g.id IN $ids RETURN g.id AS id,g.source_hash AS hash", ids=ids)).data()
    hashes = {r['id']:r['hash'] for r in existing}
    changed = [g for g in games if hashes.get(g['id']) != g['source_hash']]
    changed_ids = [g['id'] for g in changed] + [id for id in deleted if id in hashes]
    # Even an unchanged path can carry newly filled evaluation values.
    await (await run('initial_position', '''MERGE (p:Position {key:$key}) ON CREATE SET p.fen=$fen,p.variant='chess',
        p.occurrences=0,p.games=0,p.score_kind='unscored' ''', key=ROOT, fen=START)).consume()
    replaced_ids = [id for id in changed_ids if id in hashes]
    previous = await (await run('previous_paths', '''MATCH (g:Game)-[v:AT_PLY]->(p:Position) WHERE g.id IN $ids
        RETURN g.id AS id,g.white_id AS white_id,g.black_id AS black_id,
        collect({key:p.key,ply:v.ply,from_key:v.from_key,uci:v.uci,san:v.san,move_key:v.move_key}) AS steps''',
        ids=replaced_ids)).data() if replaced_ids else []
    pd, rd, md = delta_rows(previous, changed)
    position_rows, old_only = position_payload(positions, pd)
    invalid = await (await run('positions', '''UNWIND $rows AS row MERGE (p:Position {key:row[0]})
        ON CREATE SET p.occurrences=0,p.games=0
        SET p.fen_id=row[1],p.fen=row[2],p.variant=row[3],p.score_kind=row[4],
            p.score_cp=row[5],p.mate_in=row[6],p.mate_for_white=row[7],
            p.tablebase_wdl=row[8],p.tablebase_dtz=row[9],
            p.occurrences=p.occurrences+row[10],p.games=p.games+row[11]
        WITH p WHERE p.occurrences<0 OR p.games<0 OR p.occurrences<p.games
        RETURN count(p) AS n''', rows=position_rows)).single()
    if invalid['n']:
        raise ValueError('Graph counter underflow; publication rolled back')
    # A removed/corrected path can reference nodes absent from the new source.
    # Subtract their counts without clearing evaluations or other properties.
    if old_only:
        invalid = await (await run('removed_position_counts', '''UNWIND $rows AS row MATCH (p:Position {key:row[0]})
            SET p.occurrences=p.occurrences+row[1],p.games=p.games+row[2]
            WITH p WHERE p.occurrences<0 OR p.games<0 OR p.occurrences<p.games
            RETURN count(p) AS n''', rows=old_only)).single()
        if invalid['n']:
            raise ValueError('Graph counter underflow; publication rolled back')
    if not changed_ids:
        return {"changed_games": 0, "unchanged_games": len(games), 'query_seconds': dict(queries.seconds)}
    players = {g[f'{c}_id']: dict(id=g[f'{c}_id'], name=g[f'{c}_name']) for g in changed for c in ('white','black')}
    await (await run('players', 'UNWIND $rows AS row MERGE (p:Player {id:row.id}) SET p.name=row.name', rows=list(players.values()))).consume()
    if replaced_ids:
        await (await run('remove_paths', '''MATCH (g:Game)-[r:AT_PLY]->() WHERE g.id IN $ids DELETE r''', ids=replaced_ids)).consume()
        await (await run('remove_owners', '''MATCH ()-[r:PLAYED]->(g:Game) WHERE g.id IN $ids DELETE r''', ids=replaced_ids)).consume()
    metadata = [{k:v for k,v in g.items() if k != 'steps'} for g in changed]
    await (await run('game_metadata', '''UNWIND $rows AS row MERGE (g:Game {id:row.id}) SET g += row''',
        rows=metadata)).consume()
    await (await run('game_owners', '''UNWIND $rows AS row MATCH (g:Game {id:row.id})
        MATCH (w:Player {id:row.white_id}),(b:Player {id:row.black_id})
        CREATE (w)-[:PLAYED {color:'white'}]->(g),(b)-[:PLAYED {color:'black'}]->(g)''', rows=metadata)).consume()
    paths = [[g['id'], [[s[field] for field in STEP_FIELDS] for s in g['steps']]] for g in changed]
    await (await run('game_paths', '''UNWIND $rows AS game MATCH (g:Game {id:game[0]})
        UNWIND game[1] AS row MATCH (p:Position {key:row[0]})
        CREATE (g)-[v:AT_PLY]->(p) SET v.ply=row[1],v.from_key=row[2],v.uci=row[3],
        v.san=row[4],v.move_key=row[5]''', rows=paths)).consume()
    # Validate and remove zero-count relationships while they are already bound.
    # This preserves transaction rollback but avoids resending and rematching
    # every counter row two more times for validation and cleanup.
    for name, query, rows in (
        ('player_positions', '''UNWIND $rows AS player MATCH (a:Player {id:player[0]})
        UNWIND player[1] AS row MATCH (p:Position {key:row[0]})
        MERGE (a)-[r:REACHED {color:row[1]}]->(p) ON CREATE SET r.occurrences=0,r.games=0
        SET r.occurrences=r.occurrences+row[2],r.games=r.games+row[3]''', player_payload(rd)),
        ('position_moves', '''UNWIND $rows AS row MATCH (a:Position {key:row[1]}),(b:Position {key:row[2]})
        MERGE (a)-[r:MOVE {key:row[0]}]->(b) ON CREATE SET r.occurrences=0,r.games=0,r.uci=row[3],r.san=row[4]
        SET r.occurrences=r.occurrences+row[5],r.games=r.games+row[6]''',
         [[r[k] for k in ('move_key','source','target','uci','san','occurrences','games')] for r in md]),
    ):
        query += ''' WITH r, r.occurrences<r.games OR r.games<0 OR r.occurrences<0 AS invalid
            FOREACH (_ IN CASE WHEN NOT invalid AND r.occurrences=0 THEN [1] ELSE [] END | DELETE r)
            RETURN sum(CASE WHEN invalid THEN 1 ELSE 0 END) AS n'''
        if (await (await run(name, query, rows=rows)).single())['n']:
            raise ValueError('Graph relationship counter underflow; publication rolled back')
    if deleted:
        await (await run('delete_games', 'MATCH (g:Game) WHERE g.id IN $ids DETACH DELETE g', ids=deleted)).consume()
    # Unused position nodes remain reusable, explicitly carrying zero counts.
    return {"changed_games": len(changed_ids), "unchanged_games": len(games)-len(changed),
            'query_seconds': dict(queries.seconds)}


async def counts(session):
    result = {}
    for key, query in {
        'games': 'MATCH (n:Game) RETURN count(n) AS n',
        'positions': 'MATCH (n:Position) RETURN count(n) AS n',
        'occurrences': 'MATCH ()-[r:AT_PLY]->() RETURN count(r) AS n',
        'moves': 'MATCH ()-[r:MOVE]->() RETURN count(r) AS n',
    }.items():
        result[key] = (await (await session.run(query)).single())['n']
    return result
