"""Read-only, bounded queries for the imported chess graph (not the SQL corpus)."""
from datetime import datetime
import re

POSITION = "p{.key,.fen_id,.fen,.score_kind,.score_cp,.mate_in,.mate_for_white,.tablebase_wdl,.tablebase_dtz}"


async def rows(tx, query, **parameters):
    return await (await tx.run(query, **parameters)).data()


async def players(tx, search):
    return await rows(tx, '''MATCH (p:Player) WHERE toLower(p.name) CONTAINS $search
        RETURN p.id AS id,p.name AS name ORDER BY name LIMIT 25''', search=search.lower())


async def scope(tx, player, color):
    if not player:
        data = await rows(tx, 'MATCH (g:Game) RETURN count(g) AS games')
        return {'player': None, 'player_id': None, 'color': 'all', 'games': data[0]['games']}
    data = await rows(tx, '''MATCH (p:Player {name:$player})
        OPTIONAL MATCH (p)-[r:PLAYED]->(g:Game) WHERE $color='all' OR r.color=$color
        RETURN p.id AS player_id,p.name AS player,count(g) AS games''', player=player, color=color)
    return {**(data[0] if data else {'player': player, 'player_id': None, 'games': 0}), 'color': color}


async def ranking(tx, player, color):
    coverage = await scope(tx, player, color)
    if player and coverage['player_id'] is None:
        return {'coverage': coverage, 'rows': []}
    if player:
        query = f'''MATCH (:Player {{id:$player_id}})-[r:REACHED]->(p:Position)
            WHERE ($color='all' OR r.color=$color) AND r.occurrences>0
            WITH p,sum(r.occurrences) AS appearances,sum(r.games) AS games
            RETURN {POSITION} AS position,appearances,games
            ORDER BY appearances DESC,p.key LIMIT 1001'''
    else:
        query = f'''MATCH (p:Position) WHERE p.occurrences>0
            RETURN {POSITION} AS position,p.occurrences AS appearances,p.games AS games
            ORDER BY p.occurrences DESC,p.key LIMIT 1001'''
    return {'coverage': coverage, 'rows': await rows(tx, query, **coverage)}


async def position(tx, key, coverage):
    if coverage['player']:
        query = f'''MATCH (p:Position {{key:$key}})
            OPTIONAL MATCH (:Player {{id:$player_id}})-[r:REACHED]->(p)
            WHERE $color='all' OR r.color=$color
            RETURN {POSITION} AS position,coalesce(sum(r.occurrences),0) AS appearances,
                coalesce(sum(r.games),0) AS games'''
    else:
        query = f'''MATCH (p:Position {{key:$key}})
            RETURN {POSITION} AS position,p.occurrences AS appearances,p.games AS games'''
    result = await rows(tx, query, key=key, **coverage)
    return result[0] if result else None


def game_filter():
    return '''($player='' OR EXISTS { MATCH (owner:Player)-[played:PLAYED]->(g)
        WHERE (owner.id=$player_id OR ($other<>'' AND owner.name=$other))
        AND ($color='all' OR played.color=$color) })'''


async def neighbors(tx, key, coverage, direction, offset, other='', max_ply=0):
    # Direction is a server-owned enum, never interpolated user Cypher.
    pattern = ('(p:Position {key:$key})<-[m:MOVE]-(other:Position)' if direction == 'incoming'
               else '(p:Position {key:$key})-[m:MOVE]->(other:Position)')
    if coverage['player'] or max_ply:
        endpoint = 'p' if direction == 'incoming' else 'other'
        query = f'''MATCH {pattern} WHERE m.occurrences>0
            MATCH (g:Game)-[v:AT_PLY]->({endpoint}) WHERE v.move_key=m.key
            AND ($max_ply=0 OR v.ply<=$max_ply) AND {game_filter()}
            WITH other,m,count(v) AS appearances,count(DISTINCT g) AS games'''
    else:
        query = f'''MATCH {pattern} WHERE m.occurrences>0
            WITH other,m,m.occurrences AS appearances,m.games AS games'''
    query += ''' RETURN other{.key,.fen_id,.fen,.score_kind,.score_cp,.mate_in,.mate_for_white} AS position,
        m.key AS move_key,m.san AS move,appearances,games
        ORDER BY appearances DESC,m.key SKIP $offset LIMIT 7'''
    data = await rows(tx, query, key=key, offset=offset, other=other, max_ply=max_ply,
                      **{**coverage, 'player': coverage['player'] or ''})
    return {'rows': data[:6], 'has_more': len(data) > 6, 'offset': offset}


async def neighborhood(tx, key, player, color, incoming, outgoing, other='', max_ply=0):
    coverage = await scope(tx, player, color)
    selected = await position(tx, key, coverage)
    if not selected:
        return None
    if max_ply or other:
        result = await rows(tx, f'''MATCH (p:Position {{key:$key}})
            OPTIONAL MATCH (g:Game)-[v:AT_PLY]->(p)
            WHERE ($max_ply=0 OR v.ply<=$max_ply) AND {game_filter()}
            RETURN count(v) AS appearances,count(DISTINCT g) AS games''', key=key,
            other=other, max_ply=max_ply, **{**coverage, 'player': player})
        selected.update(result[0])
    return {**selected, 'coverage': coverage,
            'incoming': await neighbors(tx, key, coverage, 'incoming', incoming, other, max_ply),
            'outgoing': await neighbors(tx, key, coverage, 'outgoing', outgoing, other, max_ply)}


async def games(tx, key, player, color, move_key, offset, other='', max_ply=0):
    coverage = await scope(tx, player, color)
    if move_key:
        # A move request must be adjacent to the currently selected position.
        query = '''MATCH (a:Position)-[m:MOVE {key:$move_key}]->(p:Position)
            WHERE a.key=$key OR p.key=$key
            MATCH (g:Game)-[v:AT_PLY]->(p) WHERE v.move_key=m.key'''
    else:
        query = 'MATCH (p:Position {key:$key})<-[v:AT_PLY]-(g:Game) WHERE true'
    if player:
        query += ''' AND EXISTS { MATCH (owner:Player)-[played:PLAYED]->(g)
            WHERE (owner.id=$player_id OR ($other<>'' AND owner.name=$other))
            AND ($color='all' OR played.color=$color) }'''
    query += ' AND ($max_ply=0 OR v.ply<=$max_ply)'
    query += ''' WITH g,p,min(v.ply) AS ply
        RETURN g.id AS game_id,g.white_name AS white,g.black_name AS black,g.played_at AS played_at,
            g.mode AS mode,g.white_result AS white_result,g.black_result AS black_result,ply,p.fen AS fen
        ORDER BY g.played_at DESC,g.id DESC SKIP $offset LIMIT 16'''
    result = await rows(tx, query, key=key, move_key=move_key, offset=offset, other=other, max_ply=max_ply, **coverage)
    for row in result:
        if row['played_at']:
            # PostgreSQL's ::text uses +00; Python 3.10 needs +00:00.
            stamp = re.sub(r'([+-]\d{2})$', r'\1:00', row['played_at'])
            row['played_at'] = datetime.fromisoformat(stamp).isoformat()
    return {'rows': result[:15], 'has_more': len(result) > 15, 'offset': offset, 'coverage': coverage}


async def read(driver, database, operation, *args):
    # One snapshot per response, explicitly read-only; a fixed timeout bounds CPU.
    async with driver.session(database=database, default_access_mode='READ') as session:
        async with await session.begin_transaction(timeout=20 if operation.__name__ == 'snapshot' else 6) as tx:
            return await operation(tx, *args)
