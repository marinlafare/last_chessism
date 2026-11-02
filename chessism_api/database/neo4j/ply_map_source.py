"""Bounded reads of actual AT_PLY paths; never infer paths from MOVE edges."""
from neo4j import Query
from .config import GraphConfig
from .runtime import connect


async def read(after, limit=500):
    config = GraphConfig.from_env()
    async with connect(config) as driver, driver.session(database=config.database,default_access_mode='READ') as session:
        rows = await session.run(Query('''MATCH (g:Game) WHERE g.id>$after
            WITH g ORDER BY g.id LIMIT $limit
            CALL (g) {
                OPTIONAL MATCH (g)-[v:AT_PLY]->(:Position)
                RETURN coalesce(max(v.ply),0) AS last_ply
            }
            CALL (g) {
                MATCH (g)-[v:AT_PLY]->(p:Position) WHERE v.ply>=1 AND v.ply<=30
                WITH v,p ORDER BY v.ply
                RETURN collect({ply:v.ply,fen_id:p.fen_id,games:p.games,san:v.san}) AS path
            }
            RETURN g.id AS id,g.source_hash AS source_hash,
                g.white_id AS white_id,g.black_id AS black_id,
                g.white_name AS white_name,g.black_name AS black_name,g.played_at AS played_at,
                last_ply,path ORDER BY id''', timeout=20),after=after,limit=limit)
        return await rows.data()


async def expand(games):
    """Fetch only ten captured games, verifying hashes before exposing paths."""
    if not games:
        return []
    config = GraphConfig.from_env()
    async with connect(config) as driver, driver.session(database=config.database,default_access_mode='READ') as session:
        result = await session.run(Query('''UNWIND $games AS item
            OPTIONAL MATCH (g:Game {id:item.game_id})
            CALL (g,item) {
                OPTIONAL MATCH (g)-[v:AT_PLY]->(p:Position)
                WHERE v.ply>=item.start_ply AND v.ply<=item.end_ply
                WITH v,p ORDER BY v.ply
                RETURN collect(CASE WHEN v IS NOT NULL THEN {ply:v.ply,fen:p.fen,
                    fen_key:p.key,san:v.san} END) AS path
            }
            RETURN item.game_id AS game_id,g.source_hash AS source_hash,path''',timeout=8),games=games)
        rows = {r['game_id']:r for r in await result.data()}
    for game in games:
        row = rows.get(game['game_id'])
        if row is None or row['source_hash'] != game['source_hash']:
            raise ValueError('This game changed since the saved map. Run CUDA summaries to refresh it.')
        expected=max(0,game['end_ply']-max(1,game['start_ply'])+1)
        if len(row['path']) != expected:
            raise ValueError('The captured game path is incomplete. Synchronize the graph before refreshing the map.')
        game['path'] = row['path']
        game.pop('source_hash',None)
    return games
