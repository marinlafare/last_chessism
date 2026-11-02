"""Explicit graph schema installation, activation, bounded backfill and checks."""
import argparse
import asyncio
import json
import os
import uuid
from sqlalchemy import text
from chessism_api.database.postgresql.runtime import bind_runtime_engine, configured_storage
from chessism_api.database.postgresql.engine import AsyncDBSession
from .runtime import connect
from .config import GraphConfig
from . import sync_schema as schema, sync_graph as graph, sync_source as source


async def backfill(session, player, limit):
    if not 1 <= limit <= 50000:
        raise ValueError('Backfill must contain between 1 and 50,000 games per request')
    p = (await session.execute(text(f"SELECT id,player_name FROM {configured_storage().table('player')} WHERE player_name=:name"),
                              {'name':player.lower()})).mappings().first()
    if not p:
        raise ValueError('Player not found')
    ids = list((await session.execute(text(f'''SELECT g.link FROM {configured_storage().table('game_player')} gp
        JOIN {configured_storage().table('game')} g ON g.link=gp.game_link
        WHERE gp.player_id=:id AND g.fens_done AND g.n_moves>0 ORDER BY g.link LIMIT :limit'''),
        {'id':p['id'],'limit':limit})).scalars())
    if len(ids) != limit:
        raise ValueError(f'Only {len(ids)} complete games available; requested {limit}')
    token = uuid.uuid4()
    await session.execute(text('''INSERT INTO graph_sync.backfill(id,player_id,player_name,game_ids)
        VALUES(:id,:player,:name,:ids)'''), {'id':token,'player':p['id'],'name':p['player_name'],'ids':ids})
    for offset in range(0, len(ids), 1000):
        await session.execute(text("INSERT INTO graph_sync.event(kind,ids,batch_id) VALUES('game',:ids,:batch)"),
                              {'ids':ids[offset:offset+1000],'batch':token})
    return {'backfill_id':str(token),'player':p['player_name'],'games':len(ids),'first_game':ids[0],'last_game':ids[-1]}


async def verify(session, driver, config, run_id):
    ids = list((await session.execute(text('SELECT game_ids FROM graph_sync.backfill WHERE id=:id'), {'id':uuid.UUID(run_id)})).scalar_one())
    checked = 0
    async with driver.session(database=config.database) as gs:
        for offset in range(0,len(ids),25):
            games, positions, deleted = await source.read_games(session,ids[offset:offset+25],configured_storage())
            actual = await (await gs.run('''MATCH (g:Game) WHERE g.id IN $ids OPTIONAL MATCH (g)-[v:AT_PLY]->(p:Position)
                RETURN g.id AS id,g.source_hash AS hash,collect({ply:v.ply,key:p.key,uci:v.uci,from_key:v.from_key}) AS steps''', ids=[g['id'] for g in games])).data()
            by_id = {r['id']:r for r in actual}
            for g in games:
                found = by_id.get(g['id'])
                expected = [{k:s[k] for k in ('ply','key','uci','from_key')} for s in g['steps']]
                if not found or found['hash'] != g['source_hash'] or sorted(found['steps'],key=lambda s:s['ply']) != expected:
                    raise RuntimeError(f"Graph path mismatch for game {g['id']}")
                checked += 1
            actual_positions = await (await gs.run('''MATCH (p:Position) WHERE p.key IN $keys
                RETURN p.key AS key,p.score_kind AS score_kind,p.score_cp AS score_cp,p.mate_in AS mate_in,
                p.mate_for_white AS mate_for_white,p.tablebase_wdl AS tablebase_wdl,p.tablebase_dtz AS tablebase_dtz''',
                keys=[p['key'] for p in positions])).data()
            position_map = {r['key']:r for r in actual_positions}
            for p in positions:
                if position_map.get(p['key']) != {k:p[k] for k in ('key','score_kind','score_cp','mate_in','mate_for_white','tablebase_wdl','tablebase_dtz')}:
                    raise RuntimeError(f"Graph evaluation mismatch for FEN {p['fen_id']}")
        # These checks scan only the bounded first-test graph, not PostgreSQL.
        for query in (
            '''MATCH (p:Position) OPTIONAL MATCH (g:Game)-[r:AT_PLY]->(p)
               WITH p,count(r) AS n,count(DISTINCT g) AS games WHERE p.occurrences<>n OR p.games<>games RETURN count(p) AS n''',
            '''MATCH (a:Player)-[played:PLAYED]->(g:Game)-[v:AT_PLY]->(p:Position)
               WITH a,p,played.color AS color,count(v) AS n,count(DISTINCT g) AS games
               OPTIONAL MATCH (a)-[r:REACHED {color:color}]->(p)
               WITH r,n,games WHERE r IS NULL OR r.occurrences<>n OR r.games<>games RETURN count(*) AS n''',
            '''MATCH (g:Game)-[v:AT_PLY]->() WITH v.move_key AS key,count(v) AS n,count(DISTINCT g) AS games
               OPTIONAL MATCH ()-[m:MOVE {key:key}]->()
               WITH m,n,games WHERE m IS NULL OR m.occurrences<>n OR m.games<>games RETURN count(*) AS n''',
        ):
            if (await (await gs.run(query)).single())['n']:
                raise RuntimeError('Graph appearance counters do not match game paths')
        report = {'verified_games':checked, 'paths':'passed','counts':'passed', 'evaluations':'passed', **await graph.counts(gs)}
        if checked != len(ids):
            raise RuntimeError('Backfill source games changed or disappeared before verification')
        await session.execute(text('UPDATE graph_sync.backfill SET verification=CAST(:report AS jsonb) WHERE id=:id'),
            {'report':json.dumps(report),'id':uuid.UUID(run_id)})
        return report


async def main(args):
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        if args.install:
            async with AsyncDBSession() as session, session.begin():
                await schema.install(session, configured_storage())
            config = GraphConfig.from_env()
            async with connect(config) as driver:
                await graph.install(driver, config.database)
        async with AsyncDBSession() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='30s'"))
            if args.activate:
                await session.execute(text("UPDATE graph_sync.control SET enabled=true,phase='current' WHERE id"))
            if args.player:
                if not await session.scalar(text('SELECT enabled FROM graph_sync.control WHERE id')):
                    raise ValueError('Enable change capture before requesting a backfill')
                result = await backfill(session, args.player, args.limit)
            elif args.verify:
                config = GraphConfig.from_env()
                async with connect(config) as driver:
                    result = await verify(session, driver, config, args.verify)
            else:
                result = await schema.status(session)
            print(json.dumps(result,default=str),flush=True)
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install',action='store_true')
    parser.add_argument('--activate',action='store_true')
    parser.add_argument('--player')
    parser.add_argument('--limit',type=int,default=5000)
    parser.add_argument('--verify')
    asyncio.run(main(parser.parse_args()))
