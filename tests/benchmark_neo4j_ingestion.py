"""End-to-end synthetic benchmark; refuses any non-disposable service address.

Run with the old image's /app PYTHONPATH for baseline and /workspace for candidate.
Fixture cleanup applies ONLY to the dedicated test services named below.
"""
import asyncio
import hashlib
import json
import logging
import os
import random
import resource
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlparse

import asyncpg
import chess
from neo4j import AsyncGraphDatabase
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.database.neo4j import sync_worker as worker, sync_graph as graph, sync_schema as schema
from chessism_api.database.postgresql.relational_storage import RelationalStorage


async def main():
    url=os.environ['GRAPH_BULK_TEST_DB']; uri=os.environ['GRAPH_BULK_TEST_URI']
    if urlparse(url).hostname!='graph-bulk-test-db' or urlparse(uri).hostname!='graph-bulk-test-neo4j':
        raise RuntimeError('Synthetic services only; never run this against live databases.')
    label=os.environ.get('GRAPH_BULK_LABEL','candidate')
    conn=await asyncpg.connect(url)
    engine=create_async_engine(url.replace('postgresql://','postgresql+asyncpg://'))
    factory=async_sessionmaker(engine,expire_on_commit=False)
    driver=AsyncGraphDatabase.driver(uri,auth=None,telemetry_disabled=True)
    layout=RelationalStorage('public','archive','reads','relational_v3')
    config=SimpleNamespace(database='neo4j')
    try:
        # Schema/data belong exclusively to this explicitly named synthetic DB.
        if not await conn.fetchval("SELECT to_regclass('game') IS NOT NULL"):
            await conn.execute('''CREATE TABLE player(id bigint PRIMARY KEY,player_name text,joined bigint,deleted_at timestamptz);
              CREATE TABLE game_mode(id smallint PRIMARY KEY,name text);
              CREATE TABLE game(link bigint PRIMARY KEY,white_player_id bigint,black_player_id bigint,
                mode_id smallint,played_at timestamptz,white_result float,black_result float,
                white_str_result text,black_str_result text,n_moves int,fens_done bool);
              CREATE TABLE game_player(game_link bigint,player_id bigint,is_white bool);
              CREATE TABLE fen(fen_id bigint PRIMARY KEY,fen text,score integer,analysis_source text,tablebase_wdl int,tablebase_dtz int);
              CREATE TABLE game_fen_association(game_link bigint,fen_id bigint,n_move int,is_white bool);
              CREATE INDEX ON game_fen_association(game_link,n_move,is_white);
              CREATE TABLE moves(link bigint,n_move int,white_move text,black_move text,PRIMARY KEY(link,n_move));
              CREATE TABLE cloud_analysis_run(id text PRIMARY KEY,job_id text);
              CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY,run_id text);
              CREATE TABLE benchmark_report(label text PRIMARY KEY,result jsonb);
              INSERT INTO game_mode VALUES(1,'blitz');
              INSERT INTO player SELECT i,'synthetic-'||i,1,NULL FROM generate_series(1,20) i;
            ''')
            positions={};games=[];links=[];moves=[];owners=[]
            for id in range(1,501):
                board=chess.Board(); rng=random.Random(id); sans=[]
                for ply in range(1,81):
                    if board.is_game_over():break
                    move=rng.choice(list(board.legal_moves))
                    sans.append(board.san_and_push(move))
                    fen=board.fen().rsplit(' ',2)[0]
                    positions.setdefault(fen,len(positions)+1)
                    links.append((id,positions[fen],(ply+1)//2,ply%2==1))
                for i in range(0,len(sans),2):
                    moves.append((id,i//2+1,sans[i],sans[i+1] if i+1<len(sans) else None))
                white=1+(id%10);black=11+(id%10)
                games.append((id,white,black,1,1.0,0.0,'win','resigned',(len(sans)+1)//2,False))
                owners.extend([(id,white,True),(id,black,False)])
            await conn.copy_records_to_table('game',columns=['link','white_player_id','black_player_id','mode_id',
                'white_result','black_result','white_str_result','black_str_result','n_moves','fens_done'],records=games)
            await conn.copy_records_to_table('game_player',records=owners)
            await conn.copy_records_to_table('fen',records=[(id,fen,25,'stockfish',None,None) for fen,id in positions.items()])
            await conn.copy_records_to_table('game_fen_association',records=links)
            await conn.copy_records_to_table('moves',records=moves)
            await conn.execute('ANALYZE')
        async with factory() as s,s.begin():await schema.install(s,layout)
        await conn.execute('UPDATE graph_sync.control SET enabled=false')
        await conn.execute('UPDATE game SET fens_done=false')
        await conn.execute('TRUNCATE graph_sync.event')
        await conn.execute('UPDATE graph_sync.control SET enabled=true')
        for low in range(1,501,100):
            await conn.execute('UPDATE game SET fens_done=true WHERE link BETWEEN $1 AND $2',low,low+99)
        async with driver.session() as gs:
            while (await (await gs.run('MATCH (n) WITH n LIMIT 2000 DETACH DELETE n RETURN count(*) AS n')).single())['n']:
                pass
        await graph.install(driver,'neo4j')
        policy=await worker.sync_budget.from_server(driver,'neo4j') if hasattr(worker,'sync_budget') else None
        batch_sizes=[]; started=time.perf_counter()
        with patch.object(worker,'AsyncDBSession',factory),patch.object(worker,'configured_storage',return_value=layout):
            while await conn.fetchval('SELECT EXISTS(SELECT 1 FROM graph_sync.event WHERE completed_at IS NULL)'):
                before=await conn.fetchval('SELECT sum(offset_items) FROM graph_sync.event')
                ok=await worker.tick(driver,config,policy) if policy else await worker.tick(driver,config)
                if not ok:raise AssertionError(await conn.fetch('SELECT id,error FROM graph_sync.event WHERE error IS NOT NULL'))
                after=await conn.fetchval('SELECT sum(offset_items) FROM graph_sync.event')
                batch_sizes.append(after-before)
                print(json.dumps({'label':label,'committed_games':after,'batch_games':after-before}),flush=True)
        seconds=time.perf_counter()-started
        async with driver.session() as gs:
            counts=await graph.counts(gs)
            hashes=await (await gs.run('MATCH (g:Game) RETURN g.id AS id,g.source_hash AS hash ORDER BY id')).data()
            signature=hashlib.sha256(json.dumps(hashes,sort_keys=True).encode()).hexdigest()
            for query in [
                '''MATCH (p:Position) OPTIONAL MATCH (g:Game)-[r:AT_PLY]->(p)
                  WITH p,count(r) AS n,count(DISTINCT g) AS games
                  WHERE p.occurrences<>n OR p.games<>games RETURN count(p) AS n''',
                '''MATCH (a:Player)-[played:PLAYED]->(g:Game)-[v:AT_PLY]->(p:Position)
                  WITH a,p,played.color AS color,count(v) AS n,count(DISTINCT g) AS games
                  OPTIONAL MATCH (a)-[r:REACHED {color:color}]->(p)
                  WITH r,n,games WHERE r IS NULL OR r.occurrences<>n OR r.games<>games RETURN count(*) AS n''',
            ]:
                assert (await (await gs.run(query)).single())['n']==0
        assert counts['games']==500
        assert counts['occurrences']==await conn.fetchval('SELECT count(*) FROM game_fen_association')
        result={'seconds':seconds,'games_per_second':500/seconds,'batches':batch_sizes,
                'peak_worker_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,
                'counts':counts,'path_hash':signature}
        baseline_row=await conn.fetchrow("""SELECT label,result FROM benchmark_report
            WHERE label IN ('baseline_warm','baseline') AND label<>$1
            ORDER BY CASE WHEN label='baseline_warm' THEN 0 ELSE 1 END LIMIT 1""",label)
        if label not in ('baseline','baseline_warm') and baseline_row:
            baseline=json.loads(baseline_row['result'])
            assert baseline['counts']==counts and baseline['path_hash']==signature
            result['baseline_speedup']=baseline['seconds']/seconds
            result['compared_to']=baseline_row['label']
        await conn.execute('INSERT INTO benchmark_report VALUES($1,$2::jsonb) ON CONFLICT(label) DO UPDATE SET result=EXCLUDED.result',label,json.dumps(result))
        print(json.dumps({'label':label,**result}),flush=True)

        if policy:
            # Lose the SQL acknowledgment AFTER graph commit for several events.
            # Replay must preserve exact counters and acknowledgments, then finish.
            await conn.execute('TRUNCATE graph_sync.event')
            for low in (1,101,201):
                await conn.execute("SELECT graph_sync.enqueue('game',$1::bigint[])",list(range(low,low+100)))
            with patch.object(worker,'AsyncDBSession',factory),patch.object(worker,'configured_storage',return_value=layout):
                async def lose_ack(*args):raise RuntimeError('synthetic failure after graph commit')
                with patch.object(graph,'counts',lose_ack):
                    assert not await worker.tick(driver,config,policy)
                assert await conn.fetchval('SELECT sum(offset_items) FROM graph_sync.event')==0
                await conn.execute('UPDATE graph_sync.event SET retry_at=NULL')
                while await conn.fetchval('SELECT EXISTS(SELECT 1 FROM graph_sync.event WHERE completed_at IS NULL)'):
                    assert await worker.tick(driver,config,policy)
            async with driver.session() as gs:assert await graph.counts(gs)==counts
            print('PASS: multi-event checkpoint replay after graph commit preserves all counters',flush=True)
    finally:
        await conn.close();await engine.dispose();await driver.close()


if __name__=='__main__':
    logging.getLogger('neo4j.notifications').setLevel(logging.ERROR)
    asyncio.run(main())
