import os
import json
import unittest
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import patch, AsyncMock, Mock
import chess
from neo4j import AsyncGraphDatabase
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.database.postgresql.relational_storage import RelationalStorage
from chessism_api.database.neo4j import sync_schema, sync_source, sync_graph, sync_worker, sync_manage
from chessism_api.database.neo4j.sync_backup import publication_barrier
from chessism_api.database.neo4j.sync_cloud import cloud_progress


class CodecTests(unittest.TestCase):
    def test_scores_are_typed_and_unknown_is_not_inferred(self):
        self.assertEqual(sync_source.evaluation({'score':125,'analysis_source':'stockfish'})['score_cp'],125)
        self.assertIsNone(sync_source.evaluation({'score':125})['score_cp'])
        self.assertEqual(sync_source.evaluation({'score':-9996,'analysis_source':'stockfish'})['mate_in'],4)
        self.assertIsNone(sync_source.evaluation({'score':9999,'analysis_source':'tablebase'})['mate_in'])

    def test_tablebase_values_use_white_perspective(self):
        row={'score':-1000,'analysis_source':'tablebase','fen':'8/8/8/8/8/4k3/8/4K2r b - -',
             'tablebase_wdl':2,'tablebase_dtz':17}
        result=sync_source.evaluation(row)
        self.assertEqual(result['tablebase_wdl'],-2)
        self.assertEqual(result['tablebase_dtz'],-17)
        self.assertIsNone(result['score_cp'])

    def test_repetition_counts_are_not_distinct_games(self):
        step={'key':'a','from_key':'b','move_key':'x','uci':'a1a2','san':'Ra2'}
        p,r,m=sync_graph.contributions([{'white_id':1,'black_id':2,'steps':[step,step]}])
        self.assertEqual(p['a','occurrences'],2)
        self.assertEqual(p['a','games'],1)
        deleted,_,_=sync_graph.delta_rows([{'white_id':1,'black_id':2,'steps':[step,step]}],[])
        self.assertEqual(deleted[0]['occurrences'],-2)


class BackupBarrierTests(unittest.IsolatedAsyncioTestCase):
    async def test_early_release_is_idempotent(self):
        session=AsyncMock();factory=Mock(return_value=session)
        session.__aenter__.return_value=session
        with patch('chessism_api.database.neo4j.sync_backup.AsyncDBSession',factory):
            async with publication_barrier() as release:
                self.assertIn('pg_advisory_xact_lock',str(session.execute.call_args.args[0]))
                await release();await release()
            session.rollback.assert_awaited_once()

    async def test_failure_releases_publication(self):
        session=AsyncMock();session.__aenter__.return_value=session
        with patch('chessism_api.database.neo4j.sync_backup.AsyncDBSession',Mock(return_value=session)):
            with self.assertRaisesRegex(RuntimeError,'failed dump'):
                async with publication_barrier():
                    raise RuntimeError('failed dump')
            session.rollback.assert_awaited_once()


@unittest.skipUnless(os.getenv('GRAPH_SYNC_TEST_DB'), 'requires disposable PostgreSQL and Neo4j')
class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_outbox_paths_replay_scores_updates_deletion_and_backup_barrier(self):
        engine=create_async_engine(os.environ['GRAPH_SYNC_TEST_DB'])
        factory=async_sessionmaker(engine,expire_on_commit=False)
        layout=RelationalStorage('public','archive','reads','relational_v3')
        driver=AsyncGraphDatabase.driver(os.environ['GRAPH_SYNC_TEST_URI'],auth=None,telemetry_disabled=True)
        config=SimpleNamespace(database='neo4j')
        try:
            async with engine.begin() as c:
                raw=(await c.get_raw_connection()).driver_connection
                await raw.execute('''CREATE TABLE player(id bigint PRIMARY KEY,player_name text,joined bigint,deleted_at timestamptz);
                    CREATE TABLE game_mode(id smallint PRIMARY KEY,name text);
                    CREATE TABLE game(link bigint PRIMARY KEY,white_player_id bigint,black_player_id bigint,
                      mode_id smallint,played_at timestamptz,white_result float,black_result float,
                      white_str_result text,black_str_result text,n_moves int,fens_done bool);
                    CREATE TABLE game_player(id bigint GENERATED ALWAYS AS IDENTITY,game_link bigint,player_id bigint,is_white bool);
                    CREATE TABLE fen(fen_id bigint PRIMARY KEY,fen text,score float,analysis_source text,tablebase_wdl int,tablebase_dtz int);
                    CREATE TABLE game_fen_association(id bigint GENERATED ALWAYS AS IDENTITY,game_link bigint,fen_id bigint,n_move int,is_white bool);
                    CREATE TABLE moves(link bigint,n_move int,white_move text,black_move text);
                    CREATE TABLE cloud_analysis_run(id text PRIMARY KEY,job_id text);
                    CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY,run_id text);
                    INSERT INTO player VALUES(1,'lafareto',1,NULL),(2,'opponent',1,NULL);
                    INSERT INTO game_mode VALUES(1,'blitz');''')
            async with factory() as s,s.begin():
                await sync_schema.install(s,layout)
                await s.execute(text('UPDATE graph_sync.control SET enabled=true'))
            await sync_graph.install(driver,'neo4j')
            # Repeating positions and moves must survive a game path, but share nodes.
            sans=['Nf3','Nf6','Ng1','Ng8','Nf3','Nf6','e4','e5']
            board=chess.Board(); fens={}; saved=[]
            for san in sans:
                board.push_san(san); fen=board.fen().rsplit(' ',2)[0]
                fens.setdefault(fen,len(fens)+1); saved.append(fens[fen])
            async with factory() as s,s.begin():
                for fen,id in fens.items():
                    await s.execute(text("INSERT INTO fen VALUES(:id,:fen,35,'stockfish',NULL,NULL)"),{'id':id,'fen':fen})
                for game in (1,2):
                    await s.execute(text("INSERT INTO game VALUES(:id,1,2,1,now(),1,0,'win','resigned',4,false)"),{'id':game})
                    await s.execute(text('INSERT INTO game_player(game_link,player_id,is_white) VALUES(:id,1,true),(:id,2,false)'),{'id':game})
                    for i in range(4):
                        await s.execute(text('INSERT INTO moves VALUES(:id,:n,:w,:b)'),{'id':game,'n':i+1,'w':sans[i*2],'b':sans[i*2+1]})
                    for i,id in enumerate(saved):
                        await s.execute(text('INSERT INTO game_fen_association(game_link,fen_id,n_move,is_white) VALUES(:game,:fen,:n,:white)'),
                            {'game':game,'fen':id,'n':i//2+1,'white':i%2==0})
                await s.execute(text('UPDATE game SET fens_done=true'))
            with patch.object(sync_worker,'AsyncDBSession',factory),patch.object(sync_worker,'configured_storage',return_value=layout),patch.object(sync_manage,'configured_storage',return_value=layout):
                with patch.object(sync_worker.LOG, 'info') as log:
                    self.assertTrue(await sync_worker.tick(driver,config))
                metrics = json.loads(log.call_args.args[1])
                self.assertTrue({'positions', 'game_paths', 'player_positions', 'position_moves'} <= metrics['query_seconds'].keys())
                self.assertTrue(all(value >= 0 for value in metrics['query_seconds'].values()))
                async with driver.session() as gs:
                    initial=await sync_graph.counts(gs)
                    self.assertEqual(initial['games'],2);self.assertEqual(initial['occurrences'],16)
                    self.assertEqual(initial['positions'],len(fens))
                async with factory() as s,s.begin():
                    batch=await sync_manage.backfill(s,'lafareto',2)
                # Replaying committed graph work before acknowledging its SQL event is harmless.
                self.assertTrue(await sync_worker.tick(driver,config))
                async with factory() as s,s.begin():
                    report=await sync_manage.verify(s,driver,config,batch['backfill_id'])
                    self.assertEqual(report['verified_games'],2)
                async with driver.session() as gs:
                    self.assertEqual(await sync_graph.counts(gs),initial)
                async with factory() as s:
                    await s.execute(text('UPDATE fen SET score=9996 WHERE fen_id=1'))
                    await s.rollback()
                self.assertFalse(await sync_worker.tick(driver,config))
                async with factory() as s,s.begin():
                    await s.execute(text('UPDATE fen SET score=9996 WHERE fen_id=1'))
                async with factory() as barrier,barrier.begin():
                    await barrier.execute(text('SELECT pg_advisory_xact_lock(:key)'),{'key':sync_schema.LOCK})
                    self.assertFalse(await sync_worker.tick(driver,config))
                    async with factory() as observer:
                        control = (await observer.execute(text('SELECT phase,detail,heartbeat FROM graph_sync.control WHERE id'))).mappings().one()
                        self.assertEqual(control['phase'], 'waiting')
                        self.assertIn('backup snapshot', control['detail'])
                        self.assertIsNotNone(control['heartbeat'])
                self.assertTrue(await sync_worker.tick(driver,config))
                self.assertTrue(await sync_worker.tick(driver,config))  # Apply after durable score classification.
                async with driver.session() as gs:
                    row=await (await gs.run('MATCH (p:Position {fen_id:1}) RETURN p.score_cp AS cp,p.mate_in AS mate')).single()
                    self.assertIsNone(row['cp']);self.assertEqual(row['mate'],4)
                async with factory() as s,s.begin():
                    await s.execute(text("UPDATE game SET white_str_result='checkmate' WHERE link=1"))
                self.assertTrue(await sync_worker.tick(driver,config))
                # Ownership is captured before claims are deleted. A FEN absent
                # from the graph is excluded, and jobs never share counters.
                jobs=[SimpleNamespace(id=id,status='complete',imported=1,target=1,created_at=datetime.now(timezone.utc))
                      for id in ('cloud-a','cloud-b')]
                async with factory() as s,s.begin():
                    await s.execute(text("INSERT INTO fen VALUES(99,'not-in-graph',NULL,NULL,NULL,NULL)"))
                    await s.execute(text("INSERT INTO cloud_analysis_run VALUES('run-a','cloud-a'),('run-b','cloud-b')"))
                    await s.execute(text("INSERT INTO cloud_fen_claim SELECT fen,CASE WHEN fen_id=1 THEN 'run-a' ELSE 'run-b' END FROM fen WHERE fen_id IN (1,99)"))
                    await s.execute(text("UPDATE fen SET score=12,analysis_source='stockfish' WHERE fen_id IN (1,99)"))
                    await s.execute(text('DELETE FROM cloud_fen_claim'))
                self.assertTrue(await sync_worker.tick(driver,config))
                async with factory() as s:
                    state=await cloud_progress(s,jobs)
                    self.assertEqual((state['cloud-a']['total'],state['cloud-a']['processed']),(1,0))
                    self.assertIsNone(state['cloud-b']['total'])
                self.assertTrue(await sync_worker.tick(driver,config))
                self.assertTrue(await sync_worker.tick(driver,config))
                async with factory() as s:
                    state=await cloud_progress(s,jobs)
                    self.assertEqual((state['cloud-a']['total'],state['cloud-a']['processed']),(1,1))
                    self.assertEqual((state['cloud-b']['total'],state['cloud-b']['processed']),(0,0))
                    self.assertEqual(state['cloud-b']['status'],'complete')
                async with driver.session() as gs:
                    self.assertEqual(await sync_graph.counts(gs),initial)
                async with factory() as s,s.begin():
                    await s.execute(text('DELETE FROM game WHERE link=2'))
                self.assertTrue(await sync_worker.tick(driver,config))
                async with driver.session() as gs:
                    final=await sync_graph.counts(gs)
                    self.assertEqual(final['games'],1);self.assertEqual(final['occurrences'],8)
                # Larger batches may span several events. They must not cross
                # score events; failures must roll back every acknowledgement.
                async with factory() as s,s.begin():
                    await s.execute(text("SELECT graph_sync.enqueue('game',ARRAY[1]::bigint[])"))
                    await s.execute(text("SELECT graph_sync.enqueue('game',ARRAY[1]::bigint[])"))
                    await s.execute(text("SELECT graph_sync.enqueue('score',ARRAY[1]::bigint[])"))
                    await s.execute(text("SELECT graph_sync.enqueue('game',ARRAY[1]::bigint[])"))
                self.assertTrue(await sync_worker.tick(driver,config))
                async with factory() as s:
                    pending=(await s.execute(text('SELECT kind FROM graph_sync.event WHERE completed_at IS NULL ORDER BY id'))).scalars().all()
                    self.assertEqual(pending,['score','game'])
                self.assertTrue(await sync_worker.tick(driver,config)) # classify score
                self.assertTrue(await sync_worker.tick(driver,config)) # publish score
                self.assertTrue(await sync_worker.tick(driver,config)) # following game
                # Validate underflow in the write statement itself, before any
                # of that graph transaction's earlier changes may be committed.
                async with driver.session() as gs:
                    game=await (await gs.run('MATCH (g:Game {id:1}) RETURN g.source_hash AS hash')).single()
                    relation=await (await gs.run('''MATCH (:Player)-[r:REACHED]->(:Position {fen_id:1})
                        RETURN elementId(r) AS id,r.occurrences AS occurrences LIMIT 1''')).single()
                    await (await gs.run('MATCH ()-[r]->() WHERE elementId(r)=$id SET r.occurrences=0',id=relation['id'])).consume()
                    with self.assertRaisesRegex(ValueError,'underflow'):
                        await gs.execute_write(sync_graph.publish,[],[],[1])
                    still=await (await gs.run('MATCH (g:Game {id:1}) RETURN g.source_hash AS hash')).single()
                    self.assertEqual(still['hash'],game['hash'])
                    self.assertEqual(await sync_graph.counts(gs),final)
                    await (await gs.run('MATCH ()-[r]->() WHERE elementId(r)=$id SET r.occurrences=$n',
                        id=relation['id'],n=relation['occurrences'])).consume()
                # Exercise the durable retry SQL, including its interval expression.
                async with factory() as s,s.begin():
                    await s.execute(text("SELECT graph_sync.enqueue('game',ARRAY[1]::bigint[])"))
                async def fail(*args):
                    raise RuntimeError('synthetic temporary failure')
                with patch.object(sync_graph,'publish',new=fail):
                    self.assertFalse(await sync_worker.tick(driver,config))
                async with factory() as s,s.begin():
                    self.assertEqual((await sync_schema.status(s))['failed_events'],1)
                    await s.execute(text('UPDATE graph_sync.event SET retry_at=NULL WHERE completed_at IS NULL'))
                self.assertTrue(await sync_worker.tick(driver,config))
                # A shortened path has old-only nodes: subtract counts without
                # erasing their evaluations, and remove zero-count edges. The
                # replay must not subtract twice.
                async with driver.session() as gs:
                    before_node = dict(await (await gs.run('''MATCH (p:Position {fen_id:$id})
                        RETURN p.fen AS fen,p.score_kind AS kind,p.score_cp AS cp,
                               p.occurrences AS occurrences''', id=saved[-1])).single())
                async with factory() as s,s.begin():
                    await s.execute(text('DELETE FROM moves WHERE link=1 AND n_move=4'))
                    await s.execute(text('DELETE FROM game_fen_association WHERE game_link=1 AND n_move=4'))
                    await s.execute(text('UPDATE game SET n_moves=3 WHERE link=1'))
                self.assertTrue(await sync_worker.tick(driver,config))
                async with driver.session() as gs:
                    node = dict(await (await gs.run('''MATCH (p:Position {fen_id:$id})
                        RETURN p.fen AS fen,p.score_kind AS kind,p.score_cp AS cp,
                               p.occurrences AS occurrences''', id=saved[-1])).single())
                    self.assertEqual(node, {**before_node, 'occurrences':0})
                    self.assertEqual((await sync_graph.counts(gs))['occurrences'],6)
                    self.assertEqual((await (await gs.run('''MATCH ()-[r:REACHED|MOVE]->()
                        WHERE r.occurrences=0 RETURN count(r) AS n''')).single())['n'],0)
                async with factory() as s,s.begin():
                    await s.execute(text("SELECT graph_sync.enqueue('game',ARRAY[1]::bigint[])"))
                self.assertTrue(await sync_worker.tick(driver,config))
                async with driver.session() as gs:
                    self.assertEqual((await sync_graph.counts(gs))['occurrences'],6)
        finally:
            await driver.close();await engine.dispose()


if __name__=='__main__':
    unittest.main()
