"""Native administration and graph outbox checks; never connect to Neo4j."""
import asyncio
import json
from collections import Counter
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.database.neo4j import sync_source, sync_ingestion, sync_manage, ply_map_store
from chessism_api.operations import player_deletion as deletion
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests import check_canonical_cuda_postgres as cuda_fixture


class CanonicalAdminGraphTests(CanonicalDatabaseTestCase):
    add_game=cuda_fixture.CanonicalCudaTests.add_game

    async def asyncSetUp(self):
        await super().asyncSetUp();await canonical.initialize(self.db)
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database=self.name,
            port=55432,query={'host':self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        self.people={p:await self.db.fetchval('INSERT INTO player(player_name,joined) VALUES($1,1) RETURNING id',p) for p in ('alice','bob')}
        self.mode=await self.db.fetchval("INSERT INTO game_mode(name) VALUES('blitz') RETURNING id")
        self.fens={}
        await self.add_game(1,'bob');await self.add_game(2,'alice')
        await self.add_game(3,'bob',moves=0);await self.add_game(4,'bob',rules='chess960')
        await self.db.execute('UPDATE fen SET n_games=(SELECT count(*) FROM game_fen_association a WHERE a.fen_id=fen.fen_id)')

    async def test_disabled_outbox_and_explicit_activation_capture_native_paths(self):
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM graph_sync.event'),0)
        await self.db.execute('UPDATE graph_sync.control SET enabled=true')
        await self.db.execute('UPDATE game SET white_elo=white_elo+1')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM graph_sync.event'),0)
        await self.db.execute("UPDATE game SET white_str_result='win updated' WHERE link=1")
        self.assertEqual(await self.db.fetchval('SELECT ids FROM graph_sync.event'),[1])
        async with read_transaction(self.factory) as session:
            games,positions,removed=await sync_source.read_games(session,[1,2,3,4,99])
        self.assertEqual([g['id'] for g in games],[1,2]);self.assertEqual(removed,[3,4,99])
        self.assertEqual([len(g['steps']) for g in games],[22,22])
        self.assertEqual(len(positions),4)
        self.assertTrue(all(len(g['source_hash'])==64 for g in games))

    async def test_outbox_and_map_revision_roll_back_with_source(self):
        await self.db.execute('UPDATE graph_sync.control SET enabled=true')
        revision=await self.db.fetchval('SELECT revision FROM graph_map.control')
        transaction=self.db.transaction();await transaction.start()
        await self.db.execute("UPDATE game SET white_str_result='changed' WHERE link=1")
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM graph_sync.event'),1)
        self.assertGreater(await self.db.fetchval('SELECT revision FROM graph_map.control'),revision)
        await transaction.rollback()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM graph_sync.event'),0)
        self.assertEqual(await self.db.fetchval('SELECT revision FROM graph_map.control'),revision)
        await self.db.execute('UPDATE fen SET score=score+1')
        self.assertEqual(await self.db.fetchval('SELECT revision FROM graph_map.control'),revision)
        self.assertEqual(await self.db.fetchval('SELECT kind FROM graph_sync.event'),'score')

    async def test_counter_updates_do_not_wait_for_cloud_claim_or_outbox_locks(self):
        await self.db.execute('UPDATE graph_sync.control SET enabled=true')
        await self.db.execute("INSERT INTO fen(fen) SELECT 'noop-'||i||' w - -' FROM generate_series(1,30000) i")
        blocker=await self.connect()
        async with blocker.transaction():
            await blocker.execute('LOCK cloud_fen_claim,cloud_analysis_run,graph_sync.event IN ACCESS EXCLUSIVE MODE')
            await self.db.execute("SET lock_timeout='200ms'")
            for _ in range(8):await self.db.execute("INSERT INTO fen(fen) VALUES('noop-1 w - -') ON CONFLICT(fen) DO UPDATE SET n_games=fen.n_games WHERE false")
            await self.db.execute('UPDATE fen SET n_games=n_games+1')
            await self.db.execute('UPDATE fen SET score=score,analysis_source=analysis_source')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM graph_sync.event'),0)

    async def test_backfill_unique_self_play_games_and_progress(self):
        await self.db.execute('UPDATE graph_sync.control SET enabled=true')
        async with write_transaction(self.factory) as session:result=await sync_manage.backfill(session,'alice',2)
        self.assertEqual(result['games'],2)
        self.assertEqual(await self.db.fetchval('SELECT game_ids FROM graph_sync.backfill'),[1,2])
        with self.assertRaisesRegex(ValueError,'Only 2 complete'):
            async with write_transaction(self.factory) as session:await sync_manage.backfill(session,'alice',3)
        start=await self.db.fetchval("SELECT clock_timestamp()-interval '1 minute'")
        run=SimpleNamespace(player_name='alice',status='complete',started_at=start,finished_at=None)
        await self.db.execute("INSERT INTO graph_sync.event(kind,ids,offset_items) VALUES('game',ARRAY[1,2],1)")
        with patch.object(sync_ingestion,'AsyncDBSession',self.factory):report=await sync_ingestion.ingestion_progress(run)
        self.assertEqual((report['processed'],report['total']),(1,2))

    async def test_disabled_foreign_key_trigger_and_graph_catalog_drift_rejected(self):
        transaction=self.db.transaction();await transaction.start()
        await self.db.execute('ALTER TABLE game DISABLE TRIGGER ALL')
        with self.assertRaises(canonical.SchemaMismatch):await canonical.validate(self.db)
        await transaction.rollback()
        self.assertEqual(await canonical.validate(self.db),canonical.VERSION)
        await self.db.execute('ALTER TABLE graph_sync.event ALTER COLUMN offset_items DROP NOT NULL')
        with self.assertRaises(canonical.SchemaMismatch):await canonical.validate(self.db)

    async def test_deletion_preserves_shared_games_and_all_evaluations(self):
        await self.db.execute('UPDATE player SET joined=0 WHERE player_name=\'bob\'')
        before=await self.db.fetch('SELECT fen_id,score,analysis_source,analyzed_at FROM fen ORDER BY fen_id')
        with patch.object(deletion,'AsyncDBSession',self.factory):
            preview=await deletion.get_player_deletion_preview('alice')
            self.assertEqual((preview['exclusive_games'],preview['shared_games']),(3,1))
            self.assertEqual(await deletion._delete_exclusive_game_batch('alice',[1,2,3,4]),3)
            self.assertEqual(await deletion._delete_exclusive_game_batch('alice',[1,3,4]),0)
            await deletion._demote_player('alice')
        self.assertEqual(await self.db.fetch('SELECT fen_id,score,analysis_source,analyzed_at FROM fen ORDER BY fen_id'),before)
        self.assertEqual(await self.db.fetchval('SELECT array_agg(link) FROM game'),[2])
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'),22)
        self.assertEqual(await self.db.fetchval("SELECT joined FROM player WHERE player_name='alice'"),0)

    async def test_locked_deletion_skips_without_decrement_or_partial_mutation(self):
        await self.db.execute("UPDATE player SET joined=0 WHERE player_name='bob'")
        before=await self.db.fetchval('SELECT sum(n_games) FROM fen')
        blocker=await self.connect()
        async with blocker.transaction():
            await blocker.fetch('SELECT link FROM game WHERE link=1 FOR UPDATE')
            with patch.object(deletion,'AsyncDBSession',self.factory):self.assertEqual(await deletion._delete_exclusive_game_batch('alice',[1]),0)
        self.assertEqual(await self.db.fetchval('SELECT sum(n_games) FROM fen'),before)
        self.assertTrue(await self.db.fetchval('SELECT EXISTS(SELECT 1 FROM game WHERE link=1)'))
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_saved_map_publication_invalidation_retention_and_recovery(self):
        from tests.check_ply_map_postgres import exercise
        await exercise(self.factory)

    async def test_score_chunking_and_cloud_attribution_precede_claim_deletion(self):
        await self.db.execute("""INSERT INTO cloud_analysis_job(id,status,target,imported)
            VALUES('job-a','running',1100,0),('job-b','running',1100,0);
            INSERT INTO cloud_analysis_run(id,job_id,status) VALUES('run-a','job-a','running'),('run-b','job-b','running');
            INSERT INTO fen(fen) SELECT 'position-'||i||' w - -' FROM generate_series(1,2300) i;
            INSERT INTO cloud_fen_claim(fen,run_id)
            SELECT 'position-'||i||' w - -',CASE WHEN i<=1100 THEN 'run-a' ELSE 'run-b' END
            FROM generate_series(1,2200) i;
            UPDATE graph_sync.control SET enabled=true;""")
        async with self.db.transaction():
            await self.db.execute("UPDATE fen SET score=20,analysis_source='stockfish',analyzed_at=now() WHERE fen LIKE 'position-%'")
            await self.db.execute('DELETE FROM cloud_fen_claim')
        events=await self.db.fetch('SELECT * FROM graph_sync.event ORDER BY id')
        totals=Counter()
        for event in events:
            self.assertEqual(event['kind'],'score');self.assertTrue(1<=len(event['ids'])<=1000)
            self.assertEqual(event['ids'],sorted(event['ids']));totals[event['source_job_id']]+=len(event['ids'])
        self.assertEqual(totals,{'job-a':1100,'job-b':1100,None:100})
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'),0)

    async def test_mixed_progress_keeps_duplicates_offsets_ownership_and_retry_state(self):
        from datetime import timedelta
        await self.db.execute('UPDATE graph_sync.control SET enabled=true')
        now=await self.db.fetchval('SELECT clock_timestamp()')
        run=SimpleNamespace(player_name='alice',status='complete',started_at=now-timedelta(minutes=10),finished_at=now)
        async def event(kind,ids,offset=0,done=False,error=None,at=None):
            return await self.db.fetchval("""INSERT INTO graph_sync.event(kind,ids,offset_items,completed_at,error,created_at)
                VALUES($1,$2,$3,$4,$5,$6) RETURNING id""",kind,ids,offset,now if done else None,error,at or now-timedelta(minutes=5))
        fens=list(self.fens.values())
        await event('game',[1,99,2],offset=2)
        duplicate=await event('game',[1],error='temporary failure')
        await event('score',[fens[0],fens[1],999999],offset=1)
        await event('player',list(self.people.values()),done=True)
        await event('game',[2],at=run.started_at-timedelta(seconds=1))
        await event('game',[2],at=run.finished_at+timedelta(seconds=1))
        with patch.object(sync_ingestion,'AsyncDBSession',self.factory):
            row=await sync_ingestion.ingestion_progress(run)
            self.assertEqual((row['processed'],row['total'],row['status']),(2,5,'retrying'))
            await self.db.execute('UPDATE graph_sync.event SET completed_at=$1,error=NULL WHERE id=$2',now,duplicate)
            self.assertEqual((await sync_ingestion.ingestion_progress(run))['processed'],3)
            await self.db.execute('UPDATE graph_sync.event SET completed_at=$1,error=NULL',now)
            row=await sync_ingestion.ingestion_progress(run)
            self.assertEqual((row['processed'],row['total'],row['status']),(5,5,'complete'))
            run.status='running';run.finished_at=None
            self.assertEqual((await sync_ingestion.ingestion_progress(run))['status'],'waiting')
            await self.db.execute('UPDATE graph_sync.control SET enabled=false')
            self.assertIsNone(await sync_ingestion.ingestion_progress(run))

    async def test_cancelled_graph_request_retains_outbox_and_releases_transaction(self):
        from unittest.mock import MagicMock
        from chessism_api.database.neo4j import sync_worker
        await self.db.execute('UPDATE graph_sync.control SET enabled=true')
        fen=next(iter(self.fens.values()))
        event_id=await self.db.fetchval("INSERT INTO graph_sync.event(kind,ids,matched_ids) VALUES('score',ARRAY[$1::bigint],ARRAY[$1::bigint]) RETURNING id",fen)
        entered=asyncio.Event()
        async def blocked(*args,**kwargs):entered.set();await asyncio.Event().wait()
        driver=MagicMock();graph=AsyncMock();graph.run.side_effect=blocked
        driver.session.return_value.__aenter__.return_value=graph
        with patch.object(sync_worker,'AsyncDBSession',self.factory):
            task=asyncio.create_task(sync_worker.tick(driver,SimpleNamespace(database='synthetic')))
            await asyncio.wait_for(entered.wait(),3);task.cancel();task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        event=await self.db.fetchrow('SELECT * FROM graph_sync.event WHERE id=$1',event_id)
        self.assertIsNone(event['completed_at']);self.assertEqual(event['offset_items'],0)
        async with self.db.transaction():
            self.assertEqual(await self.db.fetchval('SELECT id FROM graph_sync.event WHERE id=$1 FOR UPDATE NOWAIT',event_id),event_id)
        self.assertEqual(self.engine.pool.checkedout(),0)
