"""Retirement boundaries against disposable canonical PostgreSQL, never live data."""
import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, HTTPException
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations.research_algorithms import config, jobs, repository
from chessism_api.operations.research_algorithms import blunder_development_reuse as history
from chessism_api.operations.research_algorithms import blunder_players as players
from chessism_api.routers import research_algorithms as routes
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class ResearchRetirementTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def test_retired_routes_cannot_enqueue_and_current_routes_remain(self):
        app = FastAPI(); app.include_router(routes.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            with patch.object(routes, 'enqueue_run', AsyncMock()) as enqueue:
                for path in ('blunder-validation', 'blunder-shadow-pilot', 'blunder-threshold-comparison',
                             'blunder-cohort-summaries', 'blunder-strength-comparison'):
                    self.assertEqual((await client.post('/'+path, json={})).status_code, 404)
                enqueue.assert_not_awaited()
        paths = {r.path for r in routes.router.routes}
        self.assertTrue({'/blunder-training', '/blunder-training-v2', '/blunder-full-development',
                         '/blunder-final-evaluation', '/cuda-validation'} <= paths)

    async def test_saved_definition_replay_rejected_without_new_run_or_redis_submission(self):
        redis = AsyncMock()
        with patch.object(routes, 'AsyncDBSession', self.factory):
            for operation in sorted(config.RETIRED_OPERATIONS):
                identity = str(uuid.uuid4())
                await self.db.execute('INSERT INTO algorithm_definition(id,name,config) VALUES($1,$2,$3::jsonb)',
                    identity, operation, json.dumps({'operation': operation}))
                with self.assertRaises(HTTPException) as caught:
                    await routes.enqueue_run(uuid.UUID(identity), SimpleNamespace(id=None), redis)
                self.assertEqual(caught.exception.status_code, 409)
                self.assertIn('Saved reports remain available', caught.exception.detail)
        redis.enqueue_job.assert_not_awaited()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM algorithm_run'), 0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM algorithm_definition'), 5)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_stale_worker_tickets_fail_durably_before_source_files_or_calculation(self):
        with patch.object(jobs, 'AsyncDBSession', self.factory), patch.object(repository, 'AsyncDBSession', self.factory), \
             patch.object(jobs, 'materialize', AsyncMock()) as source, patch.object(jobs, 'workspace') as workspace, \
             patch.object(jobs, 'publish', AsyncMock()) as publish:
            for operation in sorted(config.RETIRED_OPERATIONS):
                identity = str(uuid.uuid4())
                await self.db.execute("INSERT INTO algorithm_run(id,name,config,status,progress,cancel_requested) VALUES($1,$2,$3::jsonb,'queued','{}',false)",
                    identity, operation, json.dumps({'operation': operation}))
                with self.assertRaisesRegex(ValueError, 'historical comparison'):
                    await jobs.run_algorithm_job({}, run_id=identity)
                row = await self.db.fetchrow('SELECT status,error,finished_at FROM algorithm_run WHERE id=$1', identity)
                self.assertEqual(row['status'], 'failed'); self.assertIsNotNone(row['finished_at'])
                self.assertIn('Saved reports remain available', row['error'])
                self.assertEqual((await jobs.run_algorithm_job({}, run_id=identity))['status'], 'not_queued')
        source.assert_not_awaited(); workspace.assert_not_called(); publish.assert_not_awaited()
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_saved_reports_and_exploration_reservations_are_preserved(self):
        identity = str(uuid.uuid4()); values = [11, 99, 2**63-1]
        report = {'format': 'chessism_blunder_strength_comparison_v1',
                  'source': {'inspected_game_ids': values, 'inspected_game_ids_sha256': history.digest(values)}}
        await self.db.execute("INSERT INTO algorithm_run(id,name,config,status,progress,cancel_requested,result) VALUES($1,'saved',$2::jsonb,'complete','{}',false,$3::jsonb)",
            identity, json.dumps({'operation': 'rating_blunder_strength_comparison_v1'}), json.dumps(report))
        before = await self.db.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', identity)
        with patch.object(routes, 'AsyncDBSession', self.factory), patch.object(repository, 'AsyncDBSession', self.factory):
            self.assertEqual((await routes.run_details(uuid.UUID(identity)))['result'], report)
            self.assertEqual((await repository.list_runs())['runs'][0]['id'], identity)
        async with read_transaction(self.factory) as session:
            self.assertEqual(await history.exploration_ids(session), set(values))
        self.assertEqual(await self.db.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', identity), before)
        await self.db.execute("UPDATE algorithm_run SET result=jsonb_set(result::jsonb,'{source,inspected_game_ids_sha256}','\"broken\"')")
        async with read_transaction(self.factory) as session:
            with self.assertRaisesRegex(ValueError, 'checksum'): await history.exploration_ids(session)

    async def test_current_training_inventory_requires_fresh_native_player_revisions(self):
        await self.db.execute("INSERT INTO player(player_name,joined) VALUES('alice',1),('opponent',0)")
        async with read_transaction(self.factory) as session:
            await players.prepare(session)
            with self.assertRaisesRegex(ValueError, 'summary refreshes'): await players.profiles(session)
        await self.db.execute('UPDATE player_stockfish_state SET published_revision=revision')
        async with read_transaction(self.factory) as session:
            rows = await players.profiles(session)
            self.assertEqual([r['player_name'] for r in rows], ['alice'])
            self.assertEqual(await players.current_revision(session, 'alice'), rows[0]['revision'])
            self.assertIsNone(await players.current_revision(session, 'opponent'))
        await self.db.execute("UPDATE player SET deleted_at=clock_timestamp() WHERE player_name='alice'")
        async with read_transaction(self.factory) as session:
            self.assertIsNone(await players.current_revision(session, 'alice'))
            with self.assertRaisesRegex(ValueError, 'Empty'): await players.profiles(session)
