"""Existing coefficient and salience research contracts on private PostgreSQL."""
import asyncio
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import asyncpg
from sqlalchemy.exc import DBAPIError

from chessism_api.operations import coefficient_research as coefficients
from chessism_api.operations.salience_comparison import build, artifacts, readers
from chessism_api.routers import research as api
from tests.check_canonical_cuda_postgres import CanonicalCudaTests


class CanonicalAuxiliaryResearchTests(CanonicalCudaTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.folder=Path(self.temp.name)
        changes=(patch.object(coefficients,'AsyncDBSession',self.factory),
            patch.object(api,'AsyncDBSession',self.factory),
            patch.object(coefficients,'RESEARCH_ARTIFACT_DIR',self.folder),
            patch.object(coefficients,'RESEARCH_ARTIFACT_DISPLAY_DIR',self.folder),
            patch.object(api,'analysis_jobs_active',AsyncMock(return_value=False)),
            patch.object(build,'AsyncDBSession',self.factory),
            patch.object(build.shutil,'disk_usage',return_value=SimpleNamespace(free=5_000_000_000)),
            patch.object(build,'bind_runtime_engine',AsyncMock(return_value=self.engine)),
            patch.dict('os.environ',{'SALIENCE_COMPARISON_DIR':self.temp.name,'DATABASE_URL':'postgresql://unused'}))
        for change in changes:change.start();self.addCleanup(change.stop)
        self.redis=SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace(job_id='test-job')),
            get=AsyncMock(return_value=None),set=AsyncMock())

    async def create(self):
        return await api.create_coefficient_experiment(api.CoefficientExperimentConfig(),
            SimpleNamespace(id=None),self.redis)

    async def test_coefficient_selection_uses_unique_games_weights_and_exact_scores(self):
        config=coefficients.normalized_config(None)
        overview=await coefficients.get_coefficient_research_overview(config)
        rows=await coefficients._load_aggregated_observations(config)
        self.assertEqual(overview['totals'],{'games':2,'candidate_positions':44})
        self.assertEqual(sum(r['positions'] for r in rows),4)
        self.assertAlmostEqual(sum(r['weight'] for r in rows),2)
        self.assertTrue(all(r['rating_bin']=='1400-1599' and r['mode']=='blitz' for r in rows))
        self.assertEqual(rows,await coefficients._load_aggregated_observations(config))
        # NULL provenance remains excluded; integer extrema are filtered without ABS overflow.
        await self.db.execute('UPDATE fen SET score=-2147483648')
        self.assertEqual(await coefficients._load_aggregated_observations(config),[])
        await self.db.execute('UPDATE fen SET score=20,analysis_source=NULL,analyzed_at=NULL')
        self.assertEqual(await coefficients._load_aggregated_observations(config),[])
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_coefficient_api_job_artifact_and_research_decision_round_trip(self):
        original=await self.db.fetch('SELECT * FROM fen ORDER BY fen_id')
        queued=await self.create();identifier=queued['experiment_id']
        self.redis.enqueue_job.assert_awaited_once()
        result=await coefficients.run_chessism_coefficient_experiment({'redis':self.redis},identifier)
        self.assertEqual(result['dataset']['eligible_games'],2)
        record=await api.coefficient_experiment(identifier,self.redis)
        self.assertEqual(record['status'],'complete');self.assertEqual(record['job_id'],'test-job')
        self.assertEqual(record['result']['source_contract_version'],2)
        artifact=json.loads(next(self.folder.glob('*.json')).read_text())
        self.assertEqual(artifact['dataset_summary'],result['dataset'])
        decision=await api.decide_coefficient_experiment(identifier,
            api.CoefficientDecisionRequest(action='keep_lichess',strategy='global',notes=' Reviewed '))
        self.assertEqual(decision['decision_notes'],'Reviewed')
        self.assertFalse(decision['decision_config']['production_changed'])
        self.assertEqual((await coefficients.list_coefficient_experiments())[0]['id'],identifier)
        self.assertEqual(await self.db.fetch('SELECT * FROM fen ORDER BY fen_id'),original)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_coefficient_query_failure_and_cancellation_propagate(self):
        queued=await self.create();identifier=queued['experiment_id']
        await self.db.execute('ALTER TABLE game_analysis_coverage RENAME TO broken_test_coverage')
        with self.assertRaises(DBAPIError):
            await coefficients.run_chessism_coefficient_experiment({},identifier)
        record=await api.coefficient_experiment(identifier,self.redis)
        self.assertEqual(record['status'],'failed');self.assertIn('game_analysis_coverage',record['error'])
        self.assertFalse(list(self.folder.glob('*.json')))
        self.assertEqual(self.engine.pool.checkedout(),0)
        queued=await self.create()
        with patch.object(coefficients,'_load_aggregated_observations',AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await coefficients.run_chessism_coefficient_experiment({},queued['experiment_id'])
        self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertEqual((await api.coefficient_experiment(identifier,self.redis))['status'],'failed')

    async def test_coefficient_schema_enforces_existing_states_decisions_and_owner_reference(self):
        for column,value,error in (('status','invalid',asyncpg.CheckViolationError),
            ('decision','invalid',asyncpg.CheckViolationError),('created_by','absent',asyncpg.ForeignKeyViolationError)):
            identifier=(await self.create())['experiment_id']
            with self.assertRaises(error):
                await self.db.execute(f'UPDATE coefficient_research_experiment SET {column}=$1 WHERE id=$2',value,identifier)
            await self.db.execute("UPDATE coefficient_research_experiment SET status='complete' WHERE id=$1",identifier)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM coefficient_research_experiment'),3)

    async def test_salience_snapshot_reads_native_metadata_positions_and_moves_without_writes(self):
        before=await self.db.fetch('SELECT * FROM player_salience_summary ORDER BY player_id')
        identifier=await build.build(' BOB ')
        summary=artifacts.overview(identifier)
        self.assertEqual((summary['games'],summary['positions']),(1,22))
        self.assertEqual(summary['source_salience_status'],'stale')
        detail=readers.game_detail(identifier,1)
        self.assertEqual(detail['game']['white']['name'],'alice')
        self.assertEqual(detail['game']['black']['name'],'bob')
        self.assertEqual(len(detail['moves']),22)
        self.assertEqual(len(detail['comparison']['positions']),22)
        self.assertEqual(readers.games(identifier)['games'][0]['is_white'],0)
        self.assertEqual(await self.db.fetch('SELECT * FROM player_salience_summary ORDER BY player_id'),before)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_salience_preserves_report_limitations_and_hides_incomplete_files(self):
        with patch.object(build.shutil,'disk_usage',return_value=SimpleNamespace(free=1000)):
            with self.assertRaisesRegex(RuntimeError,'5 GB'):await build.build('bob')
        # The existing SQLite/detail contract has one row per game, not per side.
        with self.assertRaisesRegex(ValueError,'self-play'):await build.build('alice')
        self.assertEqual(artifacts.listing(),{'comparisons':[]})
        self.assertTrue(list(self.folder.glob('*.building')))
        def stop(_db):
            self.assertEqual(self.engine.pool.checkedout(),0)
            raise RuntimeError('synthetic compute failure')
        with patch.object(build,'compute',side_effect=stop):
            with self.assertRaisesRegex(RuntimeError,'synthetic'):await build.build('bob')
        self.assertEqual(artifacts.listing(),{'comparisons':[]})
        self.assertEqual(self.engine.pool.checkedout(),0)
