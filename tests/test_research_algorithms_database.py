"""Opt-in integration tests: all writes are to connection-local TEMP tables."""

from contextlib import asynccontextmanager
from pathlib import Path
import os
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from chessism_api.database.models import AlgorithmRun
from chessism_api.operations.research_algorithms import config, inputs, jobs, storage
from chessism_api.operations.research_algorithms import builder_inputs, builder_runner
from chessism_api.operations.research_algorithms.builder_schema import normalize_builder
from chessism_api.operations.research_algorithms.backups import algorithm_backup_manifest, validate_algorithm_backup
from chessism_api.operations.research_algorithms.repository import run_payload


@unittest.skipUnless(os.getenv('CHESSISM_DB_INTEGRATION') == '1', 'opt-in database test')
class AlgorithmDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine(os.environ['DATABASE_URL'])
        self.connection = await self.engine.connect()
        self.factory = sessionmaker(bind=self.connection, class_=AsyncSession, expire_on_commit=False)
        await self.connection.execute(text('''CREATE TEMP TABLE game (
            link BIGINT PRIMARY KEY, played_at TIMESTAMPTZ, n_moves INT, time_elapsed DOUBLE PRECISION)'''))
        await self.connection.execute(text('''INSERT INTO game VALUES
            (1, '2026-01-01', 10, 20), (2, '2026-01-02', 20, 40),
            (3, '2026-01-03', 30, NULL), (4, '2026-01-04', 40, 80)'''))
        await self.connection.execute(text('''CREATE TEMP TABLE algorithm_definition (
            id VARCHAR(36) PRIMARY KEY, name VARCHAR(100), matrix_definition_id VARCHAR(36),
            config JSON, created_by VARCHAR(36), created_at TIMESTAMPTZ DEFAULT NOW())'''))
        await self.connection.execute(text('''CREATE TEMP TABLE algorithm_run (
            id VARCHAR(36) PRIMARY KEY, definition_id VARCHAR(36), name VARCHAR(100), config JSON,
            status VARCHAR(16), cancel_requested BOOLEAN, progress JSON, result JSON, error VARCHAR(2000),
            created_by VARCHAR(36), created_at TIMESTAMPTZ DEFAULT NOW(), started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ)'''))
        await self.connection.commit()
        self.settings = config.normalize_algorithm({'columns': ['moves', 'elapsed_seconds'], 'x': 'moves', 'y': 'elapsed_seconds', 'max_rows': 4},
            {'row_type': 'game', 'feature_columns': ['moves', 'elapsed_seconds'], 'filters': {'max_rows': 4}})

    async def asyncTearDown(self):
        await self.connection.close()
        await self.engine.dispose()

    async def test_real_source_stream_and_statistics(self):
        with TemporaryDirectory() as temporary, patch.object(inputs, 'AsyncDBSession', self.factory), patch.object(storage, 'FREE_FLOOR', 0):
            folder = Path(temporary)
            source = await inputs.materialize(self.settings, folder, AsyncMock())
            result = await jobs.calculate(self.settings, folder, source, AsyncMock())
            self.assertEqual(result['input_rows'], 4)
            self.assertEqual(result['included_rows'], 3)
            self.assertEqual(result['excluded_rows'], 1)
            self.assertEqual(result['missing_by_column']['elapsed_seconds'], 1)
            self.assertAlmostEqual(result['correlations'][0][1], 1)
            self.assertEqual([point['row_key'] for point in result['scatter']['points']], ['1', '2', '4'])
            self.assertEqual(len(result['source']['input_sha256']), 64)
            self.assertFalse(result['source']['inputs_retained'])

    async def test_builder_typed_source_and_branched_calculation(self):
        await self.connection.execute(text('ALTER TABLE game ADD COLUMN mode TEXT'))
        await self.connection.execute(text("UPDATE game SET mode = CASE WHEN link < 3 THEN 'blitz' ELSE 'rapid' END"))
        await self.connection.commit()
        recipe = {'row_type': 'game', 'feature_columns': ['moves', 'elapsed_seconds', 'mode', 'started_at'], 'filters': {'max_rows': 4}}
        settings = normalize_builder({'columns': recipe['feature_columns'], 'steps': [
            {'id': 'pace', 'input': 'input', 'op': 'calculate', 'column': 'seconds_per_move', 'expression': 'safe_divide(elapsed_seconds, moves)'},
            {'id': 'summary', 'input': 'pace', 'op': 'aggregate', 'group_by': ['mode'], 'metrics': [{'name': 'mean_seconds', 'op': 'mean', 'column': 'seconds_per_move'}]},
            {'id': 'dates', 'input': 'input', 'op': 'calculate', 'column': 'day', 'expression': 'utc_day(started_at)'},
        ], 'outputs': [{'input': 'summary', 'type': 'table'}, {'input': 'dates', 'type': 'table', 'columns': ['day']}]}, recipe)
        with patch.object(builder_inputs, 'AsyncDBSession', self.factory), patch.object(storage, 'FREE_FLOOR', 0):
            result = await builder_runner.execute(settings, AsyncMock())
        self.assertEqual(result['outputs'][0]['rows'], [['blitz', 2.0], ['rapid', 2.0]])
        self.assertEqual(result['outputs'][1]['rows'][0], ['2026-01-01'])
        self.assertEqual(result['input_rows'], 4)
        self.assertEqual(result['steps'][0]['missing']['seconds_per_move'], 1)

    async def test_whole_job_durable_result_cleanup_and_backup_integrity(self):
        run_id = str(uuid.uuid4())
        async with self.factory() as session, session.begin():
            session.add(AlgorithmRun(id=run_id, name='Temporary integration run', config=self.settings, status='queued'))
        @asynccontextmanager
        async def local_lock(**kwargs):
            async with self.factory() as session, session.begin(): yield session
        with TemporaryDirectory() as temporary, patch.object(storage, 'WORK_ROOT', Path(temporary)), patch.object(storage, 'FREE_FLOOR', 0), \
                patch.object(jobs, 'AsyncDBSession', self.factory), patch.object(inputs, 'AsyncDBSession', self.factory), \
                patch.object(jobs, 'Reporter', return_value=AsyncMock()), patch.object(jobs, 'matrix_catalog_lock', local_lock):
            reply = await jobs.run_algorithm_job({}, run_id=run_id)
            self.assertEqual(reply['status'], 'complete')
            self.assertEqual(list(Path(temporary).iterdir()), [])
        async with self.factory() as session:
            row = await session.get(AlgorithmRun, run_id)
            self.assertEqual(run_payload(row, result=True)['result']['included_rows'], 3)
            original = await algorithm_backup_manifest(session)
            self.assertEqual(original['completed_runs']['count'], 1)
            self.assertEqual(validate_algorithm_backup(original, original)['status'], 'passed')
            # Active progress is deliberately outside the completed-result digest.
            row.progress = {'phase': 'complete', 'detail': 'different display text'}
            await session.flush()
            self.assertEqual(await algorithm_backup_manifest(session), original)
            row.result = {'incorrect': 'modified'}
            await session.flush()
            with self.assertRaises(ValueError):
                validate_algorithm_backup(original, await algorithm_backup_manifest(session))

    async def test_builder_job_publishes_sample_and_is_covered_by_backup_digest(self):
        config = normalize_builder({'columns': ['moves', 'elapsed_seconds'], 'steps': [
            {'id': 'pace', 'op': 'calculate', 'input': 'input', 'column': 'seconds_per_move',
             'expression': 'safe_divide(elapsed_seconds, moves)'},
        ], 'outputs': [{'input': 'pace', 'type': 'table'}]}, self.settings['matrix'])
        config['sample_run'] = True
        run_id = str(uuid.uuid4())
        async with self.factory() as session, session.begin():
            session.add(AlgorithmRun(id=run_id, name='Builder temporary test', config=config, status='queued'))
        @asynccontextmanager
        async def local_lock(**kwargs):
            async with self.factory() as session, session.begin(): yield session
        with TemporaryDirectory() as temporary, patch.object(storage, 'WORK_ROOT', Path(temporary)), patch.object(storage, 'FREE_FLOOR', 0), \
                patch.object(jobs, 'AsyncDBSession', self.factory), patch.object(builder_inputs, 'AsyncDBSession', self.factory), \
                patch.object(jobs, 'Reporter', return_value=AsyncMock()), patch.object(jobs, 'matrix_catalog_lock', local_lock):
            self.assertEqual((await jobs.run_algorithm_job({}, run_id=run_id))['status'], 'complete')
            self.assertEqual(list(Path(temporary).iterdir()), [])
        async with self.factory() as session:
            row = await session.get(AlgorithmRun, run_id)
            self.assertTrue(row.result['sample_run'])
            self.assertEqual(row.result['outputs'][0]['rows'][2], [30., None, None])
            self.assertEqual((await algorithm_backup_manifest(session))['completed_runs']['count'], 1)


if __name__ == '__main__': unittest.main()
