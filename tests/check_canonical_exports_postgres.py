"""Structured source/preview/file contracts on canonical PostgreSQL; no live credentials."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
import uuid
from unittest.mock import AsyncMock, patch

import chess
import httpx
from fastapi import FastAPI
import pyarrow.parquet as pq
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations.exports import catalog, config, files, jobs, preview, service
from chessism_api.operations.exports.analytical_admin import view_select
from chessism_api.operations.exports.auth import require_client
from chessism_api.operations.exports.query import ExportSpec, compile_query
from chessism_api.operations.exports.writer import ParquetOutput, arrow_schema
from chessism_api.redis_client import get_redis_pool
from chessism_api.routers.exports import router
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class CanonicalExportTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp(); await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test', database=self.name,
            port=55432, query={'host': self.socket}))
        self.addAsyncCleanup(self.engine.dispose); self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.directory = TemporaryDirectory(); self.addCleanup(self.directory.cleanup)
        for field, value in [('ROOT', Path(self.directory.name)), ('FREE_FLOOR', 0), ('BATCH_ROWS', 1)]:
            change = patch.object(config, field, value); change.start(); self.addCleanup(change.stop)
        self.players = {}
        for name, joined in [('alice', 2**40+23), ('bob', 1), ('shell', 0)]:
            self.players[name] = await self.db.fetchval('INSERT INTO player(player_name,joined) VALUES($1,$2) RETURNING id', name, joined)
        mode = await self.db.fetchval("INSERT INTO game_mode(name) VALUES('blitz') RETURNING id")
        self.large_id = 2**53+1
        games = [(10, 1, 'chess', None, 1.25), (11, 1, 'chess', None, .5), (12, 0, 'chess', None, 0),
                 (13, 2, 'chess', None, 5.0), (self.large_id, 1, 'chess', None, 60.5),
                 (20, 1, 'chess960', None, 1.0), (21, 1, 'chess', '8/8/8/8/8/8/4K3/7k w - - 0 1', 1.0)]
        for link, moves, rules, setup, seconds in games:
            await self.db.execute('''INSERT INTO game(link,white_player_id,black_player_id,mode_id,played_at,
                white_elo,black_elo,white_result,black_result,white_str_result,black_str_result,
                time_control,eco,time_elapsed,n_moves,rules,initial_setup)
                VALUES($1,$2,$3,$4,'2026-01-01T00:00:00.123456Z',$5,$6,1,0,'win','resigned','600','no_eco',$7,$8,$9,$10)''',
                link, self.players['alice'], self.players['alice' if link == 13 else 'bob'], mode,
                2**31-1 if link == self.large_id else 1500, 2**31-1 if link == self.large_id else 1501,
                seconds, moves, rules, setup)
            for move in range(1, moves+1):
                await self.db.execute("INSERT INTO moves VALUES($1,$2,'e4',$3,.125,.25,599.5,598.25)",
                    link, move, '--' if link == 11 else 'e5')
        board = chess.Board(); fens = [' '.join(board.fen().split()[:4])]
        for san in ('e4', 'e5', 'Nf3'): board.push_san(san); fens.append(' '.join(board.fen().split()[:4]))
        self.fens = []
        for fen, score in zip(fens, [None, 0, -(2**31), 2**31-1]):
            self.fens.append(await self.db.fetchval('''INSERT INTO fen(fen,n_games,score,analysis_source,analyzed_at)
                VALUES($1,3,$2,$3,$4) RETURNING fen_id''', fen, score, 'stockfish' if score is not None else None,
                datetime.now(timezone.utc) if score is not None else None))
        for link, move, side, fen in [(10,0,True,0),(10,1,True,1),(10,1,False,2),(11,1,True,1),
                                     (13,1,True,1),(13,1,False,2),(13,2,True,0),(13,2,False,0),
                                     (self.large_id,1,True,3)]:
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)', link, self.fens[fen], move, side)

    async def rows(self, source, fields=None, **kwargs):
        spec = ExportSpec(source=source, select=fields or list(catalog.SOURCES[source].columns), **kwargs)
        async with read_transaction(self.factory) as session:
            query, params = compile_query(spec)
            return [dict(row) for row in (await session.execute(query, params)).mappings().all()]

    def job(self, spec):
        document = service.request_document(spec); identity = uuid.uuid4().hex
        files.job_dir(identity).mkdir(); files.write_json(files.job_dir(identity)/'request.json', document)
        return SimpleNamespace(id=identity, created_at=datetime.now(timezone.utc),
            request_sha256=hashlib.sha256(files.canonical_bytes(document)).hexdigest())

    async def test_every_source_and_column_has_native_unique_identity_and_expected_counts(self):
        expected = {'games': 5, 'players': 3, 'positions': 4, 'game_positions': 9, 'moves': 9, 'player_games': 10}
        for name, count in expected.items():
            with self.subTest(source=name):
                rows = await self.rows(name); self.assertEqual(len(rows), count)
                identities = [tuple(row[k] for k in catalog.SOURCES[name].keys) for row in rows]
                self.assertEqual(len(set(identities)), count)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_standard_scope_keeps_zero_move_games_and_self_play_sides(self):
        games = await self.rows('games', ['game_id', 'moves'])
        self.assertEqual([r['game_id'] for r in games], [10,11,12,13,self.large_id])
        self.assertIn({'game_id': 12, 'moves': 0}, games)
        sides = await self.rows('player_games', ['game_id','player_id','is_white'], where={'field':'game_id','op':'eq','value':13})
        self.assertEqual(sides, [{'game_id':13,'player_id':self.players['alice'],'is_white':True},
                               {'game_id':13,'player_id':self.players['alice'],'is_white':False}])
        self.assertEqual(len(await self.rows('moves', ['san'])), 9)
        self.assertEqual(len(await self.rows('player_games', ['player'])), 10)

    async def test_repetitions_initial_occurrences_and_missing_move_scores_are_preserved(self):
        occurrences = await self.rows('game_positions', ['game_id','move_number','is_white','fen_id','san'])
        initial = next(r for r in occurrences if r['move_number'] == 0)
        self.assertIsNone(initial['san']); self.assertEqual(initial['fen_id'], self.fens[0])
        self.assertEqual(sum(r['game_id']==13 and r['fen_id']==self.fens[0] for r in occurrences), 2)
        moves = await self.rows('moves', ['game_id','is_white','score_cp','fen_id'])
        missing = next(r for r in moves if r['game_id']==self.large_id and not r['is_white'])
        self.assertIsNone(missing['score_cp']); self.assertIsNone(missing['fen_id'])
        self.assertEqual([r['score_cp'] for r in await self.rows('positions', ['score_cp'])], [None,0,-(2**31),2**31-1])

    async def test_explicit_numeric_types_round_trip_all_sources_through_parquet(self):
        for source in catalog.SOURCES:
            spec = ExportSpec(source=source, select=list(catalog.SOURCES[source].columns))
            expected = await self.rows(source); path = config.ROOT/(source+'.parquet')
            writer = ParquetOutput(path, spec)
            try: writer.write(expected)
            finally: writer.close()
            table = pq.read_table(path)
            self.assertEqual(table.schema, arrow_schema(spec)); self.assertEqual(table.to_pylist(), expected)
        self.assertEqual((await self.rows('games'))[0]['elapsed_seconds'], 1.25)
        self.assertEqual((await self.rows('games'))[-1]['average_rating'], 2**31-1)
        self.assertEqual((await self.rows('players'))[0]['joined'], 2**40+23)

    async def test_fractional_and_large_integer_filters_and_stable_tie_order(self):
        rows = await self.rows('games',['game_id'],where={'field':'elapsed_seconds','op':'eq','value':1.25})
        self.assertEqual(rows,[{'game_id':10}])
        rows = await self.rows('players',['player_name'],where={'field':'joined','op':'eq','value':2**40+23})
        self.assertEqual(rows,[{'player_name':'alice'}])
        rows = await self.rows('player_games',['game_id','is_white'],order_by=[{'field':'played_at','direction':'desc'}])
        self.assertEqual(rows,[{'game_id':g,'is_white':s} for g in [10,11,12,13,self.large_id] for s in [True,False]])

    async def test_preview_is_bounded_and_preserves_large_identity_timestamp_and_float(self):
        spec = ExportSpec(source='games', select=['game_id','played_at','elapsed_seconds'], limit=3)
        result = await preview.structured_preview(spec, session_factory=self.factory)
        self.assertEqual(result['row_count'],3); self.assertTrue(result['has_more'])
        self.assertEqual(result['rows'][0],[10,'2026-01-01T00:00:00.123456+00:00',1.25])
        spec = ExportSpec(source='games',select=['game_id'],where={'field':'game_id','op':'eq','value':self.large_id})
        self.assertEqual((await preview.structured_preview(spec,session_factory=self.factory))['rows'],[[str(self.large_id)]])

    async def test_analytical_view_projections_match_same_canonical_sources(self):
        # Execute the installer projection without creating roles, views or credentials.
        for name, source in catalog.SOURCES.items():
            async with read_transaction(self.factory) as session:
                rows = [dict(r) for r in (await session.execute(text(view_select(source,catalog.TABLES)))).mappings().all()]
            expected = await self.rows(name)
            key = lambda row: tuple(row[k] for k in source.keys)
            self.assertEqual(sorted(rows,key=key),sorted(expected,key=key))

    async def test_file_snapshot_is_consistent_during_concurrent_game_changes(self):
        spec = ExportSpec(source='games',select=['game_id','white_rating','elapsed_seconds'],limit=3); job = self.job(spec)
        calls = []
        async def checkpoint(identity, **values):
            calls.append(values)
            if values.get('phase') == 'reading':
                await self.db.execute('UPDATE game SET white_elo=7,time_elapsed=9')
        with patch.object(jobs,'checkpoint',checkpoint): await jobs.construct(job,session_factory=self.factory)
        rows = pq.read_table(files.job_dir(job.id)/'data.parquet').to_pylist()
        self.assertEqual([r['white_rating'] for r in rows],[1500]*3)
        self.assertEqual([r['elapsed_seconds'] for r in rows],[1.25,.5,0])
        manifest = json.loads((files.job_dir(job.id)/'manifest.json').read_bytes())
        self.assertEqual(manifest['catalog_version'],2); self.assertTrue(manifest['limit_reached'])
        self.assertEqual(manifest['file']['sha256'],files.sha256(files.job_dir(job.id)/'data.parquet'))
        self.assertEqual(calls[-1]['status'],'complete'); self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_empty_file_keeps_declared_types_and_does_not_claim_truncation(self):
        spec=ExportSpec(source='player_games',select=['game_id','is_white','elapsed_seconds'],where={'field':'game_id','op':'eq','value':0})
        job=self.job(spec)
        with patch.object(jobs,'checkpoint',AsyncMock()): await jobs.construct(job,session_factory=self.factory)
        table=pq.read_table(files.job_dir(job.id)/'data.parquet')
        self.assertEqual(table.num_rows,0); self.assertEqual(table.schema,arrow_schema(spec))
        manifest=json.loads((files.job_dir(job.id)/'manifest.json').read_bytes())
        self.assertFalse(manifest['limit_reached']); self.assertEqual(manifest['selection_order'],
            [{'field':'game_id','direction':'asc'},{'field':'is_white','direction':'desc'}])

    async def test_database_and_writer_failures_propagate_without_complete_publication(self):
        for failure in ('database','writer'):
            spec=ExportSpec(source='games',select=['game_id']);job=self.job(spec); checkpoint=AsyncMock()
            async def fail(*args,**kwargs): raise RuntimeError('synthetic read failure')
            def factory():
                session=self.factory();session.stream=fail;return session
            with patch.object(jobs,'checkpoint',checkpoint):
                if failure=='database':
                    with self.assertRaisesRegex(RuntimeError,'synthetic read'):await jobs.construct(job,session_factory=factory)
                else:
                    with patch.object(jobs.ParquetOutput,'write',side_effect=RuntimeError('synthetic file failure')),self.assertRaisesRegex(RuntimeError,'synthetic file'):
                        await jobs.construct(job,session_factory=self.factory)
            self.assertFalse((files.job_dir(job.id)/'data.parquet').exists())
            self.assertFalse(any(c.kwargs.get('status')=='complete' for c in checkpoint.call_args_list))
            self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_read_only_preview_and_database_failure_propagation(self):
        def write(*args,**kwargs): return text('DELETE FROM player'),{}
        with patch.object(preview,'compile_query',write),self.assertRaises(DBAPIError):
            await preview.structured_preview(ExportSpec(source='players',select=['player_id']),session_factory=self.factory)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player'),3);self.assertEqual(self.engine.pool.checkedout(),0)
        await self.db.execute('ALTER TABLE player RENAME COLUMN joined TO unavailable')
        with self.assertRaises(DBAPIError):
            await preview.structured_preview(ExportSpec(source='players',select=['joined']),session_factory=self.factory)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_repeated_preview_cancellation_finishes_rollback_and_close(self):
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        def factory():
            session=self.factory();execute,rollback,close=session.execute,session.rollback,session.close
            async def reading(sql,*args,**kwargs):
                if 'AS "game_id"' in str(sql):entered.set();await asyncio.Future()
                return await execute(sql,*args,**kwargs)
            async def delayed():rolling.set();await release.wait();await rollback()
            async def closing():
                try:await close()
                finally:closed.set()
            session.execute=reading;session.rollback=delayed;session.close=closing;return session
        task=asyncio.create_task(preview.structured_preview(ExportSpec(source='games',select=['game_id']),session_factory=factory))
        try:
            await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
            for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
        finally:
            release.set();task.cancel();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertTrue(closed.is_set());self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_repeated_file_cancellation_drains_writer_before_closing_or_publishing(self):
        loop=asyncio.get_running_loop();entered=asyncio.Event();release=threading.Event();finished=threading.Event()
        original=jobs.ParquetOutput.write;closes=[];old_close=jobs.ParquetOutput.close
        def write(writer,rows):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):raise RuntimeError('test release timeout')
            original(writer,rows);finished.set()
        def close(writer):closes.append(finished.is_set());old_close(writer)
        spec=ExportSpec(source='games',select=['game_id']);job=self.job(spec);checkpoint=AsyncMock()
        with patch.object(jobs.ParquetOutput,'write',write),patch.object(jobs.ParquetOutput,'close',close),patch.object(jobs,'checkpoint',checkpoint):
            task=asyncio.create_task(jobs.construct(job,session_factory=self.factory))
            try:
                await asyncio.wait_for(entered.wait(),2)
                for _ in range(3):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
            finally:release.set();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertEqual(closes,[True]);self.assertEqual(self.engine.pool.checkedout(),0)
        self.assertFalse((files.job_dir(job.id)/'data.parquet').exists())
        self.assertFalse(any(c.kwargs.get('status')=='complete' for c in checkpoint.call_args_list))

    async def test_http_catalog_and_preview_use_approved_keys_types_and_no_sensitive_sources(self):
        app=FastAPI();app.include_router(router,prefix='/exports/v1');redis=AsyncMock()
        app.dependency_overrides[require_client]=lambda:SimpleNamespace(id='test',allowed_sources=list(catalog.SOURCES),max_rows=1000)
        app.dependency_overrides[get_redis_pool]=lambda:redis
        with patch.object(preview,'AsyncDBSession',self.factory):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                catalog_response=(await client.get('/exports/v1/catalog')).json()
                self.assertEqual(catalog_response['version'],2)
                response=await client.post('/exports/v1/preview',json={'source':'player_games','select':['game_id','is_white','elapsed_seconds'],'limit':2})
                self.assertEqual(response.status_code,200,response.text)
                self.assertEqual(response.json()['rows'],[[10,True,1.25],[10,False,1.25]])
                response=await client.post('/exports/v1/preview',json={'source':'player_games','select':['player_game_id']})
                self.assertEqual(response.status_code,422)
                response=await client.post('/exports/v1/preview',json={'source':'account','select':['password_hash']})
                self.assertEqual(response.status_code,422)
