"""Canonical FEN cache publication/pages on isolated PostgreSQL, with synthetic Redis."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import random
from unittest.mock import AsyncMock, patch

import asyncpg
import chess
import httpx
from fastapi import FastAPI
from sqlalchemy import URL
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, fen_appearances as store
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations import common_positions as common, pending_repetitions as bins
from chessism_api.operations import common_position_pages as pages
from chessism_api.routers import fens as routes
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class CanonicalAppearanceTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp(); await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test', database=self.name,
            port=55432, query={'host': self.socket}))
        self.addAsyncCleanup(self.engine.dispose); self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.redis = AsyncMock(); self.cached = {}
        self.redis.mget.side_effect = lambda keys: [self.cached.get(k) for k in keys]
        self.redis.get.side_effect = lambda key: self.cached.get(key)
        self.redis.set.side_effect = lambda key, value, **kwargs: self.cached.update({key: value})
        self.redis.delete.side_effect = lambda key: self.cached.pop(key, None)
        self.ctx = {'redis': self.redis, 'session_factory': self.factory}
        owner = await self.db.fetchval("INSERT INTO player(player_name,joined) VALUES('alice',1) RETURNING id")
        mode = await self.db.fetchval("INSERT INTO game_mode(name) VALUES('blitz') RETURNING id")
        self.positions = {}
        for link, moves in ((1, ['e4', 'e5', 'Nf3', 'Nc6']), (2, ['e4', 'c5']), (3, ['d4', 'd5'])):
            await self.db.execute('''INSERT INTO game(link,white_player_id,black_player_id,mode_id,played_at,
                white_elo,black_elo,white_result,black_result,white_str_result,black_str_result,
                time_control,eco,time_elapsed,n_moves,rules)
                VALUES($1,$2,$2,$3,now(),1500,1500,1,0,'win','resigned','600','no_eco',60,$4,'chess')''',
                link, owner, mode, len(moves)//2)
            board = chess.Board()
            for ply, san in enumerate(moves, 1):
                board.push_san(san); fen = ' '.join(board.fen().split()[:4])
                key = await self.add_fen(fen, 100 if ply == 1 and san == 'e4' else 15)
                await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)', link, key, (ply+1)//2, ply%2==1)
            await self.db.executemany('INSERT INTO moves VALUES($1,$2,$3,$4,1,1,599,599)',
                [(link, i//2+1, moves[i], moves[i+1]) for i in range(0, len(moves), 2)])
        board = chess.Board(); rng = random.Random(23)
        while len(self.positions) < 31:
            if board.is_game_over(): board.reset()
            board.push(rng.choice(list(board.legal_moves)))
            fen = ' '.join(board.fen().split()[:4])
            if fen not in self.positions: await self.add_fen(fen, len(self.positions)%5)
        board = chess.Board(); board.push_san('e4'); self.e4 = ' '.join(board.fen().split()[:4])
        self.stamp = await self.db.fetchval('SELECT refreshed_at FROM database_summary WHERE id=1')

    async def add_fen(self, fen, appearances):
        if fen not in self.positions:
            score = 0 if len(self.positions)%2 else None
            key = await self.db.fetchval('''INSERT INTO fen(fen,n_games,score,analysis_source,analyzed_at)
                VALUES($1,$2,$3,$4,$5) RETURNING fen_id''', fen, appearances, score,
                'stockfish' if score is not None else None, datetime.now(timezone.utc) if score is not None else None)
            self.positions[fen] = (key, appearances, score)
        return self.positions[fen][0]

    async def refresh(self):
        self.assertEqual(await bins.run_pending_repetition_refresh(self.ctx), {'status': 'complete'})
        self.assertEqual(await common.run_commonness_refresh(self.ctx), {'status': 'complete'})

    async def payload(self):
        return await bins.get_fen_appearances(self.redis, session_factory=self.factory)

    async def cache_rows(self, table):
        return [dict(r) for r in await self.db.fetch('SELECT * FROM '+table+' ORDER BY 1')]

    async def test_histogram_and_pending_endpoint_match_stored_counts_including_zero(self):
        await self.refresh(); payload = await self.payload()
        counts = Counter(r[1] for r in self.positions.values())
        unscored = Counter(r[1] for r in self.positions.values() if r[2] is None)
        self.assertEqual(payload['rows'], [{'appearances': n, 'total_fens': counts[n], 'unscored_fens': unscored[n]} for n in sorted(counts)])
        self.assertFalse(payload['stale']); self.assertIsNone(payload['refresh_error'])
        pending = await bins.get_pending_repetitions(self.redis, session_factory=self.factory)
        self.assertEqual(pending['rows'], [{'repetitions': n, 'fen_count': unscored[n]} for n in sorted(unscored)])
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_modal_labels_use_unique_moves_and_verify_example_board(self):
        await self.refresh(); payload = (await self.payload())['commonness']
        first = payload['rows'][0]
        self.assertEqual((first['fen'], first['appearances'], first['label_occurrences']), (self.e4, 100, 2))
        self.assertEqual(first['label'], '1. e4'); self.assertEqual(first['example_line'], '1. e4')
        # All fixture games are self-play: occurrences must not be doubled by joining participants.
        async with read_transaction(self.factory) as session:
            modal = await store.modal_move(session, self.positions[self.e4][0])
            self.assertEqual(modal['example_game_link'], 1)
        self.cached.clear()
        await self.db.execute("UPDATE moves SET white_move='Qh9' WHERE n_move=1")
        await common.run_commonness_refresh(self.ctx)
        self.assertIsNone((await self.payload())['commonness']['rows'][0]['example_line'])

    async def test_keyset_pages_have_no_missing_or_duplicate_fens_across_ties(self):
        await self.refresh(); first = (await self.payload())['commonness']
        expected = [r['fen'] for r in await self.db.fetch('SELECT fen FROM fen WHERE n_games>1 ORDER BY n_games DESC,fen')]
        rows = first['rows']; cursor = first['next_cursor']
        while cursor:
            page = await pages.build_page(cursor, redis=self.redis, session_factory=self.factory)
            self.assertEqual(page['status'], 'complete'); self.assertLessEqual(len(page['rows']), 10)
            rows += page['rows']; cursor = page['next_cursor']
            self.assertLessEqual(len(rows), len(expected))
        self.assertEqual([r['fen'] for r in rows], expected)
        self.assertEqual([r['rank'] for r in rows], list(range(1, len(expected)+1)))

    async def test_changed_source_rejects_cached_page_and_success_worker_reuses_it(self):
        await self.refresh(); cursor = (await self.payload())['commonness']['next_cursor']
        self.assertEqual((await pages.get_common_position_page(self.redis, cursor, session_factory=self.factory))['status'], 'pending')
        await pages.run_commonness_page_refresh(self.ctx, cursor)
        self.assertEqual((await pages.get_common_position_page(self.redis, cursor, session_factory=self.factory))['status'], 'complete')
        await self.db.execute('UPDATE database_summary SET refreshed_at=clock_timestamp() WHERE id=1')
        self.assertEqual((await pages.get_common_position_page(self.redis, cursor, session_factory=self.factory))['status'], 'changed')
        self.assertEqual((await pages.build_page(cursor, session_factory=self.factory))['status'], 'changed')

    async def test_initialized_empty_caches_and_empty_published_snapshots_are_distinct(self):
        initial = await self.payload()
        self.assertIsNone(initial['refreshed_at']); self.assertEqual(initial['commonness']['status'], 'pending')
        await self.db.execute('DELETE FROM game; DELETE FROM fen')
        await self.refresh(); final = await self.payload()
        self.assertEqual(final['rows'], []); self.assertIsNotNone(final['refreshed_at'])
        self.assertEqual(final['commonness']['status'], 'complete'); self.assertEqual(final['commonness']['rows'], [])
        self.assertFalse(final['commonness']['stale']); self.assertIsNone(final['commonness']['next_cursor'])

    async def test_cache_constraints_and_source_deletion_preserve_old_snapshot_until_refresh(self):
        await self.refresh(); before = (await self.payload())['commonness']['rows']
        for sql in ("INSERT INTO fen_appearance_summary(id) VALUES(2)",
                    "INSERT INTO fen_appearance_bin VALUES(-1,1,0)",
                    "INSERT INTO fen_appearance_bin VALUES(999,1,2)",
                    "UPDATE fen_commonness_position SET rank=11 WHERE rank=1"):
            with self.assertRaises(asyncpg.CheckViolationError): await self.db.execute(sql)
        await self.db.execute('DELETE FROM game; DELETE FROM fen')
        self.assertEqual((await self.payload())['commonness']['rows'], before)
        await self.refresh(); self.assertEqual((await self.payload())['commonness']['rows'], [])

    async def test_missing_singletons_fail_without_bootstrapping_or_empty_success(self):
        for table, reader, worker in (
                ('fen_appearance_summary', self.payload, lambda: bins.run_pending_repetition_refresh(self.ctx)),
                ('fen_commonness_summary', lambda: common.get_common_positions(self.redis, self.stamp, session_factory=self.factory),
                    lambda: common.run_commonness_refresh(self.ctx))):
            await self.db.execute('DELETE FROM '+table)
            with self.assertRaises(RuntimeError): await reader()
            with self.assertRaises(NoResultFound): await worker()
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM '+table), 0)
            await self.db.execute('INSERT INTO '+table+' DEFAULT VALUES')
        async with self.factory() as session:
            with self.assertRaisesRegex(ValueError, 'caller-owned'): await store.read_common(session)

    async def test_failed_refresh_rolls_back_replacement_and_keeps_previous_metadata(self):
        await self.refresh()
        for method, table, meta, worker in (
                ('replace_bins', 'fen_appearance_bin', 'fen_appearance_summary', bins.run_pending_repetition_refresh),
                ('replace_common', 'fen_commonness_position', 'fen_commonness_summary', common.run_commonness_refresh)):
            before = await self.cache_rows(table); old_stamp = await self.db.fetchval('SELECT refreshed_at FROM '+meta)
            original = getattr(store, method)
            async def broken(*args): await original(*args); raise RuntimeError('synthetic publication failure')
            with patch.object(store, method, broken), self.assertRaisesRegex(RuntimeError, 'synthetic publication'):
                await worker(self.ctx)
            self.assertEqual(await self.cache_rows(table), before)
            self.assertEqual(await self.db.fetchval('SELECT refreshed_at FROM '+meta), old_stamp)
            self.assertIn('previous snapshot', await self.db.fetchval('SELECT last_error FROM '+meta))
            self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_late_failure_cannot_overwrite_newer_attempt_or_completed_snapshot(self):
        for kind, worker in (('appearance', bins.run_pending_repetition_refresh), ('commonness', common.run_commonness_refresh)):
            async with write_transaction(self.factory) as session: old = await store.mark_attempt(session, kind)
            await worker(self.ctx)
            async with write_transaction(self.factory) as session: await store.mark_failure(session, kind, old, 'stale failure')
            self.assertIsNone(await self.db.fetchval('SELECT last_error FROM '+store.metadata_table(kind)))
            # Even the latest attempt cannot mark a publication that finished afterward as failed.
            current = await self.db.fetchval('SELECT attempted_at FROM '+store.metadata_table(kind))
            async with write_transaction(self.factory) as session: await store.mark_failure(session, kind, current, 'late failure')
            self.assertIsNone(await self.db.fetchval('SELECT last_error FROM '+store.metadata_table(kind)))

    async def test_contending_refreshes_do_not_replace_existing_data(self):
        await self.refresh()
        for key, worker, table in ((bins.LOCK_ID, bins.run_pending_repetition_refresh, 'fen_appearance_bin'),
                                   (common.LOCK_ID, common.run_commonness_refresh, 'fen_commonness_position')):
            before = await self.cache_rows(table)
            async with self.db.transaction():
                await self.db.execute('SELECT pg_advisory_xact_lock($1)', key)
                self.assertEqual(await worker(self.ctx), {'status': 'already_refreshing'})
            self.assertEqual(await self.cache_rows(table), before)

    async def test_common_snapshot_and_histogram_each_read_one_source_snapshot(self):
        await self.refresh()
        for worker, table in ((common.run_commonness_refresh, 'fen_commonness_position'),
                              (bins.run_pending_repetition_refresh, 'fen_appearance_bin')):
            before = await self.cache_rows(table); original = store.source_stamp
            async def concurrent(session):
                stamp = await original(session)
                await self.db.execute('UPDATE fen SET n_games=n_games+1000; UPDATE database_summary SET refreshed_at=clock_timestamp()')
                return stamp
            self.cached.clear()
            with patch.object(store, 'source_stamp', concurrent): await worker(self.ctx)
            after = await self.cache_rows(table)
            self.assertEqual(after, before)
            payload = await self.payload()
            self.assertTrue(payload['commonness']['stale'] if worker is common.run_commonness_refresh else payload['stale'])
            await self.refresh()

    async def test_repeated_worker_cancellation_waits_for_all_database_cleanup(self):
        await self.refresh()
        for worker, table in ((bins.run_pending_repetition_refresh, 'fen_appearance_bin'),
                              (common.run_commonness_refresh, 'fen_commonness_position')):
            before = await self.cache_rows(table)
            entered, rolling, release = (asyncio.Event() for _ in range(3)); closures = []
            async def pause(*args): entered.set(); await asyncio.Future()
            def factory():
                session = self.factory(); rollback, close = session.rollback, session.close
                closed = asyncio.Event(); closures.append(closed)
                async def delayed(): rolling.set(); await release.wait(); await rollback()
                async def closing():
                    try: await close()
                    finally: closed.set()
                session.rollback = delayed; session.close = closing; return session
            with patch.object(store, 'publish_stamp', pause):
                task = asyncio.create_task(worker({**self.ctx, 'session_factory': factory}))
                try:
                    await asyncio.wait_for(entered.wait(), 2); task.cancel()
                    await asyncio.wait_for(rolling.wait(), 2)
                    for _ in range(2): task.cancel(); await asyncio.sleep(0); self.assertFalse(task.done())
                finally:
                    release.set(); task.cancel(); await asyncio.gather(task, return_exceptions=True)
            self.assertTrue(task.cancelled()); self.assertTrue(all(c.is_set() for c in closures))
            self.assertEqual(self.engine.pool.checkedout(), 0); self.assertEqual(await self.cache_rows(table), before)

    async def test_page_timeout_finishes_read_cleanup_before_error(self):
        await self.refresh(); cursor = (await self.payload())['commonness']['next_cursor']
        entered, rolling, release, closed = (asyncio.Event() for _ in range(4))
        async def pause(*args, **kwargs): entered.set(); await asyncio.Future()
        def factory():
            session = self.factory(); rollback, close = session.rollback, session.close
            async def delayed(): rolling.set(); await release.wait(); await rollback()
            async def closing():
                try: await close()
                finally: closed.set()
            session.rollback = delayed; session.close = closing; return session
        with patch.object(pages, 'describe_positions', pause):
            task = asyncio.create_task(common.await_refresh(pages.build_page(cursor, session_factory=factory), timeout=.2))
            try:
                await asyncio.wait_for(entered.wait(), 2); await asyncio.wait_for(rolling.wait(), 2)
                self.assertFalse(task.done()); release.set()
                with self.assertRaises(asyncio.TimeoutError): await task
            finally:
                release.set()
                if not task.done(): task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(closed.is_set()); self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_actual_http_histogram_unscored_and_page_contracts(self):
        await self.refresh(); app = FastAPI(); app.include_router(routes.router, prefix='/fens')
        async def redis(): return self.redis
        app.dependency_overrides[routes.get_redis_pool] = redis
        with patch.object(bins, 'AsyncDBSession', self.factory), patch.object(common, 'AsyncDBSession', self.factory), patch.object(pages, 'AsyncDBSession', self.factory):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.get('/fens/appearance_histogram'); self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['rows'], (await self.payload())['rows'])
                response = await client.get('/fens/pending/repetition_histogram'); self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['definition'], 'score_is_null')
                cursor = (await self.payload())['commonness']['next_cursor']
                response = await client.get('/fens/common_positions/page', params={'cursor': cursor})
                self.assertEqual(response.status_code, 200); self.assertEqual(response.json()['status'], 'pending')
