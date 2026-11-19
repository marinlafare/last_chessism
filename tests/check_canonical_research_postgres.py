"""Real canonical training/COPY reads; synthetic data, no CUDA or model publication."""
import asyncio
from datetime import datetime, timedelta, timezone
import io
from unittest.mock import AsyncMock, patch

import asyncpg
import numpy as np
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, research_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations.research_algorithms import blunder_training as training
from chessism_api.operations.research_algorithms import blunder_v2_source as v2
from chessism_api.operations.research_algorithms.blunder_v2_job import configuration
from chessism_api.operations.research_algorithms.blunder_model import TRANSITION
from chessism_api.operations.research_algorithms.blunder_v2_model import DTYPE
from chessism_api.operations.canonical_blunders import binary_source as binary, compute
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class CanonicalResearchTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.people = {name: await self.db.fetchval(
            'INSERT INTO player(player_name,joined,deleted_at) VALUES($1,$2,$3) RETURNING id',
            name, 0 if name == 'shell' else 1,
            datetime.now(timezone.utc) if name == 'removed' else None)
            for name in ('alice', 'bob', 'shell', 'removed')}
        self.modes = {mode: await self.db.fetchval(
            'INSERT INTO game_mode(name) VALUES($1) RETURNING id', mode)
            for mode in ('bullet', 'blitz', 'rapid', 'daily')}
        self.fens = []
        for i, score in enumerate((100, -100, 9998, 0, None, -2**31)):
            self.fens.append(await self.db.fetchval('''INSERT INTO fen(fen,score,analysis_source,analyzed_at,tablebase_wdl)
                VALUES($1,$2,$3,$4,$5) RETURNING fen_id''',
                f'4k3/8/8/8/8/8/8/{i+1}K{6-i} w - -', score,
                None if score is None else 'tablebase' if i == 3 else 'stockfish',
                datetime.now(timezone.utc) if score is not None else None, 0 if i == 3 else None))
        for link in range(1, 19):
            await self.add_game(link)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def add_game(self, link):
        white, black = ('shell', 'shell') if link == 6 else ('removed', 'removed') if link == 7 else (
            ('alice', 'alice') if link == 14 else ('alice', 'shell') if link == 17 else ('alice', 'bob'))
        moves = 0 if link == 13 else 1 if link == 18 else 11
        await self.db.execute('''INSERT INTO game(link,white_player_id,black_player_id,mode_id,played_at,
            white_elo,black_elo,white_result,black_result,white_str_result,black_str_result,
            time_control,eco,time_elapsed,n_moves,rules,initial_setup)
            VALUES($1,$2,$3,$4,$5,1500,$6,1,$7,'win',$8,'600','no_eco',120,$9,$10,$11)''',
            link, self.people[white], self.people[black],
            None if link == 16 else self.modes['daily' if link == 15 else 'blitz'],
            datetime(2025, 1, 1, tzinfo=timezone.utc)+timedelta(days=link),
            1850 if link == 3 else 1500, 1 if link == 5 else 0,
            'timeout' if link == 2 else 'resigned', moves, 'chess960' if link == 11 else 'chess',
            '8/8/8/8/8/4K3/8/6k1 w - -' if link == 12 else None)
        total = 2 if link == 18 else 22
        unscored = total if link == 9 else 1 if link == 4 else 0
        await self.db.execute('''INSERT INTO game_analysis_coverage(link,total_positions,analyzed_positions,
            unscored_positions,is_fully_analyzed,equal_positions,small_positions,clear_positions,decisive_positions,mate_positions)
            VALUES($1,$2,$3,$4,$5,0,0,0,0,0)''', link, total, total-unscored, unscored, not unscored)
        if link in (11, 12, 13):
            return
        await self.db.executemany('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)', [
            (link, self.fens[2 if link == 8 else 3 if link == 10 else
                             4 if link == 9 or link == 4 and ply == total else 1 if ply == total else 0],
             (ply+1)//2, ply % 2 == 1) for ply in range(1, total+1)])

    async def freeze(self, version, reporter=None, factory=None):
        stream = io.BytesIO()
        work = training.freeze_source if version == 1 else v2.freeze
        config = training.configuration(100) if version == 1 else configuration(100)
        result = await work(config, reporter or AsyncMock(), stream, session_factory=factory or self.factory)
        return result, stream.getvalue()

    async def selected(self, sql, **params):
        async with read_transaction(self.factory) as session:
            return [dict(row) for row in (await session.execute(text(sql), params)).mappings()]

    async def test_training_selection_keeps_different_v1_v2_contracts_and_unique_games(self):
        v1_rows = await self.selected(reads.TRAINING_GAMES, sample_seed=20261027, games_per_group=100)
        v2_rows = await self.selected(reads.v2_selection(), games_per_group=100)
        self.assertEqual([g['link'] for g in v1_rows], [1, 8, 10, 14, 17])
        self.assertEqual([g['link'] for g in v2_rows], [1, 2, 3, 8, 10, 14, 17, 18])
        self.assertEqual(len({g['link'] for g in v2_rows}), len(v2_rows))
        # Even misleading saved coverage cannot include a variant/custom/empty game.
        self.assertTrue({11, 12, 13}.isdisjoint(g['link'] for g in v2_rows))
        owners = await self.selected(reads.eligible_games(owners=True)+' SELECT * FROM eligible')
        row = next(g for g in owners if g['link'] == 17)
        self.assertTrue(row['white_complete']); self.assertFalse(row['black_complete'])

    async def test_stratified_sampling_is_deterministic_and_rating_arithmetic_does_not_overflow(self):
        await self.db.execute('UPDATE game SET white_elo=2147483647,black_elo=2147483647 WHERE link=1')
        expected = await self.db.fetch('''SELECT DISTINCT ON(band) link FROM (
            SELECT link,LEAST(8,GREATEST(0,FLOOR(((white_elo::bigint+black_elo)/2.0-1000)/200))) band
            FROM game WHERE link=ANY($1::bigint[])) r ORDER BY band,hashtextextended(link::text,20261027),link''', [1, 8, 10, 14, 17])
        for _ in range(2):
            rows = await self.selected(reads.TRAINING_GAMES, sample_seed=20261027, games_per_group=1)
            self.assertEqual([g['link'] for g in rows], sorted(r['link'] for r in expected))
        self.assertNotIn(1, [g['link'] for g in await self.selected(reads.v2_selection(), games_per_group=100)])

    async def test_v1_frozen_transitions_preserve_prior_perspective_kinds_and_whole_game_splits(self):
        (dataset, report, _), raw = await self.freeze(1)
        rows = np.frombuffer(raw, dtype=TRANSITION)
        self.assertEqual(report['selected_games'], 5); self.assertEqual(report['positions'], 110)
        self.assertEqual(report['fitting_games'], 3); self.assertEqual(report['games_without_eligible_cp'], 2)
        self.assertEqual(report['fitting_positions'], 6)
        self.assertEqual(int(dataset.games.sum()), 3)
        for game in (1, 14, 17):
            part = rows[rows['game'] == game]
            self.assertEqual(len(part), 22); self.assertEqual(len(set(part['split'])), 1)
            self.assertEqual(part['before'][0], 15); self.assertEqual(part['before'][1], -100)
            self.assertEqual(part['after'][-1], 100)
        self.assertTrue(np.all(rows[rows['game'] == 8]['after_kind'] == 1))
        self.assertTrue(np.all(rows[rows['game'] == 10]['after_kind'] == 2))

    async def test_v2_frozen_fen_identities_chronology_and_exact_predecessors(self):
        (metadata, arrays, report), raw = await self.freeze(2)
        rows = np.frombuffer(raw, dtype=DTYPE)
        self.assertEqual(report['selected_games'], 8); self.assertEqual(report['positions'], 156)
        self.assertEqual([g['link'] for g in metadata], [1, 2, 3, 8, 10, 14, 17, 18])
        self.assertEqual(int(arrays['cp_count'].sum()), 112)
        for index, game in enumerate(metadata):
            part = rows[rows['game'] == index]
            self.assertEqual(len(part), game['total_positions']); self.assertEqual(part['before'][0], 15)
            np.testing.assert_array_equal(part['before'][1:], part['after'][:-1])
            if game['link'] not in (8, 10):
                self.assertEqual(part['fen'][0], self.fens[0]); self.assertEqual(part['fen'][-1], self.fens[1])
        repeated, again = await self.freeze(2)
        self.assertEqual(raw, again); self.assertEqual(report['sha256'], repeated[2]['sha256'])

    async def test_real_binary_copy_matches_scalar_packing_in_mixed_order_including_integer_minimum(self):
        await self.db.execute('UPDATE game_fen_association SET fen_id=$1 WHERE game_link=1 AND n_move=2 AND is_white', self.fens[5])
        games = [dict(game_link=link, total_positions=22) for link in (10, 1, 8, 9)]
        async with read_transaction(self.factory) as session:
            actual = await binary.load(session, games)
            expected = []
            for index, game in enumerate(games):
                rows = (await session.execute(text(reads.positions()), {'links': [game['game_link']], 'row_limit': 23})).all()
                expected.append(compute.pack(game, [(r[1], r[2], r[3]) for r in rows], index))
        self.assertEqual(actual.tobytes(), np.concatenate(expected).tobytes())
        row = actual[(actual['game'] == 1) & (actual['ply'] == 3)][0]
        self.assertEqual(row['after'], -2**31); self.assertEqual(row['ak'], 1)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_binary_requires_caller_transaction_and_preserves_it(self):
        async with self.factory() as session:
            with self.assertRaisesRegex(ValueError, 'caller-owned'):
                await binary.load(session, [dict(game_link=1, total_positions=22)])
            self.assertFalse(session.in_transaction())
        async with read_transaction(self.factory) as session:
            await binary.load(session, [dict(game_link=1, total_positions=22)])
            self.assertTrue(session.in_transaction())
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')), 'on')

    async def test_coverage_mismatch_is_an_error_for_both_training_versions_and_binary(self):
        await self.db.execute('UPDATE game_analysis_coverage SET total_positions=21,analyzed_positions=21 WHERE link=1')
        for version in (1, 2):
            with self.assertRaisesRegex(ValueError, '[Cc]overage'): await self.freeze(version)
            self.assertEqual(self.engine.pool.checkedout(), 0)
        async with read_transaction(self.factory) as session:
            with self.assertRaisesRegex(ValueError, 'count|[Cc]overage'):
                await binary.load(session, [dict(game_link=1, total_positions=21)])

    async def test_move_zero_and_missing_predecessor_are_never_accepted_as_complete_v2_input(self):
        await self.db.execute('UPDATE game_fen_association SET n_move=0 WHERE game_link=1 AND n_move=1 AND is_white')
        for version in (1, 2):
            with self.assertRaisesRegex(ValueError, '[Uu]nordered|[Mm]issing'): await self.freeze(version)
        async with read_transaction(self.factory) as session:
            with self.assertRaisesRegex(ValueError, '[Mm]issing'):
                await binary.load(session, [dict(game_link=1, total_positions=22)])
        await self.db.execute('UPDATE game_fen_association SET n_move=12 WHERE game_link=1 AND n_move=0 AND is_white')
        with self.assertRaisesRegex(ValueError, '[Mm]issing'): await self.freeze(2)

    async def test_training_bounds_reject_whole_selection_and_oversize_game_without_truncation(self):
        for module, version in ((training, 1), (v2, 2)):
            with patch.object(module, 'MAX_ROWS', 1), self.assertRaisesRegex(ValueError, 'limit|exceeds'):
                await self.freeze(version)
        await self.db.execute('UPDATE game_analysis_coverage SET total_positions=32769,analyzed_positions=32769 WHERE link=1')
        for version in (1, 2):
            with self.assertRaisesRegex(ValueError, 'bounded'): await self.freeze(version)

    async def test_active_cloud_guard_and_terminal_cloud_statuses(self):
        await self.db.execute("INSERT INTO cloud_analysis_job(id,status,target,imported) VALUES('research-guard','running',1,0)")
        for version in (1, 2):
            with self.assertRaisesRegex(ValueError, 'active cloud'): await self.freeze(version)
        for status in ('complete', 'cancelled', 'limit_reached', 'failed'):
            await self.db.execute('UPDATE cloud_analysis_job SET status=$1', status)
            for version in (1, 2): await self.freeze(version)

    async def test_empty_selection_fails_explicitly_and_never_publishes(self):
        await self.db.execute('UPDATE player SET joined=0')
        for version in (1, 2):
            with self.assertRaisesRegex(ValueError, 'No .*games'): await self.freeze(version)
        self.assertIsNone(await self.db.fetchval("SELECT to_regclass('algorithm_run')"))

    async def test_each_training_snapshot_remains_consistent_during_concurrent_evaluation_change(self):
        for version in (1, 2):
            await self.db.execute('UPDATE fen SET score=100 WHERE fen_id=$1', self.fens[0])
            _, expected = await self.freeze(version)
            reporter = AsyncMock()
            async def change():
                await self.db.execute('UPDATE fen SET score=300 WHERE fen_id=$1', self.fens[0])
            reporter.check.side_effect = change
            _, actual = await self.freeze(version, reporter=reporter)
            self.assertEqual(actual, expected)
            _, newer = await self.freeze(version)
            self.assertNotEqual(newer, expected)

    async def test_readonly_sessions_utc_cleanup_and_database_error_propagation(self):
        for version in (1, 2):
            sessions = []
            def factory():
                session = self.factory(); sessions.append(session); return session
            reporter = AsyncMock()
            async def forbidden():
                session = sessions[-1]
                self.assertEqual(await session.scalar(text('SHOW transaction_read_only')), 'on')
                self.assertEqual(await session.scalar(text('SHOW transaction_isolation')), 'repeatable read')
                self.assertEqual(await session.scalar(text('SHOW TimeZone')), 'UTC')
                await session.execute(text('DELETE FROM fen'))
            reporter.check.side_effect = forbidden
            with self.assertRaises(DBAPIError): await self.freeze(version, reporter=reporter, factory=factory)
            self.assertEqual(self.engine.pool.checkedout(), 0)
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen'), 6)
            target = patch.object(reads, 'TRAINING_GAMES', 'SELECT 1/0') if version == 1 else patch.object(reads, 'v2_selection', return_value='SELECT 1/0')
            with target, self.assertRaises(DBAPIError): await self.freeze(version)
            self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_repeated_cancellation_drains_rollback_and_closes_even_if_rollback_fails(self):
        for version in (1, 2):
            for fail_rollback in (False, True):
                entered, rolling, release, closed = (asyncio.Event() for _ in range(4))
                async def pause(): entered.set(); await asyncio.Future()
                reporter = AsyncMock(); reporter.check.side_effect = pause
                def factory():
                    session = self.factory(); rollback, close = session.rollback, session.close
                    async def delayed():
                        rolling.set(); await release.wait(); await rollback()
                        if fail_rollback: raise RuntimeError('synthetic rollback failure')
                    async def closing():
                        try: await close()
                        finally: closed.set()
                    session.rollback = delayed; session.close = closing; return session
                task = asyncio.create_task(self.freeze(version, reporter=reporter, factory=factory))
                try:
                    await asyncio.wait_for(entered.wait(), 2); task.cancel()
                    await asyncio.wait_for(rolling.wait(), 2)
                    for _ in range(2):
                        task.cancel(); await asyncio.sleep(0); self.assertFalse(task.done())
                finally:
                    release.set(); task.cancel(); await asyncio.gather(task, return_exceptions=True)
                self.assertTrue(task.cancelled()); self.assertTrue(closed.is_set())
                self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_schema_constraints_replace_legacy_nullable_and_unverified_source_fixtures(self):
        for sql in ('UPDATE game SET white_result=NULL WHERE link=1',
                    'UPDATE game SET played_at=NULL WHERE link=1'):
            with self.assertRaises(asyncpg.NotNullViolationError): await self.db.execute(sql)
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.db.execute("UPDATE fen SET analysis_source='unknown' WHERE fen_id=$1", self.fens[0])
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO game_fen_association VALUES(1,$1,1,true)', self.fens[0])
        before = await canonical.catalog_digest(self.db)
        for version in (1, 2): await self.freeze(version)
        self.assertEqual(await canonical.catalog_digest(self.db), before)
        self.assertIsNone(await self.db.fetchval("SELECT to_regclass('algorithm_run')"))
