"""Raw integer bounds and unchanged accuracy policy on disposable PostgreSQL."""
import math

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.accuracy import lichess_move_accuracy, lichess_win_percent
from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.engine_summary import refresh_engine_summaries
from chessism_api.database.postgresql.game_coverage import refresh_coverage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.evaluations import score_kind
from chessism_api.operations.game_analysis_views import get_scored_game_analysis_page
from chessism_api.operations.mate_distance import mate_distance
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import PLAYED, seed


class CanonicalScoreBoundsTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        ids = await seed(self.db)
        self.fens = ids['fens'][1:3]
        # Two positions, one own move per side: the game's accuracy reduces to
        # its single move accuracy, giving an independent scalar formula oracle.
        await self.db.execute('DELETE FROM game_fen_association')
        await self.db.execute('DELETE FROM game WHERE link NOT IN (101,105)')
        await self.db.execute('UPDATE game SET n_moves=1')
        for link in (101, 105):
            for fen_id, white in zip(self.fens, (True, False)):
                await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,1,$3)',
                                      link, fen_id, white)
        await self.db.execute('UPDATE cuda_summary_control SET manual=false')
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}),
            connect_args={'server_settings': {'statement_timeout': '5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def scores(self, values, source='stockfish'):
        for fen_id, value in zip(self.fens, values):
            await self.db.execute('''UPDATE fen SET score=$2,analysis_source=$3,analyzed_at=$4,
                tablebase_wdl=NULL,tablebase_dtz=NULL,wdl_win=NULL,wdl_draw=NULL,wdl_loss=NULL
                WHERE fen_id=$1''', fen_id, value, source, PLAYED if source else None)
        async with write_transaction(self.factory) as session:
            await refresh_coverage(session)

    async def raw(self):
        return await self.db.fetch('''SELECT fen_id,score,analysis_source,analyzed_at,
            tablebase_wdl,tablebase_dtz,wdl_win,wdl_draw,wdl_loss
            FROM fen WHERE fen_id=ANY($1::bigint[]) ORDER BY fen_id''', self.fens)

    async def summaries(self):
        before = await self.raw()
        async with write_transaction(self.factory) as session:
            result = await refresh_engine_summaries(session)
        self.assertEqual(result, {'game_players': 4, 'games': 2})
        rows = await self.db.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,is_white')
        self.assertEqual([(r['game_link'], r['is_white']) for r in rows],
                         [(101, False), (101, True), (105, False), (105, True)])
        self.assertEqual(await self.raw(), before)
        self.assertEqual(self.engine.pool.checkedout(), 0)
        for row in rows:
            self.assertEqual(row['analyzed_player_moves'], 1)
            for key in ('analyzed_player_moves', 'mate_for_positions', 'mate_against_positions'):
                self.assertIs(type(row[key]), int)
            for key in ('own_move_cp_gain', 'own_move_cp_loss', 'game_efficiency',
                        'mean_win_percent_loss', 'median_win_percent_loss'):
                self.assertIs(type(row[key]), float)
                self.assertTrue(math.isfinite(row[key]))
            self.assertGreaterEqual(row['game_efficiency'], 0.)
            self.assertLessEqual(row['game_efficiency'], 100.)
        return rows

    async def test_paginated_raw_aggregates_accept_full_integer_range(self):
        for value in (-2147483648, 2147483647, -1500, -1000, -300, 0, 300, 1000, 1500, -9998, 9998):
            with self.subTest(score=value):
                await self.scores((value, value), source=None)
                before = await self.raw()
                for page, link in ((1, 105), (2, 101)):
                    result = await get_scored_game_analysis_page(
                        factory=self.factory, status='fully', page=page, page_size=1)
                    self.assertEqual((result['page'], result['total'], result['total_pages']), (page, 2, 2))
                    self.assertEqual([row['link'] for row in result['rows']], [link])
                    row = result['rows'][0]
                    for key, expected in (('avg_score', value), ('avg_abs_score', abs(value)),
                                          ('max_abs_score', abs(value))):
                        self.assertEqual(row[key], float(expected))
                        self.assertIs(type(row[key]), float)
                self.assertEqual(await self.raw(), before)
                self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_summary_extrema_preserve_both_perspectives_and_legacy_formula(self):
        for value in (-2147483648, 2147483647, 0):
            with self.subTest(score=value):
                await self.scores((value, value), source=None)
                # Unattributed extrema are accepted raw values, not valid mate distances.
                self.assertEqual(score_kind(value, None), 'unknown')
                self.assertIsNone(mate_distance(value, None))
                for row in await self.summaries():
                    sign = 1 if row['is_white'] else -1
                    before = 15 if row['is_white'] else value * sign
                    self.assertAlmostEqual(row['game_efficiency'],
                                           lichess_move_accuracy(before, value * sign), places=9)
                    # Accuracy historically classifies magnitude >=9000 as mate,
                    # even without attribution; do not silently change that policy.
                    self.assertEqual(row['mate_for_positions'], 2 if value * sign >= 9000 else 0)
                    self.assertEqual(row['mate_against_positions'], 2 if value * sign <= -9000 else 0)
                    self.assertEqual(row['final_player_cp'], 0. if value == 0 else None)

    async def test_cp_probability_bounds_do_not_clamp_raw_storage_or_deltas(self):
        for value, bounded in ((-1500, -1000), (-1000, -1000), (-300, -300), (0, 0),
                               (300, 300), (1000, 1000), (1500, 1000)):
            with self.subTest(score=value):
                await self.scores((value, -value))
                self.assertEqual([r['score'] for r in await self.raw()], [value, -value])
                self.assertEqual(lichess_win_percent(value), lichess_win_percent(bounded))
                for row in await self.summaries():
                    before = 15 if row['is_white'] else -value
                    bounded_before = 15 if row['is_white'] else -bounded
                    self.assertAlmostEqual(row['game_efficiency'],
                        lichess_move_accuracy(bounded_before, bounded), places=9)
                    loss = max(0., lichess_win_percent(bounded_before) - lichess_win_percent(bounded))
                    self.assertAlmostEqual(row['mean_win_percent_loss'], loss, places=9)
                    self.assertAlmostEqual(row['median_win_percent_loss'], loss, places=9)
                    self.assertEqual(row['own_move_cp_gain'], float(max(0, value - before)))
                    self.assertEqual(row['own_move_cp_loss'], float(max(0, before - value)))
                    self.assertEqual(row['final_player_cp'], float(-value if row['is_white'] else value))
                    self.assertIs(type(row['final_player_cp']), float)
                    self.assertEqual((row['mate_for_positions'], row['mate_against_positions']), (0, 0))

    async def test_mate_encoding_remains_distinct_from_bounded_cp(self):
        for values, distances in (((-9998, 9997), (2, 3)), ((-10000, 10000), (0, 0))):
            with self.subTest(scores=values):
                await self.scores(values)
                for value, distance in zip(values, distances):
                    self.assertEqual(score_kind(value, 'stockfish'), 'mate')
                    self.assertEqual(mate_distance(value, 'stockfish'), distance)
                for row in await self.summaries():
                    self.assertEqual((row['mate_for_positions'], row['mate_against_positions']), (1, 1))
                    self.assertEqual((row['own_move_cp_gain'], row['own_move_cp_loss']), (0., 0.))
                    self.assertIsNone(row['final_player_cp'])
                    before, after = (15, values[0]) if row['is_white'] else (-values[0], -values[1])
                    self.assertAlmostEqual(row['game_efficiency'], lichess_move_accuracy(before, after), places=9)

    async def test_tablebase_score_and_metadata_remain_separate_from_cp(self):
        await self.scores((1000, -1000))
        # Both fixture positions have Black to move: raw Syzygy WDL/DTZ use
        # that perspective, while stored score and probability WDL use White.
        for fen_id, wdl, dtz, win, loss in ((self.fens[0], -2, -7, 1000, 0),
                                           (self.fens[1], 2, 8, 0, 1000)):
            await self.db.execute('''UPDATE fen SET analysis_source='tablebase',tablebase_wdl=$2,tablebase_dtz=$3,
                wdl_win=$4,wdl_draw=0,wdl_loss=$5 WHERE fen_id=$1''', fen_id, wdl, dtz, win, loss)
        for row in await self.summaries():
            self.assertEqual((row['mate_for_positions'], row['mate_against_positions']), (0, 0))
            self.assertEqual((row['own_move_cp_gain'], row['own_move_cp_loss']), (0., 0.))
            self.assertIsNone(row['final_player_cp'])
            self.assertAlmostEqual(row['game_efficiency'], 100., places=9)
        for row in await self.raw():
            self.assertEqual(score_kind(row['score'], row['analysis_source']), 'tablebase')
            self.assertIsNone(mate_distance(row['score'], row['analysis_source']))
