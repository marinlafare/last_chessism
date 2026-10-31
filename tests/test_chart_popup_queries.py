import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations import player_score_position_page as pages
from chessism_api.operations.player_blunder_page import blunder_rows_sql


def result(rows):
    value = MagicMock()
    value.mappings.return_value.all.return_value = rows
    value.all.return_value = rows
    return value


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.session = SimpleNamespace(info={SESSION_KEY: RelationalStorage('core', 'archive', 'reads')})

    def test_bound_occurrences_before_scores_no_frequency_ranking(self):
        sql = str(pages.occurrences_sql(self.session))
        self.assertIn('source AS MATERIALIZED', sql)
        self.assertEqual(sql.count('LIMIT :scan_limit'), 2)
        self.assertNotIn('GROUP BY', sql)
        self.assertNotIn('COUNT(', sql)
        self.assertNotIn('f.fen,', sql)
        self.assertLess(sql.index('LIMIT :scan_limit'), sql.index('JOIN "core"."fen"'))

    def test_cursor_scope_and_large_integer_id(self):
        cursor = pages.encode_cursor('scope', 2**62, False, 2**60)
        self.assertEqual(pages.decode_cursor(cursor, 'scope'), (2**62, False, 2**60))
        with self.assertRaises(ValueError):
            pages.decode_cursor(cursor, 'another-bin')
        with self.assertRaises(ValueError):
            pages.decode_cursor('bad', 'scope')

    def test_color_filter_precedes_game_limit_and_scopes_cursor(self):
        for session, predicate in [(self.session, "gp.is_white=(:color='white')"),
                                   (SimpleNamespace(info={}), 'color=:color')]:
            sql = str(pages.candidate_games_sql(session))
            self.assertLess(sql.index(predicate), sql.index('LIMIT :game_limit'))
        scopes = [pages.scope_key('alice', ['blitz'], 5, color) for color in [None, 'white', 'black']]
        self.assertEqual(len(set(scopes)), 3)
        cursor = pages.encode_cursor(scopes[1], 10, True, 20)
        with self.assertRaises(ValueError):
            pages.decode_cursor(cursor, scopes[2])

    def test_blunders_read_owner_index_and_gpu_not_full_engine_view(self):
        sql = blunder_rows_sql(self.session, 'blunder_count >= :blunder_count')
        self.assertIn('white_owner=:player AND white_complete', sql)
        self.assertIn('black_owner=:player AND black_complete', sql)
        self.assertIn('white_count >= :blunder_count', sql)
        self.assertIn('model_id=:model', sql)
        self.assertNotIn('canonical_blunder_engine', sql)
        self.assertNotIn('saved.blunder_count', sql)
        self.assertIn('UNION ALL', sql)
        self.assertIn('game_player_id=gp.id', sql)


class PageTests(unittest.IsolatedAsyncioTestCase):
    def session(self, records, games=None):
        session = SimpleNamespace(info={}, scalar=AsyncMock(return_value='alice'), execute=AsyncMock())
        session.execute.side_effect = [result(games or [{'game_link': 10, 'is_white': True}]), result(records),
            result([(r['fen_id'], 'board '+str(r['fen_id'])) for r in records])]
        return session

    def row(self, id, score=0, white=True, source='stockfish'):
        return dict(game_link=10, is_white=white, occurrence_id=10000-id, fen_id=id,
                    score=score, analysis_source=source)

    async def test_returns_ten_and_seeks_after_last_matching_occurrence(self):
        session = self.session([self.row(i) for i in range(1, 30)])
        page = await pages.read_position_page(session, 'alice', ['blitz'],
            dict(id=6,lower=-50,upper=50,lower_inclusive=True,upper_inclusive=True))
        self.assertEqual(len(page['rows']), 10)
        self.assertEqual(pages.decode_cursor(page['next_cursor'], page['snapshot']), (10, True, 9990))
        self.assertNotIn('appearances', page['rows'][0])
        self.assertEqual(session.execute.call_args.args[1]['ids'], list(range(1,11)))

    async def test_score_kind_perspective_boundaries_and_duplicate_fens(self):
        rows = [self.row(1, 50), self.row(2, -50), self.row(3,51), self.row(4,None),
                self.row(5,0,source='tablebase'), self.row(6,9998), self.row(7,-100,False),
                self.row(7,-100,False), self.row(8,150,True)]
        session = self.session(rows)
        page = await pages.read_position_page(session, 'alice', ['blitz'],
            dict(id=7,lower=50,upper=150,lower_inclusive=False,upper_inclusive=True))
        self.assertEqual([r['fen_id'] for r in page['rows']], ['3','7','8'])
        self.assertEqual(page['rows'][1]['score_min'],100)
        self.assertIsNone(page['next_cursor'])

    async def test_sparse_batch_returns_cursor_without_scanning_entire_player(self):
        session = self.session([self.row(i,1000) for i in range(1,514)])
        page = await pages.read_position_page(session, 'alice', ['blitz'],
            dict(id=6,lower=-50,upper=50,lower_inclusive=True,upper_inclusive=True))
        self.assertEqual(page['rows'], [])
        self.assertEqual(page['scanned_positions'],512)
        self.assertEqual(pages.decode_cursor(page['next_cursor'],page['snapshot']), (10,True,9488))
        self.assertEqual(session.execute.await_count,2)

    async def test_negative_interval_is_player_relative_for_both_colors(self):
        interval = dict(id=5, lower=-150, upper=-50, lower_inclusive=True, upper_inclusive=False)
        for color, white, sign in [('white', True, 1), ('black', False, -1)]:
            session = self.session([self.row(i, score * sign, white) for i, score in
                                    enumerate([-151, -150, -100, -51, -50, 100], 1)])
            page = await pages.read_position_page(session, 'alice', ['blitz'], interval, color=color)
            self.assertEqual([r['score_min'] for r in page['rows']], [-150, -100, -51])
            self.assertTrue(all(r['player_color'] == color for r in page['rows']))
            self.assertEqual(page['score_perspective'], 'player')
            self.assertEqual(session.execute.call_args_list[0].args[1]['color'], color)

    async def test_same_fen_with_opposite_player_colors_keeps_each_perspective(self):
        session = self.session([self.row(1, 25, True), self.row(1, 25, False), self.row(1, 25, False)])
        page = await pages.read_position_page(session, 'alice', ['blitz'],
            dict(id=6, lower=-50, upper=50, lower_inclusive=True, upper_inclusive=True))
        self.assertEqual([(r['player_color'], r['score_min']) for r in page['rows']],
                         [('white', 25), ('black', -25)])

    async def test_invalid_color_rejected_before_database_access(self):
        session = self.session([])
        with self.assertRaises(ValueError):
            await pages.read_position_page(session, 'alice', ['blitz'], {'id': 5}, color='red')
        session.scalar.assert_not_called()

    async def test_empty_games_advance_without_getting_stuck(self):
        games = [dict(game_link=i,is_white=True) for i in range(100,67,-1)]
        session = self.session([], games)
        page = await pages.read_position_page(session,'alice',['blitz'],
            dict(id=6,lower=-50,upper=50,lower_inclusive=True,upper_inclusive=True))
        self.assertEqual(pages.decode_cursor(page['next_cursor'],page['snapshot']), (69,True,0))


if __name__ == '__main__':
    unittest.main()
