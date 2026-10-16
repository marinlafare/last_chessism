import unittest
from types import SimpleNamespace

from sqlalchemy import text
from chessism_api.database.relational_storage import RelationalStorage,SESSION_KEY
from chessism_api.database.relational_reads import player_rows_sql,player_ratings_sql
from scripts.db_refactor_application_checks import comparable_response,response_differences,rating_read_cases


class RelationalReadTests(unittest.TestCase):
    def setUp(self):
        self.session=SimpleNamespace(info={SESSION_KEY:RelationalStorage('candidate','archive','reads')})

    def test_legacy_text_and_string_are_unchanged(self):
        sql='SELECT count(*) FROM game_player WHERE player_name=:player'
        for query in (sql,text(sql)):
            self.assertEqual(str(player_rows_sql(SimpleNamespace(info={}),query,())),sql)

    def test_count_never_joins_game_or_other_player_names(self):
        sql=str(player_rows_sql(self.session,'SELECT count(*) FROM game_player',()))
        self.assertIn('"candidate"."game_player"',sql)
        self.assertNotIn('JOIN',sql)
        self.assertIn('gp.player_id=',sql)

    def test_projection_contains_only_needed_dimensions(self):
        sql=str(player_rows_sql(self.session,'SELECT result FROM game_player',('result',)))
        self.assertIn('g.white_result ELSE g.black_result',sql)
        self.assertNotIn('game_mode',sql)
        self.assertNotIn('opponent_name',sql)
        self.assertIn('ON g.link=gp.game_link',sql)
        sql=str(player_rows_sql(self.session,text('WITH ranked AS (SELECT mode FROM game_player) SELECT * FROM ranked'),('mode',)))
        self.assertIn('),ranked AS',sql)
        self.assertIn('"candidate"."game_mode"',sql)
        with self.assertRaises(ValueError):
            player_rows_sql(self.session,'SELECT 1',('bad; DROP TABLE game',))

    def test_only_expected_top_level_generated_time_is_excluded(self):
        data={'generated_at':'2026-10-12T20:00:00+00:00','points':[{'generated_at':'saved','score':0.0}],
              'game_time':'2025-01-01'}
        comparable=comparable_response('get_player_engine_insights',data)
        self.assertNotIn('generated_at',comparable)
        self.assertEqual(comparable['points'],data['points'])
        self.assertIn('generated_at',data)
        self.assertIs(comparable_response('get_player_games_page',data),data)
        for timestamp in (None,'not-a-date','2026-10-12T20:00:00'):
            with self.assertRaises(ValueError):
                comparable_response('get_player_engine_insights',dict(data,generated_at=timestamp))

    def test_real_changes_still_fail(self):
        self.assertEqual(response_differences({'v':[1,2]},{'v':[2,1]})[0]['path'],'$.v[0]')
        self.assertEqual(response_differences({'v':1},{'v':1.0})[0]['kind'],'type')
        self.assertEqual(response_differences({'a':1},{}),[{'path':'$.a','kind':'missing_key'}])

    def test_rating_query_filters_before_ranking_and_decodes_after(self):
        legacy=text('SELECT mode, rating FROM game_player')
        self.assertEqual(str(player_ratings_sql(SimpleNamespace(info={}),legacy,
            local_timestamp_sql='g.played_at')),str(legacy))
        sql=str(player_ratings_sql(self.session,legacy,local_timestamp_sql='g.played_at'))
        eligible,daily=sql.split('), daily AS (')
        self.assertIn('eligible AS MATERIALIZED',eligible)
        self.assertIn('gp.player_id=',eligible)
        self.assertIn('g.mode_id = ANY(ARRAY(',eligible)
        self.assertIn('g.played_at >= CAST(:date_from_utc',eligible)
        self.assertIn('g.played_at < CAST(:date_to_utc',eligible)
        self.assertNotIn('JOIN "candidate"."game_mode"',eligible)
        self.assertIn('DISTINCT ON (mode_id, local_date)',daily)
        self.assertIn('link DESC, is_white ASC',daily)
        self.assertIn('FROM daily d JOIN modes',daily)
        self.assertIn('g.black_player_id=(SELECT id FROM owner)',eligible)
        self.assertIn('g.white_player_id=(SELECT id FROM owner)',eligible)
        self.assertIn('UNION ALL',eligible)
        self.assertIn('gp.game_link=g.link AND gp.is_white=g.is_white',eligible)
        self.assertIn('ORDER BY m.name, d.local_date',daily)

    def test_rating_retest_covers_modes_and_calendar_boundaries(self):
        from datetime import date
        cases=rating_read_cases([{'player_name':'alice','cohort':'sample','year':2026}])
        self.assertEqual([row[-1] for row in cases[:4]],
            [('alice',mode) for mode in ('all','bullet','blitz','rapid')])
        self.assertEqual(cases[-1][-1],('alice','all',date(2026,1,1),date(2026,12,31)))


if __name__=='__main__':
    unittest.main()
