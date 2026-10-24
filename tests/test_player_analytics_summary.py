import os
import unittest
from datetime import datetime, timezone

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')

from chessism_api.operations.player_analytics_summary import summarize_day, combine_partitions, played_hours
from chessism_api.operations.player_catalog_snapshot import rank_catalog
from chessism_api.operations.player_timezone import resolve_player_timezone


class PlayerSummaryTests(unittest.TestCase):
    def test_hours_round_like_postgresql(self):
        self.assertEqual(played_hours(18), 0.01)
        self.assertEqual(played_hours(54), 0.02)
        self.assertEqual(played_hours(0), 0)

    def row(self, **values):
        return {'mode': 'blitz', 'time_elapsed': 360, 'played_at': datetime(2025, 3, 9, 6, 59, tzinfo=timezone.utc),
                'result': 1, 'rating': 2003, 'link': 1, 'color': 'white',
                'opponent_name': 'bob', 'eco': 'https://www.chess.com/openings/Test-Opening', **values}

    def test_partition_merging_and_sparse_ratings(self):
        zone = resolve_player_timezone({'timezone': 'America/New_York'})
        first = summarize_day([self.row()], zone)
        second = summarize_day([self.row(link=2, result=0.5, rating=2012,
                    played_at=datetime(2025, 3, 9, 7, 1, tzinfo=timezone.utc))], zone)
        result = combine_partitions([first, second])
        self.assertEqual(result['hours']['modes']['blitz'], {'games': 2, 'seconds': 720})
        self.assertEqual([row['hour'] for row in result['activity']['rows']], [1, 3])
        self.assertEqual(len(result['ratings']['rows']), 1)
        self.assertEqual(result['ratings']['rows'][0]['rating'], 2012)
        self.assertEqual(result['openings']['rows'][0]['key'], 'Test-Opening')
        self.assertEqual(result['opponents']['rows'][0]['draws'], 1)

    def test_unknown_results_null_dates_and_modes(self):
        zone = resolve_player_timezone({'timezone': 'UTC'})
        result = combine_partitions([summarize_day([
            self.row(played_at=None, result=None, rating=None), self.row(mode=None, time_elapsed=None),
        ], zone)])
        self.assertEqual(result['distribution']['counts'][0]['rating'], None)
        self.assertEqual(result['activity']['rows'][0]['mode'], None)
        self.assertEqual(result['ratings']['rows'], [])
        ranked = rank_catalog(result['opponents']['rows'], 'all', 'all')
        self.assertEqual(ranked[0]['unknown'], 1)

    def test_selfplay_perspectives_and_rating_ties(self):
        zone = resolve_player_timezone({'timezone': 'UTC'})
        result = combine_partitions([summarize_day([
            self.row(opponent_name='alice'), self.row(color='black', opponent_name='alice', rating=1999, result=0),
        ], zone)])
        self.assertEqual(result['hours']['modes']['blitz']['games'], 2)
        self.assertEqual(result['ratings']['rows'][0]['rating'], 1999)
        self.assertEqual(rank_catalog(result['opponents']['rows'], 'blitz', 'all')[0]['games'], 2)
        self.assertEqual(rank_catalog(result['opponents']['rows'], 'blitz', 'white')[0]['wins'], 1)


if __name__ == '__main__':
    unittest.main()
