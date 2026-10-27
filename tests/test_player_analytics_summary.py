import os
import unittest
from datetime import datetime, timezone

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')

from chessism_api.operations.player_analytics_summary import summarize_day, combine_partitions, played_hours
from chessism_api.operations.player_catalog_snapshot import catalog_ratio_bin, rank_catalog
from chessism_api.operations.player_timezone import resolve_player_timezone


class PlayerSummaryTests(unittest.TestCase):
    def test_hours_round_like_postgresql(self):
        self.assertEqual(played_hours(18), 0.01)
        self.assertEqual(played_hours(54), 0.02)
        self.assertEqual(played_hours(0), 0)

    def test_catalog_outcome_bins_have_deterministic_ties(self):
        def row(key, games, wins, losses, draws, **values):
            return dict(key=key, games=games, wins=wins, losses=losses, draws=draws,
                        mode='blitz', color='white', **values)
        rows = [row('frequent', 100, 40, 50, 10), row('winning', 90, 70, 10, 10),
                row('drawing', 80, 10, 10, 60), row('perfect-rate', 3, 3, 0, 0),
                row('tied-b', 90, 70, 10, 10), row('tied-a', 90, 70, 10, 10),
                row('perfect-loss', 2, 0, 2, 0), row('perfect-draw', 2, 0, 0, 2)]
        for sort, first in [('games', 'frequent'), ('wins', 'perfect-rate'), ('losses', 'perfect-loss'), ('draws', 'perfect-draw')]:
            ranked = rank_catalog(rows, 'all', 'all', sort)
            self.assertEqual(ranked[0]['key'], first)
            scores = [item['games'] for item in ranked]
            self.assertEqual(scores, sorted(scores, reverse=True))
            self.assertEqual([item['rank'] for item in ranked], list(range(1, len(ranked) + 1)))
        self.assertEqual([item['key'] for item in rank_catalog(rows, 'all', 'all', 'wins', ratio_bin=7)],
                         ['tied-a', 'tied-b', 'winning'])
        self.assertEqual(rank_catalog(rows, 'rapid', 'all', 'wins'), [])
        self.assertEqual(rank_catalog(rows, 'all', 'black', 'wins'), [])
        with self.assertRaises(ValueError):
            rank_catalog(rows, 'all', 'all', 'wins; DROP TABLE game')

    def test_catalog_bin_pools_games_and_orders_by_ratio_not_sample_size(self):
        def row(key, games, wins, mode='blitz'):
            return dict(key=key, games=games, wins=wins, losses=0, draws=0, mode=mode, color='white')
        rows = [row('mixed', 1, 1), row('mixed', 99, 0, 'bullet'), row('larger', 40, 20), row('smaller', 20, 11)]
        self.assertEqual([item['key'] for item in rank_catalog(rows, 'all', 'all', 'wins', ratio_bin=5)], ['smaller', 'larger'])
        self.assertEqual([item['key'] for item in rank_catalog(rows, 'all', 'all', 'wins', ratio_bin=0)], ['mixed'])
        self.assertEqual(rank_catalog([row('empty', 0, 0)], 'all', 'all', 'wins'), [])

    def test_catalog_minimum_applies_to_selected_groups_before_ranking(self):
        def row(key, games=1, mode='blitz', color='white'):
            return dict(key=key, games=games, wins=games, losses=0, draws=0, mode=mode, color=color)
        rows = [row('single'), row('two', 2), row('mixed-modes'), row('mixed-modes', mode='bullet'),
                row('mixed-colors'), row('mixed-colors', color='black')]
        for sort in ('games', 'wins', 'losses', 'draws'):
            options = dict(min_games=2, ratio_bin=None if sort == 'games' else 9 if sort == 'wins' else 0)
            ranked = rank_catalog(rows, 'all', 'all', sort, **options)
            self.assertEqual([item['key'] for item in ranked], ['mixed-colors', 'mixed-modes', 'two'])
            self.assertEqual([item['rank'] for item in ranked], [1, 2, 3])
            ranked = rank_catalog(rows, 'all', 'white', sort, **options)
            self.assertEqual([item['key'] for item in ranked], ['mixed-modes', 'two'])
            ranked = rank_catalog(rows, 'blitz', 'white', sort, **options)
            self.assertEqual([item['key'] for item in ranked], ['two'])
            self.assertEqual(rank_catalog([row('single')], 'all', 'all', sort, min_games=2), [])
        # The low-level grouping helper can still be used without the list minimum.
        self.assertEqual(rank_catalog([row('single')], 'all', 'all')[0]['games'], 1)

    def test_catalog_bins_cover_all_percentages_once_with_exact_boundaries(self):
        for outcome in ('wins', 'losses', 'draws'):
            rows = [dict(key=str(count), games=100, wins=0, losses=0, draws=0, mode='blitz', color='white')
                    for count in range(101)]
            for count, row in enumerate(rows):
                row[outcome] = count
            seen = []
            for ratio_bin in range(10):
                ranked = rank_catalog(rows, 'all', 'white', outcome, ratio_bin=ratio_bin)
                actual = sorted(int(row['key']) for row in ranked)
                expected = list(range(ratio_bin * 10, ratio_bin * 10 + (11 if ratio_bin == 9 else 10)))
                self.assertEqual(actual, expected)
                seen.extend(actual)
            self.assertEqual(sorted(seen), list(range(101)))
        rows = [dict(key=str(wins), games=1000000, wins=wins, losses=0, draws=0, mode='blitz', color='white')
                for wins in (899999, 900000)]
        self.assertEqual([r['key'] for r in rank_catalog(rows, 'all', 'all', 'wins', ratio_bin=8)], ['899999'])
        self.assertEqual([r['key'] for r in rank_catalog(rows, 'all', 'all', 'wins', ratio_bin=9)], ['900000'])

    def test_catalog_bins_reject_invalid_selections(self):
        self.assertEqual(catalog_ratio_bin('wins', None), 9)
        for value in (-1, 10, 1.5, '9', True):
            with self.assertRaises(ValueError):
                catalog_ratio_bin('wins', value)
        with self.assertRaises(ValueError):
            catalog_ratio_bin('games', 9)

    def test_ponziani_example_percentage_precedes_game_count_in_each_outcome(self):
        for outcome in ('wins', 'losses', 'draws'):
            rows = []
            for key, games, count in [('Ponziani', 10, 9), ('Trompowsky', 5, 5), ('Alekhine', 5, 5),
                                      ('Larger-perfect', 20, 20), ('One-game', 1, 1)]:
                row = dict(key=key, games=games, wins=0, losses=0, draws=0, mode='blitz', color='white')
                row[outcome] = count
                rows.append(row)
            ranked = rank_catalog(rows, 'all', 'white', outcome, min_games=2, ratio_bin=9)
            self.assertEqual([row['key'] for row in ranked], ['Larger-perfect', 'Alekhine', 'Trompowsky', 'Ponziani'])
            self.assertEqual([row['rank'] for row in ranked], [1, 2, 3, 4])

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
