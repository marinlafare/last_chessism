from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from scripts.audit_blunder_data import Audit, Connections, Reporter

POLICY = {'training_before': '2023-01-01T00:00:00+00:00', 'test_from': '2025-01-01T00:00:00+00:00'}


def game(link, year=2022, white='alice', black='bob', mode='bullet', band=3):
    return {'link': link, 'mode': mode, 'played_at': datetime(year, 6, 1, tzinfo=timezone.utc),
            'white_name': white, 'black_name': black, 'total_positions': 40, 'rating_band': band}


class AuditTests(unittest.TestCase):
    def test_historical_roles_and_both_player_holdouts(self):
        a = Audit(POLICY, {'training': [1, 2], 'validation': [3, 4], 'protected': [5]}, set(range(1, 6)))
        rows = [game(1), game(2, 2024), game(3, 2024), game(4), game(5), game(6, black='held'),
                game(7, 2026), game(8, 2026, black='held')]
        with patch('chessism_api.operations.research_algorithms.blunder_balanced_sampling.held_player', side_effect=lambda n: n == 'held'):
            for row in rows: a.add(row)
        r = a.report()
        self.assertEqual([a.counts[s, 'bullet'] for s in range(4)], [1, 1, 1, 1])
        self.assertEqual(r['excluded']['historical_role_conflict'], 2)
        self.assertEqual(r['excluded']['held_player_before_test_date'], 1)
        self.assertEqual(r['excluded']['previous_test_exploration_or_unknown'], 1)
        self.assertFalse(r['all_group_coverage_possible_in_current_pool'])
        self.assertFalse(r['test_predictions_evaluated']); self.assertFalse(r['training_started'])

    def test_count_cap_bound_and_empty_groups(self):
        a = Audit(POLICY, {'training': [], 'validation': [], 'protected': []}, set())
        with patch('chessism_api.operations.research_algorithms.blunder_balanced_sampling.held_player', return_value=False):
            for i in range(100):
                a.add(game(i+1, 2026, white='dominant' if i < 80 else f'w{i}', black=f'b{i}'))
        row = next(r for r in a.report()['groups'] if r['split'] == 'fresh_test' and r['mode'] == 'bullet')
        self.assertEqual(row['player_cap_count_upper_bound'], 22)
        self.assertEqual(row['largest_player_share'], .8)

    def test_components_merge_all_prior_games(self):
        c = Connections()
        c.add({'a', 'b'}); c.add({'c', 'd'}); c.add({'a', 'd'}); c.add({'e', 'f'})
        self.assertEqual(c.report()['components'], 2)
        self.assertEqual(c.report()['largest_component_games'], 3)
        self.assertEqual(c.report()['games'], 4)
        self.assertFalse(c.report()['current_independence_requirement_met'])

    def test_reject_duplicate_budget_and_invalid_roles(self):
        with self.assertRaisesRegex(ValueError, 'roles'):
            Audit(POLICY, {'training': [1], 'validation': [1], 'protected': []}, {1})
        a = Audit(POLICY, {'training': [], 'validation': [], 'protected': []}, set())
        with patch('chessism_api.operations.research_algorithms.blunder_balanced_sampling.held_player', return_value=False):
            a.add(game(1))
            with self.assertRaisesRegex(ValueError, 'Duplicate'): a.add(game(1))
            with patch('scripts.audit_blunder_data.MAX_GAMES', 1), self.assertRaisesRegex(ValueError, 'budget'): a.add(game(2))

    def test_boundary_dates_are_not_shifted(self):
        a = Audit(POLICY, {'training': [], 'validation': [], 'protected': []}, set())
        with patch('chessism_api.operations.research_algorithms.blunder_balanced_sampling.held_player', return_value=False):
            for i, stamp in enumerate(POLICY.values(), 1):
                a.add({**game(i), 'played_at': datetime.fromisoformat(stamp)})
        self.assertEqual(a.counts[0, 'bullet'], 0)
        self.assertEqual(a.counts[1, 'bullet'], 1)
        self.assertEqual(a.counts[2, 'bullet'], 1)


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_cannot_read_scores_for_historical_replay(self):
        with self.assertRaisesRegex(ValueError, 'cannot replay'):
            await Reporter().progress('verifying_development_roles')
