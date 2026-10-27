"""Offline shadow quotas, three-rule agreement, versioned upserts and stop guards."""
from copy import deepcopy
import unittest
from unittest.mock import AsyncMock, patch

import numpy as np

from chessism_api.operations.research_algorithms import blunder_shadow_source as source
from chessism_api.operations.research_algorithms import blunder_shadow_compute as compute
from chessism_api.operations.research_algorithms import blunder_shadow_store as store
from chessism_api.operations.research_algorithms import blunder_shadow_job as job
from tests.fixed_blunder_fixtures import synthetic


PLAYERS = ['a', 'b', 'c', 'd', 'e', 'f']
PARENT_ID = '00000000-0000-4000-8000-000000000077'
FITS = [{'mode': mode, 'status': 'fitted', 'a': .2, 'b': .4} for mode in ('bullet', 'blitz', 'rapid')]


def parent():
    return {'status': 'complete', 'result': {'status': 'frozen_final_evaluation', 'cpu_cuda_passed': True,
        'canonical': False, 'production_changed': False, 'final_evaluation': {'refitted': False},
        'fits': deepcopy(FITS), 'model_id': 'm'*64}}


def record(game=1, count=1):
    return {'game': game, 'player': 'a', 'mode': 'blitz', 'old15': count, 'baseline20': count,
        'candidate20': count, 'moves': 20, 'unassessed': 0, 'input_sha256': str(count)*64}


def numerical_fixture():
    rows, games = synthetic()
    # The synthetic source is purely numerical, not a real game chronology.
    for i, g in enumerate(games):
        g.update(player=f'p{i}', player_rating=1000+i*1000, opponent_rating=1200+i*800,
                 played_at='2025-01-01T00:00:00+00:00', input_sha256='i'*64)
    return rows, games


class ShadowTests(unittest.TestCase):
    def test_selection_quotas_dedup_and_smoke_spread(self):
        pool = []
        for p, player in enumerate(PLAYERS):
            for m, mode in enumerate(source.QUOTAS):
                for i in range(450):
                    pool.append({'player': player, 'mode': mode, 'link': 1+p*10000+m*1000+i,
                        'total_positions': 30, 'saved_blunders': 1, 'saved_moves': 15})
        result = source.choose(pool, PLAYERS)
        self.assertEqual(len(result), 6000)
        self.assertEqual(len({g['link'] for g in result}), 6000)
        self.assertEqual({g['player'] for g in result[:500]}, set(PLAYERS))
        for p in PLAYERS:
            for mode, count in source.QUOTAS.items():
                self.assertEqual(sum(g['player'] == p and g['mode'] == mode for g in result), count)
        # A game shared by selected profiles has only one assigned owner.
        pool[1350]['link'] = pool[0]['link']
        selected = source.choose(pool, PLAYERS)
        self.assertEqual(len(selected), len({g['link'] for g in selected}))

    def test_shortages_not_padded_and_missing_reference_stops(self):
        pool = [{'player': 'a', 'mode': 'rapid', 'link': 1, 'total_positions': 2, 'saved_moves': 1, 'saved_blunders': 0}]
        self.assertEqual(len(source.choose(pool, PLAYERS)), 1)
        pool[0]['saved_blunders'] = None
        with self.assertRaisesRegex(ValueError, 'missing'): source.choose(pool, PLAYERS)
        with self.assertRaisesRegex(ValueError, 'duplicate'): source.choose(pool*2, PLAYERS)

    def test_three_rules_and_unchanged_mate_handling(self):
        rows, games = numerical_fixture()
        # Nonfinite score is an intentionally unassessed synthetic observation.
        result = compute.calculate(np, rows, games, FITS)
        self.assertEqual(result['status'], 'passed')
        self.assertFalse(any(result['checks'].values()))
        self.assertEqual(result['checked_moves'], len(rows))
        self.assertEqual(len(result['records']), 3)
        self.assertTrue(any(r['category'] == 'rating_removed' for r in result['examples']))
        self.assertTrue(all(r['old15'] == r['baseline20'] == r['candidate20'] == 1
                            for r in result['examples'] if r['category'] == 'forced_mate'))
        altered = deepcopy(games); altered[0]['saved_blunders'] += 1
        self.assertEqual(compute.calculate(np, rows, altered, FITS)['checks']['saved_game_mismatches'], 1)

    def test_same_formula_benchmark_and_empty_blunder_batch(self):
        rows, games = numerical_fixture()
        result = compute.benchmark(np, rows, games, FITS)
        self.assertEqual(result['repeats'], 3)
        self.assertEqual(len(result['cpu_vectorized_seconds']), 3)
        rows['before'] = rows['after'] = 0
        rows['before_kind'] = rows['after_kind'] = 0
        self.assertEqual(compute.vector_summary(np, rows, games, FITS).tolist(), [0, 0, 0])
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            compute.vector_summary(np, rows, games, FITS, lambda: (_ for _ in ()).throw(RuntimeError('cancel')))

    def test_incremental_update_retry_and_rebuild_are_equivalent(self):
        initial, n = store.merge(None, [record()], 'm')
        self.assertEqual(n, 1)
        same, n = store.merge(initial, [record()], 'm')
        self.assertEqual(n, 0); self.assertEqual(same, initial)
        added, _ = store.merge(initial, [record(2, 3)], 'm')
        rebuilt, _ = store.merge(None, [record(), record(2, 3)], 'm')
        self.assertEqual(added['groups'], rebuilt['groups'])
        updated, n = store.merge(added, [record(1, 2)], 'm')
        self.assertEqual(n, 1)
        self.assertEqual(updated['groups'][0]['candidate20'], 5)
        self.assertEqual(updated['groups'][0]['games'], 2)
        self.assertEqual(initial['records'][0]['candidate20'], 1)

    def test_version_identity_and_count_guards(self):
        initial, _ = store.merge(None, [record()], 'm')
        with self.assertRaisesRegex(ValueError, 'version'): store.merge(initial, [record()], 'n')
        with self.assertRaisesRegex(ValueError, 'duplicate'): store.merge(initial, [record(), record()], 'm')
        with self.assertRaisesRegex(ValueError, 'owner'): store.merge(initial, [{**record(), 'player': 'b'}], 'm')
        with self.assertRaisesRegex(ValueError, 'exceed'): store.merge(initial, [record(count=100)], 'm')
        with self.assertRaisesRegex(ValueError, 'counts'): store.merge(initial, [record(count=-1)], 'm')

    def test_no_moves_or_only_unassessed_are_not_zero_blunder_games(self):
        empty = {**record(1, 0), 'moves': 0}
        unsupported = {**record(2, 0), 'moves': 20, 'unassessed': 20}
        supported = record(3, 0)
        saved, _ = store.merge(None, [empty, unsupported, supported], 'm')
        self.assertEqual(saved['summary_version'], 2)
        group = saved['groups'][0]
        self.assertEqual(group['games'], 3)
        self.assertEqual(group['assessed_games'], 1)
        self.assertEqual(group['no_assessed_player_moves'], 2)
        self.assertEqual(group['histograms']['candidate20'], [1, 0, 0, 0, 0, 0, 0])

    def test_config_pins_parent_and_no_tuning_or_publication(self):
        config = job.configuration(PARENT_ID, parent(), PLAYERS)
        self.assertEqual(config['smoke_games'], 500); self.assertEqual(config['max_games'], 6000)
        self.assertEqual(config['threshold'], .2); self.assertEqual(config['k0'], .00368208)
        self.assertFalse(config['canonical']); self.assertFalse(config['independent_calibration_test'])
        with self.assertRaisesRegex(ValueError, 'six'): job.configuration(PARENT_ID, parent(), PLAYERS[:5])
        changed = parent(); changed['result']['cpu_cuda_passed'] = False
        with self.assertRaises(ValueError): job.configuration(PARENT_ID, changed, PLAYERS)
        from chessism_api.routers.research_algorithms import ShadowBlunderRequest
        from pydantic import ValidationError
        args = {'parent_run_id': PARENT_ID, 'players': PLAYERS, 'acknowledgement': 'sampled_shadow_only_no_live_changes'}
        ShadowBlunderRequest(**args)
        with self.assertRaises(ValidationError): ShadowBlunderRequest(**args, threshold=.3)


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_before_shadow_write(self):
        reporter = AsyncMock(); reporter.check.side_effect = RuntimeError('cancel')
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            await store.publish(reporter, 'm', [record()], 0)
