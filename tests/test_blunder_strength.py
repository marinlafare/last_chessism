from collections import Counter
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np

from chessism_api.operations.research_algorithms import blunder_strength_compute as compute
from chessism_api.operations.research_algorithms import blunder_strength_source as source
from chessism_api.operations.research_algorithms import blunder_strength_job as job
from chessism_api.operations.research_algorithms import blunder_fixed as old
from chessism_api.operations.research_algorithms import blunder_v2_model as model
from chessism_api.operations.research_algorithms.blunder_shadow_compute import context
from tests.test_blunder_shadow import numerical_fixture, FITS, PARENT_ID


def fixture():
    full, games = numerical_fixture()
    rows = np.concatenate((full[:600], full[-16:]))
    v = context(games, FITS); ids = rows['game']
    flag = model.decisions(np, *[rows[k] for k in ('before', 'after', 'before_kind', 'after_kind')],
        *[v[k][ids] for k in ('u', 'd', 'a', 'b')], threshold=.15)
    for i, g in enumerate(games):
        g.update(outcome=[1., .5, 0.][i], saved_moves=int((ids == i).sum()),
            saved_blunders=int(((ids == i) & (rows['sql'] == 1)).sum()),
            cohort_candidate15=int(((ids == i) & (flag == 1)).sum()))
    return rows, games


def sample_record(game, player='alice', rating=1200, opponent=1300):
    return {'game': game, 'player': player, 'mode': 'bullet', 'played_at': '2024-01-01T00:00:00+00:00',
        'player_rating': rating, 'opponent_rating': opponent, 'is_white': True,
        'moves': 20, 'old15': 2, 'candidate15': 1}


class StrengthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows, cls.games = fixture()
        cls.result = compute.calculate(np, cls.rows, cls.games, FITS)

    def test_all_candidates_use_fixed_constants_and_agree(self):
        r = self.result
        self.assertEqual(r['status'], 'passed'); self.assertFalse(any(r['checks'].values()))
        self.assertLess(r['max_prediction_error'], 1e-10)
        self.assertLess(r['max_metric_error'], 1e-10)
        self.assertEqual(len(r['candidates']), 8)
        self.assertTrue(all(g['counts']['coefficient_only'] == g['old15'] for g in r['records']))
        self.assertEqual(r['gpu_batches'][0]['players'], ['p0', 'p1', 'p2'])

    def test_fixed_formula_examples_and_mates(self):
        losses = []
        for rating in (800, 2300, 3000):
            u = (rating-2300)/400
            losses.append(compute.expected_scalar(0, u, 0, .2472667694091797, 2.3239002227783203)
                          -compute.expected_scalar(-200, u, 0, .2472667694091797, 2.3239002227783203))
        self.assertLess(losses[0], .15); self.assertGreater(losses[1], .15); self.assertGreater(losses[2], losses[1])
        for _, _, a, b in compute.CANDIDATES:
            self.assertEqual(int(model.decisions(np, 105., -9998., 0, 1, -4., -3., a, b, threshold=.15)), 1)
        self.assertEqual(self.result['checks']['mate_rule_mismatches'], 0)

    def test_equal_game_calibration_weights_and_draw(self):
        group = next(g for g in self.result['groups'] if g['kind'] == 'all' and g['candidate'] == 'coefficient_only')
        losses = []
        for i, game in enumerate(self.games):
            scores = self.rows['after'][(self.rows['game'] == i) & (self.rows['after_kind'] == 0)]
            predictions = model.expected(np, scores, 0, 0, 0, 0)
            losses.append(float(np.mean((predictions-game['outcome'])**2)))
        self.assertAlmostEqual(group['mse'], np.mean(losses))
        self.assertAlmostEqual(sum(b['game_weight'] for b in group['bins']), 3.)
        self.assertTrue(all(sum(g['histogram']) == g['assessed_games'] for g in self.result['groups']))

    def test_partitioned_batches_identical_and_bad_references_detected(self):
        with patch.object(compute, 'BATCH_ROWS', 75):
            r = compute.calculate(np, self.rows, self.games, FITS)
        self.assertEqual(r['records'], self.result['records']); self.assertEqual(r['status'], 'passed')
        altered = deepcopy(self.games); altered[0]['cohort_candidate15'] += 1
        self.assertEqual(compute.calculate(np, self.rows, altered, FITS)['checks']['frozen_candidate_mismatches'], 1)
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            compute.calculate(np, self.rows, self.games, FITS, lambda: (_ for _ in ()).throw(RuntimeError('cancel')))

    def test_sampling_deterministic_capped_and_unique_across_owners(self):
        records = [sample_record(i+1, p, opponent=1200+(i%3)*300) for p in ('alice', 'bob', 'carl') for i in range(400)]
        records += [sample_record(5000+i, 'alice', rating=2800) for i in range(80)]
        results = []
        with patch.object(source, 'held_player', return_value=False):
            for sequence in (records, list(reversed(records))):
                pool = source.Pool({1, 2, 3}, '2025-01-01T00:00:00+00:00')
                for r in sequence: pool.add(r)
                chosen, audit = pool.choose(); results.append(chosen)
                self.assertEqual(len(chosen), len({r['game'] for r in chosen}))
                self.assertFalse({1, 2, 3} & {r['game'] for r in chosen})
                self.assertLessEqual(max(Counter((r['player'], r['mode'], source.band(r['player_rating'])) for r in chosen).values()), 50)
                self.assertTrue(any(c['selected'] < c['target'] for c in audit['cells_before_metadata_checks']))
        self.assertEqual(results[0], results[1])

    def test_future_dates_held_owners_and_outcomes_do_not_influence_sample(self):
        with patch.object(source, 'held_player', side_effect=lambda n:n == 'held'):
            pool = source.Pool(set(), '2025-01-01T00:00:00+00:00')
            for r in [sample_record(1), sample_record(2, 'held'), {**sample_record(3), 'played_at': '2026-01-01T00:00:00+00:00'}]: pool.add(r)
            self.assertEqual([r['game'] for r in pool.choose()[0]], [1])
            self.assertEqual(pool.skipped['held_out_owner'], 1)

    def test_artifact_checksums_paths_and_decoded_integrity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); (root/'blunder-cohorts').mkdir()
            path = root/'blunder-cohorts'/'alice.gz'
            header = {'player': 'alice', 'model_id': 'm', 'threshold': .15, 'revision': 1, 'parent_report_sha256': 'p'}
            raw = b''.join((json.dumps(r)+'\n').encode() for r in (header, sample_record(1)))
            with gzip.open(path, 'wb') as f: f.write(raw)
            p = {'player': 'alice', 'revision': 1, 'artifact': {'relative_path': 'blunder-cohorts/alice.gz',
                'bytes': path.stat().st_size, 'sha256': source.file_sha(path), 'records': 1,
                'decoded_sha256': hashlib.sha256(raw).hexdigest()}}
            parent = {'model_id': 'm', 'config': {'parent_report_sha256': 'p'}}
            with patch.object(source, 'WORK_ROOT', root/'tmp'), patch.object(source, 'held_player', return_value=False):
                pool = source.Pool(set(), '2025-01-01T00:00:00+00:00')
                source.read_artifact(p, parent, pool, lambda:None)
                self.assertEqual(len(pool.choose()[0]), 1)
                p['artifact']['decoded_sha256'] = 'bad'
                with self.assertRaisesRegex(ValueError, 'Decoded'): source.read_artifact(p, parent, pool, lambda:None)

    def test_config_refuses_tuning_publication_and_stale_sources(self):
        row = {'status': 'complete', 'result': {'format': 'chessism_blunder_cohort_report_v1', 'status': 'passed',
            'canonical': False, 'production_changed': False, 'config': {'threshold': .15}, 'stale_players': [],
            'cohort_membership_changed': False, 'players': [{}], 'inventory': [{}], 'checks': {'numerical': 0},
            'model_id': 'm', 'fits': FITS}}
        config = job.configuration(PARENT_ID, row, {'version': 'development_reuse_v1', 'references': ['r']})
        self.assertEqual(config['threshold'], .15); self.assertEqual(config['k0'], .00368208)
        self.assertEqual(len(config['candidates']), 8); self.assertFalse(config['canonical'])
        from chessism_api.routers.research_algorithms import StrengthBlunderRequest
        from pydantic import ValidationError
        args = dict(parent_run_id=PARENT_ID, acknowledgement='compare_rating_strengths_only_no_live_changes')
        StrengthBlunderRequest(**args)
        with self.assertRaises(ValidationError): StrengthBlunderRequest(**args, threshold=.2)
        row['result']['stale_players'] = ['alice']
        with self.assertRaises(ValueError): job.configuration(PARENT_ID, row, config['sampling_policy'])
        with self.assertRaises(ValueError): job.verify_revisions({'inventory': [{'player_name': 'a', 'revision': 1}]}, [{'player_name': 'a', 'revision': 2}])


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_or_cancelled_reservations_are_protected(self):
        r = {'format': source.RESERVATION, 'source': {'inspected_game_ids': [1, 2], 'inspected_game_ids_sha256': source.history.digest([1, 2])}}
        result = MagicMock(); result.scalars.return_value.all.return_value = [r]
        session = AsyncMock(); session.execute.return_value = result
        self.assertEqual(await source.exploration_ids(session), {1, 2})
        r['source']['inspected_game_ids'].append(3)
        with self.assertRaisesRegex(ValueError, 'checksum'): await source.exploration_ids(session)

    async def test_explored_games_enter_old_history_exclusions(self):
        from tests.test_blunder_development_reuse import result_row, session_for, REF
        with patch.object(source, 'exploration_ids', AsyncMock(return_value={8, 9})):
            roles, seen, _ = await source.history.history(session_for([result_row()]), source.history.plan([REF]), AsyncMock())
        self.assertTrue({8, 9} <= set(roles['protected'])); self.assertTrue({8, 9} <= seen)
