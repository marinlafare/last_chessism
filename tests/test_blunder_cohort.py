from copy import deepcopy
import unittest

import numpy as np

from chessism_api.operations.research_algorithms import blunder_cohort_compute as compute
from chessism_api.operations.research_algorithms import blunder_cohort_job as job
from chessism_api.operations.research_algorithms.blunder_cohort_batch import SharedBatch
from chessism_api.operations.research_algorithms import blunder_threshold_compare as audit
from tests.test_blunder_shadow import numerical_fixture, FITS, PARENT_ID


class CohortTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows, cls.games = numerical_fixture()
        cls.result = compute.calculate(np, cls.rows, cls.games, FITS)

    def test_gpu_contract_and_scalar_reference(self):
        self.assertEqual(self.result['status'], 'passed')
        self.assertFalse(any(self.result['checks'].values()))
        from chessism_api.operations.research_algorithms.blunder_shadow_compute import calculate
        previous = calculate(np, self.rows, self.games, FITS)['records']
        paired = audit.calculate(np, self.rows, self.games, FITS, previous)
        for a, b in zip(self.result['records'], paired['records']):
            for key in (*compute.FIELDS, 'moves', 'unassessed'):
                self.assertEqual(a[key], b[key])
            for key in compute.CHANGES: self.assertEqual(a[key], b['changes'][key])

    def test_grouping_histograms_and_merges(self):
        all_groups = compute.Groups(); all_groups.add(self.result['records'])
        split = compute.Groups()
        for record in self.result['records']: split.add([record])
        self.assertEqual(all_groups.report(), split.report())
        for mode, g in all_groups.modes.items():
            self.assertTrue(all(sum(h) == g['assessed_games'] for h in g['histograms'].values()))
            self.assertEqual(g['candidate15']-g['old15'], g['added_vs_old']-g['still_removed'])
        merged = job.aggregate_players([all_groups.report(), split.report()])
        self.assertEqual(merged['by_mode']['bullet']['candidate15'], 2*all_groups.modes['bullet']['candidate15'])

    def block(self, index):
        rows = self.rows[self.rows['game'] == index][:20].copy(); rows['game'] = 0
        game = deepcopy(self.games[index]); game['saved_moves'] = len(rows)
        game['saved_blunders'] = int((rows['sql'] == 1).sum())
        return rows, [game]

    def test_shared_batch_across_players_and_same_game_identity(self):
        a, ga = self.block(0); b, gb = self.block(1)
        # Shared game appears once in each player's own perspective, not lost
        # through global game-ID deduplication.
        gb[0]['link'] = ga[0]['link']; b['link'] = gb[0]['link']
        batch = SharedBatch(np, FITS, target_rows=40)
        batch.append(a, ga); self.assertFalse(batch.ready)
        batch.append(b, gb); self.assertTrue(batch.ready)
        result = batch.flush()
        self.assertEqual(result['batch']['players'], [ga[0]['player'], gb[0]['player']])
        self.assertEqual(len(result['records']), 2)
        self.assertEqual(result['records'], compute.calculate(np, a, ga, FITS)['records']+compute.calculate(np, b, gb, FITS)['records'])
        self.assertFalse(batch.ready); self.assertIsNone(batch.flush())

    def test_shared_buffer_bound_tail_and_zero_move_games(self):
        rows, games = self.block(0)
        batch = SharedBatch(np, FITS, target_rows=1)
        batch.append(rows, games)
        with self.assertRaisesRegex(ValueError, 'Flush'): batch.append(rows, games)
        self.assertEqual(len(batch.flush()['records']), 1)
        empty = deepcopy(games[0]); empty.update(saved_moves=0, saved_blunders=0)
        batch.append(rows[:0], [empty]); result = batch.flush()
        groups = compute.Groups(); groups.add(result['records'])
        g = groups.modes[empty['mode']]
        self.assertEqual(g['games'], 1); self.assertEqual(g['assessed_games'], 0)
        self.assertEqual(sum(g['saved_scope_histogram']), 0)
        with self.assertRaises(ValueError): SharedBatch(np, FITS, target_rows=1000000)

    def test_frozen_config_and_endpoint_contract(self):
        row = {'status': 'complete', 'result': {'format': audit.FORMAT, 'status': 'passed',
            'canonical': False, 'production_changed': False, 'refitted': False,
            'checks': {'new_cpu_move': 0}, 'parent_reference_checks': {'saved': 0}, 'fits': FITS, 'model_id': 'frozen'}}
        config = job.configuration(PARENT_ID, row)
        self.assertEqual(config['threshold'], .15); self.assertEqual(config['gpu_batch_rows'], 262144)
        self.assertFalse(config['canonical']); self.assertIsNone(config['resume_run_id'])
        bad = deepcopy(row); bad['result']['refitted'] = True
        with self.assertRaises(ValueError): job.configuration(PARENT_ID, bad)
        from chessism_api.routers.research_algorithms import CohortBlunderRequest
        from pydantic import ValidationError
        args = dict(parent_run_id=PARENT_ID, acknowledgement='all_player_shadow_summaries_no_live_changes')
        CohortBlunderRequest(**args)
        with self.assertRaises(ValidationError): CohortBlunderRequest(**args, threshold=.2)

    def test_cancellation(self):
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            compute.calculate(np, self.rows, self.games, FITS, lambda: (_ for _ in ()).throw(RuntimeError('cancel')))
