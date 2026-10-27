"""Offline role lineage and final-test isolation tests."""
import copy
import hashlib
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from chessism_api.operations.research_algorithms import blunder_development_reuse as reuse
from chessism_api.operations.research_algorithms import blunder_v2_job as job, blunder_refinement
from tests.test_blunder_balanced import REF


def frozen(roles=None):
    known = list(range(1, 8))
    value = {'sha256': REF['sha256'], 'positions': 70, 'selected_games': 6,
        'split': {k: REF[k] for k in ('training_before', 'test_from')},
        'seen_game_ids': known, 'seen_game_ids_sha256': hashlib.sha256(json.dumps(known).encode()).hexdigest(),
        'game_ids_by_split': {'0': [1, 2], '1': [3, 4], '2': [5], '3': [6]}}
    if roles is not None:
        value.update(development_roles=roles, development_roles_sha256=reuse.digest(roles))
    return value


def result_row(value=None, status='complete', fmt=reuse.REPORT_FORMAT):
    return {'status': status, 'config': job.configuration(1000), 'result': {'format': fmt, 'source': value or frozen()}}


def session_for(rows):
    session = AsyncMock()
    results = []
    for row in rows:
        result = MagicMock(); result.mappings.return_value.one_or_none.return_value = row
        results.append(result)
    empty = MagicMock(); empty.scalars.return_value.all.return_value = []
    results.append(empty)
    results.append(empty)
    session.execute.side_effect = results
    return session


class RoleTests(unittest.TestCase):
    def test_conflicts_unknowns_and_test_roles_are_protected(self):
        roles = reuse.manifest({1, 2, 6}, {2, 3, 5}, {5, 6}, set(range(1, 8)))
        self.assertEqual(roles, {'training': [1], 'validation': [3], 'protected': [2, 4, 5, 6, 7]})

    def test_manifest_verification_corruption_and_overlap(self):
        roles = {'training': [1, 2], 'validation': [3, 4], 'protected': [5, 6, 7]}
        value = frozen(roles)
        self.assertEqual(reuse.verified_manifest(value)[0]['training'], {1, 2})
        altered = copy.deepcopy(value); altered['development_roles']['protected'].remove(7)
        with self.assertRaisesRegex(ValueError, 'corrupt'): reuse.verified_manifest(altered)
        altered['development_roles']['protected'].append(1)
        altered['development_roles_sha256'] = reuse.digest(altered['development_roles'])
        with self.assertRaisesRegex(ValueError, 'disjoint'): reuse.verified_manifest(altered)
        altered = copy.deepcopy(value); altered['seen_game_ids'].append(8)
        with self.assertRaisesRegex(ValueError, 'corrupt'): reuse.verified_manifest(altered)

    def test_role_audit_allows_only_same_role_development(self):
        roles = {'training': [1], 'validation': [2], 'protected': [3, 4]}
        prior = {1, 2, 3, 4}
        selected = [{'link': 1, 'split': 0}, {'link': 2, 'split': 1}, {'link': 5, 'split': 2}, {'link': 6, 'split': 3}]
        audit = reuse.audit_selection(selected, roles, prior)
        self.assertEqual(audit['reused_training_games'], 1)
        self.assertEqual(audit['reused_validation_games'], 1)
        self.assertEqual(audit['test_prior_overlap'], 0)
        for item in ({'link': 1, 'split': 1}, {'link': 2, 'split': 0}, {'link': 3, 'split': 0},
                     {'link': 4, 'split': 2}, {'link': 1, 'split': 2}, {'link': 2, 'split': 3}):
            with self.assertRaisesRegex(ValueError, 'leakage'):
                reuse.audit_selection([item], roles, prior)

    def test_partition_manifest_corruption_and_duplicates(self):
        value = frozen(); value['game_ids_by_split']['2'].append(1)
        with self.assertRaisesRegex(ValueError, 'disagree'): reuse.split_ids(value)
        value = frozen(); value['game_ids_by_split'].pop('3')
        with self.assertRaisesRegex(ValueError, 'incomplete'): reuse.split_ids(value)
        for invalid in ([True], [-1], ['1'], [1, 1]):
            with self.assertRaises(ValueError): reuse.ids(invalid)

    def test_versioned_config_preserves_model_and_old_policy(self):
        policy = reuse.plan([REF])
        config = job.configuration(1000, policy, blunder_refinement.policy())
        self.assertEqual(config['implementation_version'], 4)
        self.assertEqual(config['model']['k0'], .00368208)
        self.assertEqual(config['model']['threshold'], .2)
        self.assertEqual(config['sampling']['player_share_limit'], .1)
        self.assertFalse(config['canonical'])
        with self.assertRaisesRegex(ValueError, 'requires'):
            job.configuration(1000, policy)
        altered = copy.deepcopy(policy); altered['player_share_limit'] = .2
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            job.configuration(1000, altered, blunder_refinement.policy())


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_partition_history_releases_only_known_development(self):
        roles, seen, proof = await reuse.history(session_for([result_row()]), reuse.plan([REF]), AsyncMock())
        self.assertEqual(roles, {'training': [1, 2], 'validation': [3, 4], 'protected': [5, 6, 7]})
        self.assertEqual(seen, set(range(1, 8)))
        self.assertEqual(proof[0]['method'], 'saved_partition_manifest')

    async def test_unknown_roles_fail_closed(self):
        value = frozen(); value.pop('game_ids_by_split')
        roles, _, _ = await reuse.history(session_for([result_row(value)]), reuse.plan([REF]), AsyncMock())
        self.assertEqual(roles['protected'], list(range(1, 8)))
        self.assertEqual(roles['training'], [])

    async def test_failed_reserved_source_remains_in_history(self):
        roles = {'training': [1, 2], 'validation': [3, 4], 'protected': [5, 6, 7]}
        row = result_row(frozen(roles), status='failed', fmt=reuse.RESERVATION_FORMAT)
        actual, _, proof = await reuse.history(session_for([row]), reuse.plan([REF]), AsyncMock())
        self.assertEqual(actual, roles)
        self.assertEqual(proof[0]['method'], 'verified_transitive_role_manifest')
        row['result']['format'] = reuse.REPORT_FORMAT
        with self.assertRaisesRegex(ValueError, 'Unfinished'):
            await reuse.history(session_for([row]), reuse.plan([REF]), AsyncMock())

    async def test_missing_or_changed_reference_stops(self):
        with self.assertRaisesRegex(ValueError, 'unavailable'):
            await reuse.history(session_for([None]), reuse.plan([REF]), AsyncMock())
        row = result_row(); row['result']['source']['sha256'] = 'b'*64
        with self.assertRaisesRegex(ValueError, 'changed'):
            await reuse.history(session_for([row]), reuse.plan([REF]), AsyncMock())

    async def test_legacy_replay_must_match_full_input_hash(self):
        value = frozen()
        for key in ('game_ids_by_split', 'seen_game_ids', 'seen_game_ids_sha256'): value.pop(key)
        session = session_for([result_row(value)])
        selected = MagicMock(); selected.mappings.return_value = [{'link': 1, 'total_positions': 1, 'split': 0}]
        old_result = session.execute.side_effect
        session.execute.side_effect = [next(old_result), selected]
        with patch.object(reuse.source, 'selection_sql', return_value='SELECT 1'), \
             patch.object(reuse.source, 'partitions', return_value=value['split']), \
             patch.object(reuse.source, 'read_positions', AsyncMock(return_value=([], 1, 'b'*64))):
            with self.assertRaisesRegex(ValueError, 'Historical source changed'):
                await reuse.history(session, reuse.plan([REF]), AsyncMock())

    async def test_cancellation_before_history_reads(self):
        reporter = AsyncMock(); reporter.check.side_effect = RuntimeError('cancel')
        session = AsyncMock()
        with self.assertRaisesRegex(RuntimeError, 'cancel'):
            await reuse.history(session, reuse.plan([REF]), reporter)
        session.execute.assert_not_called()
