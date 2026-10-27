"""Role-locked development reuse; final evaluation always uses fresh games.

No outcome affects selection. Historical test reservations (even unevaluated),
ambiguous identities and conflicting roles are blocked everywhere. The same
balanced quotas, chronology, player holdout and 10% player caps still apply.
"""
import hashlib
import json
from datetime import datetime

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_balanced_sampling as balanced, blunder_balanced_source as old_source
from . import blunder_v2_source as source

VERSION = 'development_reuse_v1'
RESERVATION_FORMAT = 'chessism_blunder_source_reservation_v1'
REPORT_FORMAT = 'chessism_rating_blunder_candidate_v2'
ROLE_KEYS = ('training', 'validation', 'protected')


def plan(references):
    return {**balanced.plan(references), 'version': VERSION,
        'excluded_overlap_policy': 'role_locked_development_fresh_test_protected_unknown',
        'history_checkpoint': 'before_fitting_includes_failed_cancelled_runs'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def ids(values):
    if not isinstance(values, list) or len(values) > balanced.MAX_SEEN_GAMES or any(type(i) is not int or i <= 0 for i in values):
        raise ValueError('Invalid or oversized historical identity list.')
    if len(values) != len(set(values)):
        raise ValueError('Duplicate historical identity.')
    return set(values)


def manifest(training, validation, protected, seen):
    """Conflicts/unknowns fail closed, including train↔validation role changes."""
    protected = protected | (training & validation) | (seen - training - validation)
    value = {key: sorted(values) for key, values in zip(ROLE_KEYS,
        (training-protected, validation-protected, protected))}
    if set().union(*(set(v) for v in value.values())) != seen or len(seen) > balanced.MAX_SEEN_GAMES:
        raise ValueError('Historical role manifest does not cover the known identities.')
    return value


def verified_manifest(frozen):
    seen = ids(frozen['seen_game_ids'])
    if hashlib.sha256(json.dumps(frozen['seen_game_ids']).encode()).hexdigest() != frozen.get('seen_game_ids_sha256'):
        raise ValueError('Historical identity manifest is corrupt.')
    value = frozen['development_roles']
    if set(value) != set(ROLE_KEYS) or digest(value) != frozen.get('development_roles_sha256'):
        raise ValueError('Historical role manifest is corrupt.')
    sets = {k: ids(v) for k, v in value.items()}
    if any(sets[a] & sets[b] for a, b in (('training', 'validation'), ('training', 'protected'), ('validation', 'protected'))):
        raise ValueError('Historical roles are not disjoint.')
    if set().union(*sets.values()) != seen:
        raise ValueError('Historical roles do not cover all seen games.')
    return sets, seen


async def reference_plan(session):
    rows = (await session.execute(text("""SELECT id,status,result FROM algorithm_run
        WHERE config->>'operation'='smooth_rating_blunder_v2'
          AND (status='complete' OR result->>'format'='chessism_blunder_source_reservation_v1')
        ORDER BY created_at DESC,id LIMIT 101"""))).mappings().all()
    if len(rows) > 100:
        raise ValueError('Research history exceeds its bounded scan.')
    references, hashes = [], set()
    for row in rows:
        result = row['result'] or {}
        if result.get('format') not in (REPORT_FORMAT, RESERVATION_FORMAT):
            raise ValueError('Unrecognized research history; source roles cannot be proven.')
        frozen = result['source']
        if frozen['sha256'] in hashes:
            continue
        hashes.add(frozen['sha256'])
        references.append({'id': str(row['id']), 'sha256': frozen['sha256'],
            **{key: frozen['split'][key] for key in ('training_before', 'test_from')}})
        # Only the new role-aware manifest is transitive for role reuse.
        if frozen.get('development_roles') is not None:
            verified_manifest(frozen)
            break
    return plan(references)


def split_ids(frozen):
    by_split = frozen['game_ids_by_split']
    if set(by_split) != {'0', '1', '2', '3'}:
        raise ValueError('Historical split manifest is incomplete.')
    groups = {key: ids(value) for key, value in by_split.items()}
    union = set().union(*groups.values())
    if sum(len(v) for v in groups.values()) != len(union) or len(union) != frozen['selected_games']:
        raise ValueError('Historical partition counts or identities disagree.')
    return groups


async def history(session, policy, reporter):
    training, validation, protected, seen = set(), set(), set(), set()
    verified, replayed = [], 0
    for ref in policy['references']:
        await reporter.check()
        row = (await session.execute(text('SELECT status,config,result FROM algorithm_run WHERE id=:id'),
                                     {'id': ref['id']})).mappings().one_or_none()
        if row is None or not row['result']:
            raise ValueError('Referenced role history is unavailable.')
        result = row['result']; frozen = result['source']
        expected_ref = {'id': ref['id'], 'sha256': frozen['sha256'],
            **{key: frozen['split'][key] for key in ('training_before', 'test_from')}}
        if expected_ref != ref or result.get('format') not in (REPORT_FORMAT, RESERVATION_FORMAT):
            raise ValueError('Referenced role history changed.')
        if result['format'] == REPORT_FORMAT and row['status'] != 'complete':
            raise ValueError('Unfinished reports cannot establish historical roles.')
        if frozen.get('development_roles') is not None:
            roles, known = verified_manifest(frozen)
            training.update(roles['training']); validation.update(roles['validation']); protected.update(roles['protected'])
            seen.update(known); method = 'verified_transitive_role_manifest'
        elif frozen.get('game_ids_by_split') is not None:
            groups = split_ids(frozen)
            known = ids(frozen['seen_game_ids'])
            if hashlib.sha256(json.dumps(frozen['seen_game_ids']).encode()).hexdigest() != frozen.get('seen_game_ids_sha256'):
                raise ValueError('Historical identity manifest is corrupt.')
            if not set().union(*groups.values()) <= known:
                raise ValueError('Partition identity is missing from the historical manifest.')
            training.update(groups['0']); validation.update(groups['1'])
            protected.update(groups['2'] | groups['3']); seen.update(known)
            method = 'saved_partition_manifest'
        elif frozen.get('seen_game_ids') is not None:
            # Old identities without provable roles cannot be reused.
            known = ids(frozen['seen_game_ids'])
            if hashlib.sha256(json.dumps(frozen['seen_game_ids']).encode()).hexdigest() != frozen.get('seen_game_ids_sha256'):
                raise ValueError('Historical identity manifest is corrupt.')
            protected.update(known); seen.update(known); method = 'unknown_roles_protected'
        else:
            replayed += frozen['positions']
            if replayed > balanced.MAX_REPLAY_ROWS:
                raise ValueError('Historical verification exceeds its six-million-row budget.')
            metadata = [dict(r) for r in (await session.execute(text(source.selection_sql(session)), row['config'])).mappings()]
            boundaries = source.partitions(metadata)
            if any(boundaries[k] != frozen['split'][k] for k in ('training_before', 'test_from')):
                raise ValueError('Historical chronological boundaries changed.')
            if sum(g['total_positions'] for g in metadata) > source.MAX_ROWS:
                raise ValueError('Historical source exceeds its row bound.')
            _, _, sha = await source.read_positions(session, metadata, reporter, phase='verifying_development_roles')
            if sha != ref['sha256']:
                raise ValueError('Historical source changed; development roles cannot be verified.')
            known = {g['link'] for g in metadata}
            training.update(g['link'] for g in metadata if g['split'] == 0)
            validation.update(g['link'] for g in metadata if g['split'] == 1)
            protected.update(g['link'] for g in metadata if g['split'] not in (0, 1))
            seen.update(known); method = 'full_frozen_source_hash_match'
        if len(seen) > balanced.MAX_SEEN_GAMES:
            raise ValueError('Historical identity budget exceeded.')
        verified.append({**ref, 'method': method, 'games': len(known)})
    from .blunder_strength_source import exploration_ids
    explored = await exploration_ids(session)
    protected.update(explored); seen.update(explored)
    from .blunder_full_source import additional_roles
    full_training, full_validation = await additional_roles(session)
    training.update(full_training); validation.update(full_validation)
    seen.update(full_training | full_validation)
    return manifest(training, validation, protected, seen), seen, verified


def candidate_sql(session):
    # Role/date checks happen BEFORE ranking, so forbidden games cannot consume
    # the limited per-cell pool and starve eligible development observations.
    original = 'FROM eligible WHERE NOT(link=ANY(CAST(:excluded AS bigint[])))'
    replacement = original + '''
        AND (NOT(link=ANY(CAST(:prior_training AS bigint[]))) OR played_at<CAST(:training_before AS timestamptz))
        AND (NOT(link=ANY(CAST(:prior_validation AS bigint[]))) OR
            (played_at>=CAST(:training_before AS timestamptz) AND played_at<CAST(:test_from AS timestamptz)))
        AND (played_at<CAST(:test_from AS timestamptz) OR NOT(link=ANY(CAST(:seen AS bigint[]))))'''
    query = old_source.candidate_sql(session)
    if query.count(original) != 1:
        raise ValueError('Balanced source contract changed; role filtering must be reviewed.')
    return query.replace(original, replacement)


def audit_selection(metadata, roles, seen):
    prior = {k: set(v) for k, v in roles.items()}
    selected = {g['link'] for g in metadata}
    test = {g['link'] for g in metadata if g['split'] in (2, 3)}
    wrong = [g['link'] for g in metadata if
             (g['link'] in prior['training'] and g['split'] != 0) or
             (g['link'] in prior['validation'] and g['split'] != 1)]
    if selected & prior['protected'] or test & seen or wrong:
        raise ValueError('Historical partition leakage detected; no fitting is permitted.')
    return {'reused_training_games': len(selected & prior['training']),
        'reused_validation_games': len(selected & prior['validation']),
        'protected_game_overlap': 0, 'test_prior_overlap': 0, 'role_violations': 0,
        'prior_game_overlap': len(selected & seen)}


async def select(session, config, reporter, *, verify_current_history=True):
    policy = config['sampling']
    if verify_current_history and policy != await reference_plan(session):
        raise ValueError('Research history changed while queued; submit a fresh role-reuse request.')
    roles, seen, proof = await history(session, policy, reporter)
    await reporter.progress('selecting_reusable_development', detail='Role-locked training/validation reuse; fresh test games; unchanged player caps.')
    query = candidate_sql(session)
    params = {**policy, 'excluded': roles['protected'], 'prior_training': roles['training'],
        'prior_validation': roles['validation'], 'seen': sorted(seen),
        'training_before': datetime.fromisoformat(policy['training_before']), 'test_from': datetime.fromisoformat(policy['test_from'])}
    pool = [dict(r) for r in (await session.execute(text(query), params)).mappings()]
    if len(pool) > 3*3*9*3*balanced.POOL_PER_CELL:
        raise ValueError('Candidate metadata pool exceeds its bound.')
    metadata, audit = await file_operation(balanced.choose, pool, config['games_per_group'], policy)
    audit.update(audit_selection(metadata, roles, seen), verified_references=proof,
        reuse_policy=VERSION, prior_known_games=len(seen), excluded_protected_games=len(roles['protected']),
        eligible_prior_training=len(roles['training']), eligible_prior_validation=len(roles['validation']),
        candidate_pool_sql_sha256=hashlib.sha256(query.encode()).hexdigest())
    updated_seen = sorted(seen | {g['link'] for g in metadata})
    updated_roles = manifest(set(roles['training']) | {g['link'] for g in metadata if g['split'] == 0},
        set(roles['validation']) | {g['link'] for g in metadata if g['split'] == 1},
        set(roles['protected']) | {g['link'] for g in metadata if g['split'] in (2, 3)}, set(updated_seen))
    audit['limitations'].extend([
        'Development games may be reused only in the same role; validation remains selected-on evidence, not an independent test.',
        'All historical test reservations, ambiguous roles and conflicts are protected. Every new test game is absent from all previous known cohorts.'])
    split = {k: policy[k] for k in ('training_before', 'test_from')}
    split['held_out_players'] = 'Same deterministic player holdout; no held player enters training or validation.'
    # Private selection context is removed after the CP rebalance, before saving.
    frozen = {'sampling': VERSION, 'sampling_audit': audit, '_reuse_context': (roles, seen),
        'game_ids_by_split': {str(s): [g['link'] for g in metadata if g['split'] == s] for s in range(4)},
        'seen_game_ids': updated_seen, 'seen_game_ids_sha256': hashlib.sha256(json.dumps(updated_seen).encode()).hexdigest(),
        'development_roles': updated_roles, 'development_roles_sha256': digest(updated_roles)}
    return metadata, split, frozen, query


async def checkpoint(frozen, reporter, *, session_factory=None):
    """Reserve identities before fitting; failed/cancelled runs keep this proof."""
    verified_manifest(frozen)
    await reporter.check()
    payload = {'format': RESERVATION_FORMAT, 'source': frozen, 'canonical': False, 'production_changed': False}
    async with (session_factory or AsyncDBSession)() as session, session.begin():
        saved = await session.scalar(text('''UPDATE algorithm_run SET result=CAST(:result AS jsonb)
            WHERE id=:id AND status='running' AND NOT cancel_requested RETURNING id'''),
            {'id': reporter.run_id, 'result': json.dumps(payload, allow_nan=False)})
        if saved is None:
            from .repository import RunCancelled
            raise RunCancelled('Source identities could not be reserved; fitting did not start.')
