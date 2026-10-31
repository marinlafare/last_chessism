"""Explicit, bounded, reviewed repair. Caller owns the transaction.

No engine runs, implicit installation, new-position increments, or automatic retry.
Fresh results replace legacy evaluations; they do not identify the old engine.
"""
from datetime import datetime, timezone
import json

import chess
from sqlalchemy import text
from stockfish_core import PROFILE_ID, ENGINE_SHA256, NODES, THREADS, HASH_MIB, MULTIPV
from stockfish_core.analysis import board_from_fen

from chessism_api.database.relational_storage import identifier, storage_for
from chessism_api.evaluations import score_kind, validate_result
from chessism_api.operations.fen_results import stage_fen_results
from .artifacts import VERSION, MAX_FENS, digest, verify
from .plan import require_idle, snapshot
from .profile import profile as repair_profile
from . import aggregates

ACK = 'publish-reviewed-matching-evaluations'
POLICY = 'matching_score_and_wdl_v1'
REPLACEMENT_POLICY = 'reviewed_same_bucket_replacement_v1'
REPLACEMENT_ACK = 'replace-reviewed-legacy-evaluations'
WDL = ('wdl_win', 'wdl_draw', 'wdl_loss')


def validate_line(board, line):
    validate_result({'fen': board.fen(), **line}, source='stockfish')
    score = line['score']
    kind = score_kind(score, 'stockfish')
    if line.get('score_kind') != kind:
        raise ValueError('Native score kind conflicts with scalar encoding')
    distance, winner = line.get('signed_mate_distance'), line.get('mate_winner')
    if kind == 'mate':
        if type(distance) is not int or abs(distance) > 1000:
            raise ValueError('Invalid native mate distance')
        expected = (10000-abs(distance)) * (1 if score > 0 else -1)
        if (score != expected or winner != ('white' if score > 0 else 'black')
                or (distance != 0 and (distance > 0) != (score > 0))):
            raise ValueError('Native mate winner/distance conflicts with scalar encoding')
        if distance == 0 and (not board.is_checkmate() or (score > 0) == board.turn):
            raise ValueError('Mate zero must describe an actual terminal checkmate')
    elif distance is not None or winner is not None:
        raise ValueError('Centipawn result has mate metadata')
    moves = (line.get('next_moves') or '').split()
    if not moves and not board.is_game_over():
        raise ValueError('Missing nonterminal principal variation')
    replay = board.copy(stack=False)
    for move in moves:
        replay.push_uci(move)  # Checks syntax and legality at every ply.
    if board.is_game_over() and (moves or (board.is_checkmate() and (kind != 'mate' or distance != 0))
                                or (not board.is_checkmate() and score != 0)):
        raise ValueError('Invalid terminal evaluation')
    return moves


def reviewed(plan, shadow, selected_ids, *, plan_sha256, shadow_sha256, acknowledgement,
             policy=POLICY, confirmation=None, endgame_review=None):
    """Validate independently of the human-readable shadow review labels."""
    verify(plan, 'plan'); verify(shadow, 'shadow')
    if policy not in (POLICY, REPLACEMENT_POLICY):
        raise ValueError('Unknown repair publication policy')
    if (acknowledgement != (ACK if policy == POLICY else REPLACEMENT_ACK) or plan['sha256'] != plan_sha256
            or shadow['sha256'] != shadow_sha256 or shadow.get('plan_sha256') != plan_sha256):
        raise ValueError('Explicit approval of these exact plan/shadow checksums is required')
    if (not 1 <= len(selected_ids) <= MAX_FENS or len(set(selected_ids)) != len(selected_ids)
            or any(type(i) is not int or i <= 0 for i in selected_ids)):
        raise ValueError('Choose 1 to 16 unique explicit FEN IDs')
    profile = shadow.get('profile', {})
    expected = repair_profile(NODES if policy == POLICY else profile.get('nodes'))
    if any(profile.get(k) != v for k, v in expected.items()):
        raise ValueError('Shadow engine profile is not the verified repair profile')
    inputs = {item['before']['fen_id']: item for item in plan['items']}
    candidates = shadow.get('candidates', [])
    if (len(candidates) != len(inputs) or len({c['fen_id'] for c in candidates}) != len(inputs)
            or {c['fen_id'] for c in candidates} != set(inputs) or not set(selected_ids) <= set(inputs)):
        raise ValueError('Shadow and selected identities must match the saved plan')
    selected = []
    for candidate in candidates:
        old = inputs[candidate['fen_id']]
        if candidate['before'] != old['before'] or candidate['snapshot_sha256'] != old['snapshot_sha256']:
            raise ValueError('Shadow input differs from the saved snapshot')
        if candidate['fen_id'] not in selected_ids:
            continue
        row = candidate['replacement']
        if row.get('fen') != old['before']['fen'] or row.get('analysis_source') != 'stockfish':
            raise ValueError('Replacement FEN/source differs from the reviewed input')
        if policy == POLICY and (row.get('score') != old['before']['score']
                or any(row.get(k) != old['before'].get(k) for k in WDL)):
            raise ValueError('Only matching score and WDL replacements may be published')
        board = board_from_fen(row['fen'])
        if (type(row.get('piece_count')) is not int or row['piece_count'] != len(board.piece_map())
                or old['before'].get('piece_count') not in (None, row['piece_count'])):
            raise ValueError('Piece count differs from the position')
        stamp = datetime.fromisoformat(row['analyzed_at'])
        if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
            raise ValueError('A genuine, timezone-aware shadow analysis timestamp is required')
        if plan.get('created_at') and stamp < datetime.fromisoformat(plan['created_at']):
            raise ValueError('Shadow analysis predates its frozen input')
        moves = validate_line(board, row)
        alternatives = candidate.get('alternative_lines', [])
        if len(alternatives) >= MULTIPV or (board.is_game_over() and alternatives):
            raise ValueError('Too many or unexpected terminal alternatives')
        first_moves = {moves[0]} if moves else set()
        for line in alternatives:
            pv = validate_line(board, line)
            if not pv or pv[0] in first_moves:
                raise ValueError('Duplicate or empty continuation')
            first_moves.add(pv[0])
        selected.append(candidate)
    if policy == REPLACEMENT_POLICY:
        # Rating-group projections also contain advantage-bucket counts. This
        # bounded policy deliberately refuses category changes instead of leaving
        # that projection stale or initiating a full-database rebuild implicitly.
        for candidate in selected:
            old = aggregates.contribution(candidate['before'])
            new = aggregates.contribution({**candidate['replacement'], 'n_games': candidate['before']['n_games']})
            if any(old[f'{b}_positions'] != new[f'{b}_positions'] for b in aggregates.BINS):
                raise ValueError('Cross-bucket replacement needs a separate rating-projection policy')
        validate_mate_evidence(plan, shadow, selected, confirmation, endgame_review)
    return sorted(selected, key=lambda c: c['before']['fen'])


def validate_mate_evidence(plan, shadow, selected, confirmation, endgame_review):
    mates = [c for c in selected if abs(c['before']['score']) >= 9000 or c['replacement']['score_kind'] == 'mate']
    if not mates:
        return
    if any(c['replacement']['score_kind'] != 'mate' for c in mates):
        raise ValueError('A historical mate cannot be downgraded by this repair policy')
    verify(confirmation, 'shadow'); verify(endgame_review, 'endgame_review')
    expected = repair_profile(confirmation.get('profile', {}).get('nodes'))
    if (any(confirmation['profile'].get(k) != v for k, v in expected.items())
            or expected['nodes'] <= shadow['profile']['nodes']
            or confirmation.get('plan_sha256') != plan['sha256']
            or endgame_review.get('plan_sha256') != plan['sha256']
            or endgame_review.get('shadow_sha256') != shadow['sha256']):
        raise ValueError('Mate confirmation requires a higher-budget search and matching tablebase evidence')
    records = confirmation.get('candidates', [])
    inputs = {i['before']['fen_id']: i for i in plan['items']}
    if len(records) != len(inputs) or {c['fen_id'] for c in records} != set(inputs):
        raise ValueError('Confirmation candidates differ from the frozen plan')
    by_id = {}
    for candidate in records:
        item = inputs[candidate['fen_id']]
        if candidate['before'] != item['before'] or candidate['snapshot_sha256'] != item['snapshot_sha256']:
            raise ValueError('Confirmation input differs from the frozen plan')
        by_id[candidate['fen_id']] = candidate['replacement']
    evidence = endgame_review.get('records', [])
    if len({r['fen_id'] for r in evidence}) != len(evidence):
        raise ValueError('Duplicate tablebase evidence')
    evidence = {r['fen_id']: r for r in evidence}
    for candidate in mates:
        row = candidate['replacement']; other = by_id[candidate['fen_id']]
        board = board_from_fen(row['fen'])
        validate_line(board, other)
        if (other.get('fen') != row['fen'] or other.get('analysis_source') != 'stockfish'
                or any(other.get(k) != row.get(k) for k in ('score', 'score_kind', 'signed_mate_distance', 'mate_winner'))):
            raise ValueError('Mate distance/winner did not stabilize; retain the old evaluation')
        record = evidence.get(candidate['fen_id'], {})
        if record.get('status') != 'probed_not_published' or record.get('wdl_white') != (2 if row['score'] > 0 else -2):
            raise ValueError('Local tablebase does not confirm the winning side')


async def prepare(session):
    # READ COMMITTED gives each post-lock recheck the latest committed version.
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL READ COMMITTED'))
    await session.execute(text("SET LOCAL statement_timeout='10s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='10s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text('SET LOCAL jit=off'))
    layout = storage_for(session)
    if not layout or not layout.compact:
        raise ValueError('Repair publication requires compact relational storage')
    await session.execute(text(f'SET LOCAL search_path={layout.search_path}'))
    if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('chessism:evaluation-repair:v1',0))")):
        raise ValueError('Another repair transaction is running; no retry was started')
    return layout


async def guard(session, layout):
    await require_idle(session, read_only=False)
    # Short, fail-fast read locks close phantom association/ownership/claim and
    # new-job races, including writers that do not use ingestion advisory locks.
    # No analysis is performed inside this transaction. Reads remain available.
    names = [layout.table(n) for n in ('fen', 'fen_continuation', 'game_fen_association', 'game_player', 'player')]
    names += ['public.cloud_fen_claim', 'public.cloud_analysis_job',
              'public.ingestion_pipeline_run', 'public.algorithm_run']
    for name in sorted(names):
        await session.execute(text(f'LOCK TABLE {name} IN SHARE MODE NOWAIT'))
    await require_idle(session, read_only=False)
    if await session.scalar(text("SELECT current_setting('session_replication_role')")) != 'origin':
        raise ValueError('Publication requires normal trigger execution')
    for queue in ('fen_summary_refresh', 'canonical_blunder_dirty'):
        relation = layout.table(queue)
        if await session.scalar(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': relation}):
            if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {relation})')):
                raise ValueError('Wait for pending summary refreshes before repair publication')


async def install(session):
    """Explicit additive admin operation, never API startup or auto-migration."""
    layout = await prepare(session)
    await guard(session, layout)
    relation = layout.table('evaluation_repair_receipt')
    function = f'{identifier(layout.core)}.reject_evaluation_repair_mutation'
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {relation} (
        id text PRIMARY KEY CHECK(length(id)=64),
        committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        actor text NOT NULL CHECK(length(actor) BETWEEN 1 AND 200),
        payload jsonb NOT NULL)'''))
    await session.execute(text(f'''CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        RAISE EXCEPTION 'Evaluation repair receipts are immutable'; END $$'''))
    await session.execute(text(f'DROP TRIGGER IF EXISTS evaluation_repair_immutable ON {relation}'))
    await session.execute(text(f'''CREATE TRIGGER evaluation_repair_immutable
        BEFORE UPDATE OR DELETE OR TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {function}()'''))
    return {'installed': 'evaluation_repair_receipt', 'evaluations_replaced': 0}


async def publish(session, plan, shadow, selected_ids, *, actor, plan_sha256, shadow_sha256, acknowledgement,
                  policy=POLICY, confirmation=None, endgame_review=None):
    selected = reviewed(plan, shadow, selected_ids, plan_sha256=plan_sha256,
                        shadow_sha256=shadow_sha256, acknowledgement=acknowledgement,
                        policy=policy, confirmation=confirmation, endgame_review=endgame_review)
    if not isinstance(actor, str) or not 1 <= len(actor.strip()) <= 200:
        raise ValueError('An explicit reviewer identity is required')
    key_data = {'policy': policy, 'plan': plan_sha256, 'shadow': shadow_sha256, 'ids': sorted(selected_ids)}
    if policy == REPLACEMENT_POLICY:
        key_data.update(confirmation=confirmation['sha256'] if confirmation else None,
                        endgame_review=endgame_review['sha256'] if endgame_review else None)
    key = digest(key_data)
    layout = await prepare(session)
    relation = layout.table('evaluation_repair_receipt')
    if not await session.scalar(text('SELECT to_regclass(:name) IS NOT NULL'), {'name': relation}):
        raise ValueError('Install the repair receipt table explicitly before publishing')
    if not await session.scalar(text('''SELECT EXISTS(SELECT 1 FROM pg_trigger
        WHERE tgrelid=to_regclass(:name) AND tgname='evaluation_repair_immutable'
        AND tgenabled IN ('O','A'))'''), {'name': relation}):
        raise ValueError('Immutable repair receipt protection must be enabled')
    previous = await session.scalar(text(f'SELECT payload FROM {relation} WHERE id=:id'), {'id': key})
    if previous is not None:
        # This confirms the original commit, not that its values are still latest.
        return {'status': 'already_published', 'receipt_id': key, 'evaluations_replaced': 0,
                'original_selected_ids': previous['selected_ids']}
    await guard(session, layout)
    trigger = await session.scalar(text('''SELECT EXISTS(SELECT 1 FROM pg_trigger
        WHERE tgrelid=to_regclass(:name) AND tgname='fen_summary_refresh' AND tgenabled IN ('O','A'))'''),
        {'name': layout.table('fen')})
    if not trigger:
        raise ValueError('The durable FEN refresh trigger must be installed and enabled')
    # Same order as cloud/local result writers. NOWAIT refuses work in flight.
    await session.execute(text(f'''SELECT fen_id FROM {layout.table('fen')}
        WHERE fen_id=ANY(CAST(:ids AS bigint[])) ORDER BY fen FOR UPDATE NOWAIT'''), {'ids': sorted(selected_ids)})
    before = []
    for item in plan['items']:
        current = await snapshot(session, item['before']['fen_id'])
        if current is None or current['snapshot_sha256'] != item['snapshot_sha256']:
            raise ValueError(f"Stale or claimed repair input: FEN {item['before']['fen_id']}")
        if item['before']['fen_id'] in selected_ids:
            before.append(current)
    rows = [c['replacement'] for c in selected]
    continuations = [{'fen_fen': c['before']['fen'], 'rank': rank,
                      'move': line['next_moves'].split()[0], 'score': line['score']}
                     for c in selected for rank, line in enumerate(c['alternative_lines'], 2)]
    await stage_fen_results(session, rows, continuations,
                            fen_ids={c['before']['fen']: c['fen_id'] for c in selected}, count_new=False)
    # Shared writer timestamps normal live analysis at import. For this reviewed
    # shadow, retain the actual saved analysis time instead of publication time.
    stamps = [{'fen_id': c['fen_id'], 'analyzed_at': c['replacement']['analyzed_at']} for c in selected]
    await session.execute(text(f'''UPDATE {layout.table('fen')} f SET analyzed_at=r.analyzed_at
        FROM jsonb_to_recordset(CAST(:rows AS jsonb)) r(fen_id bigint,analyzed_at timestamptz)
        WHERE f.fen_id=r.fen_id'''), {'rows': json.dumps(stamps)})
    games = sorted({r['game_link'] for item in before for r in item['references']})
    queued = list((await session.execute(text(f'''SELECT game_link FROM {layout.table('fen_summary_refresh')}
        WHERE game_link=ANY(CAST(:games AS bigint[])) ORDER BY game_link'''), {'games': games})).scalars())
    if queued != games:
        raise ValueError('Not every affected game was durably queued; rolling back all replacements')
    after = [await snapshot(session, c['fen_id']) for c in selected]
    aggregate_change = await aggregates.apply(session, selected) if policy == REPLACEMENT_POLICY else None
    result = {'status': 'published', 'receipt_id': key, 'evaluations_replaced': len(selected),
              'selected_ids': sorted(selected_ids), 'untouched_ids': sorted(set(
                  i['before']['fen_id'] for i in plan['items'])-set(selected_ids)),
              'numeric_scores_changed': sum(c['before']['score'] != c['replacement']['score'] for c in selected),
              'wdl_changed': sum(any(c['before'].get(k) != c['replacement'].get(k) for k in WDL) for c in selected),
              'database_totals_delta': aggregate_change['database'].get('nonzero_scored_fens', 0) if aggregate_change else 0,
              'affected_games': games}
    payload = {'version': VERSION, 'policy': policy, **result, 'actor': actor,
               'acknowledgement': acknowledgement, 'plan': plan, 'shadow': shadow,
               'before': before, 'after': after, 'aggregate_changes': aggregate_change,
               'confirmation': confirmation, 'endgame_review': endgame_review}
    await session.execute(text(f'INSERT INTO {relation}(id,actor,payload) VALUES(:id,:actor,CAST(:payload AS jsonb))'),
                          {'id': key, 'actor': actor, 'payload': json.dumps(payload, allow_nan=False)})
    return result
