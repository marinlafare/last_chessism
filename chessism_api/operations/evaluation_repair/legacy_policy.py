"""User-approved legacy attribution, distinct from verified engine provenance.

Only ordinary scores above five pieces may be assumed. Small positions are
freshly probed; unsupported probes and larger historical mates stay untouched.
"""
from datetime import datetime, timezone

import chess

from chessism_api.evaluations import integer, validate_result
from chessism_api.database.compact_storage import integer_result

POLICY = 'legacy_cp_assumption_and_fresh_tablebase_v1'
SOURCE = 'stockfish_legacy_assumed'
ACK = 'repair-legacy-cp-and-probe-tablebases'
MAX_BATCH = 500
MISSING = "score IS NOT NULL AND (analysis_source IS NULL OR btrim(analysis_source)='')"
PIECES = "char_length(translate(split_part(fen,' ',1),'12345678/',''))"


def classify(row):
    if row.get('score') is None or str(row.get('analysis_source') or '').strip():
        raise ValueError('Only scored positions without provenance are eligible')
    integer(row['score'], 'score', -2147483648, 2147483647)
    count = sum(c.isalpha() for c in row['fen'].split()[0])
    if not 2 <= count <= 32:
        raise ValueError('Invalid piece count')
    if count <= 5:
        return 'tablebase', count
    return ('held_mate' if abs(row['score']) >= 9000 else 'assumed'), count


def prepare(rows, tablebase):
    from chessism_api.operations.tablebase import _probe_batch_sync
    candidates, pending, skipped = [], [], []
    for row in rows:
        kind, count = classify(row)
        if kind == 'held_mate':
            skipped.append({'fen_id': row['fen_id'], 'reason': 'historical_mate_encoding'})
        elif kind == 'assumed':
            candidates.append({'before': row, 'replacement': {**row, 'analysis_source': SOURCE}})
        else:
            board = chess.Board(row['fen'])
            if not board.is_valid() or board.castling_rights:
                skipped.append({'fen_id': row['fen_id'], 'reason': 'unsupported_tablebase_position'})
            else:
                pending.append({**row, 'piece_count': count})
    if pending:
        if tablebase is None:
            raise ValueError('Local tablebases are required; no Stockfish fallback')
        solved, unavailable = _probe_batch_sync(tablebase, pending)
        by_fen = {row['fen']: row for row in rows}
        stamp = datetime.now(timezone.utc).isoformat()
        for result in solved:
            result = integer_result(result)
            validate_result(result, source='tablebase')
            old = by_fen[result['fen']]
            replacement = {**old, **result, 'analysis_source': 'tablebase', 'analyzed_at': stamp}
            candidates.append({'before': old, 'replacement': replacement})
        skipped.extend({'fen_id': by_fen[row['fen']]['fen_id'], 'reason': 'tablebase_unavailable'}
                       for row in unavailable)
    return sorted(candidates, key=lambda c: c['before']['fen_id']), skipped


def validate(candidates):
    if not candidates or len(candidates) > MAX_BATCH:
        raise ValueError('Repair batch must be bounded and nonempty')
    seen = set()
    for candidate in candidates:
        old, new = candidate['before'], candidate['replacement']
        if old['fen_id'] in seen or old.keys() != new.keys():
            raise ValueError('Duplicate identity or changed row schema')
        seen.add(old['fen_id'])
        kind, count = classify(old)
        changed = {key for key in old if old[key] != new[key]}
        if kind == 'assumed':
            if changed != {'analysis_source'} or new['analysis_source'] != SOURCE:
                raise ValueError('Assumption may only change the source, never a score or timestamp')
        elif kind == 'tablebase':
            allowed = {'piece_count', 'score', 'next_moves', 'wdl_win', 'wdl_draw', 'wdl_loss',
                       'analysis_source', 'analyzed_at', 'tablebase_wdl', 'tablebase_dtz'}
            if changed - allowed or new['analysis_source'] != 'tablebase' or new['piece_count'] != count:
                raise ValueError('Unexpected tablebase replacement fields')
            validate_result(new, source='tablebase')
            from chessism_api.operations.tablebase import _tablebase_score_payload
            expected = _tablebase_score_payload(chess.Board(old['fen']), new['tablebase_wdl'])
            if any(new[key] != value for key, value in expected.items()):
                raise ValueError('Tablebase score/WDL disagree with the probe')
            stamp = datetime.fromisoformat(new['analyzed_at'])
            if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
                raise ValueError('A real tablebase analysis timestamp is required')
        else:
            raise ValueError('Larger historical mate encodings must remain unchanged')
