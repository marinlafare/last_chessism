"""Read-only evidence for held endgames; WDL/DTZ are not mate distances."""
import argparse
import json
import chess
import chess.syzygy
from stockfish_core.analysis import board_from_fen
from .artifacts import VERSION, read, write, verify


def legacy_pv(before):
    board = board_from_fen(before['fen'])
    moves = str(before.get('next_moves') or '').split()
    try:
        for move in moves:
            board.push_uci(move)
    except ValueError:
        return {'legal': False, 'ends_checkmate': False, 'proves_forced_mate': False}
    mate = board.is_checkmate()
    return {'legal': True, 'plies': len(moves), 'ends_checkmate': mate,
            'winner': ('black' if board.turn else 'white') if mate else None,
            'proves_forced_mate': False}


def review(plan, shadow, directory):
    verify(plan, 'plan'); verify(shadow, 'shadow')
    if shadow['plan_sha256'] != plan['sha256']:
        raise ValueError('Shadow report belongs to another plan')
    inputs = {i['before']['fen_id']: i for i in plan['items']}
    records = []
    seen = set()
    with chess.syzygy.open_tablebase(directory) as tablebase:
        for item in shadow['candidates']:
            fen_id = item['fen_id']
            if fen_id in seen or fen_id not in inputs or item['before'] != inputs[fen_id]['before']:
                raise ValueError('Mismatched or duplicate shadow candidate')
            seen.add(fen_id)
            if abs(item['before']['score']) < 9000 and item['replacement']['score_kind'] != 'mate':
                continue
            before = inputs[fen_id]['before']
            board = board_from_fen(before['fen'])
            record = {'fen_id': fen_id, 'legacy_pv': legacy_pv(before),
                      'verifies_historical_mate_distance': False}
            try:
                wdl = tablebase.probe_wdl(board)
                dtz = tablebase.probe_dtz(board)
                record.update(wdl_white=wdl if board.turn else -wdl,
                              dtz_side_to_move=dtz, status='probed_not_published')
            except chess.syzygy.MissingTableError:
                record['status'] = 'missing_table_requires_review'
            records.append(record)
    if seen != set(inputs):
        raise ValueError('Incomplete shadow report')
    return {'version': VERSION, 'kind': 'endgame_review', 'plan_sha256': plan['sha256'],
            'shadow_sha256': shadow['sha256'], 'records': records, 'scores_changed': 0,
            'notes': ['WDL and DTZ do not verify a mate-in-N distance.',
                      'A legal mating PV does not prove all opponent replies are forced.',
                      'Four-field stored FENs use halfmove clock 0, like the analysis profile.'],
            'status': 'review_evidence_only_not_published'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--shadow', required=True)
    parser.add_argument('--directory', default='/syzygy')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    report = write(args.output, review(read(args.plan, 'plan'), read(args.shadow, 'shadow'), args.directory))
    print(json.dumps({'report': args.output, 'sha256': report['sha256'], 'records': report['records'],
                      'scores_changed': 0}))
