"""One dedicated, verified local Stockfish process; deliberately no DB imports."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import time

import chess
import chess.engine
from stockfish_core import PROFILE_ID, ENGINE_SHA256, NODES, THREADS, HASH_MIB, MULTIPV
from stockfish_core.analysis import verify_binary, board_from_fen, configure, analyse
from chessism_api.evaluations import validate_result, score_kind
from .artifacts import VERSION, verify, read, write, seal
from .profile import profile, BUDGETS


def review_class(old_score, replacement):
    if abs(old_score) >= 9000 and replacement['score_kind'] != 'mate':
        return 'hold_legacy_mate_not_reproduced'
    if old_score == replacement['score']:
        return 'matching_number_not_historical_provenance'
    return 'changed_value_requires_review'


def normalize_line(board, info):
    if info.get('lowerbound') or info.get('upperbound'):
        raise ValueError('Bound-only engine scores cannot be published as exact evaluations')
    score = info['score'].white()
    encoded = score.score(mate_score=10000)
    kind = 'mate' if score.is_mate() else 'cp'
    if kind != score_kind(encoded, 'stockfish'):
        raise ValueError('Typed engine score conflicts with the current scalar storage encoding')
    replay = board.copy(stack=False)
    pv = info.get('pv', [])
    if not pv and not board.is_game_over():
        raise ValueError('Engine returned no principal variation for a nonterminal position')
    for move in pv:
        if move not in replay.legal_moves:
            raise ValueError('Engine principal variation contains an illegal move')
        replay.push(move)
    wdl = info.get('wdl')
    if wdl is not None:
        wdl = wdl.white()
    return {'score': encoded, 'score_kind': kind, 'signed_mate_distance': score.mate(),
            'mate_winner': ('white' if encoded > 0 else 'black') if kind == 'mate' else None,
            'next_moves': ' '.join(move.uci() for move in pv) or None,
            'wdl_win': wdl.wins if wdl is not None else None,
            'wdl_draw': wdl.draws if wdl is not None else None,
            'wdl_loss': wdl.losses if wdl is not None else None,
            'depth': info.get('depth'), 'nodes': info.get('nodes')}


async def run(plan, *, binary='/usr/local/bin/stockfish', nodes=NODES):
    verify(plan, 'plan')
    configured = profile(nodes)
    # Validate every FEN and the exact binary before starting any process.
    boards = [board_from_fen(item['before']['fen']) for item in plan['items']]
    verify_binary(binary)
    started = time.monotonic()
    transport, engine = await chess.engine.popen_uci([binary])
    candidates = []
    try:
        await configure(engine, THREADS, HASH_MIB)
        engine_id = dict(engine.id)
        for item, board in zip(plan['items'], boards):
            if board.is_game_over():
                native = chess.engine.Mate(0) if board.is_checkmate() else chess.engine.Cp(0)
                lines = [{'score': chess.engine.PovScore(native, board.turn), 'pv': []}]
            else:
                # Fail closed, terminate our own engine, and write no successful
                # report on timeout; never restart a shared engine or retry forever.
                lines = await asyncio.wait_for(analyse(engine, board, nodes, MULTIPV), 30)
            if not isinstance(lines, list) or not 1 <= len(lines) <= MULTIPV:
                raise ValueError('Invalid engine result set')
            normalized = [normalize_line(board, line) for line in lines]
            replacement = {'fen': item['before']['fen'], 'analysis_source': 'stockfish',
                'analyzed_at': datetime.now(timezone.utc).isoformat(),
                'piece_count': len(board.piece_map()), **normalized[0]}
            validate_result(replacement, source='stockfish')
            for line in normalized[1:]:
                validate_result({'fen': replacement['fen'], **line}, source='stockfish')
            candidates.append({'fen_id': item['before']['fen_id'],
                'before': item['before'], 'snapshot_sha256': item['snapshot_sha256'],
                'replacement': replacement, 'alternative_lines': normalized[1:],
                # No CP loss/change percentage: the historical numeric encoding
                # is unknown. Differences require review, not an equality gate.
                'numeric_score_changed': item['before']['score'] != replacement['score'],
                'review': review_class(item['before']['score'], replacement),
                'old_interpretation': 'unknown'})
    finally:
        try:
            await asyncio.wait_for(engine.quit(), 5)
        finally:
            transport.close()
    return seal({'version': VERSION, 'kind': 'shadow', 'plan_sha256': plan['sha256'],
        'scores_changed': 0, 'status': 'needs_review_not_published',
        'review_counts': {label: sum(item['review'] == label for item in candidates) for label in (
            'hold_legacy_mate_not_reproduced', 'matching_number_not_historical_provenance',
            'changed_value_requires_review')},
        'profile': {**configured, 'engine': engine_id},
        'seconds': round(time.monotonic()-started, 3), 'candidates': candidates})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--nodes', type=int, choices=BUDGETS, default=NODES)
    args = parser.parse_args()
    report = write(args.output, asyncio.run(run(read(args.plan, 'plan'), nodes=args.nodes)))
    print(json.dumps({'report': args.output, 'sha256': report['sha256'],
                      'fens': len(report['candidates']), 'seconds': report['seconds'],
                      'status': report['status'], 'scores_changed': 0}))
