"""Explicit repair-only budgets; never changes the normal analysis profile."""
from stockfish_core import PROFILE_ID, ENGINE_SHA256, NODES, THREADS, HASH_MIB, MULTIPV

BUDGETS = (NODES, 1_000_000, 10_000_000, 20_000_000)


def profile(nodes=NODES):
    if type(nodes) is not int or nodes not in BUDGETS:
        raise ValueError('Unsupported bounded repair search budget')
    return dict(id=PROFILE_ID if nodes == NODES else f'sf16-1-repair-n{nodes}-t1-h256-pv4-v1',
                binary_sha256=ENGINE_SHA256, nodes=nodes, threads=THREADS,
                hash_mib=HASH_MIB, multipv=MULTIPV, perspective='white',
                mate_encoding='python_chess_mate_score_10000')
