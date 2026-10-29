"""Shared evaluation input contract; source metadata is not a CUDA feature.

The verified policy matches the canonical blunder reader. Historical matrix
recipes retain their explicitly versioned legacy policy. Neither policy invents
an engine, a timestamp, or a mate distance for an old evaluation.
"""
import math

CONTRACT = 'verified_evaluation_v1'
KINDS = {'unknown': -1, 'unscored': -1, 'cp': 0, 'mate': 1, 'tablebase': 2}


def kind_sql(score, source, *, verified=True, codes=False):
    """Expressions are internal SQL identifiers, never user input."""
    def value(kind):
        return str(KINDS[kind]) if codes else repr(kind)
    unknown = (f" WHEN COALESCE({source},'') NOT LIKE 'stockfish%' THEN {value('unknown')}"
               if verified else '')
    return (f"CASE WHEN {score} IS NULL THEN {value('unscored')}"
            f" WHEN {source}='tablebase' THEN {value('tablebase')}"
            f"{unknown} WHEN ABS({score})>=9000 THEN {value('mate')} ELSE {value('cp')} END")


def score_kind(score, source, *, verified=True):
    if score is None:
        return 'unscored'
    finite_number(score, 'score')
    if source == 'tablebase':
        return 'tablebase'
    if verified and not str(source or '').startswith('stockfish'):
        return 'unknown'
    return 'mate' if abs(score) >= 9000 else 'cp'


def finite_number(value, field):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f'{field} must be a finite number')


def integer(value, field, low, high):
    finite_number(value, field)
    if value != int(value) or not low <= value <= high:
        raise ValueError(f'{field} must be an integer in [{low}, {high}]')


def validate_result(row, *, source):
    """Check a fresh engine result before any writes. Legacy restore is separate."""
    if not isinstance(row.get('fen'), str) or not row['fen']:
        raise ValueError('Evaluation requires a FEN')
    integer(row.get('score'), 'score', -2147483648, 2147483647)
    if row.get('analysis_source') not in (None, source):
        raise ValueError('Evaluation source conflicts with its writer')
    if row.get('next_moves') is not None and not isinstance(row['next_moves'], str):
        raise ValueError('Invalid principal variation')
    wdl = [row.get(key) for key in ('wdl_win', 'wdl_draw', 'wdl_loss')]
    if any(value is not None for value in wdl):
        for key, value in zip(('wdl_win', 'wdl_draw', 'wdl_loss'), wdl):
            integer(value, key, 0, 1000)
        if sum(wdl) != 1000:
            raise ValueError('Evaluation WDL must sum to 1000')
    if source == 'stockfish':
        if any(row.get(key) is not None for key in ('tablebase_wdl', 'tablebase_dtz')):
            raise ValueError('Stockfish result contains tablebase metadata')
    elif source == 'tablebase':
        integer(row.get('tablebase_wdl'), 'tablebase_wdl', -2, 2)
        if row.get('tablebase_dtz') is not None:
            integer(row['tablebase_dtz'], 'tablebase_dtz', -2147483648, 2147483647)
    else:
        raise ValueError('Unsupported fresh evaluation source')


def validate_batch(rows, *, source):
    seen = set()
    for row in rows:
        validate_result(row, source=source)
        if row['fen'] in seen:
            raise ValueError('Duplicate FEN in evaluation batch')
        seen.add(row['fen'])
    return seen
