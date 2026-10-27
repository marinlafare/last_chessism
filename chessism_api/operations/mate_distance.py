"""Decode the established python-chess mate_score=10000 storage convention."""
import math


def mate_distance(score, source):
    # Untyped legacy values may be centipawns or a mate sentinel. Do not guess.
    if score is None or not str(source or '').startswith('stockfish'):
        return None
    value = abs(float(score))
    if not math.isfinite(value) or not value.is_integer() or not 9000 <= value <= 10000:
        return None
    return int(10000 - value)


def mate_distance_sql(score='player_score', source='analysis_source'):
    return f'''CASE WHEN {source} LIKE 'stockfish%' AND ABS({score}) BETWEEN 9000 AND 10000
        AND {score}=TRUNC({score}::numeric) THEN (10000-ABS({score}))::int END'''
