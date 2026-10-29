"""Historical scalar oracle for explicit research/validation only.

Never imported by production summaries, endpoints or canonical CUDA inference.
Retained solely to reproduce old/new comparisons on isolated research inputs.
"""
from ..player_game_scores import lichess_winning_chances

LICHESS_BLUNDER_THRESHOLD = 0.30


def is_lichess_blunder(previous_player_score, player_score, previous_kind='cp', score_kind='cp'):
    if previous_kind == 'cp' and score_kind == 'cp':
        return (lichess_winning_chances(previous_player_score)
                - lichess_winning_chances(player_score)) >= LICHESS_BLUNDER_THRESHOLD
    if previous_kind == 'cp' and score_kind == 'mate' and player_score < 0:
        return previous_player_score >= -700
    if previous_kind == 'mate' and previous_player_score > 0:
        if score_kind == 'mate' and player_score < 0:
            return True
        if score_kind == 'cp':
            return player_score <= 700
    return False
