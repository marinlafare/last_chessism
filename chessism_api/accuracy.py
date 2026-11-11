"""Shared, persistence-independent game accuracy policy."""
import math


# Kept in sync with Lichess's current move-judgment implementation:
# https://github.com/lichess-org/lila/blob/master/modules/tree/src/main/Advice.scala
# https://github.com/lichess-org/scalachess/blob/master/core/src/main/scala/eval.scala
LICHESS_WINNING_CHANCE_MULTIPLIER = -0.00368208
LICHESS_CP_CEILING = 1_000.0
LICHESS_ACCURACY_SCALE = 103.1668100711649
LICHESS_ACCURACY_DECAY = 0.04354415386753951
LICHESS_ACCURACY_OFFSET = -3.166924740191411
LICHESS_ACCURACY_UNCERTAINTY_BONUS = 1.0


def lichess_winning_chances(centipawns: float) -> float:
    """Return Lichess winning chances on its published -1 to +1 scale."""
    bounded_cp = max(-LICHESS_CP_CEILING, min(LICHESS_CP_CEILING, float(centipawns)))
    exponent = LICHESS_WINNING_CHANCE_MULTIPLIER * bounded_cp
    return 2.0 / (1.0 + math.exp(exponent)) - 1.0


def lichess_win_percent(centipawns: float) -> float:
    """Convert a player-oriented CP score to Lichess's 0–100 WinPercent."""
    return 50.0 + 50.0 * lichess_winning_chances(centipawns)


def lichess_move_accuracy(before_cp: float, after_cp: float) -> float:
    """Return current Lichess move accuracy for player-oriented evaluations."""
    win_percent_loss = max(
        0.0,
        lichess_win_percent(before_cp) - lichess_win_percent(after_cp),
    )
    if win_percent_loss == 0:
        return 100.0
    raw_accuracy = (
        LICHESS_ACCURACY_SCALE
        * math.exp(-LICHESS_ACCURACY_DECAY * win_percent_loss)
        + LICHESS_ACCURACY_OFFSET
        + LICHESS_ACCURACY_UNCERTAINTY_BONUS
    )
    return max(0.0, min(100.0, raw_accuracy))


