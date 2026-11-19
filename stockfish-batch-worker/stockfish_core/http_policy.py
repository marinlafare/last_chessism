"""Finite HTTP execution bounds for the existing local analysis workload."""
import math

from . import NODES, STALL_SECONDS

MAX_ATTEMPTS = 3
RETRY_DELAYS = (1, 2)
CONNECT_SECONDS = 5
ENGINE_START_SECONDS = 20
ENGINE_CLOSE_SECONDS = 5
QUEUE_SECONDS = STALL_SECONDS
# The existing API scheduling estimate; the stall allowance covers slow positions.
SECONDS_PER_FEN = 2.1
ANALYSIS_JOB_SECONDS = 24 * 60 * 60


def execution_seconds(count, nodes=NODES):
    # Interactive requests can use a larger node budget. Each search still has
    # the existing stall bound; reserve one stall interval for ARQ cleanup.
    per_fen = min(STALL_SECONDS, SECONDS_PER_FEN * nodes / NODES)
    maximum = ANALYSIS_JOB_SECONDS - 2 * STALL_SECONDS - ENGINE_CLOSE_SECONDS - 2 * CONNECT_SECONDS
    return min(maximum, STALL_SECONDS + math.ceil(count * per_fen))


def request_seconds(count, nodes=NODES):
    # All HTTP attempts share this deadline; retries do not multiply it.
    return QUEUE_SECONDS + execution_seconds(count, nodes) + ENGINE_CLOSE_SECONDS + 2 * CONNECT_SECONDS
