"""Explicit isolated connections and imports for analysis failure regressions."""
from functools import lru_cache
import importlib.util
import os
from pathlib import Path
import sys
import unittest

from redis.asyncio import Redis


def isolated_test_redis_url():
    url = os.environ.get("CHESSISM_ANALYSIS_TEST_REDIS_URL")
    if not url:
        raise unittest.SkipTest("requires explicitly isolated analysis test Redis")
    return url


def isolated_test_redis():
    return Redis.from_url(isolated_test_redis_url(), decode_responses=True,
                          socket_connect_timeout=2, socket_timeout=2)


@lru_cache(maxsize=1)
def stockfish_analysis_module():
    root = Path(__file__).resolve().parents[1]
    for directory in (root / "stockfish-service", root / "stockfish-batch-worker"):
        if str(directory) not in sys.path:
            sys.path.insert(0, str(directory))
    # The API has its own routers.analysis; load the service without that collision.
    name = "_chessism_stockfish_analysis_tests"
    spec = importlib.util.spec_from_file_location(name, root / "stockfish-service/routers/analysis.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
