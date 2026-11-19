"""Dedicated metadata-summary worker; never starts/stops cloud or engine jobs."""
import argparse
import asyncio
import logging

from chessism_api.database.postgresql.runtime import bind_runtime_engine
from .player_analytics_summary import refresh_pending


async def run(args):
    from constants import CONN_STRING
    engine = await bind_runtime_engine(CONN_STRING)
    try:
        while True:
            result = await refresh_pending()
            if result:
                print(result, flush=True)
            if args.once:
                return
            await asyncio.sleep(0.25 if result and result['status'] != 'retrying' else 5)
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true', help='Process at most one bounded player batch')
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(parser.parse_args()))
