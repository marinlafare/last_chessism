"""Dedicated metadata-summary worker; never starts/stops cloud or engine jobs."""
import argparse
import asyncio
import logging

from chessism_api.database.runtime import bind_runtime_engine, configured_storage
from chessism_api.database.player_analytics_schema import install_player_analytics
from .player_analytics_summary import refresh_pending


async def run(args):
    from constants import CONN_STRING
    engine = await bind_runtime_engine(CONN_STRING)
    try:
        if args.install:
            async with engine.begin() as connection:
                await install_player_analytics(connection, configured_storage())
            print('Player analytics tables and transactional invalidation installed.', flush=True)
            return
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
    parser.add_argument('--install', action='store_true', help='Explicit additive schema migration; no backfill in this command')
    parser.add_argument('--once', action='store_true', help='Process at most one bounded player batch')
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(parser.parse_args()))
