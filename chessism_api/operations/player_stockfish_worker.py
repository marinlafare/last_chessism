"""Background score summaries; no engine execution or cloud-job management."""
import argparse
import asyncio
import logging

from sqlalchemy import text
from chessism_api.database.runtime import bind_runtime_engine, configured_storage
from chessism_api.database.stockfish_summary_schema import install_stockfish_summaries
from chessism_api.database.engine import AsyncDBSession
from .player_stockfish_summary import refresh_pending


async def cloud_busy():
    # Do not compete with the active import/verification pipeline. Status reads
    # are tiny and never load job documents, results or credentials.
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='2s'"))
        return bool(await session.scalar(text("""SELECT EXISTS(SELECT 1 FROM cloud_analysis_job
            WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))""")))


async def run(args):
    from constants import CONN_STRING
    engine = await bind_runtime_engine(CONN_STRING)
    try:
        if args.install:
            async with engine.begin() as connection:
                await install_stockfish_summaries(connection, configured_storage())
            print('Stockfish summary queue installed; no backfill performed.', flush=True)
            return
        waiting = False
        while True:
            try:
                if await cloud_busy():
                    if not waiting:
                        print('Waiting for active cloud analysis before refreshing summaries.', flush=True)
                    waiting = True
                    result = None
                else:
                    waiting = False
                    result = await refresh_pending()
                    if result:
                        print(result, flush=True)
            except Exception:
                logging.exception('Summary worker will retry; analysis jobs are untouched.')
                result = None
            if args.once:
                return
            await asyncio.sleep(0.25 if result and result['status'] != 'retrying' else 10)
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install', action='store_true')
    parser.add_argument('--once', action='store_true')
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(parser.parse_args()))
