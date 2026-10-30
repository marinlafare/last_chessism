"""Explicit, one-time reference-data import. No background work is scheduled."""
import argparse
import asyncio
import json
import os
import time

from .catalog import load_catalog


async def main(args):
    started = time.monotonic()
    catalog = load_catalog()  # Complete validation before connecting to the DB.
    result = {'source_revision': catalog.revision, 'opening_lines': catalog.lines,
              'book_moves': len(catalog.records)}
    if args.install:
        from sqlalchemy import text
        from chessism_api.database.runtime import bind_runtime_engine
        from chessism_api.database.engine import AsyncDBSession
        from .store import publish, TABLE
        engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
        try:
            async with AsyncDBSession() as session, session.begin():
                result['inserted'] = await publish(session, catalog)
                result['stored_rows'] = await session.scalar(text(f'SELECT count(*) FROM {TABLE}'))
                result['table_and_index_bytes'] = await session.scalar(
                    text('SELECT pg_total_relation_size(CAST(:table AS regclass))'), {'table': TABLE})
        finally:
            await engine.dispose()
    result['seconds'] = round(time.monotonic() - started, 3)
    print(json.dumps(result))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install', action='store_true', help='Create and populate only the opening reference table')
    asyncio.run(main(parser.parse_args()))
