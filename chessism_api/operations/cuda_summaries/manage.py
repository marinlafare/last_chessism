"""Explicit additive installation and idle activation; never requests inference."""
import argparse
import asyncio
import json
import os
from chessism_api.database.runtime import bind_runtime_engine, configured_storage
from chessism_api.database.engine import AsyncDBSession
from . import schema, runs


async def main(args):
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with AsyncDBSession() as session, session.begin():
            if args.install:
                await schema.install(session, configured_storage())
            if args.activate:
                await schema.activate(session, configured_storage())
            if args.enable_scores:
                from sqlalchemy import text
                if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {schema.RUN} WHERE status IN {runs.ACTIVE})')):
                    raise ValueError('Wait until the active manual CUDA run finishes before enabling score distribution.')
                await session.execute(text(f'UPDATE {schema.CONTROL} SET scores=true WHERE id AND manual'))
            if args.enable_accuracy:
                from sqlalchemy import text
                if await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM {schema.RUN} WHERE status IN {runs.ACTIVE})')):
                    raise ValueError('Wait for the active manual run before enabling accuracy.')
                await session.execute(text(f'UPDATE {schema.CONTROL} SET scores=true,accuracy=true WHERE id AND manual'))
            if args.enable_metadata:
                from .metadata_store import activate
                await activate(session)
            if args.enable_salience:
                from .salience_store import activate
                await activate(session)
            if args.install_frequencies:
                from .frequency_store import install
                await install(session, configured_storage())
            if args.enable_frequencies:
                from .frequency_store import activate
                await activate(session)
            print(json.dumps(await runs.status(session), default=str))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--install', action='store_true')
    group.add_argument('--activate', action='store_true')
    group.add_argument('--enable-scores', action='store_true')
    group.add_argument('--enable-accuracy', action='store_true')
    group.add_argument('--enable-metadata', action='store_true')
    group.add_argument('--enable-salience', action='store_true')
    group.add_argument('--install-frequencies', action='store_true')
    group.add_argument('--enable-frequencies', action='store_true')
    asyncio.run(main(parser.parse_args()))
