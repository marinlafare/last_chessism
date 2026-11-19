"""Local administrator commands; never expose token provisioning over the API."""
import argparse
import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import secrets
import uuid

from sqlalchemy import select

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.runtime import bind_runtime_engine
from .auth import token_hash
from .catalog import SOURCES, TABLES
from .config import MAX_ROWS
from .models import ExportClient
from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup
from .service import now


async def main(args):
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        if args.action == 'install':
            async with engine.begin() as connection:
                raw = await connection.get_raw_connection()
                await canonical.validate(raw.driver_connection)
            print('Canonical export metadata validated. Initialize an empty database with the canonical initializer first.')
        elif args.action == 'install-analytics':
            from .analytical_admin import install
            await install(engine, TABLES, args.output)
            print('Analytical views and isolated reader installed. Chess rows unchanged. Reader credential was saved privately.')
        elif args.action == 'enable-analytics':
            async with write_transaction(AsyncDBSession) as session:
                client = await session.get(ExportClient, uuid.UUID(args.id).hex)
                if not client: raise ValueError('Unknown client.')
                client.allow_analytics = True
            print('Analytical exports enabled for the specified client; existing dataset and row limits remain enforced.')
        elif args.action == 'create-client':
            if not 1 <= args.max_rows <= MAX_ROWS or not 1 <= args.days <= 365:
                raise ValueError('Invalid row limit or credential lifetime.')
            token = 'chx_' + secrets.token_urlsafe(32)
            client = ExportClient(id=uuid.uuid4().hex, name=args.name, token_hash=token_hash(token),
                allowed_sources=args.sources, max_rows=args.max_rows, expires_at=now()+timedelta(days=args.days))
            path = Path(args.output).absolute()
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            # Do not overwrite a credential or print it into terminal/tool logs.
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'w') as output:
                output.write(token+'\n'); output.flush(); os.fsync(output.fileno())
            try:
                async with write_transaction(AsyncDBSession) as session:
                    session.add(client)
            except BaseException:
                async def preserve_committed_credential():
                    async with read_transaction(AsyncDBSession) as session:
                        persisted = await session.get(ExportClient, client.id)
                    if persisted is None:
                        path.unlink()  # Only the exact private file created above.
                try:
                    await finish_analysis_cleanup(preserve_committed_credential())
                except BaseException:
                    pass  # Uncertain commit keeps its credential; never orphan an active token.
                raise
            print(json.dumps({'client_id': client.id, 'name': client.name, 'token_file': str(path),
                              'expires_at': client.expires_at.isoformat(), 'sources': client.allowed_sources,
                              'max_rows': client.max_rows}))
        elif args.action == 'revoke-client':
            async with write_transaction(AsyncDBSession) as session:
                client = await session.get(ExportClient, uuid.UUID(args.id).hex)
                if not client:
                    raise ValueError('Unknown client.')
                client.revoked_at = now()
            print('Integration token revoked; its active export will stop at the next checkpoint.')
        else:
            async with read_transaction(AsyncDBSession) as session:
                clients = (await session.scalars(select(ExportClient).order_by(ExportClient.created_at))).all()
                print(json.dumps([{'id': c.id, 'name': c.name, 'sources': c.allowed_sources,
                    'max_rows': c.max_rows, 'allow_analytics':c.allow_analytics, 'expires_at': c.expires_at.isoformat(),
                    'revoked': c.revoked_at is not None} for c in clients]))
    finally:
        await finish_analysis_cleanup(engine.dispose())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    commands.add_parser('install')
    analytical = commands.add_parser('install-analytics')
    analytical.add_argument('--output', default='/research-data/integrations/export-reader.json')
    enable = commands.add_parser('enable-analytics'); enable.add_argument('--id', required=True)
    create = commands.add_parser('create-client')
    create.add_argument('--name', required=True)
    create.add_argument('--sources', nargs='+', choices=list(SOURCES), required=True)
    create.add_argument('--max-rows', type=int, default=100000)
    create.add_argument('--days', type=int, default=90)
    create.add_argument('--output', required=True)
    revoke = commands.add_parser('revoke-client'); revoke.add_argument('--id', required=True)
    commands.add_parser('list-clients')
    asyncio.run(main(parser.parse_args()))
