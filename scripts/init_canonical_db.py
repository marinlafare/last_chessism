"""Initialize/validate an explicitly selected isolated development database only."""
import argparse
import asyncio
from pathlib import Path
import re

from chessism_api.database.postgresql.canonical import initialize, validate


def isolated_target(socket, database):
    path = Path(socket).resolve()
    if (not path.is_absolute() or not str(path).startswith('/tmp/chessism-canonical-')
            or not re.fullmatch(r'chessism_canonical_[a-z0-9_]+', database)):
        raise ValueError('Requires /tmp/chessism-canonical-* socket and chessism_canonical_* database')
    return str(path)


async def run(args):
    import asyncpg
    socket = isolated_target(args.socket, args.database)
    # No DATABASE_URL, .env, application settings or inherited PGHOST fallback.
    connection = await asyncpg.connect(host=socket, port=args.port, database=args.database,
                                      user=args.user, password='', timeout=10)
    try:
        if await connection.fetchval('SELECT inet_server_addr() IS NOT NULL'):
            raise ValueError('Development initializer refuses TCP connections')
        if args.validate_only:
            await validate(connection)
            print('Canonical foundation validated; no changes made.')
        else:
            created = await initialize(connection)
            print('Canonical foundation initialized.' if created else 'Canonical foundation already matches; no changes made.')
    finally:
        await connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--port', type=int, default=55432)
    parser.add_argument('--validate-only', action='store_true')
    asyncio.run(run(parser.parse_args()))


if __name__ == '__main__':
    main()
