"""Local credential bootstrap and read-only connectivity check. No graph DDL."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets


def create_secret(path):
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Never replace credentials for an existing data volume.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        stream.write(f'neo4j/{secrets.token_urlsafe(36)}\n')
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    init = commands.add_parser('init-secret', help='Create a private credential file, without printing its contents.')
    init.add_argument('--path', default='.local-secrets/neo4j-auth')
    commands.add_parser('check', help='Verify authentication and run a read-only health query.')
    args = parser.parse_args()
    if args.command == 'init-secret':
        try:
            path = create_secret(args.path)
        except FileExistsError:
            parser.exit(1, 'Secret already exists; it was not changed.\n')
        print(f'Created private Neo4j credential file: {path}')
        return
    from .runtime import check_connection
    try:
        result = asyncio.run(check_connection())
    except Exception as error:
        # Driver messages can contain addresses or authentication diagnostics.
        parser.exit(1, f'Neo4j check failed ({type(error).__name__}); check the local service and secret configuration.\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
