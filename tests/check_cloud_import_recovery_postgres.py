"""Private socket-only PostgreSQL. No live DB, credentials, network or cloud jobs."""
import asyncio
import argparse
from contextlib import suppress
import os
from pathlib import Path
import tempfile
import unittest
import asyncpg
from chessism_api.operations import database_restore_tests as restore


async def main(pattern='test_cloud_import_recovery_database.py'):
    if Path('/var/lib/postgresql/data/PG_VERSION').exists():
        raise RuntimeError('Production PostgreSQL data must not be mounted')
    root = Path(tempfile.mkdtemp(prefix='cloud-recovery-', dir='/restore-test'))
    pgdata, socket = root/'pgdata', root/'socket'
    pgdata.mkdir(); socket.mkdir()
    try:
        await restore._run_command_capture(str(Path(restore.PG_CTL).with_name('initdb')),
            '-D', str(pgdata), '--username=chessism_user', '--auth=trust', '--locale=C')
        await restore._run_command_capture(restore.PG_CTL, '-D', str(pgdata), '-l', str(root/'postgres.log'),
            '-w', '-o', restore._postgres_start_options(socket), 'start')
        conn = await asyncpg.connect(host=str(socket), port=55432, user='chessism_user', database='postgres')
        await conn.execute('CREATE DATABASE chessism_cloud_test')
        await conn.close()
        env = {**os.environ, 'PGHOST': str(socket), 'PGPORT': '55432',
            'CHESSISM_CLOUD_TEST_DATABASE_URL': 'postgresql+asyncpg://chessism_user@/chessism_cloud_test'}
        process = await asyncio.create_subprocess_exec('python', '-m', 'unittest', 'discover', '-s', 'tests',
            '-p', pattern, '-v', env=env)
        if await process.wait(): raise RuntimeError('Isolated PostgreSQL checks failed')
    finally:
        with suppress(Exception):
            await restore._run_command_capture(restore.PG_CTL, '-D', str(pgdata), '-m', 'fast', '-w', 'stop')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pattern', default='test_cloud_import_recovery_database.py')
    asyncio.run(main(parser.parse_args().pattern))
