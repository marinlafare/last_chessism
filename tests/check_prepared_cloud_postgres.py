"""Launch only a private disposable PostgreSQL; run with Docker --network none."""
import asyncio
import os
from pathlib import Path
import sys
import tempfile


async def command(*args, env=None):
    process = await asyncio.create_subprocess_exec(*args, env=env)
    code = await process.wait()
    if code: raise RuntimeError(f'Test command failed: {args[0]} ({code})')


async def main():
    import asyncpg
    root = Path(tempfile.mkdtemp(prefix='cloud-prepared-',dir='/restore-test'))
    socket = root/'socket'; socket.mkdir()
    pg = '/usr/lib/postgresql/15/bin/'
    try:
        await command(pg+'initdb','-D',str(root/'db'),'-U','test','--auth-local=trust','--auth-host=reject')
        await command(pg+'pg_ctl','-D',str(root/'db'),'-l',str(root/'log'),'-w','-o',
            f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'",'start')
        connection = await asyncpg.connect(host=str(socket),port=55432,user='test',database='postgres')
        await connection.execute('CREATE DATABASE chessism_cloud_test'); await connection.close()
        env = {**os.environ,'PGHOST':str(socket),'PGPORT':'55432',
            'CHESSISM_CLOUD_TEST_DATABASE_URL':'postgresql+asyncpg://test@/chessism_cloud_test',
            'DATABASE_URL':'postgresql+asyncpg://test@/chessism_cloud_test',
            'AUTH_COOKIE_NAME':'test_session','AUTH_GATE_COOKIE_NAME':'test_gate',
            'AUTH_SESSION_TTL_DAYS':'1','AUTH_GATE_TTL_MINUTES':'1','AUTH_COOKIE_SECURE':'false','SUPERADMIN_GATE_CODE':'test-only'}
        await command(sys.executable,'-m','unittest',*(sys.argv[1:] or [
            'test_cloud_prepared_selections_database','test_cloud_incremental_drafts_database',
            'test_cloud_game_drafts_database']),env=env)
    finally:
        if (root/'db'/'postmaster.pid').exists():
            await command(pg+'pg_ctl','-D',str(root/'db'),'-m','fast','-w','stop')


if __name__ == '__main__': asyncio.run(main())
