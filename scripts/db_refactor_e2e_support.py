"""Safety/configuration helpers for the private HTTP/ARQ backup rehearsal."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal

from sqlalchemy.engine import URL

ROOT = Path('/restore-test')


def verified_source(report):
    status=json.loads((report/'status.json').read_text())
    release=json.loads((report/'canonical-release.json').read_text())
    restart=json.loads((report/'canonical-restart.json').read_text())
    from scripts.db_refactor_workload import digest
    workspace=Path(status['workspace'])
    if (status.get('mode')!='isolated_canonical_release' or status.get('state')!='succeeded'
        or status.get('phase')!='canonical_release_rehearsal_complete'
        or status.get('private_postgres_stopped') is not True
        or status.get('live_database_changed') is not False
        or release.get('committed_to_private_copy') is not True
        or restart.get('contract_hash')!=digest(release.get('contract'))
        or workspace.parent!=ROOT or not workspace.name.startswith('db-refactor-baseline-release-')
        or workspace.resolve()!=workspace or workspace.is_symlink()):
        raise ValueError('An accepted, stopped canonical private release is required')
    return workspace,status['backup_id'],release['contract']


def source_control(pgdata):
    if (not (pgdata/'PG_VERSION').is_file() or (pgdata/'postmaster.pid').exists()
        or pgdata.resolve()!=pgdata or any(p.is_symlink() for p in pgdata.rglob('*'))):
        raise RuntimeError('Source must be stopped with no external tablespaces/WAL symlinks')
    return hashlib.sha256((pgdata/'global/pg_control').read_bytes()).hexdigest()


def private_environment(workspace, config, gate):
    if workspace.parent!=ROOT or not workspace.name.startswith('db-refactor-baseline-http-'):
        raise ValueError('Invalid isolated application workspace')
    socket=workspace/'socket'
    # Allowlist, not a copy of the host's environment/credentials. Disable .env
    # loading; all app secrets below are throwaway rehearsal-only values.
    env={key:os.environ[key] for key in ('PATH','LANG','LC_ALL','TZ') if key in os.environ}
    env.update({
        'PYTHONDONTWRITEBYTECODE':'1',
        'PYTHONUNBUFFERED':'1','PYTHONPATH':'/candidate:/candidate/stockfish-batch-worker',
        'PYTHON_DOTENV_DISABLED':'1',
        'DATABASE_URL':URL.create('postgresql+asyncpg',username='chessism_user',database='chessism_db',
            port=55432,query={'host':str(socket)}).render_as_string(hide_password=False),
        'CHESSISM_DATABASE_PROFILE':'relational_v2','REDIS_HOST':'127.0.0.1','QUEUE_NAME':'backup_queue',
        'SUPERADMIN_GATE_CODE':gate,'AUTH_COOKIE_NAME':'rehearsal_session',
        'AUTH_GATE_COOKIE_NAME':'rehearsal_gate','AUTH_SESSION_TTL_DAYS':'1',
        'AUTH_GATE_TTL_MINUTES':'60','AUTH_COOKIE_SECURE':'false',
        'BACKUP_VOLUME_ROOT':str(workspace/'backup-volume'),'BACKUP_VOLUME_UUID':'isolated-http-refactor',
        'DATABASE_BACKUP_APP_ID':'chessism','DATABASE_BACKUP_DISPLAY_DIR':str(workspace/'backup-volume/chessism/database'),
        'DATABASE_BACKUP_QUOTA_BYTES':'200000000000','BACKUP_FREE_FLOOR_BYTES':'20000000000',
        'BACKUP_STOP_MARGIN_BYTES':'2000000000','DATABASE_BACKUP_FULL_AFTER_INCREMENTALS':'10',
        'DATABASE_BACKUP_FULL_AFTER_DAYS':'7','PGBACKREST_STANZA':'refactor',
        'PGBACKREST_CONFIG':str(config),'CHESSISM_ARCHIVE_GAP_PATH':str(workspace/'archive-gap'),
        'DATABASE_RESTORE_TEST_ROOT':str(workspace/'restore-tests'),
        'DATABASE_RESTORE_TEST_HEADROOM_BYTES':'20000000000','DATABASE_RESTORE_TEST_TIMEOUT_SECONDS':'7200',
        'RESTORE_TEST_DB_NAME':'chessism_db','RESTORE_TEST_DB_USER':'chessism_user',
        'MATRIX_ARTIFACT_DIR':'/candidate/research_data/matrices',
        'ALGORITHM_WORK_DIR':str(workspace/'algorithm-work'),
        'FEN_ANALYSIS_BACKUP_DIR':str(workspace/'backup-volume/chessism/fen-analysis'),
    })
    return env


def configure_backup(workspace):
    volume=workspace/'backup-volume'
    repository=volume/'chessism/database/pgbackrest'
    repository.mkdir(parents=True)
    (volume/'.chessism-backup-volume').write_text('isolated-http-refactor')
    (workspace/'restore-tests').mkdir()
    config=workspace/'pgbackrest.conf'
    # Explicit private repository and WAL destinations. No production config
    # file or default stanza is consulted by API/worker/archiver processes.
    config.write_text(f'''[global]
repo1-path={repository}
repo1-retention-full=2
repo1-bundle=y
repo1-block=y
compress-type=zst
compress-level=1
process-max=2
start-fast=y
archive-timeout=120
log-level-console=info
log-level-file=off
lock-path={workspace}/pgbackrest-lock
log-path={workspace}/pgbackrest-log

[refactor]
pg1-path={workspace}/pgdata
pg1-port=55432
pg1-socket-path={workspace}/socket
pg1-user=chessism_user
''')
    config.chmod(0o600)
    return config


async def start_process(command,env,log):
    with log.open('ab') as stream:
        return await asyncio.create_subprocess_exec(*command,cwd='/candidate',env=env,
            stdout=stream,stderr=asyncio.subprocess.STDOUT,start_new_session=True)


async def stop_process(process):
    if process is None or process.returncode is not None:
        return
    os.killpg(process.pid,signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(),30)
    except asyncio.TimeoutError:
        os.killpg(process.pid,signal.SIGKILL)
        await process.wait()


def assert_services_alive(processes):
    if any(p is not None and p.returncode is not None for p in processes):
        raise RuntimeError('A private API/worker exited unexpectedly; inspect its private log')
