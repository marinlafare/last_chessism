"""One-time, offline Stage D source restore. Run only in its labeled disposable container.

Mount the exact checkpoint read-only at /checkpoint, a new owned volume at
/stage-data, and an evidence directory at /evidence. No application settings,
DATABASE_URL, Docker socket, published port or existing data directory is used.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time


CHECKPOINT_ID = 'p1-20261110T213730Z-b72de27c9a69'
ARCHIVE_SHA256 = 'd4a050c44cb0bae20a5906c4b776fd706878cc57614bf60f752d657e6b18420b'
RUN_ID = 'chessism-stage-d-20261119'
DATA = Path('/stage-data')
CHECKPOINT = Path('/checkpoint')
EVIDENCE = Path('/evidence')
PGDATA = DATA / 'pgdata'
SOCKET = Path('/tmp/chessism-canonical-stage-d-source')
DATABASE = 'chessism_history_stage_d'
ADMIN = 'chessism_stage_d_admin'
READER = 'chessism_stage_d_reader'
GIB = 1024 ** 3
MAX_DATABASE_BYTES = 150 * GIB
MIN_FREE_BYTES = 80 * GIB
MAX_SECONDS = 7200


def identifier(value):
    if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', value):
        raise ValueError('Unexpected identifier in checkpoint metadata')
    return '"' + value + '"'


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


class Restore:
    def __init__(self):
        self.started = time.time()
        self.clock = time.monotonic()
        self.cancelled = False
        self.child = None
        self.owns_cluster = False
        self.database_bytes = 0
        self.status = {'run_id': RUN_ID, 'started_at': self.started,
                       'status': 'starting', 'stage': 'Checking isolated resource identity',
                       'estimate': None, 'supervisor_pid': os.getpid(),
                       'budget': {'cpus': 4, 'memory_gib': 8, 'restore_jobs': 2,
                                  'source_gib': 150, 'minimum_free_gib': 80, 'seconds': MAX_SECONDS}}

    def update(self, **values):
        self.status.update(values)
        self.status['checked_at'] = time.time()
        self.status['elapsed_seconds'] = time.monotonic() - self.clock
        atomic_json(EVIDENCE / 'restore-status.json', self.status)

    def check_budget(self):
        if self.cancelled or (EVIDENCE / 'STOP_RESTORE').exists():
            raise RuntimeError('Graceful stop requested; preserving the isolated source')
        if time.monotonic() - self.clock > MAX_SECONDS:
            raise RuntimeError('Two-hour source restore budget reached')
        if shutil.disk_usage(DATA).free < MIN_FREE_BYTES:
            raise RuntimeError('Source filesystem reached the 80 GiB free-space floor')
        if self.database_bytes > MAX_DATABASE_BYTES:
            raise RuntimeError('Historical source exceeded the 150 GiB database budget')

    def sql(self, statement, *, database=DATABASE, user=ADMIN):
        return subprocess.check_output(
            ['psql', '-X', '--no-password', '-v', 'ON_ERROR_STOP=1', '-h', str(SOCKET),
             '-p', '55432', '-U', user, '-d', database, '-At', '-c', statement],
            text=True, stderr=subprocess.PIPE, timeout=20).strip()

    def command(self, args, stage, *, restore_progress=False):
        self.update(status='running', stage=stage, progress=None)
        with (EVIDENCE / 'restore-commands.log').open('a') as log:
            log.write(json.dumps(args) + '\n'); log.flush()
            self.child = subprocess.Popen(args, stdout=log, stderr=log, start_new_session=True)
            last_inspect = 0
            while self.child.poll() is None:
                self.check_budget()
                now = time.monotonic()
                if restore_progress and now - last_inspect >= 30:
                    last_inspect = now
                    self.inspect_progress()
                self.update()
                time.sleep(2)
            result = self.child.returncode
            self.child = None
            if result:
                raise RuntimeError(f'{args[0]} failed (exit {result}); saved evidence requires review')

    def inspect_progress(self):
        try:
            self.database_bytes = int(self.sql('SELECT pg_database_size(current_database())'))
            progress = self.sql("""SELECT coalesce(json_agg(x), '[]'::json) FROM (
                SELECT relid::regclass::text AS relation, command, bytes_processed, tuples_processed
                FROM pg_stat_progress_copy WHERE datname=current_database()) x""")
            self.update(database_bytes=self.database_bytes, free_bytes=shutil.disk_usage(DATA).free,
                        progress=json.loads(progress))
        except (subprocess.SubprocessError, ValueError):
            # Inspection is telemetry; pg_restore's exit code and exact counts are authoritative.
            self.update(progress=None)

    def stop_children(self):
        if self.child and self.child.poll() is None:
            self.update(status='stopping', stage='Cancelling restore clients; allowing transaction cleanup')
            os.killpg(self.child.pid, signal.SIGTERM)
            while self.child.poll() is None:
                self.update()
                time.sleep(2)
        self.child = None
        if self.owns_cluster and (PGDATA / 'postmaster.pid').exists():
            self.update(status='stopping', stage='Stopping isolated PostgreSQL cleanly')
            with (EVIDENCE / 'restore-commands.log').open('a') as log:
                subprocess.run(['pg_ctl', '-D', str(PGDATA), '-m', 'fast', '-w', '-t', '120', 'stop'],
                               stdout=log, stderr=log, check=False)
            # Never exit the supervisor and let Docker kill a server still cleaning up.
            while (PGDATA / 'postmaster.pid').exists():
                self.update(status='cleanup_blocked', stage='Waiting for PostgreSQL shutdown; no force termination')
                time.sleep(5)

    def verify_checkpoint(self):
        manifest = json.loads((CHECKPOINT / 'manifest.json').read_text())
        verified = json.loads((CHECKPOINT / 'verification.json').read_text())
        if (manifest['checkpoint_id'] != CHECKPOINT_ID or verified['checkpoint_id'] != CHECKPOINT_ID
                or not verified.get('restore_tested') or not verified['validation']['table_counts_match']
                or not verified['validation']['schema_matches']
                or manifest['archive_sha256'] != ARCHIVE_SHA256
                or verified['validation']['archive_sha256'] != ARCHIVE_SHA256):
            raise ValueError('Checkpoint identity or prior verification does not match authorization')
        archive = CHECKPOINT / 'database.dump'
        if archive.stat().st_size != manifest['archive_bytes']:
            raise ValueError('Checkpoint archive size mismatch')
        for name in ['manifest.json', 'verification.json', 'schema.sql', 'archive-contents.txt']:
            shutil.copyfile(CHECKPOINT / name, EVIDENCE / ('source-' + name))
        self.update(status='running', stage='1/4 — Verify checkpoint archive checksum',
                    progress={'bytes_verified': 0, 'archive_bytes': archive.stat().st_size})
        digest = hashlib.sha256(); processed = 0; last_update = 0
        with archive.open('rb') as stream:
            while chunk := stream.read(8 * 1024 * 1024):
                digest.update(chunk); processed += len(chunk)
                if time.monotonic() - last_update > 2:
                    self.check_budget(); last_update = time.monotonic()
                    self.update(progress={'bytes_verified': processed, 'archive_bytes': archive.stat().st_size})
        if digest.hexdigest() != ARCHIVE_SHA256:
            raise ValueError('Checkpoint SHA256 mismatch; restore refused')
        atomic_json(EVIDENCE / 'checkpoint-validation.json', {
            'checkpoint_id': CHECKPOINT_ID, 'archive_sha256': digest.hexdigest(),
            'archive_bytes': processed, 'checkpoint_mount_read_only': True})
        return manifest

    def execute(self):
        if os.geteuid() == 0:
            raise ValueError('Source restorer must run as the isolated postgres user')
        if (DATA / '.stage-d-owner').read_text().strip() != RUN_ID:
            raise ValueError('Disposable volume ownership marker mismatch')
        if PGDATA.exists():
            raise ValueError('Source data directory already exists; automatic overwrite refused')
        self.check_budget()
        manifest = self.verify_checkpoint()
        self.command(['initdb', '-D', str(PGDATA), '-U', ADMIN, '--encoding=UTF8',
                      '--locale=en_US.utf8', '--auth-local=trust', '--auth-host=reject'],
                     '2/4 — Initialize private historical source cluster')
        SOCKET.mkdir(mode=0o700)
        with (PGDATA / 'postgresql.conf').open('a') as config:
            config.write("\nlisten_addresses = ''\nport = 55432\n"
                         f"unix_socket_directories = '{SOCKET}'\n"
                         "shared_buffers = '1GB'\nwork_mem = '16MB'\nmaintenance_work_mem = '512MB'\n"
                         "max_parallel_maintenance_workers = 2\nmax_connections = 20\n"
                         "max_wal_size = '2GB'\narchive_mode = off\n")
        self.owns_cluster = True
        self.command(['pg_ctl', '-D', str(PGDATA), '-l', str(EVIDENCE / 'source-postgres.log'),
                      '-w', '-t', '60', 'start'], '2/4 — Start private socket-only PostgreSQL')
        roles = {manifest['database']['owner'], *manifest['database_support_roles']['names']}
        for role in sorted(roles):
            self.sql(f'CREATE ROLE {identifier(role)} NOLOGIN', database='postgres')
        self.sql(f'CREATE DATABASE {identifier(DATABASE)} TEMPLATE template0', database='postgres')
        self.command(['pg_restore', '--exit-on-error', '--no-owner', '--no-privileges', '--verbose',
                      '--jobs=2', '-h', str(SOCKET), '-p', '55432', '-U', ADMIN, '-d', DATABASE,
                      str(CHECKPOINT / 'database.dump')], '3/4 — Restore historical source', restore_progress=True)
        self.inspect_progress()
        self.check_budget()
        self.update(stage='4/4 — Verify exact table counts', progress=None)
        counts = []
        for number, table in enumerate(manifest['tables'], 1):
            self.check_budget()
            relation = identifier(table['schema']) + '.' + identifier(table['name'])
            self.update(progress={'tables_verified': number - 1, 'tables_total': len(manifest['tables']),
                                  'current_table': relation})
            # Large exact counts need time, but share the same overall guarded supervisor budget.
            query = ['psql', '-X', '--no-password', '-v', 'ON_ERROR_STOP=1', '-h', str(SOCKET),
                     '-p', '55432', '-U', ADMIN, '-d', DATABASE, '-At', '-c', f'SELECT count(*) FROM {relation}']
            with (EVIDENCE / 'current-count.txt').open('w') as output, (EVIDENCE / 'restore-commands.log').open('a') as log:
                self.child = subprocess.Popen(query, stdout=output, stderr=log, start_new_session=True)
                while self.child.poll() is None:
                    self.check_budget(); self.update()
                    try:
                        self.child.wait(timeout=0.25)
                    except subprocess.TimeoutExpired:
                        pass
                result = self.child.returncode; self.child = None
            if result:
                raise RuntimeError(f'Count verification failed for {relation}')
            actual = int((EVIDENCE / 'current-count.txt').read_text().strip())
            counts.append({**table, 'restored_rows': actual})
            atomic_json(EVIDENCE / 'restored-counts.json', counts)
            if actual != table['rows']:
                raise ValueError(f'Count mismatch for {relation}: expected {table["rows"]}, found {actual}')
        self.sql(f'CREATE ROLE {identifier(READER)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION')
        self.sql(f'GRANT CONNECT ON DATABASE {identifier(DATABASE)} TO {identifier(READER)}')
        for schema in sorted({table['schema'] for table in manifest['tables']}):
            self.sql(f'GRANT USAGE ON SCHEMA {identifier(schema)} TO {identifier(READER)}')
            self.sql(f'GRANT SELECT ON ALL TABLES IN SCHEMA {identifier(schema)} TO {identifier(READER)}')
        self.sql(f'ALTER ROLE {identifier(READER)} SET default_transaction_read_only = on')
        self.sql(f'ALTER DATABASE {identifier(DATABASE)} SET default_transaction_read_only = on')
        if self.sql('SHOW transaction_read_only', user=READER) != 'on':
            raise ValueError('Historical source did not become read-only')
        self.database_bytes = int(self.sql('SELECT pg_database_size(current_database())'))
        self.check_budget()
        atomic_json(EVIDENCE / 'source-ready.json', {
            'run_id': RUN_ID, 'database': DATABASE, 'reader_role': READER,
            'read_only': True, 'tables_verified': len(counts), 'database_bytes': self.database_bytes,
            'archive_sha256': ARCHIVE_SHA256, 'postgres_version': self.sql('SHOW server_version'),
            'original_database_accessed': False, 'checkpoint_modified': False})


def main():
    restore = Restore()
    def stop(signum, frame):
        restore.cancelled = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    failure = None
    try:
        restore.update()
        restore.execute()
    except Exception as error:
        failure = f'{type(error).__name__}: {error}'
        (EVIDENCE / 'restore-error.txt').write_text(failure + '\n')
    finally:
        restore.stop_children()
    restore.update(status='failed' if failure else 'finished',
                   stage='Source restore stopped — review saved evidence' if failure else
                         'Historical source verified and stopped — representative import next',
                   finished_at=time.time(), exit_code=1 if failure else 0,
                   message=failure, progress=None)
    return 1 if failure else 0


if __name__ == '__main__':
    raise SystemExit(main())
