"""Explicit, connection-bound authority for the separately approved live CLI.

Path arguments still use the original private-only guard. A live target cannot
be reused on another connection or outside its maintenance transaction.
"""
from dataclasses import dataclass
import re

MAINTENANCE_LOCK = 684723019
STRICT_ARCHIVE_COMMAND = 'pgbackrest --stanza=chessism --archive-async=n archive-push %p'
MIN_POSTGRES_SHARED_MEMORY_BYTES = 256 * 1024 * 1024


def shared_memory_mount_bytes(mountinfo):
    """Read the Linux PostgreSQL server's mount, not the CLI container's mount."""
    mounts = []
    for line in mountinfo.splitlines():
        left, separator, right = line.partition(' - ')
        fields = left.split()
        if separator and len(fields) >= 6 and fields[4] == '/dev/shm':
            mounts.append(right.split())
    if len(mounts) != 1 or len(mounts[0]) < 3 or mounts[0][0] != 'tmpfs':
        raise RuntimeError('Cannot verify PostgreSQL /dev/shm tmpfs; review the server deployment')
    sizes = [value[5:] for value in mounts[0][2].split(',') if value.startswith('size=')]
    match = re.fullmatch(r'([0-9]+)([kmgt]?)', sizes[0], re.IGNORECASE) if len(sizes) == 1 else None
    if not match:
        raise RuntimeError('Cannot verify PostgreSQL /dev/shm size; refusing live migration')
    return int(match[1]) * 1024 ** ('kmgt'.index(match[2].lower()) + 1 if match[2] else 0)


async def require_shared_memory(conn):
    # These read-only queries run IN PostgreSQL. Checking local /dev/shm would
    # accidentally inspect the separate maintenance runner that does no SQL work.
    if await conn.fetchval('SHOW dynamic_shared_memory_type') != 'posix':
        raise RuntimeError('Live migration requires the reviewed POSIX shared-memory configuration')
    capacity = shared_memory_mount_bytes(await conn.fetchval("SELECT pg_read_file('/proc/self/mountinfo')"))
    if capacity < MIN_POSTGRES_SHARED_MEMORY_BYTES:
        raise RuntimeError(f'PostgreSQL /dev/shm is {capacity // 1024**2} MiB; '
                           'live migration requires at least 256 MiB, matching the rehearsal')
    return capacity


async def require_strict_archiving(conn):
    mode = await conn.fetchval('SHOW archive_mode')
    command = await conn.fetchval('SHOW archive_command')
    if mode != 'on' or command != STRICT_ARCHIVE_COMMAND:
        raise RuntimeError('Live migration requires the reviewed strict synchronous archive command; do not disable WAL archiving')


async def identity(conn):
    return dict(await conn.fetchrow("""SELECT current_database() AS database,
        current_setting('data_directory') AS pgdata,current_setting('port') AS port,
        (SELECT system_identifier::text FROM pg_control_system()) AS system_identifier,
        pg_backend_pid() AS pid"""))


async def require_no_clients(conn, allowed):
    await conn.execute('SELECT pg_stat_clear_snapshot()')
    count = await conn.fetchval("""SELECT count(*) FROM pg_stat_activity
        WHERE datname=current_database() AND backend_type='client backend'
        AND NOT(pid=ANY($1::int[]))""", list(allowed))
    if count:
        raise RuntimeError('Other database clients are connected; keep all application services stopped')


@dataclass(frozen=True)
class LiveMigrationTarget:
    database: str
    pgdata: str
    port: str
    system_identifier: str
    pid: int
    observer_pid: int

    async def verify(self, conn):
        current = await identity(conn)
        if any(current[key] != getattr(self,key) for key in current):
            raise RuntimeError('Live migration target/connection identity changed')
        if not conn.is_in_transaction():
            raise RuntimeError('Live migration requires one enclosing maintenance transaction')
        if not await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_locks
            WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted
            AND classid=0 AND objid=$1 AND objsubid=1)""", MAINTENANCE_LOCK):
            raise RuntimeError('Live maintenance lock is missing')
        await require_no_clients(conn, (self.pid,self.observer_pid))


async def authorize(conn, expected, observer_pid):
    """Called only after backup/evidence/approval gates, within the transaction."""
    if not conn.is_in_transaction():
        raise RuntimeError('Maintenance transaction required before authorization')
    current = await identity(conn)
    if set(expected) != {'database','pgdata','port','system_identifier'} or any(
        current[key] != value for key,value in expected.items()
    ):
        raise RuntimeError('Database identity does not match the explicitly approved target')
    await require_no_clients(conn,(current['pid'],observer_pid))
    if not await conn.fetchval('SELECT pg_try_advisory_xact_lock($1::bigint)',MAINTENANCE_LOCK):
        raise RuntimeError('Another migration owns the maintenance lock')
    target = LiveMigrationTarget(**current,observer_pid=observer_pid)
    await target.verify(conn)
    return target
