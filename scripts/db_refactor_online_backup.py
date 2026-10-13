"""Private online worker-backup/WAL acceptance; no production backup mounts."""
import asyncio
from contextlib import ExitStack, contextmanager, suppress
import json
import os
from pathlib import Path
from unittest.mock import patch

from scripts.db_refactor_roundtrip import OfflineRoundTrip
from scripts.db_refactor_application_checks import factory_for


class PrivateRedis:
    """Worker transport stub only. Never connects to the application's queues.

    The production-sized runner independently holds the REAL backup lane for
    its entire lifetime. This stub exercises worker orchestration without
    publishing rehearsal statuses into the live UI or releasing that lane.
    """
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, *, nx=False, **kwargs):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def expire(self, *args):
        return True

    async def eval(self, script, keys, key, owner):
        if self.values.get(key) == owner:
            del self.values[key]
            return 1
        return 0


class OnlineApplicationBackup(OfflineRoundTrip):
    def prepare(self):
        super().prepare()
        self.volume = self.work / 'private-volume'
        self.root = self.volume / 'chessism'
        self.database_root = self.root / 'database'
        self.repository = self.database_root / 'pgbackrest'
        self.database_root.mkdir(parents=True)
        (self.volume / '.chessism-backup-volume').write_text('isolated-refactor-only')
        config = self.config.read_text().replace(f'repo1-path={self.work}/repository',
                                                f'repo1-path={self.repository}')
        self.config.write_text(config + '\nstart-fast=y\narchive-timeout=120\n')

    def start_options(self):
        return (f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{self.socket}' "
                "-c unix_socket_permissions=0700 -c archive_mode=on "
                f'-c "archive_command=pgbackrest --config={self.config} --stanza=refactor archive-push %p"')

    @contextmanager
    def redirected_worker(self, factory, artifact_root):
        from chessism_api.operations import database_backups as backup, matrix_backups
        from chessism_api.operations.matrix_constructor import storage
        # All file operations use new private paths. The actual command runner,
        # capacity checks, manifests, hashing and backup selection stay real.
        paths = {
            'BACKUP_VOLUME_ROOT':self.volume, 'EXPECTED_VOLUME_UUID':'isolated-refactor-only',
            'VOLUME_MARKER':self.volume/'.chessism-backup-volume', 'APP_BACKUP_ROOT':self.root,
            'DATABASE_BACKUP_ROOT':self.database_root, 'PGBACKREST_REPOSITORY':self.repository,
            'PGBACKREST_BACKUP_DIRECTORY':self.repository/'backup',
            'PGBACKREST_ARCHIVE_DIRECTORY':self.repository/'archive',
            'FEN_ANALYSIS_BACKUP_ROOT':self.root/'fen-analysis', 'RESEARCH_BACKUP_ROOT':self.root/'research',
            'MANIFEST_DIRECTORY':self.database_root/'manifests', 'BACKUP_STATUS_PATH':self.root/'backup-status.json',
            'BACKUP_USAGE_PATH':self.root/'backup-usage.json', 'BACKUP_CATALOG_PATH':self.database_root/'catalog.json',
            'PGBACKREST_GAP_PATH':self.work/'archive-gap', 'PGBACKREST_STANZA':'refactor',
            'DISPLAY_ROOT':str(self.database_root), 'FREE_FLOOR_BYTES':20_000_000_000,
            'STOP_MARGIN_BYTES':2_000_000_000, 'APP_QUOTA_BYTES':200_000_000_000,
        }
        with ExitStack() as stack:
            for key, value in paths.items():
                stack.enter_context(patch.object(backup,key,value))
            stack.enter_context(patch.dict(os.environ,{'PGBACKREST_CONFIG':str(self.config)}))
            stack.enter_context(patch.object(storage,'AsyncDBSession',factory))
            stack.enter_context(patch.object(matrix_backups,'ARTIFACT_ROOT',Path(artifact_root)))
            yield backup

    async def backup(self, conn, *, database='chessism_db', artifact_root='/candidate/research_data/matrices'):
        from chessism_api.operations.database_backup_job import run_database_backup_job
        await conn.execute('''CREATE SCHEMA refactor_wal_probe;
            CREATE TABLE refactor_wal_probe.commits(id integer PRIMARY KEY,stamp timestamptz NOT NULL DEFAULT clock_timestamp());
            INSERT INTO refactor_wal_probe.commits(id) VALUES(0)''')
        engine, factory = factory_for(self.socket,normalized=True,database=database)
        transport = PrivateRedis()
        try:
            with self.redirected_worker(factory,artifact_root) as backup:
                async def monitor():
                    while True:
                        file = backup.BACKUP_STATUS_PATH
                        if file.exists():
                            info=json.loads(file.read_text())
                            self.status.update(backup_phase=info.get('phase'),
                                backup_bytes_done=info.get('progress_completed_bytes',0),
                                backup_bytes_total=info.get('progress_total_bytes',0))
                        await asyncio.sleep(2)
                monitor_task=asyncio.create_task(monitor())
                writer_task=None
                try:
                    await self.publish('online_application_full_backup')
                    # Real committed writes continue while the worker backs up.
                    # A separate deterministic post-backup sentinel below proves
                    # WAL replay, independent of exactly when these arrive.
                    async def writer():
                        import asyncpg
                        write_conn=await asyncpg.connect(host=str(self.socket),port=55432,
                                                        user='chessism_user',database=database)
                        try:
                            for index in range(1,101):
                                await write_conn.execute('INSERT INTO refactor_wal_probe.commits(id) VALUES($1)', index)
                                self.status['writes_during_backup']=index
                                await asyncio.sleep(1)
                        finally:
                            await write_conn.close()
                    writer_task=asyncio.create_task(writer())
                    full=await run_database_backup_job({'redis':transport,'job_id':'isolated-full'})
                    writer_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await writer_task
                    writer_task=None
                    await self.publish('online_application_incremental_backup')
                    await conn.execute('INSERT INTO refactor_wal_probe.commits(id) VALUES(1001)')
                    incremental=await run_database_backup_job({'redis':transport,'job_id':'isolated-incremental'})
                    if full['type']!='full' or incremental['type']!='incr' or incremental.get('prior')!=full['backup_id']:
                        raise RuntimeError('Private full/incremental chain selection did not match')
                    manifest=json.loads(Path(incremental['manifest']).read_text())
                    if manifest['schema_version']!=7 or manifest['fen_reference_version']!='relational_v2':
                        raise RuntimeError('Application did not write the v7 relational manifest')
                    if full.get('removed_backup_ids') or incremental.get('removed_backup_ids'):
                        raise RuntimeError('Fresh private chain unexpectedly expired a recovery point')
                    await self.publish('archiving_post_backup_commits')
                    await conn.execute('INSERT INTO refactor_wal_probe.commits(id) VALUES(2001)')
                    target=await conn.fetchval('SELECT clock_timestamp()')
                    expected=await conn.fetchval('SELECT array_agg(id ORDER BY id) FROM refactor_wal_probe.commits')
                    # This later commit ensures recovery can reach AND stop at
                    # the target instead of running to the end of all WAL.
                    await conn.execute('SELECT pg_sleep(0.01)')
                    await conn.execute('INSERT INTO refactor_wal_probe.commits(id) VALUES(2002)')
                    await conn.fetchval('SELECT pg_switch_wal()')
                    await self.command('verifying_online_wal_archive','check')
                    self.status['private_backup_id']=incremental['backup_id']
                    return {'full_id':full['backup_id'], 'backup_id':incremental['backup_id'],
                        'manifest':manifest, 'target':target.isoformat(), 'expected_commits':expected,
                        'mode':'online', 'repository':str(self.repository),
                        'retention':'Private full and incremental retained; no production repository mounted.',
                        'limitations':['Real worker functions tested; Redis/HTTP/ARQ transport is not an end-to-end UI test.',
                            'This is not a canonical-schema cutover rehearsal.']}
                finally:
                    for task in (writer_task,monitor_task):
                        if task is not None:
                            task.cancel()
                            with suppress(asyncio.CancelledError):
                                await task
        finally:
            await engine.dispose()

    async def restore(self, result):
        if (self.source/'postmaster.pid').exists():
            raise RuntimeError('Stop source before starting the independent restore')
        await self.command('restoring_online_application_backup',f'--set={result["backup_id"]}',
            f'--pg1-path={self.restored}', '--archive-mode=off', '--type=time',
            f'--target={result["target"]}', '--target-action=promote', 'restore')

    async def validate(self, conn, result, *, database='chessism_db'):
        from chessism_api.operations import database_restore_tests as restore
        from chessism_api.operations.matrix_constructor.definition_backups import validate_definition_backup
        from chessism_api.operations.research_algorithms.backups import ALGORITHM_BACKUP_QUERY, validate_algorithm_backup
        from chessism_api.operations.matrix_backups import validate_matrix_bundle
        await self.publish('validating_online_application_restore')
        actual=await conn.fetchval('SELECT array_agg(id ORDER BY id) FROM refactor_wal_probe.commits')
        if actual!=result['expected_commits'] or 2001 not in actual or 2002 in actual:
            raise RuntimeError('Recovery target did not preserve the expected committed writes')
        manifest=result['manifest']
        with patch.object(restore,'RESTORE_DATABASE',database):
            identity=await restore._validate_fen_identity(self.restored.parent/'socket',manifest)
        definitions=[dict(r) for r in await conn.fetch('SELECT id,name,row_type,config FROM matrix_definition ORDER BY id')]
        for row in definitions:
            if isinstance(row['config'],str):
                row['config']=json.loads(row['config'])
        definition_check=validate_definition_backup(manifest['matrix_definitions'],definitions)
        algorithm_check=validate_algorithm_backup(manifest['algorithms'],json.loads(await conn.fetchval(ALGORITHM_BACKUP_QUERY)))
        ids=[r['id'] for r in await conn.fetch("SELECT id FROM matrix_artifact WHERE status='complete' ORDER BY id")]
        matrix_check=await asyncio.to_thread(validate_matrix_bundle,manifest['matrices'],self.root/'research'/'matrices',ids)
        return {'identity':identity,'definitions':definition_check,'algorithms':algorithm_check,
                'matrices':matrix_check,'post_backup_wal_replay':'passed','target_excludes_later_commit':True}
