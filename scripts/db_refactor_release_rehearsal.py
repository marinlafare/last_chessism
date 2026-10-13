"""Commit a canonical release on an independent PRIVATE restored copy only.

Unlike the rollback rehearsal, this leaves that disposable copy migrated, so
new connections/processes and a PostgreSQL restart can exercise the real runtime
profile. This is not a production deployment command or production approval.
"""
from unittest.mock import patch
import os

from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database import runtime
from chessism_api.database.relational_storage import storage_for, describe_contract
from scripts.db_refactor_candidate import guard, C, A
from scripts.db_refactor_cutover import inspect_cutover
from scripts.db_refactor_sequences import preserve_sequence
from scripts.db_refactor_switch_rehearsal import promote_private_candidate


def private_url(socket, database):
    return URL.create('postgresql+asyncpg', username='chessism_user', database=database,
                      port=55432, query={'host': str(socket)})


async def rejected_startup(url, profile, expected):
    with patch.dict(os.environ, {runtime.PROFILE_ENV: profile}):
        try:
            engine = await runtime.bind_runtime_engine(url, connect_args={'ssl': False})
        except RuntimeError as error:
            if expected not in str(error):
                raise
        else:
            await engine.dispose()
            raise RuntimeError('Unapproved/mismatched runtime profile was accepted')


async def check_runtime(socket, pgdata, *, database='chessism_db'):
    """Same DB initializers as services, without starting queues/cloud calls."""
    from chessism_api.database import engine as database_engine
    url = private_url(socket, database)
    checks = []
    # The caller cannot use this utility against a live/remote URL.
    probe = create_async_engine(url)
    try:
        async with probe.connect() as connection:
            await guard((await connection.get_raw_connection()).driver_connection, pgdata)
    finally:
        await probe.dispose()
    with patch.dict(os.environ, {runtime.PROFILE_ENV: 'relational_v2'}):
        for role in ('api_and_queue_workers', 'cloud_controller_and_admin_tools'):
            if role == 'api_and_queue_workers':
                await runtime.init_runtime_db(url.render_as_string(hide_password=False))
                engine = database_engine.async_engine
            else:
                engine = await runtime.bind_runtime_engine(url, connect_args={'ssl': False})
            try:
                async with database_engine.AsyncDBSession() as session:
                    if storage_for(session) != runtime.CANONICAL:
                        raise RuntimeError('Runtime session omitted the normalized storage contract')
                    if await session.scalar(text('SHOW search_path')) != runtime.CANONICAL.search_path:
                        raise RuntimeError('Runtime reader search path is incorrect')
                    # Bounded application-style reader and numeric writer. The
                    # write is rolled back, including homepage invalidation.
                    row = (await session.execute(text('SELECT link,white,black,mode FROM game ORDER BY link LIMIT 1'))).first()
                    if row:
                        await session.execute(text('UPDATE public.game SET eco=eco WHERE link=:link'), {'link': row.link})
                    await session.rollback()
                async with engine.connect() as connection:
                    contract = await describe_contract(connection, runtime.CANONICAL)
                checks.append(role)
            finally:
                await engine.dispose()
    await rejected_startup(url, 'legacy', 'legacy controller refused')
    return {'checks': checks + ['legacy_profile_rejected', 'canonical_backup_contract'],
            'contract': contract, 'cloud_calls': 0, 'queue_consumers_started': 0}


async def commit_private_release(socket, pgdata, publish, *, database='chessism_db'):
    url = private_url(socket, database)
    engine = create_async_engine(url)
    report = {'live_database_changed': False, 'production_ready': False, 'checks': [], 'sequences': []}
    try:
        async with engine.connect() as connection:
            driver = (await connection.get_raw_connection()).driver_connection
            async with connection.begin():
                await connection.execute(text('SET TRANSACTION READ ONLY'))
                inventory = await inspect_cutover(driver, pgdata)
            await publish('preserving_canonical_sequence_high_water')
            async with connection.begin():
                await connection.execute(text('SELECT 1'))
                await guard(driver, pgdata)
                for table, schema in (('moves', C), ('moves', A), ('months', C), ('game_fen_association', C)):
                    report['sequences'].append(await preserve_sequence(driver, 'public', schema, table))
            await publish('committing_private_canonical_switch')
            async with connection.begin():
                report['checks'].extend(await promote_private_candidate(connection, inventory, pgdata))
                await connection.execute(text('''CREATE TABLE public.relational_storage_release (
                    version text PRIMARY KEY,application_ready boolean NOT NULL DEFAULT false,
                    rehearsal_only boolean NOT NULL DEFAULT true,created_at timestamptz NOT NULL DEFAULT now());
                '''))
                await connection.execute(text("INSERT INTO public.relational_storage_release(version) VALUES ('relational_v2')"))
            await rejected_startup(url, 'relational_v2', 'release has not been approved')
            await rejected_startup(url, 'legacy', 'legacy controller refused')
            report['checks'].extend(['unapproved_release_rejected', 'legacy_profile_rejected'])
            await publish('approving_private_rehearsal_release')
            async with connection.begin():
                await guard(driver, pgdata)
                await connection.execute(text("UPDATE public.relational_storage_release SET application_ready=true WHERE version='relational_v2' AND rehearsal_only"))
        await publish('checking_canonical_runtime_startup')
        runtime_report = await check_runtime(socket, pgdata, database=database)
        report['checks'].extend(runtime_report['checks'])
        report['contract'] = runtime_report['contract']
        report['committed_to_private_copy'] = True
        return report
    finally:
        await engine.dispose()
