"""Canonical-only startup validation. Initialization is an explicit offline command."""
import logging
from copy import deepcopy

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from . import canonical
from .analysis_cleanup import finish_analysis_cleanup
from .engine import AsyncDBSession
from .transactions import driver_connection, read_transaction

LOG = logging.getLogger(__name__)


async def bind_runtime_engine(url, *, connect_args=None):
    """Validate before binding API/workers or constructing cloud clients; no DDL."""
    options = deepcopy(connect_args or {})
    settings = options.setdefault('server_settings', {})
    if 'search_path' in settings and settings['search_path'] != 'public':
        raise ValueError('Runtime search_path must be public; storage profiles are retired')
    settings['search_path'] = 'public'
    engine = create_async_engine(url, connect_args=options, pool_pre_ping=True)
    try:
        async with read_transaction(async_sessionmaker(engine)) as session:
            await canonical.validate(await driver_connection(session))
    except BaseException:
        try:
            await finish_analysis_cleanup(engine.dispose())
        except BaseException:
            LOG.exception('Database startup cleanup failed; preserving validation error')
        raise
    AsyncDBSession.configure(bind=engine, info={})
    return engine


async def init_runtime_db(url):
    """API/ARQ entry point using the same no-DDL validation as admin workers."""
    return await bind_runtime_engine(url)
