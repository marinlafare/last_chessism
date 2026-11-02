"""One explicit storage profile for API, queue workers and the cloud controller.

Changing the environment never migrates tables or authorizes a release. The
normalized profile requires the canonical schema AND its approved DB marker.
Legacy remains the default until a separately coordinated maintenance cutover.
"""
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from .relational_storage import RelationalStorage, SESSION_KEY, create_relational_engine

PROFILE_ENV = 'CHESSISM_DATABASE_PROFILE'
CANONICAL = RelationalStorage('public', 'chessism_archive', 'chessism_reads')
COMPACT = RelationalStorage('public', 'chessism_archive', 'chessism_reads', 'relational_v3')


def configured_storage(environ=None):
    profile = (os.environ if environ is None else environ).get(PROFILE_ENV, 'legacy')
    if profile == 'legacy':
        return None
    if profile == 'relational_v2':
        return CANONICAL
    if profile == 'relational_v3':
        return COMPACT
    raise ValueError(f'{PROFILE_ENV} must be legacy, relational_v2, or relational_v3')


async def init_runtime_db(url):
    """API/ARQ startup; legacy initialization only under the legacy profile."""
    from .engine import init_db
    await init_db(url, relational_storage=configured_storage())


async def bind_runtime_engine(url, *, connect_args=None):
    """No-DDL startup for controllers/admin tools; bind only after validation.

    In particular this must fail before constructing a cloud client or ticking
    the controller. SSL and other caller options survive the profile selection.
    """
    from .engine import AsyncDBSession
    layout = configured_storage()
    if layout:
        engine = await create_relational_engine(url, layout, connect_args=connect_args, pool_pre_ping=True)
    else:
        engine = create_async_engine(url, connect_args=connect_args or {}, pool_pre_ping=True)
        try:
            async with engine.connect() as connection:
                if await connection.scalar(text("""SELECT EXISTS(
                    SELECT 1 FROM information_schema.columns WHERE table_schema='public'
                    AND table_name='game' AND column_name='white_player_id')""")):
                    raise RuntimeError('Normalized game storage requires relational_v2; legacy controller refused')
        except BaseException:
            await engine.dispose()
            raise
    AsyncDBSession.configure(bind=engine, info={SESSION_KEY: layout} if layout else {})
    return engine
