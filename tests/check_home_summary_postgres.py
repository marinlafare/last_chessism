"""Uses an isolated, disposable schema; never alters production game/FEN data."""
import asyncio
import uuid
import logging
from unittest.mock import AsyncMock
from datetime import datetime, timedelta, timezone

import constants
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Base, HomeSummarySource, HomeTitledSummary, HomeTitledPlayerSummary
from chessism_api.database.home_summary_schema import ensure_home_summary_triggers
from chessism_api.operations.home_summary import get_home_summary, run_home_summary_refresh, needs_refresh
from chessism_api.operations import home_summary


async def check():
    schema = 'home_summary_test_' + uuid.uuid4().hex
    # Advisory locks are database-wide, not schema-local. Never compete with a
    # real summary job even though all our relations are isolated.
    home_summary.LOCK_ID = -(uuid.uuid4().int % (2**62))
    control = create_async_engine(constants.CONN_STRING)
    engine = create_async_engine(constants.CONN_STRING, connect_args={'server_settings': {'search_path': schema}})
    try:
        async with control.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[
                HomeSummarySource.__table__, HomeTitledSummary.__table__, HomeTitledPlayerSummary.__table__]))
            for sql in (
                "CREATE TABLE player (player_name text PRIMARY KEY, title text, deleted_at timestamptz, avatar text)",
                "CREATE TABLE main_character_mode_summary (player_name text, mode text, n_games bigint)",
                "CREATE TABLE game (link bigint PRIMARY KEY, white text, black text, fens_done boolean)",
                "CREATE TABLE fen (fen text PRIMARY KEY, score integer)",
                "CREATE TABLE game_fen_association (game_link bigint, fen_fen text)",
            ):
                await conn.execute(text(sql))
            await ensure_home_summary_triggers(conn)
            await ensure_home_summary_triggers(conn)  # Idempotent startup.
        AsyncDBSession.configure(bind=engine)
        redis = AsyncMock()
        empty = await get_home_summary(redis)
        assert empty['total_fens'] is None and empty['stale']
        assert empty['untitled_players'] == []
        redis.enqueue_job.assert_awaited_once()
        await run_home_summary_refresh({})
        empty = await get_home_summary(redis)
        assert empty['total_fens'] == empty['titled_games'] == 0 and not empty['stale']
        async with engine.begin() as conn:
            await conn.execute(text("""INSERT INTO player (player_name,title,deleted_at) VALUES
                ('a','GM',NULL), ('b',' im ',NULL), ('c',NULL,NULL), ('d','BOT',NULL),
                ('deleted','GM',now()), ('no-games','WFM',NULL)"""))
            await conn.execute(text("INSERT INTO game VALUES (1,'a','b',false),(2,'a','c',false),(3,'c','d',false),(4,'deleted','c',false)"))
            await conn.execute(text("INSERT INTO fen VALUES ('shared',NULL),('titled',0),('other',NULL),('orphan',NULL)"))
            await conn.execute(text("""INSERT INTO game_fen_association VALUES
                (1,'shared'),(1,'shared'),(1,'titled'),(2,'shared'),(3,'shared'),(3,'other'),(4,'other')"""))
        assert (await get_home_summary(redis))['stale']
        await run_home_summary_refresh({})
        result = await get_home_summary(redis)
        assert (result['total_fens'], result['titled_players'], result['titled_fens'], result['titled_games']) == (4, 3, 2, 2), result
        assert {p['player_name']: (p['games'],p['game_half_units']) for p in result['players']} == {
            'a': (2,3), 'b': (1,1), 'no-games': (0,0)}
        assert sum(p['game_half_units'] for p in result['players']) == result['titled_games'] * 2
        assert not result['stale']
        assert all(p['avatar'] is None for p in result['players'])
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE player SET avatar='https://images.example.invalid/a.jpg' WHERE player_name='a'"))
        with_avatar = await get_home_summary(redis)
        assert not with_avatar['stale'] and with_avatar['refreshed_at'] == result['refreshed_at']
        assert with_avatar['players'][0]['avatar'] == 'https://images.example.invalid/a.jpg'
        # Untitled bars use small, precomputed per-player/mode totals. Do not
        # include titled/deleted profiles or rescan raw games for this toggle.
        async with engine.begin() as conn:
            await conn.execute(text("""INSERT INTO main_character_mode_summary VALUES
                ('a','blitz',9000),('c','bullet',3210),('c','blitz',1000),
                ('d','rapid',7650),('deleted','blitz',99999)"""))
        with_untitled = await get_home_summary(redis)
        assert [(p['player_name'],p['games']) for p in with_untitled['untitled_players']] == [('d',7650),('c',4210)]
        assert not with_untitled['stale']
        # Scoring and processing flags never invalidate a structural summary.
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fen SET score=55"))
            await conn.execute(text("UPDATE game SET fens_done=true"))
        assert not (await get_home_summary(redis))['stale']
        async with engine.connect() as conn:
            await conn.execute(text("UPDATE player SET title=NULL WHERE player_name='a'"))
            await conn.rollback()
        assert not (await get_home_summary(redis))['stale']  # Invalidation rolls back too.
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE player SET title=NULL WHERE player_name='a'"))
        assert (await get_home_summary(redis))['stale']
        await run_home_summary_refresh({})
        result = await get_home_summary(redis)
        assert result['titled_games'] == 1 and result['titled_fens'] == 2
        assert result['players'][0]['player_name'] == 'b' and result['players'][0]['game_half_units'] == 2
        async with engine.begin() as conn:
            await conn.execute(text("DELETE FROM game WHERE link=1"))
            await conn.execute(text("DELETE FROM game_fen_association WHERE game_link=1"))
        await run_home_summary_refresh({})
        assert (await get_home_summary(redis))['titled_fens'] == 0
        # Cache reads must not access raw games, FENs or associations. Profiles
        # remain available only for cheap avatar/title lookups.
        async with engine.begin() as conn:
            for table in ('game', 'fen', 'game_fen_association'):
                await conn.execute(text(f'ALTER TABLE {table} RENAME TO hidden_{table}'))
        assert not (await get_home_summary(redis))['stale']
        assert [(p['player_name'],p['games']) for p in (await get_home_summary(redis))['untitled_players']] == [('a',9000),('d',7650),('c',4210)]
        try:
            logging.getLogger('chessism_api.operations.home_summary').disabled = True
            await run_home_summary_refresh({})
        except Exception:
            pass
        else:
            raise AssertionError('Expected missing-source failure')
        finally:
            logging.getLogger('chessism_api.operations.home_summary').disabled = False
        retained = await get_home_summary(redis)
        assert retained['total_fens'] == 4 and retained['refresh_error']
        now = datetime.now(timezone.utc)
        assert not needs_refresh({'attempted_at':now,'last_error':'failed'},now)
        assert needs_refresh({'attempted_at':now-timedelta(minutes=6),'last_error':'failed'},now)
        print('PASS: exact counts, live avatars without rebuilding, deletion, rollback, scoring exclusion, cheap cache reads, debounce and failure retention.', flush=True)
    finally:
        await engine.dispose()
        async with control.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await control.dispose()


if __name__ == '__main__':
    asyncio.run(check())
