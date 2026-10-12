"""Exercise ranking filters using an isolated schema, never production rows."""
import asyncio
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import constants
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.ask_db import get_top_main_characters_by_time_control


async def check():
    schema = 'home_rankings_test_' + uuid.uuid4().hex
    control = create_async_engine(constants.CONN_STRING)
    engine = create_async_engine(constants.CONN_STRING, connect_args={'server_settings': {'search_path': schema}})
    try:
        async with control.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as conn:
            for sql in (
                "CREATE TABLE game (link bigint)",
                "CREATE TABLE player (player_name text PRIMARY KEY, title text, name text, avatar text, url text, joined bigint, deleted_at timestamptz)",
                """CREATE TABLE main_character_mode_summary (mode text, player_name text, n_games int,
                    rating int, avg_game_rating int, last_rating int, wins int, draws int, losses int,
                    as_white int, as_black int, last_game_at timestamptz)""",
                """INSERT INTO player (player_name,title,joined) VALUES
                    ('gm','GM',1),('im',' im ',1),('nm','NM',1),('wfm','WFM',1),('untitled',NULL,1),
                    ('empty',' ',1),('bot','BOT',1),('deleted','GM',1)""",
                "UPDATE player SET deleted_at=now() WHERE player_name='deleted'",
                """INSERT INTO main_character_mode_summary (player_name,mode,rating,n_games) VALUES
                    ('gm','blitz',3100,10),('im','blitz',3200,2),('nm','blitz',2800,4),('wfm','blitz',2700,9),
                    ('untitled','blitz',3300,12),('empty','blitz',NULL,3),('bot','blitz',2500,1),
                    ('deleted','blitz',4000,10),('gm','bullet',3400,11)""",
            ):
                await conn.execute(text(sql))
        AsyncDBSession.configure(bind=engine)
        async def names(status, limit=50):
            result = await get_top_main_characters_by_time_control('blitz', limit, status)
            assert result['title_status'] == status
            return [p['player_name'] for p in result['players']]
        assert await names('titled') == ['im','gm','nm','wfm']
        assert await names('untitled') == ['untitled','bot','empty']
        assert await names('titled', 1) == ['im']  # Filtering must precede LIMIT.
        assert await names('untitled', 1) == ['untitled']
        assert await names('all') == ['untitled','im','gm','nm','wfm','bot','empty']
        rows = (await get_top_main_characters_by_time_control('blitz', 1, 'titled'))['players']
        assert rows[0]['title'] == 'IM' and rows[0]['rating'] == 3200
        assert (await get_top_main_characters_by_time_control('rapid', 50, 'titled'))['players'] == []
        try:
            await names('invalid')
        except ValueError:
            pass
        else:
            raise AssertionError('Invalid filter accepted')
        print('PASS: titled/untitled filters before LIMIT; blitz sorting, normalized titles, deleted exclusion, missing ratings, empty results and legacy all-player requests.')
    finally:
        await engine.dispose()
        async with control.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await control.dispose()


asyncio.run(check())
