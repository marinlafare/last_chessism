"""Disposable, network-isolated PostgreSQL parity and invalidation checks."""
import asyncio
import faulthandler
from contextlib import suppress
from datetime import date
from fractions import Fraction
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

os.environ['DATABASE_URL'] = 'postgresql+asyncpg://chessism_user@127.0.0.1:1/postgres'


async def main():
    faulthandler.dump_traceback_later(45, repeat=True)
    import asyncpg
    from sqlalchemy import text
    from tests.check_db_refactor_candidate_postgres import FIXTURE, command
    from scripts.db_refactor_candidate import prepare_reference, build_candidate, C, V
    from scripts.db_refactor_application_checks import factory_for
    from chessism_api.database.player_analytics_schema import install_player_analytics, analytics_table
    from chessism_api.database.relational_storage import storage_for
    from chessism_api.operations import player_analytics_summary as summaries, player_hero_analytics as hero
    from chessism_api.operations import player_rating_distribution as histogram, player_game_catalog as catalog
    from chessism_api.database import ask_db

    root = Path(tempfile.mkdtemp(prefix='db-refactor-baseline-player-summary-', dir='/restore-test'))
    socket = root / 'socket'; socket.mkdir()
    pgctl = '/usr/lib/postgresql/15/bin/pg_ctl'
    conn = None
    engines = []
    try:
        await command('/usr/lib/postgresql/15/bin/initdb', '-D', str(root / 'pgdata'), '-U', 'chessism_user', '--auth-local=trust', '--auth-host=reject')
        await command(pgctl, '-D', str(root / 'pgdata'), '-l', str(root / 'postgres.log'), '-w', '-o',
                      f"-c listen_addresses='' -c port=55432 -c unix_socket_directories='{socket}'", 'start')
        conn = await asyncpg.connect(host=str(socket), port=55432, user='chessism_user', database='postgres')
        await conn.execute(FIXTURE)
        await conn.execute('''ALTER TABLE player ADD COLUMN country text,ADD COLUMN location text,
            ADD COLUMN timezone text,ADD COLUMN timezone_source text,ADD COLUMN deleted_at timestamptz;
            UPDATE player SET timezone='America/New_York',timezone_source='user';
            INSERT INTO player(player_name,joined,timezone) VALUES('empty',1,'UTC');
            INSERT INTO game SELECT (jsonb_populate_record(NULL::game,to_json(game)::jsonb ||
                jsonb_build_object('link',v.link,'played_at',v.stamp,'white','alice','black','alice'))).* FROM game
                CROSS JOIN (VALUES(5,'2025-03-09T06:59:00Z'),(6,'2025-03-09T07:01:00Z'),(7,NULL)) v(link,stamp)
                WHERE game.link=1;
            INSERT INTO game_player SELECT g.link,gp.color,'alice','alice',gp.result,gp.rating,gp.opponent_rating,
                g.mode,g.played_at,g.eco,g.n_moves,g.time_elapsed,g.avg_elo
                FROM game g CROSS JOIN game_player gp WHERE g.link IN(5,6,7) AND gp.link=1;''')
        await conn.execute('''INSERT INTO player(player_name,joined) VALUES('one-off',0);
            INSERT INTO game SELECT (jsonb_populate_record(NULL::game,to_json(game)::jsonb ||
            jsonb_build_object('link',v.id,'played_at','2025-03-09T07:10:00Z','white','alice','black',CASE WHEN v.id=128 THEN 'one-off' ELSE 'bob' END,
                'mode','blitz','eco','https://www.chess.com/openings/' || CASE WHEN v.id<110 THEN 'Frequent' WHEN v.id<118 THEN 'Winning'
                    WHEN v.id<125 THEN 'Drawing' WHEN v.id<128 THEN 'Perfect-rate' ELSE 'Single-game' END,
                'white_result',v.result,'black_result',1-v.result))).* FROM game
            CROSS JOIN (SELECT id,CASE WHEN id<105 OR id BETWEEN 110 AND 116 OR id=118 OR id>=125 THEN 1.0
                WHEN id BETWEEN 119 AND 124 THEN 0.5 ELSE 0.0 END AS result FROM generate_series(100,128) id) v
            WHERE game.link=1;
            INSERT INTO game_player SELECT g.link,side.color,
                CASE WHEN side.color='white' THEN g.white ELSE g.black END,
                CASE WHEN side.color='white' THEN g.black ELSE g.white END,
                CASE WHEN side.color='white' THEN g.white_result ELSE g.black_result END,
                CASE WHEN side.color='white' THEN g.white_elo ELSE g.black_elo END,
                CASE WHEN side.color='white' THEN g.black_elo ELSE g.white_elo END,
                g.mode,g.played_at,g.eco,g.n_moves,g.time_elapsed,g.avg_elo
            FROM game g CROSS JOIN (VALUES('white'),('black')) side(color) WHERE g.link BETWEEN 100 AND 128;''')
        async def publish(*args, **kwargs): pass
        # Every exact 10% boundary for all three outcomes in both catalogs.
        await conn.execute('''INSERT INTO player(player_name,joined)
            SELECT 'boundary-' || family || '-' || bin,0 FROM (VALUES('wins'),('losses'),('draws')) f(family)
            CROSS JOIN generate_series(0,10) bin;
            INSERT INTO game SELECT (jsonb_populate_record(NULL::game,to_json(game)::jsonb ||
            jsonb_build_object('link',v.id,'played_at','2025-03-09T07:10:00Z','white','alice','black',lower(v.opening),
                'mode','blitz','eco','https://www.chess.com/openings/' || v.opening,
                'white_result',v.result,'black_result',1-v.result))).* FROM game
            CROSS JOIN (SELECT family.id*1000 + bin*20 + n AS id,
                'Boundary-' || family.name || '-' || bin AS opening,
                CASE WHEN n<=bin*2 THEN family.result WHEN family.result=0 THEN 1.0 ELSE 0.0 END AS result
                FROM (VALUES (1,'wins',1.0),(2,'losses',0.0),(3,'draws',0.5)) family(id,name,result)
                CROSS JOIN generate_series(0,10) bin CROSS JOIN generate_series(1,20) n) v
            WHERE game.link=1;
            INSERT INTO game_player SELECT g.link,side.color,
                CASE WHEN side.color='white' THEN g.white ELSE g.black END,
                CASE WHEN side.color='white' THEN g.black ELSE g.white END,
                CASE WHEN side.color='white' THEN g.white_result ELSE g.black_result END,
                CASE WHEN side.color='white' THEN g.white_elo ELSE g.black_elo END,
                CASE WHEN side.color='white' THEN g.black_elo ELSE g.white_elo END,
                g.mode,g.played_at,g.eco,g.n_moves,g.time_elapsed,g.avg_elo
            FROM game g CROSS JOIN (VALUES('white'),('black')) side(color) WHERE g.link>=1000;''')
        await prepare_reference(conn, root / 'pgdata')
        await build_candidate(conn, root / 'pgdata', publish)

        class Redis:
            def __init__(self): self.data = {}
            async def get(self, key): return self.data.get(key)
            async def set(self, key, value, **kwargs): self.data[key] = value

        def clean(payload):
            if isinstance(payload, dict):
                return {key: clean(value) for key, value in payload.items()
                        if key not in ('generated_at','summary_revision','summary_stale','max_age_seconds','cache_hit')}
            if isinstance(payload, list): return [clean(value) for value in payload]
            return payload

        checks = 0
        for normalized in (False, True):
            print(f'Checking normalized={normalized}', flush=True)
            engine, factory = factory_for(socket, normalized=normalized, database='postgres', readonly=False)
            engines.append(engine)
            async with factory() as session:
                layout = storage_for(session)
                connection = await session.connection()
                await install_player_analytics(connection, layout)
                await session.commit()
            for player in ('alice','bob','empty'):
                result = await summaries.refresh_player(player, batch_days=1, session_factory=factory)
                for _ in range(20):
                    if result['status'] != 'building': break
                    result = await summaries.refresh_player(player, batch_days=1, session_factory=factory)
                assert result['status'] == 'ready', result
            with patch.object(hero,'AsyncDBSession',factory), patch.object(ask_db,'AsyncDBSession',factory), \
                 patch.object(histogram,'AsyncDBSession',factory), patch.object(catalog,'AsyncDBSession',factory), \
                 patch.object(summaries,'AsyncDBSession',factory):
                legacy_cache, summary_cache = Redis(), Redis()
                for player in ('alice','bob','empty'):
                    for mode in ('all','bullet','blitz','rapid',' ALL ',' BLITZ '):
                        for start,end in ((None,None),(date(2025,3,9),date(2025,3,9))):
                            os.environ['CHESSISM_PLAYER_SUMMARIES']='0'
                            expected = await hero.get_player_behavioural_activity(player,mode,start,end)
                            ratings = await hero.get_player_behavioural_ratings(player,mode,start,end)
                            for series in ratings['series']:
                                series['points'] = [point for point in series['points'] if point['game_id'] is not None]
                            os.environ['CHESSISM_PLAYER_SUMMARIES']='1'
                            assert clean(await hero.get_player_behavioural_activity(player,mode,start,end)) == clean(expected)
                            assert clean(await hero.get_player_behavioural_ratings(player,mode,start,end)) == clean(ratings)
                            checks += 2
                        os.environ['CHESSISM_PLAYER_SUMMARIES']='0'
                        expected = await histogram.get_player_rating_distribution(player,mode,Redis())
                        os.environ['CHESSISM_PLAYER_SUMMARIES']='1'
                        assert clean(await histogram.get_player_rating_distribution(player,mode,Redis())) == clean(expected)
                        checks += 1
                        for kind in ('opponents','openings'):
                            for color in ('all','white','black'):
                                for search in ('','bo','Test'):
                                    for page in (1,2):
                                        for sort_by in ('games','wins','losses','draws'):
                                            kwargs=dict(kind=kind,mode=mode,color=color,search=search,page=page,limit=1,sort_by=sort_by)
                                            os.environ['CHESSISM_PLAYER_SUMMARIES']='0'
                                            expected=await catalog.get_player_game_catalog(player,legacy_cache,**kwargs)
                                            os.environ['CHESSISM_PLAYER_SUMMARIES']='1'
                                            actual=await catalog.get_player_game_catalog(player,summary_cache,**kwargs)
                                            assert clean(actual)==clean(expected),(normalized,player,kwargs,actual,expected)
                                            minimum = 1 if kind == 'opponents' and search else 2
                                            assert all(row['games'] >= minimum for row in actual['rows'])
                                            checks += 1
                    os.environ['CHESSISM_PLAYER_SUMMARIES']='0'
                    expected=await ask_db.get_player_hours_played(player)
                    recent=await ask_db.get_player_games_page(player,page=1,page_size=1)
                    os.environ['CHESSISM_PLAYER_SUMMARIES']='1'
                    assert clean(await ask_db.get_player_hours_played(player))==clean(expected)
                    latest=await summaries.get_latest_game(player)
                    # Unlike the old query, NULL dates never displace a real last-play date.
                    assert latest['played_at'] is None or 'T' in latest['played_at']
                # One-off openings disappear before ranking/totals/pagination,
                # including searches and cached reads, in both read paths.
                for enabled in ('0', '1'):
                    os.environ['CHESSISM_PLAYER_SUMMARIES'] = enabled
                    for sort in ('games', 'wins', 'losses', 'draws'):
                        kwargs = dict(kind='openings', color='white', limit=50, sort_by=sort,
                                      ratio_bin=None if sort == 'games' else 9 if sort == 'wins' else 0)
                        cache = Redis()
                        for _ in range(2):
                            data = await catalog.get_player_game_catalog('alice', cache, **kwargs)
                            assert data['rows'] and all(row['games'] >= 2 for row in data['rows'])
                            assert 'Perfect-rate' in {row['key'] for row in data['rows']}
                            assert 'Single-game' not in {row['key'] for row in data['rows']}
                            assert data['total_games'] == sum(row['games'] for row in data['rows'])
                            assert data['pagination']['total_groups'] == len(data['rows'])
                            assert [row['rank'] for row in data['rows']] == list(range(1, len(data['rows']) + 1))
                            empty = await catalog.get_player_game_catalog('alice', cache, search='Single-game', **kwargs)
                            assert empty['rows'] == [] and empty['total_games'] == 0
                            assert empty['pagination']['total_groups'] == empty['pagination']['total_pages'] == 0
                            assert not empty['pagination']['has_more']
                            checks += 2
                # Search includes single encounters, browsing still excludes
                # them, and cached rankings cannot leak between the two views.
                for enabled in ('0', '1'):
                    os.environ['CHESSISM_PLAYER_SUMMARIES'] = enabled
                    cache = Redis()
                    browse = await catalog.get_player_game_catalog('alice', cache, kind='opponents', limit=50)
                    assert all(row['games'] >= 2 for row in browse['rows'])
                    for search in ('one-off',):
                        data = await catalog.get_player_game_catalog('alice', cache, kind='opponents', search=search)
                        assert len(data['rows']) == 1 and data['rows'][0]['key'] == search, (normalized, enabled, search, data)
                        assert data['pagination']['total_groups'] == data['total_games'] == data['rows'][0]['games'] == 1
                        assert not data['pagination']['has_more']
                        assert 'latest_ratings' in data['rows'][0]
                        assert clean(await catalog.get_player_game_catalog('alice', cache, kind='opponents', search=search.upper())) == clean(data)
                    assert clean(await catalog.get_player_game_catalog('alice', cache, kind='opponents', limit=50)) == clean(browse)
                    # Carol's only Alice game is archived Chess960, not an
                    # eligible standard game; search must not resurrect it.
                    assert (await catalog.get_player_game_catalog('alice', cache, kind='opponents', search='carol'))['rows'] == []
                    data = await catalog.get_player_game_catalog('alice', cache, kind='opponents', search='bob')
                    assert len(data['rows']) == 1 and data['rows'][0]['games'] >= 2
                    assert data['rows'][0]['latest_ratings']['blitz'] == 2300
                    data = await catalog.get_player_game_catalog('alice', cache, kind='opponents', search='bob', mode='rapid')
                    assert len(data['rows']) == 1 and data['rows'][0]['games'] == 1
                    assert (await catalog.get_player_game_catalog('alice', cache, kind='opponents', mode='rapid'))['rows'] == []
                    checks += 4
                # Every bin matches between SQL and saved summaries, including
                # exact boundaries, totals, caches, search and paginated order.
                for kind in ('openings', 'opponents'):
                    for outcome in ('wins', 'losses', 'draws'):
                        seen = []
                        for ratio_bin in range(10):
                            kwargs = dict(kind=kind, color='white', limit=50, sort_by=outcome, ratio_bin=ratio_bin)
                            os.environ['CHESSISM_PLAYER_SUMMARIES'] = '0'
                            expected = await catalog.get_player_game_catalog('alice', legacy_cache, **kwargs)
                            os.environ['CHESSISM_PLAYER_SUMMARIES'] = '1'
                            actual = await catalog.get_player_game_catalog('alice', summary_cache, **kwargs)
                            assert clean(actual) == clean(expected), (outcome, ratio_bin, actual, expected)
                            rows = actual['rows']
                            assert all(min(9, 10 * r[outcome] // r['games']) == ratio_bin for r in rows)
                            assert rows == sorted(rows, key=lambda r: (-Fraction(r[outcome], r['games']), -r['games'], r['key']))
                            assert actual['total_games'] == sum(r['games'] for r in rows)
                            assert actual['pagination']['total_groups'] == len(rows)
                            prefix = 'Boundary' if kind == 'openings' else 'boundary'
                            boundary_keys = {r['key'] for r in rows if r['key'].startswith(f'{prefix}-{outcome}-')}
                            assert boundary_keys == {f'{prefix}-{outcome}-{i}' for i in ([9,10] if ratio_bin == 9 else [ratio_bin])}
                            seen.extend(r['key'] for r in rows)
                            for enabled, cache in (('0', legacy_cache), ('1', summary_cache)):
                                os.environ['CHESSISM_PLAYER_SUMMARIES'] = enabled
                                assert clean(await catalog.get_player_game_catalog('alice', cache, **kwargs)) == clean(actual)
                                for page in (1,2):
                                    paged = await catalog.get_player_game_catalog('alice', cache, **{**kwargs, 'limit': 1, 'page': page})
                                    assert paged['rows'] == rows[page-1:page]
                                    assert paged['pagination']['has_more'] == (page < len(rows))
                                searched = await catalog.get_player_game_catalog('alice', cache, search='Boundary', **kwargs)
                                # Searching opponents includes one-game groups
                                # in the pre-search ranking, so global ranks can
                                # shift without changing these matching groups.
                                without_rank = lambda row: {key: value for key, value in row.items() if key != 'rank'}
                                assert [without_rank(r) for r in searched['rows']] == [without_rank(r) for r in rows if 'boundary' in r['key'].lower()]
                                checks += 4
                            checks += 1
                        baseline = await catalog.get_player_game_catalog('alice', summary_cache, kind=kind, color='white', limit=50)
                        assert sorted(seen) == sorted(r['key'] for r in baseline['rows'])
                os.environ['CHESSISM_PLAYER_SUMMARIES'] = '1'
                # Source corrections, deletions, and timezone changes invalidate
                # committed snapshots. Rollbacks do not, and retries are no-ops.
                table=lambda name: analytics_table(layout,name)
                async with factory() as session:
                    before=await session.scalar(text(f"SELECT revision FROM {table('state')} WHERE player_name='alice'"))
                    await session.execute(text(f"UPDATE {layout.table('player') if layout else 'player'} SET timezone='UTC' WHERE player_name='alice'"))
                    await session.rollback()
                    assert await session.scalar(text(f"SELECT revision FROM {table('state')} WHERE player_name='alice'"))==before
                async with factory.begin() as session:
                    await session.execute(text(f"UPDATE {layout.table('player') if layout else 'player'} SET timezone='Asia/Kolkata' WHERE player_name='alice'"))
                result=await summaries.refresh_player('alice',batch_days=1,session_factory=factory)
                assert result['status']=='building'
                async with factory() as session:
                    assert (await summaries.read_snapshot(session,'alice','hours'))['summary_stale']
                for _ in range(20):
                    if result['status'] != 'building': break
                    result=await summaries.refresh_player('alice',batch_days=1,session_factory=factory)
                assert result['status']=='ready',result
                assert (await summaries.refresh_player('alice',session_factory=factory))=={'status':'ready'}
                if normalized:
                    async with factory.begin() as session:
                        await session.execute(text(f"UPDATE {layout.table('game')} SET white_elo=2601,time_elapsed=90 WHERE link=1"))
                    assert (await summaries.refresh_player('alice',session_factory=factory))['days']==1
                    async with factory.begin() as session:
                        await session.execute(text(f"DELETE FROM {layout.table('game_player')} WHERE game_link=5 AND is_white=true"))
                    assert (await summaries.refresh_player('alice',session_factory=factory))['days']==1
                    os.environ['CHESSISM_PLAYER_SUMMARIES']='0'
                    expected=await hero.get_player_behavioural_activity('alice')
                    os.environ['CHESSISM_PLAYER_SUMMARIES']='1'
                    assert clean(await hero.get_player_behavioural_activity('alice'))==clean(expected)
                    # Engine pipeline status writes do not invalidate metadata.
                    async with factory.begin() as session:
                        before=await session.scalar(text(f"SELECT revision FROM {table('state')} WHERE player_name='alice'"))
                        await session.execute(text(f"UPDATE {layout.table('game')} SET fens_done=NOT fens_done WHERE link=1"))
                        assert await session.scalar(text(f"SELECT revision FROM {table('state')} WHERE player_name='alice'"))==before
                    # Duplicate import: neither source rows nor counters change.
                    async with factory.begin() as session:
                        await session.execute(text(f'''INSERT INTO {layout.table('game_player')}(game_link,player_id,is_white)
                            SELECT game_link,player_id,is_white FROM {layout.table('game_player')} WHERE game_link=1
                            ON CONFLICT DO NOTHING'''))
                        assert await session.scalar(text(f"SELECT revision FROM {table('state')} WHERE player_name='alice'"))==before
                    # A writer racing a refresh must leave its invalidation
                    # durable, rather than publish an older revision as current.
                    from sqlalchemy.ext.asyncio import AsyncSession
                    from sqlalchemy.orm import sessionmaker
                    from sqlalchemy.exc import DBAPIError
                    gate, written = asyncio.Event(), asyncio.Event()
                    class PausedSession(AsyncSession):
                        async def execute(self, statement, *args, **kwargs):
                            result = await super().execute(statement, *args, **kwargs)
                            if str(statement).startswith('WITH owner AS') and not gate.is_set():
                                gate.set()
                                await asyncio.wait_for(written.wait(), 10)
                            return result
                    paused = sessionmaker(engine, class_=PausedSession, expire_on_commit=False, info=factory.kw['info'])
                    async with factory.begin() as session:
                        await session.execute(text(f"UPDATE {layout.table('game')} SET white_elo=2602 WHERE link=1"))
                    refreshing = asyncio.create_task(summaries.refresh_player('alice',session_factory=paused))
                    await asyncio.wait_for(gate.wait(), 10)
                    async with factory.begin() as session:
                        await session.execute(text(f"UPDATE {layout.table('game')} SET white_elo=2603 WHERE link=1"))
                    written.set()
                    try:
                        await refreshing
                        raise AssertionError('Concurrent change was not detected')
                    except DBAPIError as failure:
                        assert getattr(failure.orig,'sqlstate',None)=='40001'
                    assert (await summaries.refresh_player('alice',session_factory=factory))['status']=='ready'
                    async with factory() as session:
                        saved=await summaries.read_snapshot(session,'alice','distribution')
                        assert not saved['summary_stale']
                        assert any(row['rating']==2603 for row in saved['counts'])
                    # A demoted profile is not readable as hero analytics and
                    # its derived rows disappear; shared game rows remain.
                    async with factory.begin() as session:
                        await session.execute(text(f"UPDATE {layout.table('player')} SET joined=0 WHERE player_name='alice'"))
                    assert (await summaries.refresh_player('alice',session_factory=factory))['status']=='ineligible'
                    async with factory() as session:
                        assert await summaries.read_snapshot(session,'alice','hours') is None
                        assert await session.scalar(text(f"SELECT count(*) FROM {layout.table('game')}"))>0
                    try:
                        await hero.get_player_behavioural_activity('alice')
                        raise AssertionError('Ineligible hero was allowed')
                    except LookupError:
                        pass
        print(f'PASS {checks} SQL/summary parity checks; legacy/normalized ownership, self-play, null dates, DST, modes, catalog filters/pagination; rollback, incremental corrections/deletions and atomic timezone rebuild',flush=True)
        faulthandler.cancel_dump_traceback_later()
    finally:
        for engine in engines: await engine.dispose()
        if conn: await conn.close()
        with suppress(Exception): await command(pgctl,'-D',str(root/'pgdata'),'-m','fast','-w','stop')


if __name__ == '__main__':
    asyncio.run(main())
