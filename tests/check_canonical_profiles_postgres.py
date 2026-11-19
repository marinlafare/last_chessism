"""Canonical profile/statistics persistence with synthetic remote responses only."""
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import httpx
from fastapi import FastAPI
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, player_profiles as persistence
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations import players
from chessism_api.operations.models import PlayerCreateData, PlayerStatsCreateData
from chessism_api.routers import players as routes
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class CanonicalProfileTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp(); await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test', database=self.name,
            port=55432, query={'host': self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        for name, joined, deleted in (('alice', 123, False), ('bob', 456, False),
                                      ('shell', 0, False), ('removed', 123, True)):
            await self.db.execute('''INSERT INTO player(player_name,joined,timezone,timezone_source,deleted_at)
                VALUES($1,$2,'America/Mexico_City','manual',$3)''', name, joined,
                datetime(2025, 1, 1, tzinfo=timezone.utc) if deleted else None)
        await self.db.execute('''INSERT INTO account(id,name,email,password_hash,role,is_active,chess_com_nickname)
            VALUES('owner','Owner','synthetic@example.invalid','unused-test-only','user',true,'alice')''')
        self.account = dict(await self.db.fetchrow('SELECT * FROM account'))
        # Every possible remote operation is patched for the entire test lifetime.
        for name, value in (('get_profile', None), ('get_player_stats', None)):
            mock = patch.object(players, name, AsyncMock(return_value=value))
            setattr(self, name, mock.start()); self.addCleanup(mock.stop)

    def profile(self, player='alice', **changes):
        return PlayerCreateData(player_name=player, joined=999, name='Fresh profile', title='GM', **changes)

    async def refresh(self, player='alice', **kwargs):
        return await players.insert_player({'player_name': player}, session_factory=kwargs.pop('session_factory', self.factory), **kwargs)

    async def stats(self, player='alice', **kwargs):
        return await players.create_and_store_player_stats(player, session_factory=kwargs.pop('session_factory', self.factory), **kwargs)

    async def stored(self, player='alice'):
        return await players.read_player_stats(player, session_factory=self.factory)

    def remote_stats(self, rating=1800):
        return {'chess_rapid': {'last': {'rating': rating, 'percentile': 91.5}, 'best': {'rating': 1900},
                    'record': {'game_count': 10, 'win': 6, 'loss': 3, 'draw': 1}},
                'chess_blitz': {'last': {'rating': 1700, 'percentage_rank': 87.25}}, 'chess_bullet': None,
                'fide': 2000, 'puzzle_rush': {'best': {'score': 35}},
                'tactics': {'highest': {'rating': 2400}, 'lowest': {'rating': 1200}}}

    async def test_read_contracts_missing_profiles_and_full_profile_roster(self):
        self.assertIsNone(await players.read_player('absent', session_factory=self.factory))
        self.assertIsNone(await self.stored())
        row = await players.read_player(' ALICE ', session_factory=self.factory)
        self.assertEqual(row['player_name'], 'alice'); self.assertNotIn('id', row)
        self.assertEqual(set(row), set(persistence.PROFILE_FIELDS) | {'deleted_at'})
        roster = await players.get_current_players_with_games_in_db(session_factory=self.factory)
        self.assertEqual([p['player_name'] for p in roster], ['alice', 'bob'])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'), 0)
        self.get_profile.assert_not_awaited(); self.get_player_stats.assert_not_awaited()
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_missing_remote_profile_creates_only_one_shell(self):
        results = await asyncio.gather(*(self.refresh(' NEW ') for _ in range(4)))
        self.assertTrue(all(r.player_name == 'new' and r.joined == 0 for r in results))
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM player WHERE player_name='new'"), 1)
        self.assertEqual(dict(await self.db.fetchrow('SELECT * FROM account')), self.account)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 0)

    async def test_missing_remote_profile_preserves_full_and_deleted_rows_exactly(self):
        for name in ('alice', 'removed'):
            before = dict(await self.db.fetchrow('SELECT * FROM player WHERE player_name=$1', name))
            returned = await self.refresh(name)
            self.assertEqual(returned.joined, before['joined'])
            self.assertEqual(dict(await self.db.fetchrow('SELECT * FROM player WHERE player_name=$1', name)), before)

    async def test_successful_refresh_restores_deleted_profile_without_changing_identity(self):
        original = await self.db.fetchval("SELECT id FROM player WHERE player_name='removed'")
        self.get_profile.return_value = self.profile('removed')
        result = await self.refresh('removed')
        row = await self.db.fetchrow("SELECT * FROM player WHERE player_name='removed'")
        self.assertEqual(row['id'], original); self.assertIsNone(row['deleted_at'])
        self.assertEqual(result.name, 'Fresh profile'); self.assertEqual(row['joined'], 999)
        self.assertEqual(row['timezone'], 'America/Mexico_City'); self.assertEqual(row['timezone_source'], 'manual')

    async def test_remote_fetch_has_no_open_transaction_and_preserves_concurrent_timezone_edit(self):
        async def fetch(name):
            self.assertEqual(self.engine.pool.checkedout(), 0)
            await self.db.execute("UPDATE player SET timezone='Asia/Tokyo',timezone_source='curated' WHERE player_name=$1", name)
            return self.profile(name)
        self.get_profile.side_effect = fetch
        result = await self.refresh()
        self.assertEqual(result.timezone, 'Asia/Tokyo'); self.assertEqual(result.timezone_source, 'curated')
        self.get_profile.side_effect = None
        self.get_profile.return_value = self.profile(timezone='Europe/London', timezone_source='explicit')
        result = await self.refresh()
        self.assertEqual((result.timezone, result.timezone_source), ('Europe/London', 'explicit'))

    async def test_concurrent_successful_refreshes_and_shell_request_keep_one_numeric_identity(self):
        original = await self.db.fetchval("SELECT id FROM player WHERE player_name='alice'")
        self.get_profile.side_effect = [None, self.profile(), self.profile(), None]
        await asyncio.gather(*(self.refresh('ALICE') for _ in range(4)))
        row = await self.db.fetchrow("SELECT * FROM player WHERE player_name='alice'")
        self.assertEqual((row['id'], row['joined'], row['name']), (original, 999, 'Fresh profile'))
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM player WHERE player_name='alice'"), 1)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_stats_create_shell_and_fetch_without_open_database_transaction(self):
        async def profile(name):
            self.assertEqual(self.engine.pool.checkedout(), 0); return None
        async def stats(name):
            self.assertEqual(self.engine.pool.checkedout(), 0); return self.remote_stats()
        self.get_profile.side_effect = profile; self.get_player_stats.side_effect = stats
        saved = await self.stats(' UNKNOWN ')
        self.assertEqual(saved.player_name, 'unknown'); self.assertEqual(saved.chess_rapid_last_rating, 1800)
        row = await self.db.fetchrow('''SELECT p.player_name,p.joined,s.player_id FROM player_stats s
            JOIN player p ON p.id=s.player_id''')
        self.assertEqual((row['player_name'], row['joined']), ('unknown', 0))
        self.assertEqual(dict(await self.db.fetchrow('SELECT * FROM account')), self.account)

    async def test_stats_payload_preserves_fields_nulls_and_percentile_fallback(self):
        self.get_player_stats.return_value = self.remote_stats()
        result = await self.stats()
        self.assertIsInstance(result, PlayerStatsCreateData)
        expected = {'chess_rapid_last_rating': 1800, 'chess_rapid_best_rating': 1900,
            'chess_rapid_games': 10, 'chess_rapid_wins': 6, 'chess_rapid_losses': 3, 'chess_rapid_draws': 1,
            'chess_rapid_last_percentile': 91.5, 'chess_blitz_last_rating': 1700,
            'chess_blitz_last_percentile': 87.25, 'fide': 2000, 'puzzle_rush_best_score': 35,
            'tactics_highest_rating': 2400, 'tactics_lowest_rating': 1200}
        for key in persistence.STATS_FIELDS:
            self.assertEqual(getattr(result, key), expected.get(key), key)
        stored = await self.stored(' ALICE ')
        self.assertEqual({k: v for k, v in stored.items() if k != 'last_updated'}, result.model_dump())
        self.assertIsNotNone(stored['last_updated']); self.assertNotIn('player_id', stored)

    async def test_stats_replace_the_snapshot_and_update_publication_time(self):
        self.get_player_stats.return_value = self.remote_stats(); await self.stats()
        before = await self.stored()
        self.get_player_stats.return_value = {'chess_rapid': {'last': {'rating': 1500}}}
        await self.stats(); after = await self.stored()
        self.assertEqual(after['chess_rapid_last_rating'], 1500)
        self.assertIsNone(after['fide']); self.assertIsNone(after['chess_blitz_last_rating'])
        self.assertGreater(after['last_updated'], before['last_updated'])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 1)

    async def test_remote_absence_validation_failure_and_exception_preserve_saved_stats(self):
        self.get_player_stats.return_value = self.remote_stats(); await self.stats(); before = await self.stored()
        for value in (None, {}, {'fide': 'not an integer'}):
            self.get_player_stats.return_value = value
            self.assertIsNone(await self.stats()); self.assertEqual(await self.stored(), before)
        self.get_player_stats.side_effect = RuntimeError('synthetic network failure')
        with self.assertRaisesRegex(RuntimeError, 'synthetic'): await self.stats()
        self.assertEqual(await self.stored(), before)

    async def test_concurrent_stats_upserts_publish_one_complete_snapshot(self):
        self.get_player_stats.side_effect = [self.remote_stats(1800), {'chess_blitz': {'last': {'rating': 2100}}}]
        results = await asyncio.gather(self.stats(), self.stats())
        stored = await self.stored()
        payload = {k: v for k, v in stored.items() if k != 'last_updated'}
        self.assertIn(payload, [result.model_dump() for result in results])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 1)

    async def test_stats_follow_numeric_owner_rename_and_cascade_without_name_reuse(self):
        self.get_player_stats.return_value = self.remote_stats(); await self.stats()
        original = await self.db.fetchval("SELECT id FROM player WHERE player_name='alice'")
        await self.db.execute("UPDATE player SET player_name='renamed' WHERE id=$1", original)
        self.assertIsNone(await self.stored('alice'))
        self.assertEqual((await self.stored('renamed'))['chess_rapid_last_rating'], 1800)
        await self.db.execute("INSERT INTO player(player_name,joined) VALUES('alice',123)")
        self.assertIsNone(await self.stored('alice'))
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute('INSERT INTO player_stats(player_id) VALUES(999999)')
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO player_stats(player_id) VALUES($1)', original)
        await self.db.execute('DELETE FROM player WHERE id=$1', original)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 0)
        self.assertEqual(dict(await self.db.fetchrow('SELECT * FROM account')), self.account)

    async def test_adapter_requires_transaction_and_read_operations_are_readonly(self):
        async with self.factory() as session:
            with self.assertRaisesRegex(ValueError, 'caller-owned'): await persistence.read_profile(session, 'alice')
        original = persistence.read_profile
        async def readonly(session, name):
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')), 'on')
            self.assertEqual(await session.scalar(text('SHOW transaction_isolation')), 'repeatable read')
            return await original(session, name)
        with patch.object(persistence, 'read_profile', readonly):
            self.assertEqual((await players.read_player('alice', session_factory=self.factory))['joined'], 123)
        with self.assertRaises(DBAPIError):
            async with read_transaction(self.factory) as session:
                await persistence.ensure_shell(session, 'forbidden')
        self.assertIsNone(await self.db.fetchval("SELECT id FROM player WHERE player_name='forbidden'"))

    async def test_database_errors_and_failed_commit_do_not_report_a_saved_profile_or_stats(self):
        self.get_profile.return_value = self.profile(); self.get_player_stats.return_value = self.remote_stats()
        async def fail(*args, **kwargs): raise RuntimeError('synthetic database failure')
        for method, work in (('save_profile', self.refresh), ('save_stats', self.stats)):
            with patch.object(persistence, method, fail), self.assertRaisesRegex(RuntimeError, 'synthetic database'):
                await work()
            self.assertEqual(self.engine.pool.checkedout(), 0)
        def failed_commit():
            session = self.factory(); session.commit = fail; return session
        with self.assertRaisesRegex(RuntimeError, 'synthetic database'): await self.refresh(session_factory=failed_commit)
        self.assertEqual(await self.db.fetchval("SELECT joined FROM player WHERE player_name='alice'"), 123)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 0)

    async def test_cancellation_after_write_drains_cleanup_and_rolls_back_before_propagating(self):
        self.get_profile.return_value = self.profile(); self.get_player_stats.return_value = self.remote_stats()
        for method, work in (('save_profile', self.refresh), ('save_stats', self.stats)):
            entered, rolling, release = (asyncio.Event() for _ in range(3))
            closures = []
            original = getattr(persistence, method)
            async def write_then_pause(*args):
                await original(*args); entered.set(); await asyncio.Future()
            def factory():
                session = self.factory(); rollback, close = session.rollback, session.close
                closed = asyncio.Event(); closures.append(closed)
                async def delayed(): rolling.set(); await release.wait(); await rollback()
                async def closing():
                    try: await close()
                    finally: closed.set()
                session.rollback = delayed; session.close = closing; return session
            with patch.object(persistence, method, write_then_pause):
                task = asyncio.create_task(work(session_factory=factory))
                try:
                    await asyncio.wait_for(entered.wait(), 2); task.cancel()
                    await asyncio.wait_for(rolling.wait(), 2)
                    for _ in range(2): task.cancel(); await asyncio.sleep(0); self.assertFalse(task.done())
                finally:
                    release.set(); task.cancel(); await asyncio.gather(task, return_exceptions=True)
            self.assertTrue(task.cancelled()); self.assertTrue(closures)
            self.assertTrue(all(closed.is_set() for closed in closures))
            self.assertEqual(self.engine.pool.checkedout(), 0)
            self.assertEqual(await self.db.fetchval("SELECT joined FROM player WHERE player_name='alice'"), 123)
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 0)

    async def test_missing_stats_owner_cannot_create_an_orphan(self):
        async with write_transaction(self.factory) as session:
            with self.assertRaisesRegex(LookupError, 'owner'):
                await persistence.save_stats(session, PlayerStatsCreateData(player_name='absent').model_dump())
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stats'), 0)

    async def test_actual_profile_stats_roster_and_hero_routes_keep_response_shapes(self):
        app = FastAPI(); app.include_router(routes.router, prefix='/players')
        self.get_profile.return_value = self.profile('fresh'); self.get_player_stats.return_value = self.remote_stats()
        with patch.object(players, 'AsyncDBSession', self.factory):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.get('/players/alice'); self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['player_name'], 'alice'); self.assertNotIn('id', response.json())
                response = await client.get('/players/fresh'); self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['joined'], 999)
                response = await client.get('/players/alice/stats'); self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['chess_rapid_last_rating'], 1800); self.assertNotIn('player_id', response.json())
                with patch.object(persistence, 'save_stats', AsyncMock(side_effect=RuntimeError('synthetic DB outage'))):
                    self.assertEqual((await client.get('/players/alice/stats')).status_code, 500)
                response = await client.get('/players/current_players'); self.assertEqual(response.status_code, 200)
                self.assertEqual([r['player_name'] for r in response.json()], ['alice', 'bob', 'fresh'])
                self.assertEqual((await client.get('/players/alice/hero')).status_code, 200)
                for player in ('shell', 'removed', 'missing'):
                    self.assertEqual((await client.get(f'/players/{player}/hero')).status_code, 404)
        self.assertEqual(dict(await self.db.fetchrow('SELECT * FROM account')), self.account)

    async def test_batch_result_contract_tracks_failure_without_affecting_other_profiles(self):
        async def remote(name):
            if name == 'alice': raise RuntimeError('synthetic remote failure')
            return self.remote_stats()
        self.get_player_stats.side_effect = remote
        with patch.object(players, 'AsyncDBSession', self.factory):
            result = await players.update_stats_for_all_primary_players(api_delay=0)
        self.assertEqual(result, {'success': ['bob'], 'failed': ['alice']})
        self.assertIsNone(await self.stored('alice')); self.assertIsNotNone(await self.stored('bob'))
        self.assertEqual(self.engine.pool.checkedout(), 0)
