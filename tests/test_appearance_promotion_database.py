"""Promotion checks against a private, disposable socket-only PostgreSQL."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Base, Account, AppearancePreset, AppearanceSetting, AppearanceOverride
from chessism_api.operations.appearance import read_appearance, change_appearance, AppearanceConflict
from chessism_api.operations.appearance_promotion import promote_default

URL = os.environ.get('CHESSISM_CLOUD_TEST_DATABASE_URL', '')


@unittest.skipUnless(URL, 'requires explicitly isolated PostgreSQL')
class AppearancePromotionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if not URL.endswith('/chessism_cloud_test'):
            raise RuntimeError('Refusing a non-test database')
        self.schema = 'appearance_promotion_' + uuid4().hex
        self.control = create_async_engine(URL)
        async with self.control.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{self.schema}"'))
        self.engine = create_async_engine(URL, connect_args={'server_settings': {'search_path': self.schema}})
        AsyncDBSession.configure(bind=self.engine)
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[Account.__table__, AppearancePreset.__table__, AppearanceSetting.__table__, AppearanceOverride.__table__]))
        async with AsyncDBSession.begin() as session:
            session.add(Account(id='test-admin', name='Test', email='test@example.invalid', password_hash='not-real', role='admin'))
        self.rows = [
            {'scope': 'positions', 'target': 'cloud.heading', 'property': 'radius', 'value': 18},
            {'scope': 'home', 'target': 'page', 'property': 'background', 'value': '#123456'},
            {'scope': 'home', 'target': 'heading', 'property': 'fontFamily', 'value': 'Inter'},
        ]
        self.before = await change_appearance('save', {'revision': 0, 'name': 'test_1', 'overrides': self.rows}, 'test-admin')
        self.directory = tempfile.TemporaryDirectory(prefix='appearance-promotion-')
        self.backup = Path(self.directory.name) / 'before.json'

    async def asyncTearDown(self):
        self.directory.cleanup()
        await self.engine.dispose()
        async with self.control.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        await self.control.dispose()

    async def promote(self, **kwargs):
        return await promote_default(source_id=self.before['active_preset_id'],
            expected_revision=self.before['revision'], backup_path=self.backup, **kwargs)

    async def test_promotion_preserves_styles_archive_and_verified_backup(self):
        result = await self.promote()
        await self.engine.dispose()
        after = await read_appearance()
        self.assertEqual(after['active_preset_id'], self.before['active_preset_id'])
        self.assertEqual(after['default_preset_id'], after['active_preset_id'])
        self.assertEqual(after['overrides'], self.before['overrides'])
        self.assertEqual({p['name'] for p in after['presets']}, {'Default', 'test_2'})
        backup = json.loads(self.backup.read_text())
        self.assertEqual(backup['setting']['revision'], self.before['revision'])
        self.assertEqual(backup['presets'][0]['name'], 'test_1')
        self.assertEqual(len(backup['overrides']), len(self.rows))
        self.assertEqual(self.backup.stat().st_mode & 0o777, 0o600)
        archive = await change_appearance('activate', {'revision': after['revision'], 'preset_id': result['archive_preset_id']}, 'test-admin')
        self.assertEqual(archive['overrides'], [])  # Exactly the original source-CSS default.
        restored = await change_appearance('activate', {'revision': archive['revision'], 'preset_id': None}, 'test-admin')
        self.assertEqual(restored['overrides'], self.before['overrides'])
        self.assertEqual(restored['active_preset_id'], restored['default_preset_id'])

    async def test_default_stays_editable_but_its_reserved_name_is_protected(self):
        await self.promote()
        state = await read_appearance()
        with self.assertRaises(ValueError):
            await change_appearance('rename', {'revision': state['revision'], 'preset_id': state['default_preset_id'], 'name': 'renamed'}, 'test-admin')
        edited = [{**self.rows[0], 'value': 22}]
        saved = await change_appearance('save', {'revision': state['revision'], 'preset_id': state['default_preset_id'], 'overrides': edited}, 'test-admin')
        self.assertEqual(saved['overrides'], edited)
        self.assertEqual(len(saved['presets']), 2)

    async def test_stale_revision_cannot_promote_or_overwrite_recent_edits(self):
        edited = [{**self.rows[0], 'value': 25}]
        state = await change_appearance('save', {'revision': self.before['revision'], 'preset_id': self.before['active_preset_id'], 'overrides': edited}, 'test-admin')
        with self.assertRaises(AppearanceConflict): await self.promote()
        self.assertFalse(self.backup.exists())
        self.assertEqual((await read_appearance())['overrides'], state['overrides'])

    async def test_existing_archive_is_not_overwritten(self):
        state = await change_appearance('save', {'revision': self.before['revision'], 'name': 'test_2', 'overrides': self.rows}, 'test-admin')
        self.before['revision'] = state['revision']
        with self.assertRaises(ValueError): await self.promote()
        self.assertFalse(self.backup.exists())
        self.assertEqual((await read_appearance())['presets'], state['presets'])

    async def test_backup_failure_and_repeat_do_not_mutate_presets(self):
        self.backup.write_text('existing recovery file')
        with self.assertRaises(FileExistsError): await self.promote()
        self.assertEqual((await read_appearance())['presets'], self.before['presets'])
        self.assertEqual(self.backup.read_text(), 'existing recovery file')
        self.backup = self.backup.with_name('new-backup.json')
        await self.promote()
        state = await read_appearance()
        with self.assertRaises(AppearanceConflict): await self.promote()
        self.assertEqual((await read_appearance())['revision'], state['revision'])

    async def test_wrong_source_identity_aborts_before_backup(self):
        self.before['active_preset_id'] = 'not-the-source'
        with self.assertRaises(ValueError): await self.promote()
        self.assertFalse(self.backup.exists())
