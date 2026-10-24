"""Explicit, revision-guarded promotion of a saved theme; never run at startup.

Keeps the source preset's identity and styles, renames it Default, and preserves
the original source-CSS appearance as an empty named preset. No schema changes.
"""
import argparse
import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import create_async_engine
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import AppearancePreset as Preset, AppearanceSetting as Setting, AppearanceOverride as Override
from chessism_api.operations.appearance import snapshot, preset_name, AppearanceConflict


async def promote_default(*, source_id, expected_revision, backup_path,
                          source_name='test_1', archive_name='test_2'):
    archive_name = preset_name(archive_name)
    async with AsyncDBSession() as session, session.begin():
        # The normal style-save endpoint locks this same row. Abort rather than
        # promoting stale data if somebody saves while this change is prepared.
        await session.execute(text("SET LOCAL lock_timeout = '5s'"))
        setting = (await session.execute(select(Setting).where(Setting.id == 1).with_for_update())).scalar_one()
        if setting.revision != expected_revision:
            raise AppearanceConflict('Styles changed; inspect the new revision before promoting.')
        before = await snapshot(session)
        presets = (await session.execute(select(Preset).order_by(Preset.name_key))).scalars().all()
        source = next((p for p in presets if p.id == source_id and p.name_key == source_name.casefold()), None)
        if source is None:
            raise ValueError('The exact source preset no longer exists under the expected name.')
        if before['default_preset_id'] or any(p.name_key == archive_name.casefold() for p in presets):
            raise ValueError('Default or the archive preset already exists; nothing was changed.')
        if len(presets) >= 30:
            raise ValueError('No room for the original-default archive preset.')
        rows = (await session.execute(select(Override))).scalars().all()
        # A self-contained appearance-only backup, sufficient to reverse the
        # rename, activation and archive creation without touching other data.
        backup = {
            'format': 'chessism-appearance-promotion-v1',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'setting': {'id': setting.id, 'revision': setting.revision, 'active_preset_id': setting.active_preset_id},
            'presets': [{key: getattr(p, key) for key in ('id', 'name', 'name_key', 'schema_version', 'created_by', 'updated_at')} for p in presets],
            'overrides': [{key: getattr(row, key) for key in ('preset_id', 'scope', 'target', 'property', 'color_value', 'number_value', 'text_value')} for row in rows],
            'original_default_overrides': [],
        }
        encoded = (json.dumps(backup, indent=2, default=str) + '\n').encode()
        path = Path(backup_path)
        # Exclusive creation: never overwrite an earlier recovery copy.
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        # Verify the recovery copy before changing the preset names.
        if path.read_bytes() != encoded:
            raise ValueError('Appearance backup verification failed; nothing was changed.')
        archive = Preset(id=str(uuid.uuid4()), name=archive_name, name_key=archive_name.casefold(),
                         schema_version=source.schema_version, created_by=source.created_by)
        session.add(archive)
        source.name, source.name_key, source.updated_at = 'Default', 'default', func.now()
        setting.active_preset_id = source.id
        setting.revision += 1
        await session.flush()
        after = await snapshot(session)
        if after['default_preset_id'] != source_id or after['active_preset_id'] != source_id:
            raise ValueError('Default activation validation failed.')
        return {'revision': after['revision'], 'default_preset_id': source_id,
                'archive_preset_id': archive.id, 'overrides_preserved': len(after['overrides']),
                'backup_path': str(path), 'backup_sha256': hashlib.sha256(encoded).hexdigest()}


async def main(args):
    engine = create_async_engine(os.environ['DATABASE_URL'])
    AsyncDBSession.configure(bind=engine)
    try:
        if not args.apply:
            async with AsyncDBSession() as session:
                state = await snapshot(session)
                print(json.dumps({key: state[key] for key in ('revision', 'active_preset_id', 'default_preset_id', 'presets')}))
            return
        if not args.source_id or args.revision is None or not args.backup:
            raise ValueError('--apply requires --source-id, --revision and --backup.')
        print(json.dumps(await promote_default(source_id=args.source_id, expected_revision=args.revision,
                                              backup_path=args.backup)))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--source-id')
    parser.add_argument('--revision', type=int)
    parser.add_argument('--backup')
    asyncio.run(main(parser.parse_args()))
