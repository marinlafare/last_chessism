"""Validated, transactional presets; CSS and source files are never written."""
import json
import math
import re
import uuid
from pathlib import Path

from sqlalchemy import select, delete, func, text
from sqlalchemy.dialects.postgresql import insert
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import AppearancePreset as Preset, AppearanceSetting as Setting, AppearanceOverride as Override

REGISTRY = json.loads((Path(__file__).parents[1] / 'appearance_registry.json').read_text())
_SCOPES = json.loads((Path(__file__).parents[1] / 'appearance_scopes.json').read_text())
_CHARTS = json.loads((Path(__file__).parents[1] / 'appearance_charts.json').read_text())
REGISTRY['properties'].update(_CHARTS['properties'])
REGISTRY['scopes'] = {
    scope: {**spec, 'targets': {**_SCOPES['common_targets'], **_CHARTS['targets'], **spec['targets']}}
    for scope, spec in _SCOPES['scopes'].items()
}
# Additive registry expansion: all v1 Scored Positions preset keys remain valid.
REGISTRY['scopes'][REGISTRY['scope']]['targets'].update(REGISTRY['targets'])
MAX_OVERRIDES = 2000


class AppearanceConflict(ValueError):
    pass


def preset_name(value):
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 60:
        raise ValueError('Choose a preset name of 1–60 characters.')
    value = value.strip()
    if value.casefold() == 'default' or len(value.casefold()) > 60 or any(ord(c) < 32 for c in value):
        raise ValueError('Default is reserved; choose another name.')
    return value


def validate_overrides(rows):
    if not isinstance(rows, list) or len(rows) > MAX_OVERRIDES:
        raise ValueError(f'At most {MAX_OVERRIDES} appearance overrides are allowed.')
    output, seen = [], set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'scope', 'target', 'property', 'value'}:
            raise ValueError('Invalid appearance override.')
        scope, target, prop, value = (row[k] for k in ('scope', 'target', 'property', 'value'))
        if not all(isinstance(v, str) for v in (scope, target, prop)) or scope not in REGISTRY['scopes']:
            raise ValueError('Unknown appearance scope.')
        if prop not in REGISTRY['scopes'][scope]['targets'].get(target, {}).get('properties', []):
            raise ValueError('Unknown target or unsupported appearance property.')
        key = (scope, target, prop)
        if key in seen: raise ValueError('Duplicate appearance property.')
        seen.add(key)
        spec = REGISTRY['properties'][prop]
        if spec['kind'] == 'color':
            if not isinstance(value, str) or not re.fullmatch(r'#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?', value):
                raise ValueError('Colors must be #RRGGBB or #RRGGBBAA.')
            value, column = value.lower(), 'color_value'
        elif spec['kind'] == 'number':
            if type(value) not in (int, float) or not math.isfinite(value) or not spec['min'] <= value <= spec['max']:
                raise ValueError('Appearance size is outside its allowed range.')
            column = 'number_value'
        else:
            if value not in spec['options']: raise ValueError('Unsupported appearance choice.')
            column = 'text_value'
        output.append({'scope': scope, 'target': target, 'property': prop, column: value})
    return output


async def snapshot(session):
    setting = await session.get(Setting, 1)
    presets = (await session.execute(select(Preset).order_by(Preset.name_key))).scalars().all()
    # Check every preset, including inactive ones, on reads and restore validation.
    rows = (await session.execute(select(Override))).scalars().all()
    by_preset = {p.id: [] for p in presets}
    for row in rows:
        value = row.color_value if row.color_value is not None else row.number_value if row.number_value is not None else row.text_value
        by_preset[row.preset_id].append({'scope': row.scope, 'target': row.target, 'property': row.property, 'value': value})
    for preset in presets:
        if preset.schema_version != REGISTRY['version']: raise ValueError('Unsupported saved appearance version; saving is disabled.')
        validate_overrides(by_preset[preset.id])
    active = setting.active_preset_id if setting else None
    return {'revision': setting.revision if setting else 0, 'active_preset_id': active,
            'presets': [{'id': p.id, 'name': p.name} for p in presets],
            'overrides': by_preset.get(active, []), 'registry': REGISTRY}


async def read_appearance():
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
        return await snapshot(session)


async def change_appearance(action, payload, account_id):
    revision = payload['revision']
    prepared = validate_overrides(payload['overrides']) if action == 'save' else None
    async with AsyncDBSession() as session:
        async with session.begin():
            await session.execute(insert(Setting).values(id=1, revision=0).on_conflict_do_nothing())
            setting = (await session.execute(select(Setting).where(Setting.id == 1).with_for_update())).scalar_one()
            if setting.revision != revision:
                raise AppearanceConflict('Styles changed in another session. Your draft is preserved; reload saved styles before saving.')
            await snapshot(session)  # Never overwrite an unreadable/incompatible store.
            preset_id = payload.get('preset_id')
            preset = await session.get(Preset, preset_id) if preset_id else None
            if preset_id and not preset: raise ValueError('Preset no longer exists.')
            if action in ('save', 'rename') and (not preset or action == 'rename'):
                name = preset_name(payload.get('name'))
                if (await session.execute(select(Preset.id).where(Preset.name_key == name.casefold(), Preset.id != (preset_id or '')))).scalar():
                    raise ValueError('A preset with that name already exists.')
                if not preset:
                    if (await session.execute(select(func.count()).select_from(Preset))).scalar() >= 30:
                        raise ValueError('At most 30 presets are allowed.')
                    preset = Preset(id=str(uuid.uuid4()), name=name, name_key=name.casefold(), created_by=account_id)
                    session.add(preset)
                else:
                    preset.name, preset.name_key = name, name.casefold()
                await session.flush()
            if action == 'save':
                await session.execute(delete(Override).where(Override.preset_id == preset.id))
                session.add_all([Override(preset_id=preset.id, **row) for row in prepared])
                preset.updated_at = func.now()
                setting.active_preset_id = preset.id
            elif action == 'activate':
                setting.active_preset_id = preset_id  # None selects immutable defaults.
            elif action != 'rename':
                raise ValueError('Unknown appearance action.')
            setting.revision += 1
            await session.flush()
            result = await snapshot(session)
        return result
