"""Explicit v3 storage adapters. No discovery, startup DDL, or live activation."""
from functools import lru_cache
import math

from sqlalchemy import BigInteger, Boolean, Column, Integer, MetaData, SmallInteger, Table, text

from .relational_storage import storage_for

PLAYER_TABLES = ('months', 'player_stats', 'main_character_mode_summary', 'home_titled_player_summary')


def compact_for(session):
    layout = storage_for(session)
    return layout if layout and layout.compact else None


def exact_integer(value, field, low=-2147483648, high=2147483647):
    if value is None:
        return None
    if type(value) not in (int, float) or (isinstance(value, float) and not math.isfinite(value)):
        raise ValueError(f'{field} must be a finite integer')
    if value != int(value) or not low <= value <= high:
        raise ValueError(f'{field} must be an integer in [{low}, {high}]; no rounding allowed')
    return int(value)


def integer_result(row):
    result = dict(row)
    if 'score' in result:
        result['score'] = exact_integer(result['score'], 'score')
    for key in ('wdl_win', 'wdl_draw', 'wdl_loss'):
        if key in result:
            result[key] = exact_integer(result[key], key, 0, 1000)
    if 'tablebase_wdl' in result:
        result['tablebase_wdl'] = exact_integer(result['tablebase_wdl'], 'tablebase_wdl', -2, 2)
    return result


@lru_cache(maxsize=16)
def compact_table(model, schema):
    """Physical writes use actual integer/bool types, not legacy view types."""
    old = model.__table__
    columns = []
    for item in old.columns:
        name, kind = item.name, item.type
        if old.name == 'game_fen_association' and name == 'move_color':
            name, kind = 'is_white', Boolean()
        if old.name in PLAYER_TABLES and name == 'player_name':
            name, kind = 'player_id', BigInteger()
        if old.name == 'moves' and name == 'id':
            continue
        if old.name in ('fen', 'fen_continuation') and name == 'score':
            kind = Integer()
        if old.name == 'fen' and name in ('wdl_win', 'wdl_draw', 'wdl_loss'):
            kind = SmallInteger()
        primary = name in ('link','n_move') if old.name == 'moves' else item.primary_key
        columns.append(Column(name, kind, primary_key=primary, nullable=item.nullable))
    return Table(old.name, MetaData(), *columns, schema=schema)


async def require_compact_contract(connection, layout):
    rows = (await connection.execute(text("""SELECT table_schema,table_name,column_name,data_type
        FROM information_schema.columns WHERE table_schema IN (:core,:archive)"""),
        {'core':layout.core, 'archive':layout.archive})).mappings().all()
    types = {(r['table_schema'],r['table_name'],r['column_name']):r['data_type'] for r in rows}
    expected = {('fen','score'):'integer', ('fen_continuation','score'):'integer',
        ('game_fen_association','is_white'):'boolean',
        **{('fen',field):'smallint' for field in ('wdl_win','wdl_draw','wdl_loss')},
        **{(table,'player_id'):'bigint' for table in PLAYER_TABLES}}
    if any(types.get((layout.core,table,column)) != kind for (table,column),kind in expected.items()):
        raise RuntimeError('Compact storage has incorrect column types')
    if (layout.archive,'moves','id') in types:
        raise RuntimeError('Archived moves still have the removed identity')
    for schema,table,keys,kind in (
        (layout.core,'moves',['link','n_move'],'p'),
        (layout.archive,'moves',['link','n_move'],'p'),
        (layout.core,'game_fen_association',['game_link','n_move','is_white'],'u'),
        (layout.core,'months',['player_id','year','month'],'u'),
        (layout.core,'player_stats',['player_id'],'p'),
        (layout.core,'main_character_mode_summary',['mode','player_id'],'p'),
        (layout.core,'home_titled_player_summary',['player_id'],'p'),
    ):
        valid = await connection.scalar(text("""SELECT EXISTS(SELECT 1 FROM pg_constraint k
            JOIN pg_class t ON t.oid=k.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=:schema AND t.relname=:table AND k.contype::text=:kind
            AND k.convalidated AND ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(id,ord)
                JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=x.id ORDER BY x.ord)=CAST(:keys AS text[]))"""),
            {'schema':schema,'table':table,'keys':keys,'kind':kind})
        if not valid:
            raise RuntimeError(f'Missing compact key: {schema}.{table}')
    for table in PLAYER_TABLES:
        if not await connection.scalar(text("""SELECT EXISTS(SELECT 1 FROM information_schema.views
            WHERE table_schema=:schema AND table_name=:table)"""), {'schema':layout.reads,'table':table}):
            raise RuntimeError(f'Missing compact read adapter: {table}')
