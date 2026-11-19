"""Read-only validation of pre-canonical recovery archives.

Retained solely for existing backup manifests. Never used to bind application
sessions, select a storage profile, initialize a schema or write to a database.
"""
from dataclasses import dataclass
import re
from sqlalchemy import text


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError("Invalid relational schema identifier")
    return '"' + value + '"'


@dataclass(frozen=True)
class BackupLayout:
    core: str
    archive: str
    reads: str
    version: str = 'relational_v2'

    def __post_init__(self):
        for name in (self.core, self.archive, self.reads):
            identifier(name)
        if len({self.core, self.archive, self.reads}) != 3:
            raise ValueError("Storage, archive and read schemas must be distinct")
        if self.version not in ('relational_v2', 'relational_v3'):
            raise ValueError('Unsupported relational storage version')

    @property
    def compact(self):
        return self.version == 'relational_v3'

    def table(self, name: str, *, archived: bool = False) -> str:
        return f"{identifier(self.archive if archived else self.core)}.{identifier(name)}"

    @property
    def search_path(self):
        return f"{identifier(self.reads)}, {identifier(self.core)}, public"


async def require_storage_contract(connection, layout: BackupLayout) -> None:
    """Fail closed on a partial migration; never repair it during startup."""
    required = {
        "player": {"id", "player_name"},
        "game": {"link", "white_player_id", "black_player_id", "mode_id"},
        "game_player": {"id", "game_link", "player_id", "is_white"},
        "game_mode": {"id", "name"},
        "game_fen_association": {"id", "game_link", "fen_id", "n_move", "move_color"},
        "player_position_frequency": {"player_id", "is_white", "fen_id"},
        "game_player_salience": {"game_player_id"},
        "game_player_engine_summary": {"game_player_id"},
        "player_salience_summary": {"player_id", "status"},
        "player_salience_pending_game": {"game_player_id"},
        "opening_sequence": {"id", "opening"},
        "game_opening": {"link", "n_moves", "opening_id"},
        "game_analysis_coverage": {"link", "is_fully_analyzed"},
        "moves": {"id", "link", "n_move"},
    }
    if layout.compact:
        required['moves'].discard('id')
        required['game_fen_association'].discard('move_color')
        required['game_fen_association'].add('is_white')
        for table in ('months', 'player_stats', 'main_character_mode_summary', 'home_titled_player_summary'):
            required[table] = {'player_id'}
    rows = (await connection.execute(text("""
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t USING (table_catalog, table_schema, table_name)
        WHERE c.table_schema=:schema AND t.table_type='BASE TABLE'
    """), {"schema": layout.core})).all()
    actual = {}
    for table, column in rows:
        actual.setdefault(table, set()).add(column)
    for table, columns in required.items():
        if not columns <= actual.get(table, set()):
            raise RuntimeError(f"Incomplete relational migration: {table}")
    forbidden = {"game": {"white", "black", "mode", "rules", "initial_setup"},
                 "game_player": {"player_name", "rating", "opponent_name"},
                 "game_fen_association": {"fen_fen"},
                 "player_position_frequency": {"fen_fen", "player_name"},
                 "game_analysis_coverage": {"score_sum", "avg_score", "abs_score_sum",
                                            "avg_abs_score", "max_abs_score"}}
    if layout.compact:
        forbidden['moves'] = {'id'}
        forbidden['game_fen_association'].add('move_color')
        for table in ('months', 'player_stats', 'main_character_mode_summary', 'home_titled_player_summary'):
            forbidden[table] = {'player_name'}
    if any(columns & actual.get(table, set()) for table, columns in forbidden.items()):
        raise RuntimeError("Mixed legacy/normalized storage is not supported")
    for table in ("game", "moves"):
        exists = await connection.scalar(text("""
            SELECT EXISTS(SELECT 1 FROM information_schema.tables
                WHERE table_schema=:schema AND table_name=:table AND table_type='BASE TABLE')
        """), {"schema": layout.archive, "table": table})
        if not exists:
            raise RuntimeError(f"Missing typed archive: {table}")
    keys = (await connection.execute(text("""
        SELECT c.relname AS name,k.contype::text AS kind,k.convalidated AS validated,
            ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(id,n)
                JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=x.id ORDER BY x.n) AS columns
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema
    """),{'schema':layout.core})).mappings().all()
    for table, columns in {'player':['id'],'game':['link'],'game_player':['id'],
            'game_fen_association':['id'],'player_position_frequency':['player_id','is_white','fen_id'],
            'game_player_salience':['game_player_id'],'game_player_engine_summary':['game_player_id'],
            'player_salience_summary':['player_id'],'player_salience_pending_game':['game_player_id'],
            'game_analysis_coverage':['link'],'game_opening':['link','n_moves']}.items():
        if not any(r['name']==table and r['kind']=='p' and r['validated'] and r['columns']==columns for r in keys):
            raise RuntimeError(f'Missing normalized primary key: {table}')
    for table in ('game','game_player','game_fen_association','player_position_frequency',
                  'game_player_salience','game_player_engine_summary','player_salience_summary',
                  'player_salience_pending_game','game_opening','game_analysis_summary','moves'):
        view = await connection.scalar(text("""SELECT EXISTS(SELECT 1 FROM information_schema.views
            WHERE table_schema=:schema AND table_name=:table)"""),{'schema':layout.reads,'table':table})
        if not view:
            raise RuntimeError(f'Missing relational read contract: {table}')
    if layout.compact:
        await require_compact_contract(connection, layout)


async def describe_contract(connection, layout):
    """Compact, typed-schema metadata for backup manifests (not row payloads)."""
    await require_storage_contract(connection, layout)
    columns = (await connection.execute(text("""
        SELECT table_schema,table_name,column_name,data_type,is_nullable,is_identity
        FROM information_schema.columns WHERE table_schema IN (:core,:archive)
        ORDER BY table_schema,table_name,ordinal_position
    """), {'core':layout.core,'archive':layout.archive})).mappings().all()
    constraints = (await connection.execute(text("""
        SELECT n.nspname AS schema,c.relname AS table,k.contype::text AS kind,
               k.convalidated AS validated,pg_get_constraintdef(k.oid) AS definition
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname IN (:core,:archive)
        ORDER BY n.nspname,c.relname,k.contype,pg_get_constraintdef(k.oid)
    """), {'core':layout.core,'archive':layout.archive})).mappings().all()
    if not all(row['validated'] for row in constraints):
        raise RuntimeError('Relational constraints are not fully validated')
    views = (await connection.execute(text("""SELECT table_name,view_definition
        FROM information_schema.views WHERE table_schema=:schema ORDER BY table_name"""),
        {'schema':layout.reads})).mappings().all()
    indexes = (await connection.execute(text("""SELECT schemaname,tablename,indexname,indexdef
        FROM pg_indexes WHERE schemaname IN (:core,:archive) ORDER BY schemaname,tablename,indexname"""),
        {'core':layout.core,'archive':layout.archive})).mappings().all()
    return {'version':layout.version,'core':layout.core,'archive':layout.archive,'reads':layout.reads,
            'columns':[dict(r) for r in columns], 'constraints':[dict(r) for r in constraints],
            'views':[dict(r) for r in views], 'indexes':[dict(r) for r in indexes]}


async def validate_backup_contract(url, expected):
    """Read-only restored-schema check; no startup migrations or release writes."""
    if not isinstance(expected,dict) or expected.get('version') not in ('relational_v2', 'relational_v3'):
        raise RuntimeError('Missing normalized storage backup contract')
    layout = BackupLayout(expected.get('core'),expected.get('archive'),expected.get('reads'), expected['version'])
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine(url,connect_args={'server_settings':{
        'search_path':layout.search_path,'default_transaction_read_only':'on','statement_timeout':'30000'}})
    try:
        async with engine.connect() as connection:
            actual = await describe_contract(connection,layout)
        from .backup_contract import contract_differences
        differences = contract_differences(expected, actual)
        if differences:
            raise RuntimeError('Restored relational storage differs from its backup manifest: ' + ', '.join(differences))
        return {'version':layout.version,'schema_matches':True}
    finally:
        await engine.dispose()


PLAYER_TABLES = ('months', 'player_stats', 'main_character_mode_summary', 'home_titled_player_summary')


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
