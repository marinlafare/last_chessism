"""One independent reference table, transactional import and bounded lookups."""
from sqlalchemy import text
from chessism_api.database.relational_storage import driver_connection

TABLE = 'public.opening_book_move'
COLUMNS = 'position_key,move_uci,result_position_key,opening_labels,source_revision,importer_version'


async def install(session):
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
        position_key text NOT NULL,
        move_uci text NOT NULL CHECK(move_uci ~ '^[a-h][1-8][a-h][1-8][qrbn]?$'),
        result_position_key text NOT NULL,
        opening_labels jsonb NOT NULL CHECK(jsonb_typeof(opening_labels)='array'),
        source_revision text NOT NULL CHECK(source_revision ~ '^[0-9a-f]{{40}}$'),
        importer_version text NOT NULL,
        imported_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY(position_key,move_uci))'''))
    await session.execute(text(f"COMMENT ON TABLE {TABLE} IS 'Lichess CC0 opening catalogue: recognized position/move transitions, not engine assessments or frequency counts'"))


async def publish(session, catalog):
    """Install a new catalogue, or verify the same one; never overwrite another."""
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    await install(session)
    # Readers remain available; concurrent imports cannot interleave.
    await session.execute(text(f'LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE'))
    await session.execute(text(f'''CREATE TEMP TABLE opening_book_import ON COMMIT DROP
        AS SELECT {COLUMNS} FROM {TABLE} WITH NO DATA'''))
    driver = await driver_connection(session)
    await driver.copy_records_to_table('opening_book_import', schema_name='pg_temp',
        columns=COLUMNS.split(','), records=catalog.records)
    existing = await session.scalar(text(f'SELECT count(*) FROM {TABLE}'))
    if existing:
        different = await session.scalar(text(f'''SELECT EXISTS(
            (SELECT {COLUMNS} FROM {TABLE} EXCEPT SELECT {COLUMNS} FROM opening_book_import)
            UNION ALL
            (SELECT {COLUMNS} FROM opening_book_import EXCEPT SELECT {COLUMNS} FROM {TABLE}))'''))
        if different:
            raise ValueError('Existing opening catalogue differs; refusing to replace it implicitly')
        return False
    await session.execute(text(f'INSERT INTO {TABLE}({COLUMNS}) SELECT {COLUMNS} FROM opening_book_import'))
    await session.execute(text(f'ANALYZE {TABLE}'))
    return True


async def lookup(session, keys):
    """At most one game's positions in one indexed call; no raw game scans."""
    keys = list(dict.fromkeys(keys))
    if len(keys) > 512:
        raise ValueError('Book lookup is limited to 512 position/move pairs')
    if not keys:
        return {}
    rows = (await session.execute(text(f'''SELECT b.* FROM
        unnest(CAST(:positions AS text[]),CAST(:moves AS text[])) AS requested(position_key,move_uci)
        JOIN {TABLE} b USING(position_key,move_uci)'''),
        {'positions': [k[0] for k in keys], 'moves': [k[1] for k in keys]})).mappings()
    return {(r['position_key'], r['move_uci']): dict(r) for r in rows}
