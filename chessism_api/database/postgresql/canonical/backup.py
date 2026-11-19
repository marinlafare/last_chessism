"""Canonical backup identity; restored artifacts are checked against their own manifest."""
import re
from . import CONTRACT, SchemaMismatch, catalog_digest, validate

FORMAT = 9
REFERENCES = 'canonical_v1'
FIELDS = ('contract', 'version', 'ddl_sha256', 'catalog_sha256')


async def describe(connection):
    """Borrow the caller's snapshot. No migrations, transaction or archive work."""
    await validate(connection)
    row = await connection.fetchrow('SELECT * FROM public.canonical_schema_version')
    return {field: row[field] for field in FIELDS}


async def validate_restored(connection, expected):
    """Validate this saved version, even if the application has since advanced."""
    if (not isinstance(expected, dict) or set(expected) != set(FIELDS)
            or expected.get('contract') != CONTRACT
            or type(expected.get('version')) is not int or expected['version'] < 1
            or any(not isinstance(expected.get(key),str)
                   or not re.fullmatch('[0-9a-f]{64}',expected[key])
                   for key in ('ddl_sha256','catalog_sha256'))):
        raise SchemaMismatch('Missing or malformed canonical backup identity')
    rows = await connection.fetch('SELECT * FROM public.canonical_schema_version')
    if len(rows) != 1 or {key: rows[0][key] for key in FIELDS} != expected:
        raise SchemaMismatch('Restored canonical identity differs from its backup manifest')
    if await catalog_digest(connection) != expected['catalog_sha256']:
        raise SchemaMismatch('Restored canonical catalog differs from its backup manifest')
    # pg_dump restores sequence positions separately from the schema. A matching
    # catalog alone cannot prove that the next inserted identifier is collision-free.
    columns = await connection.fetch("""SELECT n.nspname,c.relname,a.attname,
        pg_catalog.pg_get_serial_sequence(format('%I.%I',n.nspname,c.relname),a.attname) AS sequence
        FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
        WHERE n.nspname IN ('public','graph_sync','graph_map') AND c.relkind='r'
          AND a.attnum>0 AND NOT a.attisdropped
          AND (a.attidentity<>'' OR pg_catalog.pg_get_expr(d.adbin,d.adrelid) LIKE 'nextval(%')""")
    for column in columns:
        if column['sequence'] is None:
            raise SchemaMismatch('Missing canonical identifier sequence')
        maximum_sql = await connection.fetchval("SELECT format('SELECT max(%I) FROM %I.%I',$1::text,$2::text,$3::text)",
            column['attname'],column['nspname'],column['relname'])
        sequence_sql = await connection.fetchval("SELECT format('SELECT last_value,is_called FROM %s',$1::regclass)",column['sequence'])
        maximum = await connection.fetchval(maximum_sql)
        state = await connection.fetchrow(sequence_sql)
        if maximum is not None and state['last_value']+(0 if state['is_called'] else -1) < maximum:
            raise SchemaMismatch('Restored identifier sequence is behind stored values: '+column['sequence'])
    return {'version':REFERENCES,'schema_version':expected['version'],'schema_matches':True,
            'sequences_checked':len(columns)}
