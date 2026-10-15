"""Back up role identities, never credentials; restore them disabled (NOLOGIN)."""
from sqlalchemy import text

SUPPORT_ROLES = ('chessism_export_reader', 'chessism_export_view_owner')


async def role_manifest(session, owner):
    roles = (await session.execute(text("""SELECT rolname,rolsuper,rolcreatedb,rolcreaterole,
        rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname !~ '^pg_' ORDER BY rolname"""))).mappings().all()
    extras = {row['rolname'] for row in roles} - {owner}
    if extras and extras != set(SUPPORT_ROLES):
        raise RuntimeError('Snapshot contains unsupported database roles; add an explicit safe restore contract first')
    for row in roles:
        if row['rolname'] in extras and any(row[key] for key in row if key != 'rolname'):
            raise RuntimeError('Export support role privileges differ from the safe backup contract')
    if extras and await session.scalar(text('''SELECT count(*) FROM pg_auth_members WHERE
            member IN (SELECT oid FROM pg_roles WHERE rolname IN (:a,:b))
            OR roleid IN (SELECT oid FROM pg_roles WHERE rolname IN (:a,:b))'''), {'a':SUPPORT_ROLES[0],'b':SUPPORT_ROLES[1]}):
        raise RuntimeError('Export role memberships are not supported by snapshot restore')
    return {'version':1,'names':sorted(extras),'restore_login':False,'credentials_included':False}


def restore_role_sql(manifest):
    contract = manifest.get('database_support_roles')
    if contract is None:return ''  # Existing single-owner snapshots remain valid.
    names = contract.get('names')
    if (contract.get('version') != 1 or contract.get('restore_login') is not False
        or contract.get('credentials_included') is not False or not isinstance(names,list)
        or len(names)!=len(set(names)) or (names and set(names)!=set(SUPPORT_ROLES))):
        raise RuntimeError('Unsupported snapshot role contract')
    return '; '.join(f'CREATE ROLE {name} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS'
                     for name in names)
