"""Called under the existing schema advisory lock, outside a transaction."""


async def ensure_index(connection):
    valid = await connection.fetchval('''
        SELECT indisvalid FROM pg_index
        WHERE indexrelid=to_regclass('public.ix_fen_backup_changed')
    ''')
    if valid is True:
        return
    if valid is False:
        # Rebuild only this specifically owned, unusable interrupted index.
        await connection.execute('DROP INDEX CONCURRENTLY public.ix_fen_backup_changed')
    await connection.execute('''
        CREATE INDEX CONCURRENTLY ix_fen_backup_changed ON public.fen (analyzed_at, fen)
        WHERE score IS NOT NULL
    ''')
