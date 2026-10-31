"""Full-row proofs: private copies or an explicitly authorized live transaction."""
from pathlib import Path
from contextlib import AsyncExitStack


async def require_private(connection, pgdata):
    pgdata = Path(pgdata)
    if (pgdata.name != 'pgdata' or pgdata.parent.parent != Path('/restore-test')
            or not pgdata.parent.name.startswith('compact-db-') or pgdata.is_symlink()):
        raise RuntimeError('Expected a dedicated compact-db rehearsal workspace')
    state = await connection.fetchrow("""SELECT current_setting('data_directory') AS directory,
        current_setting('port') AS port, inet_server_addr()::text AS address""")
    if (Path(state['directory']).resolve() != pgdata.resolve()
            or state['port'] != '55432' or state['address'] is not None):
        raise RuntimeError('Refusing to inspect/migrate a non-private database')


async def require_target(connection, target):
    from scripts.db_refactor_live_target import LiveMigrationTarget
    if isinstance(target, LiveMigrationTarget):
        await target.verify(connection)
    else:
        await require_private(connection, target)


async def prove_data(connection, pgdata, publish):
    """No samples, no lossy casts; all checks must pass before any DDL."""
    await require_target(connection, pgdata)
    checks = {}
    statements = {
        'association_identity': """SELECT count(*) AS conflicting_keys FROM (
            SELECT game_link,n_move,move_color FROM public.game_fen_association
            GROUP BY game_link,n_move,move_color HAVING count(*)>1) conflicts""",
        'association_sides': """SELECT count(*) FILTER(WHERE move_color IS NULL OR
            move_color NOT IN ('white','black') OR game_link IS NULL OR n_move IS NULL
            OR fen_id IS NULL) AS invalid_rows, count(*) AS rows FROM public.game_fen_association""",
        'fen_integer_values': """SELECT count(*) AS rows,
            count(*) FILTER(WHERE score IS NOT NULL AND NOT
                (score BETWEEN -2147483648 AND 2147483647 AND score=trunc(score))) AS invalid_scores,
            count(*) FILTER(WHERE (wdl_win IS NOT NULL AND NOT
                (wdl_win BETWEEN 0 AND 1000 AND wdl_win=trunc(wdl_win))) OR
                (wdl_draw IS NOT NULL AND NOT (wdl_draw BETWEEN 0 AND 1000 AND wdl_draw=trunc(wdl_draw))) OR
                (wdl_loss IS NOT NULL AND NOT (wdl_loss BETWEEN 0 AND 1000 AND wdl_loss=trunc(wdl_loss)))) AS invalid_wdl
            FROM public.fen""",
        'continuation_integer_values': """SELECT count(*) AS rows,
            count(*) FILTER(WHERE score IS NOT NULL AND NOT
                (score BETWEEN -2147483648 AND 2147483647 AND score=trunc(score))) AS invalid_scores
            FROM public.fen_continuation""",
    }
    for schema in ('public', 'chessism_archive'):
        statements[schema + '_moves_identity'] = f"""SELECT count(*) AS conflicting_keys FROM (
            SELECT link,n_move FROM {schema}.moves GROUP BY link,n_move
            HAVING count(*)>1 OR link IS NULL OR n_move IS NULL) conflicts"""
    for table in ('months','player_stats','main_character_mode_summary','home_titled_player_summary'):
        statements[table + '_player_reference'] = f"""SELECT count(*) AS orphan_rows
            FROM public.{table} s LEFT JOIN public.player p ON p.player_name=s.player_name
            WHERE p.id IS NULL"""
    async with AsyncExitStack() as stack:
        if not connection.is_in_transaction():
            await stack.enter_async_context(connection.transaction(isolation='repeatable_read', readonly=True))
        await connection.execute("SET LOCAL statement_timeout='45min'")
        await connection.execute("SET LOCAL work_mem='64MB'")
        await connection.execute("SET LOCAL max_parallel_workers_per_gather=2")
        for number, (name, sql) in enumerate(statements.items(), 1):
            await publish('proving_' + name, checks_completed=number-1, checks_total=len(statements))
            result = dict(await connection.fetchrow(sql))
            checks[name] = result
            if any(value for field, value in result.items() if field != 'rows'):
                raise ValueError(f'Lossless schema proof failed: {name}: {result}')
            await publish('proved_' + name, checks_completed=number, proofs=checks)
    return checks
