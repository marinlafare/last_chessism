"""Canonical recovery queue operations inside the caller's transaction."""


def _require_transaction(connection):
    if not connection.is_in_transaction():
        raise ValueError('Summary recovery requires a caller-owned transaction')


async def pending_batch(connection, limit):
    _require_transaction(connection)
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('Summary recovery batch must contain 1 to 500 games')
    await connection.execute("SET LOCAL statement_timeout='5s'")
    return [dict(row) for row in await connection.fetch('''
        SELECT game_link,revision FROM public.fen_summary_refresh
        WHERE retry_after<=clock_timestamp()
        ORDER BY queued_at,game_link LIMIT $1''', limit)]


async def finish_batch(connection, rows, error=None):
    """Acknowledge or postpone only the exact revisions that were processed."""
    _require_transaction(connection)
    if not rows:
        return
    await connection.execute("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='5s'")
    links = [row['game_link'] for row in rows]
    revisions = [row['revision'] for row in rows]
    if error is None:
        await connection.execute('''DELETE FROM public.fen_summary_refresh q
            USING unnest($1::bigint[],$2::bigint[]) captured(game_link,revision)
            WHERE q.game_link=captured.game_link AND q.revision=captured.revision''', links, revisions)
    else:
        await connection.execute('''UPDATE public.fen_summary_refresh q
            SET attempts=q.attempts+1,error=$3,retry_after=clock_timestamp()+interval '60 seconds'
            FROM unnest($1::bigint[],$2::bigint[]) captured(game_link,revision)
            WHERE q.game_link=captured.game_link AND q.revision=captured.revision''', links, revisions, error[:1000])


async def pending_count(connection):
    _require_transaction(connection)
    await connection.execute("SET LOCAL statement_timeout='5s'")
    return await connection.fetchval('SELECT count(*) FROM public.fen_summary_refresh')
