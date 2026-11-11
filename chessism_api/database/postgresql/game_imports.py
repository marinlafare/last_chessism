"""Canonical game persistence within an application-owned PostgreSQL transaction.

COPY stages complete import bundles. One transaction lock serializes imports and
month recounts; it is never held while downloading or parsing games. Identical
retries preserve existing rows, including analysis state. Conflicting identities
raise instead of silently acknowledging different game content.
"""
from datetime import datetime, timezone


INGESTION_COMMIT_LOCK = 731_946_237
CHUNK_SIZE = 2_000
GAME_FIELDS = (
    'link', 'played_at', 'white_elo', 'black_elo', 'white_result', 'black_result',
    'white_str_result', 'black_str_result', 'time_control', 'eco', 'time_elapsed',
    'n_moves', 'rules', 'initial_setup',
)
MOVE_FIELDS = (
    'link', 'n_move', 'white_move', 'black_move', 'white_reaction_time',
    'black_reaction_time', 'white_time_left', 'black_time_left',
)
OPENING_FIELDS = ('link', 'n_moves', 'opening')


def _require_transaction(connection):
    if not connection.is_in_transaction():
        raise ValueError('Game persistence requires an active caller-owned transaction')


def _integer(value):
    if isinstance(value, bool) or int(value) != value:
        raise ValueError('Refusing a non-integer game identity, count or rating')
    return int(value)


def _game_record(game):
    row = {name: game[name] for name in GAME_FIELDS if name != 'initial_setup'}
    for name in ('link', 'n_moves', 'white_elo', 'black_elo'):
        row[name] = _integer(row[name])
    played = row['played_at']
    if not isinstance(played, datetime) or played.utcoffset() is None:
        raise ValueError('Game played_at must be an aware timestamp')
    row['played_at'] = played.astimezone(timezone.utc)
    row['rules'] = row['rules'].strip().lower()
    row['initial_setup'] = ' '.join(str(game.get('initial_setup') or '').split()) or None
    row.update(white=game['white'].strip().lower(), black=game['black'].strip().lower(),
               mode=game.get('mode'))
    return row


def _unique_rows(rows, fields, keys):
    """Deduplicate exact input repeats without dropping contradictory children."""
    unique = {}
    for row in rows:
        record = {name: row[name] for name in fields}
        key = tuple(_integer(record[name]) for name in keys)
        record.update(zip(keys, key))
        if key in unique and unique[key] != record:
            raise ValueError('Conflicting records in one game import bundle')
        unique[key] = record
    return list(unique.values())


def _prepare(games, moves, openings):
    fields = (*GAME_FIELDS, 'white', 'black', 'mode')
    games = _unique_rows((_game_record(row) for row in games), fields, ('link',))
    moves = _unique_rows(moves, MOVE_FIELDS, ('link', 'n_move'))
    openings = _unique_rows(openings, OPENING_FIELDS, ('link', 'n_moves'))
    parents = {row['link']: row for row in games}
    move_numbers = {link: set() for link in parents}
    for row in moves:
        if row['link'] not in parents:
            raise ValueError('Move has no parent in the import bundle')
        move_numbers[row['link']].add(row['n_move'])
    for link, game in parents.items():
        if (game['n_moves'] < 0 or len(move_numbers[link]) != game['n_moves']
                or any(not 1 <= number <= game['n_moves'] for number in move_numbers[link])):
            raise ValueError('Move rows must cover the complete game, including zero-move games')
    for row in openings:
        if row['link'] not in parents or not 0 <= row['n_moves'] <= parents[row['link']]['n_moves']:
            raise ValueError('Opening has no matching game/move in the import bundle')
    return games, moves, openings


async def _stage(connection, games, moves, openings):
    # Temporary tables are private to this transaction and never change the schema.
    await connection.execute(f'''
        CREATE TEMP TABLE IF NOT EXISTS canonical_game_input ON COMMIT DROP AS
          SELECT {','.join(GAME_FIELDS)}, ''::text AS white, ''::text AS black,
                 NULL::varchar(16) AS mode FROM public.game WITH NO DATA;
        CREATE TEMP TABLE IF NOT EXISTS canonical_move_input ON COMMIT DROP AS
          SELECT {','.join(MOVE_FIELDS)} FROM public.moves WITH NO DATA;
        CREATE TEMP TABLE IF NOT EXISTS canonical_opening_input
          (link bigint, n_moves integer, opening text) ON COMMIT DROP;
        TRUNCATE pg_temp.canonical_game_input, pg_temp.canonical_move_input,
                 pg_temp.canonical_opening_input;
    ''')
    for table, rows, fields in (
        ('canonical_game_input', games, (*GAME_FIELDS, 'white', 'black', 'mode')),
        ('canonical_move_input', moves, MOVE_FIELDS),
        ('canonical_opening_input', openings, OPENING_FIELDS),
    ):
        if rows:
            await connection.copy_records_to_table(table, schema_name='pg_temp', columns=fields,
                records=[tuple(row[name] for name in fields) for row in rows])


async def _reject_conflicts(connection):
    existing = ','.join('g.' + name for name in GAME_FIELDS)
    incoming = ','.join('s.' + name for name in GAME_FIELDS)
    conflict = await connection.fetchval(f'''
        SELECT EXISTS(SELECT 1 FROM pg_temp.canonical_game_input s
          JOIN public.game g ON g.link=s.link
          JOIN public.player w ON w.id=g.white_player_id
          JOIN public.player b ON b.id=g.black_player_id
          LEFT JOIN public.game_mode m ON m.id=g.mode_id
          WHERE ROW({existing},w.player_name,b.player_name,m.name)
            IS DISTINCT FROM ROW({incoming},s.white,s.black,s.mode))
    ''')
    if conflict:
        raise ValueError('Game identity conflicts with persisted metadata')
    for table, stored, fields in (
        ('canonical_move_input', 'SELECT m.* FROM public.moves m', MOVE_FIELDS),
        ('canonical_opening_input', '''SELECT g.link,g.n_moves,s.opening
          FROM public.game_opening g JOIN public.opening_sequence s ON s.id=g.opening_id''', OPENING_FIELDS),
    ):
        # Compare both directions: an omitted child is also conflicting content.
        columns = ','.join(fields)
        if await connection.fetchval(f'''
            WITH existing AS (SELECT g.link FROM public.game g
              JOIN pg_temp.canonical_game_input i ON i.link=g.link),
            saved AS (SELECT {columns} FROM ({stored}) records JOIN existing USING(link)),
            supplied AS (SELECT {columns} FROM pg_temp.{table} JOIN existing USING(link))
            SELECT EXISTS((SELECT * FROM saved EXCEPT SELECT * FROM supplied)
                UNION ALL (SELECT * FROM supplied EXCEPT SELECT * FROM saved))
        '''):
            raise ValueError('Game identity conflicts with persisted moves or openings')


async def ingest_games(connection, games, moves, openings):
    """Insert complete bundles; return new identities, never commit or close.

    Parsing/validation precedes this boundary. Old derived calendar/rating fields
    and extraction flags in formatter input are deliberately not persisted.
    Projection invalidation and ingestion accounting belong to the same caller
    transaction, after this function returns the actual inserted identities.
    """
    _require_transaction(connection)
    games, moves, openings = _prepare(games, moves, openings)
    await connection.execute('SELECT pg_advisory_xact_lock($1)', INGESTION_COMMIT_LOCK)
    inserted, analyzable = [], []
    games.sort(key=lambda row: row['link'])
    move_groups, opening_groups = {}, {}
    for row in moves:
        move_groups.setdefault(row['link'], []).append(row)
    for row in openings:
        opening_groups.setdefault(row['link'], []).append(row)
    for start in range(0, len(games), CHUNK_SIZE):
        group = games[start:start + CHUNK_SIZE]
        await _stage(connection, group,
                     [row for game in group for row in move_groups.get(game['link'], ())],
                     [row for game in group for row in opening_groups.get(game['link'], ())])
        await _reject_conflicts(connection)
        await connection.execute('''
            INSERT INTO public.player(player_name,joined)
              SELECT name,0 FROM (SELECT white AS name FROM pg_temp.canonical_game_input
                UNION SELECT black FROM pg_temp.canonical_game_input) players ORDER BY name
              ON CONFLICT(player_name) DO NOTHING;
            INSERT INTO public.game_mode(name)
              SELECT DISTINCT mode FROM pg_temp.canonical_game_input WHERE mode IS NOT NULL
              ORDER BY mode ON CONFLICT(name) DO NOTHING;
        ''')
        rows = await connection.fetch(f'''
            INSERT INTO public.game({','.join(GAME_FIELDS)},white_player_id,black_player_id,mode_id)
              SELECT {','.join('s.' + name for name in GAME_FIELDS)},w.id,b.id,m.id
              FROM pg_temp.canonical_game_input s
              JOIN public.player w ON w.player_name=s.white
              JOIN public.player b ON b.player_name=s.black
              LEFT JOIN public.game_mode m ON m.name=s.mode ORDER BY s.link
              ON CONFLICT(link) DO NOTHING RETURNING link,is_standard,n_moves
        ''')
        links = [row['link'] for row in rows]
        inserted.extend(links)
        analyzable.extend(row['link'] for row in rows if row['is_standard'] and row['n_moves'] > 0)
        await connection.execute(f'''
            INSERT INTO public.moves({','.join(MOVE_FIELDS)})
              SELECT {','.join(MOVE_FIELDS)} FROM pg_temp.canonical_move_input
              WHERE link=ANY($1::bigint[]) ORDER BY link,n_move
        ''', links)
        await connection.execute('''
            INSERT INTO public.opening_sequence(opening)
              SELECT DISTINCT opening FROM pg_temp.canonical_opening_input WHERE link=ANY($1::bigint[])
              ORDER BY opening ON CONFLICT(opening) DO NOTHING
        ''', links)
        await connection.execute('''
            INSERT INTO public.game_opening(link,n_moves,opening_id)
              SELECT i.link,i.n_moves,s.id FROM pg_temp.canonical_opening_input i
              JOIN public.opening_sequence s ON s.opening=i.opening
              WHERE i.link=ANY($1::bigint[]) ORDER BY i.link,i.n_moves
        ''', links)
    return {'inserted_links': sorted(inserted), 'analyzable_links': sorted(analyzable)}


async def sync_month_counts(connection, player_name, months):
    """Record fetched months; count standard, nonempty games once, in UTC.

    An explicit empty month is durable. Variants, custom starts and zero-move games
    remain stored but do not change the established active-game accounting.
    """
    _require_transaction(connection)
    months = sorted(set(months))
    if not months:
        return
    if any(not 1 <= year <= 9999 or not 1 <= month <= 12 for year, month in months):
        raise ValueError('Invalid imported month')
    player_name = player_name.strip().lower()
    await connection.execute('SELECT pg_advisory_xact_lock($1)', INGESTION_COMMIT_LOCK)
    await connection.execute('''INSERT INTO public.player(player_name,joined) VALUES($1,0)
        ON CONFLICT(player_name) DO NOTHING''', player_name)
    await connection.execute('''
        INSERT INTO public.months(player_id,year,month,n_games)
        SELECT p.id,imported.year,imported.month,count(g.link)::int
        FROM unnest($2::int[],$3::int[]) imported(year,month)
        CROSS JOIN public.player p
        LEFT JOIN public.game g ON (g.white_player_id=p.id OR g.black_player_id=p.id)
          AND g.is_standard AND g.n_moves>0
          AND g.played_at >= (make_date(imported.year,imported.month,1)::timestamp AT TIME ZONE 'UTC')
          AND g.played_at < ((make_date(imported.year,imported.month,1)+interval '1 month') AT TIME ZONE 'UTC')
        WHERE p.player_name=$1 GROUP BY p.id,imported.year,imported.month
        ON CONFLICT(player_id,year,month) DO UPDATE SET n_games=EXCLUDED.n_games
    ''', player_name, [year for year, _ in months], [month for _, month in months])
