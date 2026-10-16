"""Set-based ingestion into numeric game/player storage.

Caller owns commit/rollback, month-ledger update and stale-summary notification.
Incoming bundles retain the established formatter contract; only persistence
changes. No row-by-row player lookups, move rescans or per-game network calls.
"""
from collections import defaultdict

from sqlalchemy import text

from chessism_api.database.relational_storage import driver_connection, identifier, storage_for, INGESTION_COMMIT_LOCK


START = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
ECO_PREFIX = "https://www.chess.com/openings/"
CHUNK_SIZE = 2_000


def standard_game(row):
    setup = " ".join(str(row.get("initial_setup") or "").split())
    return row.get("rules") == "chess" and setup in ("", START, START + " 0 1")


def short_eco(value):
    if value == "no_eco":
        return value
    if not isinstance(value, str) or not value.startswith(ECO_PREFIX):
        raise ValueError("Unexpected ECO format; refusing lossy conversion")
    return value[len(ECO_PREFIX):]


async def _stage(conn, layout, source, rows, *, archived=False):
    """COPY a bounded batch to a private typed temporary table."""
    target = f"relational_ingest_{source}"
    fields = await conn.fetch("""
        SELECT column_name,data_type FROM information_schema.columns
        WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position
    """, layout.archive if archived else layout.core, source)
    columns = {r["column_name"]: r["data_type"] for r in fields if r["column_name"] != "id"}
    if not columns:
        raise RuntimeError(f"Missing normalized ingestion table: {source}")
    records = []
    for row in rows:
        if set(row) - columns.keys():
            raise ValueError(f"Unexpected ingestion fields for {source}")
        values = []
        for name, kind in columns.items():
            value = row.get(name)
            if kind in ("integer", "bigint", "smallint") and isinstance(value, float):
                if not value.is_integer():
                    raise ValueError("Refusing to truncate a fractional integer field")
                value = int(value)
            values.append(value)
        records.append(tuple(values))
    await conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {target} ON COMMIT DROP AS "
                       f"SELECT * FROM {layout.table(source, archived=archived)} WITH NO DATA")
    await conn.execute(f"TRUNCATE pg_temp.{target}")
    if records:
        await conn.copy_records_to_table(target, records=records, columns=list(columns))
    return target, list(columns)


async def ingest_bundle(session, games, moves, openings):
    layout = storage_for(session)
    if layout is None:
        raise RuntimeError("Normalized ingestion needs an explicit storage contract")
    conn = await driver_connection(session)
    tables = {name: layout.table(name) for name in (
        "game", "player", "game_mode", "game_player", "moves", "opening_sequence", "game_opening")}
    archive = layout.table("game", archived=True)
    ordered = {}
    for game in games:
        link = int(game["link"])
        if link in ordered and ordered[link] != game:
            raise ValueError("Conflicting game identities in one ingestion bundle")
        ordered[link] = game
    move_groups, opening_groups = defaultdict(list), defaultdict(list)
    for row in moves:
        if row["link"] not in ordered:
            raise ValueError("Move has no parent in the ingestion bundle")
        move_groups[row["link"]].append(row)
    for row in openings:
        if row["link"] not in ordered:
            raise ValueError("Opening has no parent in the ingestion bundle")
        opening_groups[row["link"]].append(row)
    inserted = {"standard": [], "archived": []}
    links = sorted(ordered)
    # Shared dimensions and exact month ledgers span bundles. Serialize the
    # short ingestion commit, not downloads or extraction, until bulk testing
    # justifies finer locking. All importers take this before per-game locks.
    await conn.execute("SELECT pg_advisory_xact_lock($1)", INGESTION_COMMIT_LOCK)
    for start in range(0, len(links), CHUNK_SIZE):
        group = links[start:start + CHUNK_SIZE]
        rows = []
        standard = []
        for link in group:
            row = dict(ordered[link], fens_processing=False)
            if standard_game(row):
                short_eco(row["eco"])  # Reject before inserting any dimension.
                standard.append(link)
                row["fens_done"] = False
            rows.append(row)
        stage, fields = await _stage(conn, layout, "game", rows, archived=True)
        standard_ids = set(standard)
        variants = [link for link in group if link not in standard_ids]
        conflict = await conn.fetchval(f"""SELECT EXISTS(
            SELECT 1 FROM {tables['game']} WHERE link=ANY($1::bigint[])
            UNION ALL SELECT 1 FROM {archive} WHERE link=ANY($2::bigint[]))""", variants, standard)
        if conflict:
            raise ValueError("Game identity conflicts with its active/archive classification")
        field_sql = ",".join(map(identifier, fields))
        new_archive = [r["link"] for r in await conn.fetch(f"""
            INSERT INTO {archive} ({field_sql}) SELECT {field_sql} FROM {stage}
            WHERE link=ANY($1::bigint[]) ORDER BY link
            ON CONFLICT(link) DO NOTHING RETURNING link
        """, variants)]
        await conn.execute(f"""INSERT INTO {tables['player']}(player_name,joined)
            SELECT player_name,0 FROM (
                SELECT white AS player_name FROM {stage} WHERE link=ANY($1::bigint[])
                UNION SELECT black FROM {stage} WHERE link=ANY($1::bigint[])
            ) names ORDER BY player_name COLLATE "C"
            ON CONFLICT(player_name) DO NOTHING""", standard)
        await conn.execute(f"""INSERT INTO {tables['game_mode']}(name)
            SELECT DISTINCT mode FROM {stage} WHERE link=ANY($1::bigint[]) AND mode IS NOT NULL
            ORDER BY mode ON CONFLICT(name) DO NOTHING""", standard)
        kept = [f for f in fields if f not in {"white", "black", "mode", "rules", "initial_setup"}]
        values = ["CASE WHEN s.eco='no_eco' THEN s.eco ELSE substr(s.eco,32) END" if f == "eco"
                  else "s." + identifier(f) for f in kept]
        new_standard = [r["link"] for r in await conn.fetch(f"""
            INSERT INTO {tables['game']} ({','.join(map(identifier, kept))},white_player_id,black_player_id,mode_id)
            SELECT {','.join(values)},w.id,b.id,m.id FROM {stage} s
            JOIN {tables['player']} w ON w.player_name=s.white
            JOIN {tables['player']} b ON b.player_name=s.black
            LEFT JOIN {tables['game_mode']} m ON m.name=s.mode
            WHERE s.link=ANY($1::bigint[]) ORDER BY s.link
            ON CONFLICT(link) DO NOTHING RETURNING link
        """, standard)]
        await conn.execute(f"""INSERT INTO {tables['game_player']}(game_link,player_id,is_white)
            SELECT g.link,CASE WHEN side.is_white THEN g.white_player_id ELSE g.black_player_id END,side.is_white
            FROM {tables['game']} g CROSS JOIN (VALUES(false),(true)) side(is_white)
            WHERE g.link=ANY($1::bigint[]) ORDER BY g.link,side.is_white
            ON CONFLICT(game_link,is_white) DO NOTHING""", new_standard)
        for archived, new_links in ((False, new_standard), (True, new_archive)):
            children = [row for link in new_links for row in move_groups[link]]
            move_stage, move_fields = await _stage(conn, layout, "moves", children, archived=archived)
            fields_sql = ",".join(map(identifier, move_fields))
            await conn.execute(f"""INSERT INTO {layout.table('moves', archived=archived)} ({fields_sql})
                SELECT {fields_sql} FROM {move_stage} ORDER BY link,n_move
                ON CONFLICT(link,n_move) DO NOTHING""")
        opening_rows = [row for link in new_standard for row in opening_groups[link]]
        await conn.execute("""CREATE TEMP TABLE IF NOT EXISTS relational_opening_input
            (link bigint,n_moves integer,opening text) ON COMMIT DROP;
            TRUNCATE pg_temp.relational_opening_input""")
        if opening_rows:
            await conn.copy_records_to_table("relational_opening_input", columns=["link", "n_moves", "opening"],
                records=[(r['link'], r['n_moves'], r['opening']) for r in opening_rows])
        await conn.execute(f"""INSERT INTO {tables['opening_sequence']}(opening)
            SELECT DISTINCT opening COLLATE "C" FROM relational_opening_input ORDER BY 1
            ON CONFLICT(opening) DO NOTHING""")
        await conn.execute(f"""INSERT INTO {tables['game_opening']}(link,n_moves,opening_id)
            SELECT s.link,s.n_moves,d.id FROM relational_opening_input s
            JOIN {tables['opening_sequence']} d ON d.opening=s.opening
            ORDER BY s.link,s.n_moves ON CONFLICT(link,n_moves) DO NOTHING""")
        inserted['standard'].extend(new_standard)
        inserted['archived'].extend(new_archive)
    return inserted


async def mark_stale(session, player_names, game_links=()):
    layout = storage_for(session)
    parameters = {'players': sorted(set(player_names)), 'links': sorted(set(game_links))}
    result = await session.execute(text(f"""
        INSERT INTO {layout.table('player_salience_summary')}
            (player_id,status,source_game_count,source_position_count,effective_game_count,error)
        SELECT id,'stale',0,0,0,NULL FROM {layout.table('player')}
        WHERE player_name=ANY(CAST(:players AS text[])) AND COALESCE(joined,0)>0 AND deleted_at IS NULL
        ORDER BY id ON CONFLICT(player_id) DO UPDATE SET status='stale',error=NULL RETURNING player_id
    """), parameters)
    count = len(result.all())
    await session.execute(text(f"""INSERT INTO {layout.table('player_salience_pending_game')}(game_player_id)
        SELECT gp.id FROM {layout.table('game_player')} gp JOIN {layout.table('player')} p ON p.id=gp.player_id
        WHERE gp.game_link=ANY(CAST(:links AS bigint[])) AND p.player_name=ANY(CAST(:players AS text[]))
          AND COALESCE(p.joined,0)>0 AND p.deleted_at IS NULL
        ORDER BY gp.id ON CONFLICT DO NOTHING"""), parameters)
    return count
