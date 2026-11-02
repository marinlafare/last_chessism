"""Explicit, additive PostgreSQL outbox installation; no startup DDL.

Statement triggers capture committed changes from every writer, including older
running cloud/ingestion workers. Only IDs are queued, never copied FEN payloads.
"""
from sqlalchemy import text
from chessism_api.database.postgresql.relational_storage import driver_connection
from .fen_capture import function_sql as fen_capture_sql

LOCK = 731946299
VERSION = "chess_graph_v1"


async def install(session, layout):
    if layout is None or not layout.compact:
        raise ValueError("Graph ingestion requires the verified relational_v3 storage profile")
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key':LOCK})
    conn = await driver_connection(session)
    await conn.execute('''
        CREATE SCHEMA IF NOT EXISTS graph_sync;
        CREATE TABLE IF NOT EXISTS graph_sync.control (
            id boolean PRIMARY KEY DEFAULT true CHECK(id), enabled boolean NOT NULL DEFAULT false,
            version text NOT NULL, heartbeat timestamptz, phase text NOT NULL DEFAULT 'disabled',
            detail text, games bigint NOT NULL DEFAULT 0, positions bigint NOT NULL DEFAULT 0,
            occurrences bigint NOT NULL DEFAULT 0, moves bigint NOT NULL DEFAULT 0,
            processed_events bigint NOT NULL DEFAULT 0, last_batch_seconds double precision,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
        INSERT INTO graph_sync.control(id,version) VALUES(true,'chess_graph_v1') ON CONFLICT DO NOTHING;
        CREATE TABLE IF NOT EXISTS graph_sync.event (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            kind text NOT NULL CHECK(kind IN ('game','score','player')),
            ids bigint[] NOT NULL CHECK(cardinality(ids) BETWEEN 1 AND 1000),
            offset_items integer NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            completed_at timestamptz, attempts integer NOT NULL DEFAULT 0,
            retry_at timestamptz, error text, batch_id uuid);
        CREATE INDEX IF NOT EXISTS graph_sync_pending ON graph_sync.event(id) WHERE completed_at IS NULL;
        CREATE INDEX IF NOT EXISTS graph_sync_event_created ON graph_sync.event(created_at);
        ALTER TABLE graph_sync.event ADD COLUMN IF NOT EXISTS source_job_id text;
        ALTER TABLE graph_sync.event ADD COLUMN IF NOT EXISTS matched_ids bigint[];
        ALTER TABLE graph_sync.event ADD COLUMN IF NOT EXISTS started_at timestamptz;
        ALTER TABLE graph_sync.control ADD COLUMN IF NOT EXISTS cloud_tracking_since timestamptz;
        CREATE INDEX IF NOT EXISTS graph_sync_event_cloud ON graph_sync.event(source_job_id)
            WHERE source_job_id IS NOT NULL;
        CREATE TABLE IF NOT EXISTS graph_sync.backfill (
            id uuid PRIMARY KEY, player_id bigint NOT NULL, player_name text NOT NULL,
            game_ids bigint[] NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
        ALTER TABLE graph_sync.backfill ADD COLUMN IF NOT EXISTS verification jsonb;
        CREATE OR REPLACE FUNCTION graph_sync.enqueue(event_kind text, entity_ids bigint[]) RETURNS void
        LANGUAGE plpgsql AS $$ BEGIN
            IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN; END IF;
            INSERT INTO graph_sync.event(kind,ids)
            SELECT event_kind,array_agg(value ORDER BY value) FROM (
                SELECT value,(row_number() OVER(ORDER BY value)-1)/1000 AS chunk
                FROM (SELECT DISTINCT unnest(entity_ids) AS value) d WHERE value IS NOT NULL
            ) x GROUP BY chunk;
        END $$;
    ''')
    # UPDATE transition tables cannot be combined with UPDATE OF columns.
    # Compare only relevant fields; heartbeat/counter-only writes enqueue nothing.
    definitions = {
        ("game", "INSERT"): ("NEW TABLE AS new_rows", "SELECT array_agg(link) FROM new_rows WHERE fens_done"),
        ("game", "DELETE"): ("OLD TABLE AS old_rows", "SELECT array_agg(link) FROM old_rows"),
        ("game", "UPDATE"): ("OLD TABLE AS old_rows NEW TABLE AS new_rows", '''SELECT array_agg(n.link)
            FROM new_rows n JOIN old_rows o USING(link) WHERE n.fens_done AND
            (n.fens_done,n.white_player_id,n.black_player_id,n.mode_id,n.played_at,n.white_result,
             n.black_result,n.white_str_result,n.black_str_result,n.n_moves)
            IS DISTINCT FROM
            (o.fens_done,o.white_player_id,o.black_player_id,o.mode_id,o.played_at,o.white_result,
             o.black_result,o.white_str_result,o.black_str_result,o.n_moves)'''),
        ("player", "UPDATE"): ("OLD TABLE AS old_rows NEW TABLE AS new_rows", '''SELECT array_agg(n.id)
            FROM new_rows n JOIN old_rows o USING(id) WHERE
            (n.player_name,n.joined,n.deleted_at) IS DISTINCT FROM (o.player_name,o.joined,o.deleted_at)'''),
        ("player", "DELETE"): ("OLD TABLE AS old_rows", "SELECT array_agg(id) FROM old_rows"),
    }
    # Corrections to an already complete path also need projection. Ordinary
    # ingestion inserts are captured once at fens_done, not once per move.
    for table, column in (("moves", "link"), ("game_fen_association", "game_link")):
        for operation in ("UPDATE", "DELETE"):
            transition = "OLD TABLE AS old_rows" + (" NEW TABLE AS new_rows" if operation == "UPDATE" else "")
            keys = f"SELECT {column} AS id FROM old_rows"
            if operation == "UPDATE":
                keys += f" UNION SELECT {column} AS id FROM new_rows"
            definitions[table, operation] = (transition, f'''SELECT array_agg(g.link) FROM {layout.table('game')} g
                JOIN ({keys}) changed ON changed.id=g.link WHERE g.fens_done''')
    for (table, operation), (transition, select) in definitions.items():
        name = f"capture_{table}_{operation.lower()}"
        kind = "score" if table == "fen" else ("player" if table == "player" else "game")
        await conn.execute(f'''CREATE OR REPLACE FUNCTION graph_sync.{name}() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                {select} INTO affected;
                PERFORM graph_sync.enqueue('{kind}',affected); RETURN NULL;
            END $$;
            CREATE OR REPLACE TRIGGER chessism_{name} AFTER {operation} ON {layout.table(table)}
            REFERENCING {transition} FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.{name}();''')
    # Claim ownership is still present when the score UPDATE fires, before the
    # importer removes claims in the same transaction. No controller restart or
    # timestamp-based inference is required to preserve exact job attribution.
    cloud_tracking = bool(await session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"),
                                               {'name':f'{layout.core}.cloud_fen_claim'}))
    await conn.execute(fen_capture_sql(layout, cloud_tracking=cloud_tracking))
    await conn.execute(f'''CREATE OR REPLACE TRIGGER chessism_capture_fen_update
        AFTER UPDATE ON {layout.table('fen')}
        REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
        FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_fen_update();''')
    if cloud_tracking:
        await conn.execute('''UPDATE graph_sync.control
            SET cloud_tracking_since=COALESCE(cloud_tracking_since,clock_timestamp()) WHERE id''')


async def available(session):
    return bool(await session.scalar(text("SELECT to_regclass('graph_sync.control') IS NOT NULL")))


async def status(session):
    if not await available(session):
        return {"installed": False, "enabled": False, "phase": "not_installed"}
    control = dict((await session.execute(text('SELECT * FROM graph_sync.control WHERE id'))).mappings().one())
    pending = dict((await session.execute(text('''SELECT count(*) AS pending_events,
        COALESCE(sum(cardinality(ids)-offset_items),0) AS pending_items,
        count(*) FILTER(WHERE error IS NOT NULL) AS failed_events,
        min(created_at) AS oldest_pending_at FROM graph_sync.event WHERE completed_at IS NULL'''))).mappings().one())
    runs = [dict(row) for row in (await session.execute(text('''SELECT b.id,b.player_name,
        cardinality(b.game_ids) AS total_games,b.created_at,b.verification,
        COALESCE(sum(CASE WHEN e.completed_at IS NOT NULL THEN cardinality(e.ids) ELSE e.offset_items END),0) AS processed_games
        FROM graph_sync.backfill b LEFT JOIN graph_sync.event e ON e.batch_id=b.id
        GROUP BY b.id ORDER BY b.created_at DESC LIMIT 5'''))).mappings()]
    return {"installed": True, **control, **pending, "backfills": runs,
            "count_scope": "Imported graph only; not whole-system PostgreSQL totals"}
