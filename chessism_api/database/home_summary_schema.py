"""Small typed homepage cache and transaction-safe source invalidation.

Statement triggers only mark structural changes. An analysis score update never
invalidates this summary. No per-FEN tracking or per-row trigger is introduced.
"""
from sqlalchemy import text


async def ensure_home_summary_triggers(connection):
    await connection.execute(text("""
        INSERT INTO home_summary_source (id, revision) VALUES (1, 1)
        ON CONFLICT (id) DO NOTHING
    """))
    # Already installed? Avoid taking write locks on the large source tables on
    # every API/worker startup. This versioned installation is transactional.
    installed = await connection.scalar(text("""
        SELECT count(*) FROM pg_trigger
        WHERE tgname LIKE 'home_summary_v1_%' AND NOT tgisinternal
          AND tgrelid IN ('player'::regclass, 'game'::regclass,
                         'fen'::regclass, 'game_fen_association'::regclass)
    """))
    if installed == 8:
        return
    await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
    await connection.execute(text("""
        CREATE OR REPLACE FUNCTION invalidate_home_summary_v1() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE home_summary_source SET revision = revision + 1 WHERE id = 1;
            RETURN NULL;
        END $$
    """))
    for table, columns in (
        ("player", "title, deleted_at, player_name"),
        ("game", "white, black"),
        ("fen", "fen"),
        ("game_fen_association", "game_link, fen_fen"),
    ):
        for suffix, events in (
            ("rows", "INSERT OR DELETE OR TRUNCATE"),
            ("values", f"UPDATE OF {columns}"),
        ):
            name = f"home_summary_v1_{table}_{suffix}"
            # Identifiers above are constants, never user input.
            await connection.execute(text(f"DROP TRIGGER IF EXISTS {name} ON {table}"))
            await connection.execute(text(f"""
                CREATE TRIGGER {name} AFTER {events} ON {table}
                FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary_v1()
            """))
