import asyncio
import re
from urllib.parse import urlparse
import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from chessism_api.database.models import Base

async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

FEN_SCHEMA_ADVISORY_LOCK = 731_946_205

ENGINE_SUMMARY_LEGACY_COLUMNS = (
    "positions",
    "positive_positions",
    "negative_positions",
    "equal_positions",
    "transitions",
    "cp_gain_events",
    "cp_loss_events",
    "player_cp_sum",
    "total_cp_gain",
    "total_cp_loss",
    "opponent_move_cp_gain",
    "opponent_move_cp_loss",
    "tablebase_winning",
    "tablebase_drawing",
    "tablebase_losing",
    "refreshed_at",
)


async def _reshape_game_player_engine_summary(connection: asyncpg.Connection) -> bool:
    """Replace the legacy wide cache with one compact player-game score row."""
    table_exists = await connection.fetchval(
        "SELECT to_regclass('public.game_player_engine_summary') IS NOT NULL"
    )
    if not table_exists:
        return False

    columns = {
        str(row["column_name"])
        for row in await connection.fetch("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'game_player_engine_summary'
        """)
    }
    required_definitions = {
        "player_name": "VARCHAR",
        "analyzed_player_moves": "INTEGER NOT NULL DEFAULT 0",
        "own_move_cp_gain": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "own_move_cp_loss": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        "game_efficiency": "DOUBLE PRECISION",
        "mean_win_percent_loss": "DOUBLE PRECISION",
        "median_win_percent_loss": "DOUBLE PRECISION",
        "blunder_count": "INTEGER NOT NULL DEFAULT 0",
        "mate_for_positions": "INTEGER NOT NULL DEFAULT 0",
        "mate_against_positions": "INTEGER NOT NULL DEFAULT 0",
        "final_player_cp": "DOUBLE PRECISION",
        "result": "VARCHAR(8)",
        "end_by": "VARCHAR(40)",
    }
    additive_score_columns = {
        "game_efficiency",
        "mean_win_percent_loss",
        "median_win_percent_loss",
    }
    requires_cache_reset = bool(
        {"link", "color", "mate_for", "mate_against", *ENGINE_SUMMARY_LEGACY_COLUMNS}
        & columns
    ) or any(
        name not in columns and name not in additive_score_columns
        for name in required_definitions
    )

    async with connection.transaction():
        for old_name, new_name in (
            ("link", "game_link"),
            ("color", "player_color"),
            ("mate_for", "mate_for_positions"),
            ("mate_against", "mate_against_positions"),
        ):
            if old_name in columns and new_name not in columns:
                await connection.execute(
                    f"ALTER TABLE game_player_engine_summary "
                    f"RENAME COLUMN {old_name} TO {new_name}"
                )
                columns.remove(old_name)
                columns.add(new_name)

        for column_name, definition in required_definitions.items():
            if column_name not in columns:
                await connection.execute(
                    f"ALTER TABLE game_player_engine_summary "
                    f"ADD COLUMN {column_name} {definition}"
                )
                columns.add(column_name)

        if requires_cache_reset:
            # This relation is a rebuildable cache. Clearing it avoids mixing
            # legacy formulas with the player-only Lichess classification.
            await connection.execute("TRUNCATE TABLE game_player_engine_summary")

        for column_name in ENGINE_SUMMARY_LEGACY_COLUMNS:
            if column_name in columns:
                await connection.execute(
                    f"ALTER TABLE game_player_engine_summary DROP COLUMN {column_name}"
                )

        for column_name in ("player_name", "result", "end_by"):
            await connection.execute(
                f"ALTER TABLE game_player_engine_summary "
                f"ALTER COLUMN {column_name} SET NOT NULL"
            )

        legacy_game_fks = await connection.fetch("""
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'game_player_engine_summary'::regclass
              AND confrelid = 'game'::regclass
              AND contype = 'f'
        """)
        for row in legacy_game_fks:
            constraint_name = str(row["conname"]).replace('"', '""')
            await connection.execute(
                "ALTER TABLE game_player_engine_summary "
                f'DROP CONSTRAINT "{constraint_name}"'
            )

        player_fk_exists = await connection.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'game_player_engine_summary'::regclass
                  AND conname = 'fk_game_player_engine_summary_player'
            )
        """)
        if not player_fk_exists:
            await connection.execute("""
                ALTER TABLE game_player_engine_summary
                ADD CONSTRAINT fk_game_player_engine_summary_player
                FOREIGN KEY (player_name) REFERENCES player(player_name)
            """)

        game_player_fk_exists = await connection.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'game_player_engine_summary'::regclass
                  AND conname = 'fk_game_player_engine_summary_game_player'
            )
        """)
        if not game_player_fk_exists:
            await connection.execute("""
                ALTER TABLE game_player_engine_summary
                ADD CONSTRAINT fk_game_player_engine_summary_game_player
                FOREIGN KEY (game_link, player_color)
                REFERENCES game_player(link, color)
                ON DELETE CASCADE
            """)

        await connection.execute("""
            CREATE INDEX IF NOT EXISTS ix_game_player_engine_summary_player_game
            ON game_player_engine_summary (player_name, game_link)
        """)
    return requires_cache_reset


async def _ensure_fen_analysis_schema(
    *,
    user: str,
    password: str | None,
    host: str,
    port: int,
    database: str,
) -> int:
    """Apply small, idempotent FEN schema additions without a migration service."""
    connection = await asyncpg.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    migrated_no_move_games = 0
    try:
        await connection.execute("SELECT pg_advisory_lock($1)", FEN_SCHEMA_ADVISORY_LOCK)
        reshaped_engine_summaries = await _reshape_game_player_engine_summary(connection)
        if reshaped_engine_summaries:
            print("Player engine summary cache reshaped; rows will rebuild on demand.")
        redundant_fen_index_exists = await connection.fetchval(
            "SELECT to_regclass('public.ix_fen_fen') IS NOT NULL"
        )
        if redundant_fen_index_exists:
            # fen_pkey is an equivalent unique btree on fen(fen). Run outside
            # an explicit transaction so normal reads and writes remain
            # available while PostgreSQL invalidates the redundant index.
            await connection.execute(
                "DROP INDEX CONCURRENTLY IF EXISTS public.ix_fen_fen"
            )
            print("Removed redundant ix_fen_fen; fen_pkey remains authoritative.")
        existing_columns = {
            str(row["column_name"])
            for row in await connection.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'fen'
                  AND column_name = ANY($1::text[])
            """, [
                "piece_count",
                "analysis_source",
                "tablebase_wdl",
                "tablebase_dtz",
                "analyzed_at",
            ])
        }
        column_definitions = {
            "piece_count": "SMALLINT",
            "analysis_source": "VARCHAR(32)",
            "tablebase_wdl": "SMALLINT",
            "tablebase_dtz": "INTEGER",
            "analyzed_at": "TIMESTAMPTZ",
        }
        missing_columns = [
            (column_name, data_type)
            for column_name, data_type in column_definitions.items()
            if column_name not in existing_columns
        ]
        if missing_columns:
            # Keep all additions under one relation lock. Releasing the lock
            # between statements would let long-running analysis leases jump in.
            async with connection.transaction():
                for column_name, data_type in missing_columns:
                    await connection.execute(
                        f"ALTER TABLE fen ADD COLUMN {column_name} {data_type}"
                    )
        # This index is intentionally partial. Existing rows can use the immutable
        # FEN expression until piece_count is filled lazily, avoiding a 47M-row
        # table rewrite. CONCURRENTLY keeps ongoing Stockfish writes available.
        index_exists = await connection.fetchval(
            "SELECT to_regclass('public.ix_fen_pending_tablebase') IS NOT NULL"
        )
        if not index_exists:
            await connection.execute("""
                CREATE INDEX CONCURRENTLY ix_fen_pending_tablebase
                ON fen (n_games DESC, fen)
                WHERE score IS NULL
                  AND COALESCE(analysis_source, '') <> 'tablebase_unavailable'
                  AND COALESCE(
                        piece_count,
                        char_length(translate(split_part(fen, ' ', 1), '12345678/', ''))
                      ) <= 5
            """)
        # Supports the "most repeated pending FENs" view and the same priority
        # order used by global analysis selection without scanning the FEN table.
        await connection.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_unscored_n_games_desc
            ON fen (n_games DESC)
            WHERE score IS NULL
        """)
        player_columns = {
            str(row["column_name"])
            for row in await connection.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'player'
                  AND column_name = ANY($1::text[])
            """, ["deleted_at", "timezone", "timezone_source"])
        }
        for column_name, data_type in (
            ("deleted_at", "TIMESTAMPTZ"),
            ("timezone", "VARCHAR(64)"),
            ("timezone_source", "VARCHAR(32)"),
        ):
            if column_name not in player_columns:
                await connection.execute(
                    f"ALTER TABLE player ADD COLUMN {column_name} {data_type}"
                )

        game_columns = {
            str(row["column_name"])
            for row in await connection.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'game'
                  AND column_name = ANY($1::text[])
            """, ["fens_processing", "rules", "initial_setup"])
        }
        game_column_definitions = {
            "fens_processing": "BOOLEAN NOT NULL DEFAULT FALSE",
            "rules": "VARCHAR(32) NOT NULL DEFAULT 'chess'",
            "initial_setup": "VARCHAR(128)",
        }
        for column_name, definition in game_column_definitions.items():
            if column_name not in game_columns:
                await connection.execute(
                    f"ALTER TABLE game ADD COLUMN {column_name} {definition}"
                )

        fen_pipeline_columns = {
            str(row["column_name"])
            for row in await connection.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'fen_pipeline_summary'
                  AND column_name = ANY($1::text[])
            """, ["analyzable_games", "excluded_games"])
        }
        for column_name in ("analyzable_games", "excluded_games"):
            if column_name not in fen_pipeline_columns:
                await connection.execute(
                    "ALTER TABLE fen_pipeline_summary "
                    f"ADD COLUMN {column_name} BIGINT NOT NULL DEFAULT 0"
                )

        await connection.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_fen_association_game_order
            ON game_fen_association (game_link, n_move, move_color)
        """)

        # Zero-move records are not chess games that can participate in any
        # position analysis. Preserve only their stable ID and original start
        # time so future archive downloads do not keep re-importing them.
        async with connection.transaction():
            await connection.execute("""
                CREATE TEMPORARY TABLE migrated_no_move_scope (
                    game_id BIGINT NOT NULL,
                    player_name VARCHAR NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    PRIMARY KEY (game_id, player_name)
                ) ON COMMIT DROP
            """)
            await connection.execute("""
                INSERT INTO migrated_no_move_scope (game_id, player_name, year, month)
                SELECT g.link, players.player_name, g.year, g.month
                FROM game g
                CROSS JOIN LATERAL (VALUES (g.white), (g.black)) players(player_name)
                WHERE g.n_moves = 0
                ON CONFLICT DO NOTHING
            """)
            await connection.execute("""
                INSERT INTO no_moves_games (game_id, played_at)
                SELECT
                    g.link,
                    COALESCE(
                        g.played_at,
                        make_timestamptz(
                            g.year, g.month, g.day,
                            g.hour, g.minute, g.second,
                            'UTC'
                        )
                    )
                FROM game g
                WHERE g.n_moves = 0
                ON CONFLICT (game_id) DO UPDATE
                SET played_at = EXCLUDED.played_at
            """)
            await connection.execute("""
                DELETE FROM game_fen_association association
                USING game game_row
                WHERE association.game_link = game_row.link
                  AND game_row.n_moves = 0
            """)
            await connection.execute("""
                DELETE FROM moves move_row
                USING game game_row
                WHERE move_row.link = game_row.link
                  AND game_row.n_moves = 0
            """)
            delete_result = await connection.execute(
                "DELETE FROM game WHERE n_moves = 0"
            )
            migrated_no_move_games = int(delete_result.rsplit(" ", 1)[-1])
            await connection.execute("""
                UPDATE months ledger
                SET n_games = counts.n_games
                FROM (
                    SELECT
                        scope.player_name,
                        scope.year,
                        scope.month,
                        COUNT(game_row.link)::int AS n_games
                    FROM (
                        SELECT DISTINCT player_name, year, month
                        FROM migrated_no_move_scope
                    ) scope
                    LEFT JOIN game game_row
                      ON game_row.year = scope.year
                     AND game_row.month = scope.month
                     AND (
                         game_row.white = scope.player_name
                         OR game_row.black = scope.player_name
                     )
                    GROUP BY scope.player_name, scope.year, scope.month
                ) counts
                WHERE ledger.player_name = counts.player_name
                  AND ledger.year = counts.year
                  AND ledger.month = counts.month
            """)
    finally:
        try:
            await connection.execute("SELECT pg_advisory_unlock($1)", FEN_SCHEMA_ADVISORY_LOCK)
        finally:
            await connection.close()
    return migrated_no_move_games

async def init_db(connection_string: str):
    """
    Initializes the asynchronous SQLAlchemy database engine and ensures
    the database and all mapped tables exist.
    """
    global async_engine

    # Parse connection string for asyncpg (used for initial DB creation check)
    parsed_url = urlparse(connection_string)
    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port if parsed_url.port else 5432 # Default PostgreSQL port
    db_name = parsed_url.path.lstrip('/')
    if not re.fullmatch(r"[A-Za-z0-9_-]+", db_name):
        raise ValueError(f"Unsafe database name in DATABASE_URL: {db_name!r}")

    temp_conn = None
    try:
        # Connect to a default database (e.g., 'postgres') to check/create the target database
        temp_conn = await asyncpg.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database='postgres' # Connect to a default database to perform creation
        )
        
        # Check if the target database exists
        db_exists = await temp_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            db_name
        )

        if not db_exists:
            print(f"Database '{db_name}' does not exist. Creating...")
            await temp_conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")

    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists (concurrent creation attempt).")
    except Exception as e:
        print(f"Error during database existence check/creation: {e}")
        # In a production env, you might want to retry or raise
        print("Continuing with engine creation...")
        pass # Allow SQLAlchemy to handle it if asyncpg fails
    finally:
        if temp_conn:
            await temp_conn.close() # Ensure the temporary connection is closed

    # Create the SQLAlchemy async engine for the actual application
    # Note: asyncpg connection string uses 'postgresql+asyncpg://', not just 'postgresql://'
    if not connection_string.startswith("postgresql+asyncpg://"):
        connection_string = connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)

    async_engine = create_async_engine(connection_string, echo=False) # echo=True for SQL logging

    max_retries = 10
    retry_delay_seconds = 5
    
    for attempt in range(max_retries):
        try:
            # Ensure database tables exist using the async engine
            async with async_engine.begin() as conn:
                print("Ensuring database tables exist...")
                # Multiple API/worker containers start from the same image. Keep
                # their metadata checks sequential so a newly introduced table
                # cannot be created concurrently by two containers.
                await conn.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_id)"),
                    {"lock_id": FEN_SCHEMA_ADVISORY_LOCK},
                )
                await conn.run_sync(Base.metadata.create_all)
                print("Database tables checked/created.")
            migrated_no_move_games = await _ensure_fen_analysis_schema(
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
                database=db_name,
            )
            print("Incremental FEN analysis schema checked/created.")
            
            # If successful, break the loop
            print("Database connection successful.")
            break 
            
        except Exception as e:
            error_msg = str(e)
            # Check for the specific "not yet accepting connections" error
            if "CannotConnectNowError" in error_msg or "not yet accepting connections" in error_msg:
                print(f"Database is not ready (Attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay_seconds}s...")
                await asyncio.sleep(retry_delay_seconds)
            else:
                # It's a different, unexpected error
                print(f"An unexpected error occurred during table creation: {e}")
                raise # Re-raise the unknown error
    else: # This 'else' block runs if the 'for' loop completes without 'break'
        raise RuntimeError("Database connection failed after all retries. The database may be down.")
    AsyncDBSession.configure(bind=async_engine)
    if migrated_no_move_games:
        # Refresh only projections whose game counts changed. FEN rows and
        # scored-position summaries are deliberately untouched.
        from chessism_api.database.ask_db import (
            refresh_database_summary_game_counts,
            refresh_main_character_mode_summary,
        )
        await refresh_database_summary_game_counts()
        await refresh_main_character_mode_summary()
        print(
            f"Migrated {migrated_no_move_games} zero-move games to no_moves_games."
        )
    print("Asynchronous database initialization complete.")
