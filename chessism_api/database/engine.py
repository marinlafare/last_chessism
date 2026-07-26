import asyncio
import re
from urllib.parse import urlparse
import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from chessism_api.database.models import Base

async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

FEN_SCHEMA_ADVISORY_LOCK = 731_946_205


async def _ensure_fen_analysis_schema(
    *,
    user: str,
    password: str | None,
    host: str,
    port: int,
    database: str,
) -> None:
    """Apply small, idempotent FEN schema additions without a migration service."""
    connection = await asyncpg.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    try:
        await connection.execute("SELECT pg_advisory_lock($1)", FEN_SCHEMA_ADVISORY_LOCK)
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
        player_deleted_at_exists = await connection.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'player'
                  AND column_name = 'deleted_at'
            )
        """)
        if not player_deleted_at_exists:
            await connection.execute(
                "ALTER TABLE player ADD COLUMN deleted_at TIMESTAMPTZ"
            )
    finally:
        try:
            await connection.execute("SELECT pg_advisory_unlock($1)", FEN_SCHEMA_ADVISORY_LOCK)
        finally:
            await connection.close()

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
                await conn.run_sync(Base.metadata.create_all)
                print("Database tables checked/created.")
            await _ensure_fen_analysis_schema(
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
    print("Asynchronous database initialization complete.")
