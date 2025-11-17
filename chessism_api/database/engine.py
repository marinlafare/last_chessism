# chessism_api/database/engine.py

import asyncio
import time # <-- NEW
from urllib.parse import urlparse
import asyncpg # For direct async DB operations
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# --- CORRECTED IMPORT ---
# This is the correct absolute import path based on your project structure.
from chessism_api.database.models import Base
# ---

async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

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
        db_exists_query = f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"
        db_exists = await temp_conn.fetchval(db_exists_query)

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

    # --- THIS IS THE FIX ---
    # Add a retry loop to wait for the database to be ready.
    max_retries = 10
    retry_delay_seconds = 5
    
    for attempt in range(max_retries):
        try:
            # Ensure database tables exist using the async engine
            async with async_engine.begin() as conn:
                print("Ensuring database tables exist...")
                # run_sync is used to execute synchronous metadata operations (like create_all)
                # within an async context
                await conn.run_sync(Base.metadata.create_all)
                print("Database tables checked/created.")
            
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
    # --- END FIX ---
        
    # Configure the sessionmaker to use this async engine
    AsyncDBSession.configure(bind=async_engine)
    print("Asynchronous database initialization complete.")