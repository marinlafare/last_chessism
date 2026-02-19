#main.py
import os
from fastapi import FastAPI
from contextlib import asynccontextmanager
import constants

# --- MODIFIED: Import the fens and new analysis router ---
from chessism_api.routers import players, games, fens, analysis

# --- NEW: Import the init_db function ---
from chessism_api.database.engine import init_db
# --- NEW: Import Redis functions ---
from chessism_api.redis_client import get_redis_pool, close_redis_pool

CONN_STRING = constants.CONN_STRING

# lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Database Setup ---
    if not CONN_STRING:
        raise ValueError("DATABASE_URL environment variable is not set.")
    await init_db(CONN_STRING)
    
    # --- NEW: Redis Setup ---
    # Initialize the pool on startup
    await get_redis_pool()
    
    print(f"BASAL CHESSISM Server ON YO!... (DB: {CONN_STRING.split('@')[-1]})")
    yield
    
    # --- NEW: Redis Shutdown ---
    await close_redis_pool()
    print('BASAL CHESSISM Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return "BASAL CHESSISM server running."

# --- NEW: Include the router ---
# This makes the endpoint available at /players/current_players
app.include_router(players.router, prefix="/players", tags=["Players"])

# --- NEW: Include the games router ---
app.include_router(games.router, prefix="/games", tags=["Games"])

# --- NEW: Include the FENs router ---
app.include_router(fens.router, prefix="/fens", tags=["FENs"])

# --- NEW: Include the Analysis router ---
app.include_router(analysis.router, prefix="/analysis", tags=["Analysis"])
