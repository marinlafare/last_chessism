#main.py

import os
from fastapi import FastAPI
from contextlib import asynccontextmanager
import constants

# --- NEW: Import the players router ---
from chessism_api.routers import players, games # <-- ADDED 'games'

# --- NEW: Import the init_db function ---
from chessism_api.database.engine import init_db
# from database.database.db_interface import DBInterface

CONN_STRING = constants.CONN_STRING

# lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- NEW: Call the init_db function on startup ---
    await init_db(CONN_STRING)
    # DBInterface.initialize_engine_and_session(CONN_STRING)
    print(f"BASAL CHESSISM Server ON YO!... (DB: {CONN_STRING.split('@')[-1]})")
    yield
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