#main.py
import json
import logging
import os
import asyncio
from datetime import datetime, timezone
from fastapi import Depends, FastAPI
from contextlib import asynccontextmanager
import httpx
import constants

# --- MODIFIED: Import the fens and new analysis router ---
from chessism_api.routers import auth, players, games, fens, analysis, jobs, analysis_times
from chessism_api.auth import require_superuser

# --- NEW: Import the init_db function ---
from chessism_api.database.engine import init_db
from chessism_api.database.ask_db import ensure_main_character_mode_summary
# --- NEW: Import Redis functions ---
from chessism_api.redis_client import get_redis_pool, close_redis_pool

CONN_STRING = constants.CONN_STRING
STOCKFISH_STATUS_URL = os.environ.get("STOCKFISH_STATUS_URL")


class SuppressNonServerErrorAccessLog(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "uvicorn.access":
            return True
        if not record.args:
            return True
        try:
            status_code = int(record.args[-1])
        except (TypeError, ValueError):
            return True
        return status_code >= 500


def configure_access_log_filter() -> None:
    access_logger = logging.getLogger("uvicorn.access")
    if any(isinstance(item, SuppressNonServerErrorAccessLog) for item in access_logger.filters):
        return
    access_logger.addFilter(SuppressNonServerErrorAccessLog())


configure_access_log_filter()

# lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Database Setup ---
    if not CONN_STRING:
        raise ValueError("DATABASE_URL environment variable is not set.")
    await init_db(CONN_STRING)
    await ensure_main_character_mode_summary()
    
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


async def _job_progress_summary() -> dict:
    redis = await get_redis_pool()
    counts = {
        "queued": 0,
        "running": 0,
        "failed": 0,
        "completed": 0,
    }

    keys = await redis.keys("chessism:job_progress:*")
    for key in keys:
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
        except Exception:
            continue

        phase = str(payload.get("phase") or "").lower()
        if phase == "queued":
            counts["queued"] += 1
        elif phase == "failed":
            counts["failed"] += 1
        elif phase == "complete":
            counts["completed"] += 1
        else:
            counts["running"] += 1

    return counts


async def _stockfish_status_summary() -> dict:
    fallback = {
        "workers": {
            "total": None,
            "busy": None,
            "idle": None,
        },
        "version": None,
    }
    if not STOCKFISH_STATUS_URL:
        return fallback

    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            response = await client.get(STOCKFISH_STATUS_URL)
            response.raise_for_status()
            payload = response.json()
    except Exception:
        return fallback

    workers = payload.get("workers") or {}
    return {
        "workers": {
            "total": workers.get("total"),
            "busy": workers.get("busy"),
            "idle": workers.get("idle"),
        },
        "version": payload.get("version"),
    }


@app.get("/status", dependencies=[Depends(require_superuser)])
async def read_status():
    jobs, stockfish = await asyncio.gather(
        _job_progress_summary(),
        _stockfish_status_summary(),
    )
    return {
        "api": {
            "ok": True,
            "latency_ms": None,
        },
        "workers": stockfish["workers"],
        "jobs": jobs,
        "version": {
            "backend": os.getenv("APP_VERSION"),
            "stockfish": stockfish["version"],
        },
        "source": "/status",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# --- NEW: Include the router ---
app.include_router(auth.router, prefix="/auth", tags=["Auth"])

# This makes the endpoint available at /players/current_players
app.include_router(players.router, prefix="/players", tags=["Players"], dependencies=[Depends(require_superuser)])

# --- NEW: Include the games router ---
app.include_router(games.router, prefix="/games", tags=["Games"], dependencies=[Depends(require_superuser)])

# --- NEW: Include the FENs router ---
app.include_router(fens.router, prefix="/fens", tags=["FENs"], dependencies=[Depends(require_superuser)])

# --- NEW: Include the Analysis router ---
app.include_router(analysis.router, prefix="/analysis", tags=["Analysis"], dependencies=[Depends(require_superuser)])

app.include_router(jobs.router, prefix="/jobs", tags=["Jobs"], dependencies=[Depends(require_superuser)])

app.include_router(analysis_times.router, prefix="/analysis_times", tags=["Analysis Times"], dependencies=[Depends(require_superuser)])
