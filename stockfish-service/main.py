# stockfish-service/main.py

from fastapi import FastAPI

from routers.analysis import router as analysis_router
from operations.engine import engine_manager


app = FastAPI(title="Stockfish Analysis API", version="1.0.0")
app.include_router(analysis_router)


@app.on_event("startup")
async def startup_event() -> None:
    await engine_manager.initialize()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await engine_manager.shutdown()
