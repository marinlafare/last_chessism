from contextlib import asynccontextmanager

from fastapi import FastAPI

from routers.analysis import router as analysis_router
from operations.engine import engine_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine_manager.initialize()
    try:
        yield
    finally:
        await engine_manager.shutdown()


app = FastAPI(
    title="Stockfish Analysis API",
    version="1.0.0",
    lifespan=lifespan
)
app.include_router(analysis_router)
