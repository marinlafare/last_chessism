import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from routers.analysis import close_redis, router as analysis_router
from operations.engine import engine_pool


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine_pool.initialize()
    try:
        yield
    finally:
        await engine_pool.shutdown()
        await close_redis()


app = FastAPI(
    title="Stockfish Analysis API",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/status")
async def read_status():
    return {
        **engine_pool.status(),
        "version": app.version,
    }


app.include_router(analysis_router)
