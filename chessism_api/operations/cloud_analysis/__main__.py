"""Singleton cloud controller. No migrations or jobs are created by importing it."""
import argparse
import asyncio
from contextlib import suppress
from datetime import datetime, timezone
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from . import runtime
from cloud_job.launch import Client
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudControllerHeartbeat

CONTROLLER_LOCK = 731946219


async def heartbeat(connection, marker=None):
    while True:
        # The same dedicated connection owns the singleton lock; loss is fatal.
        await connection.execute(text("SELECT 1"))
        await connection.commit()
        async with AsyncDBSession() as session, session.begin():
            row = await session.get(CloudControllerHeartbeat, 1)
            if row is None:
                row = CloudControllerHeartbeat(id=1)
                session.add(row)
            row.seen_at = datetime.now(timezone.utc)
        if marker is not None:
            marker.touch(mode=0o600)
        await asyncio.sleep(20)


async def serve(args):
    url = os.environ.get("DATABASE_URL", "")
    if not url.startswith("postgresql+asyncpg://"):
        raise ValueError("Set DATABASE_URL to the local PostgreSQL asyncpg connection URL")
    from .controller import Controller
    engine = create_async_engine(url, pool_pre_ping=True,
                                 connect_args=getattr(args, "database_connect_args", {}))
    AsyncDBSession.configure(bind=engine)
    controller = Controller(Client(gcloud=args.gcloud), args.local_image)
    try:
        async with engine.connect() as connection:
            if not await connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": CONTROLLER_LOCK}):
                raise RuntimeError("Another cloud controller already owns this database")
            await connection.commit()
            pulse = asyncio.create_task(heartbeat(connection, getattr(args, "heartbeat_file", None)))
            async def work():
                while True:
                    await controller.tick()
                    await asyncio.sleep(args.poll_seconds)
            work_task = asyncio.create_task(work())
            print("Cloud controller ready; queued UI jobs authorize billable launches. Ctrl+C stops polling, not running cloud jobs.", flush=True)
            try:
                done, _ = await asyncio.wait([pulse, work_task], return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                pulse.cancel()
                work_task.cancel()
                with suppress(asyncio.CancelledError):
                    await pulse
                with suppress(asyncio.CancelledError):
                    await work_task
                # A pooled DB connection must not retain a session advisory lock.
                await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": CONTROLLER_LOCK})
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-image", required=True, help="Already-built local Stockfish worker image")
    parser.add_argument("--gcloud", default="gcloud")
    parser.add_argument("--poll-seconds", type=int, default=15, choices=range(5, 61), metavar="5..60")
    args = parser.parse_args()
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
