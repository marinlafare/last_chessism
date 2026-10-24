"""Durable run state; no algorithm code or source-game writes."""

from datetime import datetime, timezone
import time

from sqlalchemy import select, update
from sqlalchemy.orm import defer

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import AlgorithmDefinition, AlgorithmRun
from .config import ACTIVE


def now():
    return datetime.now(timezone.utc)


class RunCancelled(Exception):
    pass


def definition_payload(row):
    return {"id": row.id, "name": row.name, "matrix_definition_id": row.matrix_definition_id,
            "config": row.config, "created_at": row.created_at.isoformat()}


def run_payload(row, *, result=False):
    payload = {key: getattr(row, key) for key in ("id", "definition_id", "name", "status", "cancel_requested", "progress", "error")}
    payload.update({key: getattr(row, key).isoformat() if getattr(row, key) else None
                    for key in ("created_at", "started_at", "finished_at")})
    if result:
        payload.update(config=row.config, result=row.result)
    return payload


async def list_runs(limit=30, offset=0):
    async with AsyncDBSession() as session:
        rows = list((await session.scalars(select(AlgorithmRun).options(defer(AlgorithmRun.result), defer(AlgorithmRun.config))
                    .order_by(AlgorithmRun.created_at.desc(), AlgorithmRun.id).offset(offset).limit(limit + 1))))
        return {"runs": [run_payload(row) for row in rows[:limit]], "has_more": len(rows) > limit}


async def terminal(run_id, status, error=None):
    async with AsyncDBSession() as session, session.begin():
        await session.execute(update(AlgorithmRun).where(AlgorithmRun.id == run_id, AlgorithmRun.status.in_(ACTIVE))
            .values(status=status, error=error, finished_at=now(), progress={"phase": status, "detail": error or status}))


class Reporter:
    def __init__(self, run_id):
        self.run_id = run_id
        self.phase = None
        self.updated = 0
        self.phase_started = time.monotonic()

    async def check(self):
        async with AsyncDBSession() as session:
            row = (await session.execute(select(AlgorithmRun.status, AlgorithmRun.cancel_requested)
                                         .where(AlgorithmRun.id == self.run_id))).first()
        if not row or row.cancel_requested or row.status != "running":
            raise RunCancelled("Cancelled by the user.")

    async def progress(self, phase, processed=0, total=None, detail=""):
        changed = phase != self.phase
        current = time.monotonic()
        if not changed and current - self.updated < 1:
            return
        await self.check()
        if changed:
            self.phase, self.phase_started = phase, current
        self.updated = current
        elapsed = current - self.phase_started
        eta = (total - processed) * elapsed / processed if total and processed and processed < total and elapsed >= 3 else None
        progress = {"phase": phase, "processed": processed, "total": total, "detail": detail,
                    "eta_seconds": eta, "updated_at": now().isoformat()}
        async with AsyncDBSession() as session, session.begin():
            await session.execute(update(AlgorithmRun).where(AlgorithmRun.id == self.run_id, AlgorithmRun.status == "running").values(progress=progress))
