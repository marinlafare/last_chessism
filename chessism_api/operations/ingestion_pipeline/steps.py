"""Small, run-scoped progress snapshots; no FEN payloads or guessed counters."""
import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
import json
import time

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import IngestionPipelineStageTiming


STEP_LABELS = {
    "download_archives": ("Download archives", "Games"),
    "filter_games": ("Identify new games", "Games"),
    "parse_games": ("Parse games", "Games"),
    "save_games": ("Save games, moves and openings", "Games"),
    "extract_positions": ("Extract positions", "Positions"),
    "combine_positions": ("Deduplicate positions", "Positions"),
    "save_positions": ("Save unique positions", "Positions"),
    "link_positions": ("Link positions to games", "Positions"),
    "appearance_counts": ("Update appearance counts", "Statistics"),
    "coverage": ("Update game coverage", "Statistics"),
    "ingestion_summaries": ("Refresh ingestion summaries", "Statistics"),
    "count_endgames": ("Find eligible endgames", "Endgames"),
    "discover_endgames": ("Build endgame work list", "Endgames"),
    "claim_endgames": ("Claim endgame batches", "Endgames"),
    "probe_endgames": ("Solve endgames and best moves", "Endgames"),
    "save_endgames": ("Save endgame results", "Endgames"),
    "endgame_summaries": ("Refresh endgame coverage", "Endgames"),
    "rating_projection": ("Refresh rating statistics", "Finalization"),
    "queue_player_summaries": ("Queue player calculations", "Finalization"),
}
TTL = 7 * 24 * 3600


class StepReporter:
    """Each worker owns one hash field. Repeated batches accumulate exact work."""
    def __init__(self, ctx, run_id):
        self.redis = ctx.get("redis")
        self.run_id = run_id
        self.owner = str(ctx.get("job_id") or "direct")
        self.steps = {}
        self.active = None
        self.pulse = None
        self.previous = {}
        self.goals = {}

    def goal(self, name, total):
        self.goals[name] = max(0, int(total))

    def checkpoint(self):
        """Capture counters before a DB transaction, not its elapsed time."""
        return {name: dict(row) for name, row in self.steps.items()}

    async def rollback_attempt(self, checkpoint, *, retry_detail=None):
        """Rolled-back work must not be counted again by the next attempt."""
        now = time.time()
        for name, row in self.steps.items():
            before = checkpoint.get(name, {})
            if row.get("updated_at") == before.get("updated_at"):
                continue
            row["processed"] = before.get("processed", 0)
            row["total"] = before.get("total", self.goals.get(name, 0))
            row["status"] = "waiting" if retry_detail else "failed"
            row["detail"] = retry_detail or "Batch rolled back; its work was not committed."
            row["updated_at"] = now
        # Retain time spent on aborted attempts: it is real processing time.
        await self.publish()

    async def publish(self):
        if self.redis is None or not self.run_id:
            return
        now = time.time()
        rows = []
        for name, data in self.steps.items():
            row = {**data, "step": name, "heartbeat_at": now}
            row["elapsed_seconds"] = data["elapsed_seconds"] + (
                now - data["active_since"] if data.get("active_since") else 0
            )
            rows.append(row)
        key = f"chessism:ingestion_steps:{self.run_id}"
        merged = {**self.previous, **{row["step"]: row for row in rows}}
        await self.redis.hset(key, self.owner, json.dumps(list(merged.values())))
        await self.redis.expire(key, TTL)

    async def heartbeat(self):
        while True:
            await asyncio.sleep(5)
            try:
                await self.publish()
            except Exception:
                # Redis reporting failure must not undo a committed DB batch.
                pass

    async def __aenter__(self):
        if self.redis is not None and self.run_id:
            raw = await self.redis.hget(f"chessism:ingestion_steps:{self.run_id}", self.owner)
            if isinstance(raw, (bytes, str)):
                self.previous = {row["step"]: row for row in json.loads(raw)}
        self.pulse = asyncio.create_task(self.heartbeat())
        return self

    @asynccontextmanager
    async def step(self, name, total=None, unit="items", detail=""):
        now = time.time()
        row = self.steps.setdefault(name, {
            "status": "waiting", "processed": 0, "total": 0, "unit": unit,
            "elapsed_seconds": 0, "started_at": now, "updated_at": now,
        })
        prior = row["processed"]
        row.update(status="working", active_since=now, updated_at=now, detail=detail)
        row["fixed_total"] = name in self.goals
        row["total"] = self.goals.get(name, prior + int(total) if total is not None else None)
        self.active = name
        await self.publish()
        try:
            yield self
        except BaseException:
            row["status"] = "failed"
            raise
        else:
            if total is not None:
                row["processed"] = prior + int(total)
            row["status"] = "waiting"
        finally:
            row["elapsed_seconds"] += time.time() - row.pop("active_since")
            row["updated_at"] = time.time()
            self.active = None
            await self.publish()

    async def advance(self, count, detail=None):
        row = self.steps[self.active]
        row["processed"] += int(count)
        row["updated_at"] = time.time()
        if detail is not None:
            row["detail"] = detail
        await self.publish()

    async def __aexit__(self, kind, error, traceback):
        if self.pulse:
            self.pulse.cancel()
            with suppress(asyncio.CancelledError):
                await self.pulse
        now = time.time()
        for row in self.steps.values():
            if row["status"] != "failed":
                row["status"] = "complete"
            row["finished_at"] = now
            if row["total"] is None and row["status"] == "complete":
                row["total"] = row["processed"]
        await self.publish()
        if not self.run_id or not self.steps:
            return
        # Completion survives Redis expiry/browser refresh. No new DB schema.
        async with AsyncDBSession() as session:
            for name, row in self.steps.items():
                session.add(IngestionPipelineStageTiming(
                    run_id=self.run_id, stage=name, status=row["status"],
                    started_at=datetime.fromtimestamp(row["started_at"], timezone.utc),
                    finished_at=datetime.fromtimestamp(now, timezone.utc),
                    elapsed_ms=row["elapsed_seconds"] * 1000,
                    processed=row["processed"], total=row["total"] or 0,
                    detail=row.get("detail") or STEP_LABELS[name][0],
                ))
            await session.commit()


def merge_steps(rows):
    grouped = {}
    for row in rows:
        name = row.get("step")
        if name not in STEP_LABELS:
            continue
        item = grouped.setdefault(name, {
            "step": name, "label": STEP_LABELS[name][0], "group": STEP_LABELS[name][1],
            "processed": 0, "total": 0, "elapsed_seconds": 0, "statuses": [],
            "updated_at": 0, "heartbeat_at": 0,
        })
        item["processed"] += row.get("processed", 0)
        item["total"] = (item["total"] + row["total"]
                         if item["total"] is not None and row.get("total") is not None else None)
        # Concurrent workers: elapsed is the longest worker's processing time.
        item["elapsed_seconds"] = max(item["elapsed_seconds"], row.get("elapsed_seconds", 0))
        item["statuses"].append(row["status"])
        item["unit"] = row.get("unit", "items")
        item["fixed_total"] = item.get("fixed_total", True) and row.get("fixed_total", False)
        item["updated_at"] = max(item["updated_at"], row.get("updated_at", 0))
        item["heartbeat_at"] = max(item["heartbeat_at"], row.get("heartbeat_at", 0))
        if row.get("detail"):
            item["detail"] = row["detail"]
    for item in grouped.values():
        statuses = item.pop("statuses")
        item["status"] = ("failed" if "failed" in statuses else "working" if "working" in statuses
                          else "complete" if all(s == "complete" for s in statuses) else "waiting")
        item["rate"] = item["processed"] / item["elapsed_seconds"] if item["elapsed_seconds"] else None
    return [grouped[name] for name in STEP_LABELS if name in grouped]


async def read_steps(redis, run_id):
    raw = await redis.hgetall(f"chessism:ingestion_steps:{run_id}")
    return merge_steps([row for value in raw.values() for row in json.loads(value)])


@asynccontextmanager
async def measured(reporter, name, total=None, unit="items", detail=""):
    if reporter:
        async with reporter.step(name, total, unit, detail):
            yield
    else:
        yield
