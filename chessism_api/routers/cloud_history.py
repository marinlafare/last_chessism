"""On-demand analysis history from local storage, without any Google calls."""
from collections import defaultdict
from typing import Literal

from fastapi import APIRouter, Query
from sqlalchemy import func, select, text

from chessism_api.database.cloud_columns import TABLES
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudAnalysisJob, CloudGameDraft, CloudGameDraftEntry

router = APIRouter()


@router.get("")
async def cloud_history(
    backend: Literal["cloud_run", "batch_spot"],
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    selection = TABLES["selection"]
    async with AsyncDBSession() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        # Select only small scalar columns. Do not decode launch documents,
        # selected game IDs, checkpoints or per-FEN records to list history.
        rows = (await session.execute(select(
            CloudAnalysisJob.id, CloudAnalysisJob.status, CloudAnalysisJob.target,
            CloudAnalysisJob.imported, CloudAnalysisJob.created_at,
            CloudAnalysisJob.updated_at, CloudAnalysisJob.error,
            selection.c.mode, selection.c.player_name, selection.c.n_cpus,
            selection.c.n_vms, selection.c.nodes,
        ).join(selection, (selection.c.document_id == CloudAnalysisJob._selection_ref)
               & (selection.c.path == "$"))
          .where(func.coalesce(selection.c.backend, "batch_spot") == backend)
          .order_by(CloudAnalysisJob.created_at.desc(), CloudAnalysisJob.id.desc())
          .offset(offset).limit(limit + 1))).mappings().all()
        page = rows[:limit]
        players = defaultdict(list)
        if page:
            sources = (await session.execute(select(
                CloudGameDraft.submitted_job_id, CloudGameDraftEntry.player_name,
            ).join(CloudGameDraftEntry, CloudGameDraftEntry.draft_id == CloudGameDraft.id)
              .where(CloudGameDraft.submitted_job_id.in_([row["id"] for row in page]))
              .order_by(CloudGameDraftEntry.ordinal))).all()
            for job_id, player in sources:
                if player not in players[job_id]:
                    players[job_id].append(player)
        return {
            "jobs": [{**dict(row), "backend": backend, "players": players[row["id"]]}
                     for row in page],
            "has_more": len(rows) > limit,
        }
