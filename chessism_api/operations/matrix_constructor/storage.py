"""Working-artifact paths and publication coordination (not backup storage)."""

from contextlib import asynccontextmanager
import os
from pathlib import Path
import uuid

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession


ARTIFACT_ROOT = Path(os.getenv(
    "MATRIX_ARTIFACT_DIR", str(Path(__file__).resolve().parents[3] / "research_data" / "matrices"),
))
ARTIFACT_DISPLAY_ROOT = Path(os.getenv("MATRIX_ARTIFACT_DISPLAY_DIR", str(ARTIFACT_ROOT)))
FREE_SPACE_FLOOR = int(os.getenv("MATRIX_FREE_FLOOR_BYTES", "20000000000"))
MATRIX_CATALOG_LOCK = 731_946_217


class MatrixCatalogBusy(RuntimeError):
    pass


def relative_manifest_path(artifact_id: str) -> str:
    if str(uuid.UUID(artifact_id)) != artifact_id:
        raise ValueError("Invalid matrix artifact identifier.")
    return f"{artifact_id}/manifest.json"


def display_manifest_path(artifact_id: str) -> str:
    return str(ARTIFACT_DISPLAY_ROOT / relative_manifest_path(artifact_id))


@asynccontextmanager
async def matrix_catalog_lock(*, wait: bool = True):
    """Freeze recipe changes and legacy snapshot metadata during a backup.

    An advisory transaction lock releases even after a crashed worker; it never
    locks chess/game/FEN tables. Legacy construction may continue, but publication
    waits until the PostgreSQL recovery point and its matrix copies are consistent.
    """
    async with AsyncDBSession() as session:
        async with session.begin():
            if wait:
                await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": MATRIX_CATALOG_LOCK})
            elif not await session.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": MATRIX_CATALOG_LOCK}):
                raise MatrixCatalogBusy("Matrix storage is being backed up or updated. Please try again shortly.")
            yield session
