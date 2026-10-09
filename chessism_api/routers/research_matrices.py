"""Matrix page routing; public URLs stay stable across internal refactors."""

from fastapi import APIRouter

from .matrices.definitions import router as definitions_router
from .matrices.legacy_snapshots import router as legacy_router

# Static/definition routes precede the old single-UUID compatibility aliases.
# Assemble before research.py mounts /matrices: include_router without a prefix
# rejects the existing empty root path, which must retain its no-slash URL.
router = APIRouter(routes=[*definitions_router.routes, *legacy_router.routes])
