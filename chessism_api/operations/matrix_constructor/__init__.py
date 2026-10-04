"""Curated, reproducible matrix snapshots for Chessism research."""

from .catalog import matrix_catalog
from .jobs import run_matrix_construction_job
from .queries import estimate_matrix, normalize_matrix_config

__all__ = [
    "estimate_matrix",
    "matrix_catalog",
    "normalize_matrix_config",
    "run_matrix_construction_job",
]
