"""Canonical architecture for game ingestion.

Player creation and updates enter here conceptually, then move through parsing,
FEN extraction, durable position/link writes, summary refreshes, and tablebase
marking. Stage names, parallel progress counters, and persistent timings live in
this package so the UI and workers share one lifecycle vocabulary.
"""

from .stages import (
    FEN_EXTRACTION,
    LINKING_POSITIONS,
    PARSING_GAMES,
    SAVING_POSITIONS,
    TABLEBASE_MARKING,
    UPDATING_SUMMARIES,
)

__all__ = [
    "PARSING_GAMES",
    "FEN_EXTRACTION",
    "SAVING_POSITIONS",
    "LINKING_POSITIONS",
    "UPDATING_SUMMARIES",
    "TABLEBASE_MARKING",
]
