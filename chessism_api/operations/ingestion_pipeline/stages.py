"""Canonical ingestion stage identifiers shared by jobs, API, and UI."""

PARSING_GAMES = "parsing_games"
FEN_EXTRACTION = "fen_extraction"
SAVING_POSITIONS = "saving_positions"
LINKING_POSITIONS = "linking_positions"
UPDATING_SUMMARIES = "updating_summaries"
TABLEBASE_MARKING = "tablebase_marking"

STAGE_ORDER = (
    PARSING_GAMES,
    FEN_EXTRACTION,
    SAVING_POSITIONS,
    LINKING_POSITIONS,
    UPDATING_SUMMARIES,
    TABLEBASE_MARKING,
)
