"""HTTP request types; raw SQL and filesystem paths are never accepted."""

from datetime import date
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from chessism_api.operations.matrix_constructor.config import normalize_matrix_config, MAXIMUM_ROWS, MAXIMUM_COLUMNS, DEFAULT_ROWS

PreviewRole = Literal["all", "features", "labels"]


class MatrixFilters(BaseModel):
    players: list[str] = Field(default_factory=list, max_length=100)
    modes: list[Literal["bullet", "blitz", "rapid"]] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None
    min_moves: int = Field(0, ge=0, le=10_000)
    analyzed_only: bool = False
    max_rows: int = Field(DEFAULT_ROWS, ge=1, le=MAXIMUM_ROWS)


class MatrixRequest(BaseModel):
    name: str = Field("", max_length=100)
    row_type: Literal["game", "game_player", "move", "position", "game_position", "player_period"]
    feature_columns: list[str] = Field(min_length=1, max_length=MAXIMUM_COLUMNS)
    label_columns: list[str] = Field(default_factory=list, max_length=16)
    filters: MatrixFilters = Field(default_factory=MatrixFilters)


def request_config(request: MatrixRequest) -> dict:
    try:
        return normalize_matrix_config(request.model_dump(mode="json"))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
