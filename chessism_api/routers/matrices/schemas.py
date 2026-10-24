"""HTTP request types; raw SQL and filesystem paths are never accepted."""

from datetime import date
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from chessism_api.operations.matrix_constructor.config import MAXIMUM_ROWS, DEFAULT_ROWS
from chessism_api.operations.matrix_constructor.definitions import definition_config
from chessism_api.operations.matrix_constructor.move_scores import MAX_MOVE, MAX_COLUMNS as MOVE_SCORE_MAX_COLUMNS

PreviewRole = Literal["all", "features", "labels"]


class MatrixFilters(BaseModel):
    players: list[str] = Field(default_factory=list, max_length=100)
    modes: list[Literal["bullet", "blitz", "rapid"]] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None
    min_moves: int = Field(0, ge=0, le=10_000)
    analyzed_only: bool = False
    max_rows: int | None = Field(DEFAULT_ROWS, ge=1, le=MAXIMUM_ROWS)


class MoveScoreOptions(BaseModel):
    all_moves: bool = False
    first_move: int = Field(1, ge=1, le=MAX_MOVE)
    last_move: int | None = Field(40, ge=1, le=MAX_MOVE)
    score_perspective: Literal['player', 'white'] = 'player'


class MatrixRequest(BaseModel):
    name: str = Field("", max_length=100)
    collection: Literal['custom', 'foreign'] = 'custom'
    definition_kind: Literal['scoped', 'template'] = 'scoped'
    row_type: Literal["game", "game_player", "move", "position", "game_position", "player_period", "move_scores"]
    feature_columns: list[str] = Field(min_length=1, max_length=MOVE_SCORE_MAX_COLUMNS)
    label_columns: list[str] = Field(default_factory=list, max_length=16)
    filters: MatrixFilters = Field(default_factory=MatrixFilters)
    move_scores: MoveScoreOptions | None = None


class MatrixParameters(BaseModel):
    filters: MatrixFilters
    move_scores: MoveScoreOptions | None = None


def request_config(request: MatrixRequest) -> dict:
    try:
        return definition_config(request.model_dump(mode="json"))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
