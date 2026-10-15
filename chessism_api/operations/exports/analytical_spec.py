"""Explicit analytical contracts; row guards never truncate a successful file."""
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from .config import MAX_ROWS
from .query import typed_value
from .sql_policy import validate_sql, PARAM_TYPES

MAX_ANALYTICAL_COLUMNS = 128
MATRIX_CONTRACT_KEY = '_matrix_contract_version'
MATRIX_CONTRACT_VERSION = 4
MATRIX_GAME_ORDER = 'played_at ASC NULLS LAST, game_id ASC'


class Parameter(BaseModel):
    model_config = ConfigDict(extra='forbid')
    type: Literal['int16','int32','int64','float64','string','bool','timestamp[us, UTC]']
    value: Any

    @model_validator(mode='after')
    def check(self):
        if self.value is not None:
            typed_value(self.value, self.type)
        return self

    def bound_value(self):
        return None if self.value is None else typed_value(self.value, self.type)


class SQLExportSpec(BaseModel):
    model_config = ConfigDict(extra='forbid')
    mode: Literal['sql'] = 'sql'
    name: str = Field(default='Analytical export', min_length=1, max_length=100)
    sql: str = Field(min_length=1, max_length=32768)
    parameters: list[Parameter] = Field(default_factory=list, max_length=200)
    max_result_rows: int = Field(default=100000, ge=1, le=MAX_ROWS, strict=True)
    exclude_null_columns: bool = Field(default=False, strict=True)
    format: Literal['parquet'] = 'parquet'

    @property
    def source(self): return 'sql'
    @property
    def limit(self): return self.max_result_rows
    def plan(self): return validate_sql(self.sql, [p.type for p in self.parameters])
    @model_validator(mode='after')
    def check(self):
        self.plan()
        return self


class MoveMatrixSpec(BaseModel):
    model_config = ConfigDict(extra='forbid')
    mode: Literal['move_matrix'] = 'move_matrix'
    name: str = Field(default='Player move-score matrix', min_length=1, max_length=100)
    player: str = Field(min_length=1, max_length=100)
    game_mode: str = Field(default='bullet', min_length=1, max_length=40)
    player_win: Literal['any', 'win', 'loss', 'draw'] = Field(default='any',
        description='Recorded game outcome relative to the named player, independent of color or Stockfish scores. Unknown/unfinished results match only any.')
    max_games: int = Field(default=1000, ge=1, le=100000, strict=True)
    players: Literal['requested', 'both'] = 'requested'
    first_move: int = Field(default=1, ge=1, le=1000, strict=True)
    last_move: int = Field(default=40, ge=1, le=1000, strict=True)
    score_perspective: Literal['white', 'player'] = 'white'
    require_complete_scores: bool = Field(default=False, strict=True,
        description='True: every played move of the entire game, for both players, must have a stored score; the requested move range must also exist. False: missing scores/moves are allowed.')
    # Accepted for old callers only, never persisted in new matrix designs.
    # A smaller legacy guard must fail explicitly, not be silently discarded.
    max_result_rows: int | None = Field(default=None, ge=1, le=MAX_ROWS, strict=True,
        exclude=True, json_schema_extra={'deprecated': True},
        description='Deprecated: omit. The row bound is max_games times the number of selected sides. A supplied legacy value must cover that bound.')
    exclude_null_columns: bool = Field(default=False, strict=True, exclude=True, json_schema_extra={'deprecated': True},
        description='Deprecated: omit or false. Move matrices always preserve every requested column. Use require_complete_scores to select complete games.')
    format: Literal['parquet'] = 'parquet'

    @property
    def source(self): return 'move_matrix'
    @property
    def limit(self): return self.max_games * (2 if self.players == 'both' else 1)
    @property
    def complete_score_scope(self): return 'whole_game'
    @property
    def contract_version(self): return MATRIX_CONTRACT_VERSION
    @property
    def game_order(self): return MATRIX_GAME_ORDER
    @model_validator(mode='after')
    def check_range(self):
        fixed_columns = 4 if self.contract_version >= 4 else 3
        if not 0 <= self.last_move-self.first_move < MAX_ANALYTICAL_COLUMNS-fixed_columns:
            raise ValueError('Move range must be increasing and fit within 128 output columns, including identity and default metadata columns.')
        return self

    @model_validator(mode='after')
    def check_options(self):
        if self.exclude_null_columns:
            raise ValueError('Move matrices preserve every requested column. Omit exclude_null_columns; use require_complete_scores=true to select complete games, or use a SQL export for column dropping.')
        if self.max_result_rows is not None and self.max_result_rows < self.limit:
            raise ValueError(f'max_result_rows is no longer a move-matrix control. Omit it; max_games and players derive a {self.limit}-row bound. The supplied legacy guard would conflict with that bound.')
        return self


class StoredYearSelectionMoveMatrixSpec(MoveMatrixSpec):
    """Worker-only: preserve the output schema of pre-date-column exports."""
    @property
    def contract_version(self): return 3


class StoredIDOrderMoveMatrixSpec(MoveMatrixSpec):
    """Worker-only: preserve game-ID ordering of already-submitted jobs."""
    @property
    def contract_version(self): return 2
    @property
    def game_order(self): return 'game_id ASC'


class StoredRangeMoveMatrixSpec(StoredIDOrderMoveMatrixSpec):
    """Worker-only: preserve the requested-range rule of old immutable jobs."""
    @property
    def contract_version(self): return 1
    @property
    def complete_score_scope(self): return 'requested_moves'


class StoredLegacyMoveMatrixSpec(StoredRangeMoveMatrixSpec):
    """Worker-only compatibility for immutable requests admitted before this change.

    Never expose this model in the HTTP union: old jobs retain their explicit
    guard and optional column projection, while new jobs use derived bounds.
    """
    max_result_rows: int = Field(default=100000, ge=1, le=MAX_ROWS, strict=True)
    exclude_null_columns: bool = Field(default=False, strict=True)

    @property
    def limit(self): return self.max_result_rows

    @model_validator(mode='after')
    def check_options(self):
        if self.require_complete_scores and self.exclude_null_columns:
            raise ValueError('require_complete_scores preserves every requested move column; use it with exclude_null_columns=false.')
        return self


class AnalyticalPreviewSpec(BaseModel):
    model_config = ConfigDict(extra='forbid')
    design: SQLExportSpec | MoveMatrixSpec = Field(discriminator='mode')
    preview_rows: int = Field(default=100, ge=1, le=100, strict=True)
    preview_mode: Literal['design', 'sample'] = 'design'

    @model_validator(mode='after')
    def check_mode(self):
        if self.preview_mode == 'sample':
            if not isinstance(self.design, MoveMatrixSpec):
                raise ValueError('Sample preview is supported only for move_matrix designs.')
            if self.design.exclude_null_columns:
                raise ValueError('Sample preview preserves every column and cannot validate exclude_null_columns over the full dataset. Set it false or use preview_mode=design.')
        return self


class PreviewColumn(BaseModel):
    name: str
    type: str
    nullable: bool


class PreviewColumnSelection(BaseModel):
    exclude_null_columns: bool
    checked_scope: Literal['entire_result', 'not_checked']
    dropped_columns: list[str]


class AnalyticalPreviewResult(BaseModel):
    kind: Literal['analytical_preview', 'analytical_sample']
    preview_mode: Literal['design', 'sample'] = 'design'
    mode: Literal['sql', 'move_matrix']
    snapshot_at: str
    preview_rows: int
    columns: list[PreviewColumn]
    rows: list[list[Any]]
    row_count: int
    has_more: bool | None
    total_row_count: int | None
    entire_result_checked: bool
    column_selection: PreviewColumnSelection
    selection: dict[str, Any]
    score_semantics: dict[str, Any]
    json_integer_encoding: str


def parse_request(document):
    from .query import ExportSpec
    mode = document.get('mode')
    if mode == 'move_matrix':
        payload = dict(document)
        version = payload.pop(MATRIX_CONTRACT_KEY, 1)
        if type(version) is not int or version not in (1, 2, 3, MATRIX_CONTRACT_VERSION):
            raise ValueError('Unsupported stored move-matrix contract version.')
        if version == 1:
            model = (StoredLegacyMoveMatrixSpec if 'max_result_rows' in payload or 'exclude_null_columns' in payload
                     else StoredRangeMoveMatrixSpec)
        elif version == 2:
            model = StoredIDOrderMoveMatrixSpec
        elif version == 3:
            model = StoredYearSelectionMoveMatrixSpec
        else:
            model = MoveMatrixSpec
        return model.model_validate(payload)
    return ({'sql': SQLExportSpec, 'move_matrix': MoveMatrixSpec}.get(mode, ExportSpec)).model_validate(document)


def required_sources(spec):
    if isinstance(spec, SQLExportSpec): return spec.plan().relations
    if isinstance(spec, MoveMatrixSpec): return frozenset(('games', 'players', 'moves'))
    return frozenset((spec.source,))
