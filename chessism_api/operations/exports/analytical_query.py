"""Shared preparation and column selection for previews and saved datasets."""
from dataclasses import dataclass
import random
from typing import Any

from .analytical_reader import AnalyticalError, check_plan, output_schema
from .analytical_spec import MoveMatrixSpec, StoredLegacyMoveMatrixSpec
from .move_matrix import SCORE_SEMANTICS, plan as matrix_plan, year_plan as matrix_year_plan


@dataclass
class PreparedQuery:
    sql: str
    parameters: list
    statement: Any
    schema: Any
    selection: dict | None
    estimated_cost: float


async def prepare(conn, spec, *, selection_seed=None):
    selection = None
    if isinstance(spec, MoveMatrixSpec):
        start_year = None
        year_selection = {}
        if spec.contract_version >= 3:
            years_sql, years_parameters = matrix_year_plan(spec)
            await check_plan(conn, years_sql, years_parameters)
            years = [row['year'] for row in await conn.fetch(years_sql, *years_parameters)]
            if years:
                # Exports use their durable job identity as the seed. Previews
                # choose a fresh start; neither introduces a caller parameter.
                start_year = random.Random(selection_seed).choice(years)
            year_selection = {
                'strategy': 'random_year_forward_then_backward',
                'start_year': start_year,
                'year_search_order': ([year for year in years if year >= start_year] +
                                      [year for year in reversed(years) if year < start_year]) if years else [],
                'undated_games_policy': 'fallback_after_all_dated_years',
            }
        sql, parameters, checks = matrix_plan(spec, start_year=start_year)
        await check_plan(conn, checks, parameters)
        selection = dict(await conn.fetchrow(checks, *parameters))
        selection.update(year_selection)
        if selection['duplicates']:
            raise AnalyticalError('DUPLICATE_MOVE', 'Duplicate game/side/move records prevent a reliable matrix.')
        if selection['selected_participants'] > spec.limit:
            raise AnalyticalError('RESULT_ROWS_EXCEEDED', 'Selected game/player rows exceed max_result_rows; no partial dataset was produced.')
    else:
        sql = spec.plan().sql
        parameters = [p.bound_value() for p in spec.parameters]
    estimated_cost = await check_plan(conn, sql, parameters)
    statement = await conn.prepare(sql)
    schema = output_schema(statement.get_attributes())
    if isinstance(spec, MoveMatrixSpec) and spec.require_complete_scores:
        import pyarrow as pa
        schema = pa.schema([field.with_nullable(False) if field.name.startswith('move_') else field for field in schema])
    return PreparedQuery(sql, parameters, statement, schema, selection, estimated_cost)


def check_complete(query, count):
    if query.selection is not None and count != query.selection['selected_participants']:
        raise AnalyticalError('INCOMPLETE_MATRIX', 'Matrix row count does not match selected participants.')


def check_scores(spec, rows):
    if isinstance(spec, MoveMatrixSpec) and spec.require_complete_scores:
        names = [f'move_{number}' for number in range(spec.first_move, spec.last_move+1)]
        if any(row[name] is None for row in rows for name in names):
            raise AnalyticalError('INCOMPLETE_MATRIX', 'An eligible game contains a missing requested score; no partial matrix was produced.')


def selection_metadata(spec, query):
    if query.selection is None:
        return {'kind': 'explicit SQL selection; limits inside SQL are intentional', 'selected_games': None}
    result = {'max_games': spec.max_games, 'selected_games': query.selection['selected_games'],
            'result_row_bound': spec.limit,
            'result_row_bound_source': 'legacy_explicit' if isinstance(spec, StoredLegacyMoveMatrixSpec) else 'max_games_times_selected_sides',
            'selected_participants': query.selection['selected_participants'], 'players': spec.players,
            'player_win': spec.player_win,
            'game_order': spec.game_order, 'first_move': spec.first_move, 'last_move': spec.last_move,
            'require_complete_scores': spec.require_complete_scores,
            'complete_score_scope': spec.complete_score_scope if spec.require_complete_scores else 'none',
            'game_limit_applies_to': 'qualifying_games',
            'eligibility': ('Every played move of the entire game has a stored score for both players, and the requested move range exists for every selected participant.'
                           if spec.complete_score_scope == 'whole_game' else
                           'Every requested move has a stored score for both players.' if spec.players == 'both' else
                           'Every requested move has a stored score for the requested player.') if spec.require_complete_scores else
                          'Matching player and mode; missing moves/scores allowed.'}
    if spec.contract_version >= 3:
        selected = query.selection['selected_games']
        missing = max(0, spec.max_games-selected)
        result.update({key: query.selection[key] for key in (
            'strategy', 'start_year', 'year_search_order', 'selected_years',
            'undated_games', 'undated_games_policy')})
        result.update({
            'counts_scope': 'full_selection',
            'insufficient_games': missing > 0,
            'missing_games': missing,
            # Once fewer than the limit remain, the selection has exhausted
            # every year. Otherwise no extra total/count query is necessary.
            'available_games': selected if missing else None,
            'message': (f'Only {selected} qualifying games are available across all years; '
                        f'{spec.max_games} were requested.') if missing else None,
        })
    return result


def score_metadata(spec):
    if isinstance(spec, MoveMatrixSpec):
        return dict(SCORE_SEMANTICS, output_perspective=spec.score_perspective)
    return {'note': 'Final expressions are caller-defined; source score_cp is White perspective. See the design SQL and catalog semantics.'}


class ColumnSelection:
    """Track NULL presence in bounded batches; never substitute values/drop rows."""
    def __init__(self, schema, enabled):
        self.original = schema
        self.enabled = enabled
        self.non_null = list(schema.names)

    def observe(self, rows):
        if self.enabled:
            self.non_null = [name for name in self.non_null if all(row[name] is not None for row in rows)]

    def finish(self):
        if not self.enabled:
            return self.original
        if not self.non_null:
            raise AnalyticalError('NO_NON_NULL_COLUMNS', 'Every result column contains NULL; no columns satisfy exclude_null_columns. Change the design or disable the flag.')
        import pyarrow as pa
        # Empty results keep their typed columns (no NULL values are present).
        return pa.schema([self.original.field(name).with_nullable(False) for name in self.non_null])

    def metadata(self):
        return {'exclude_null_columns': self.enabled,
                'checked_scope': 'entire_result' if self.enabled else 'not_checked',
                'dropped_columns': [name for name in self.original.names if name not in self.non_null]}
