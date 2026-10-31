"""Synchronous bounded previews, with no job records or persistent artifacts."""
import json

import asyncpg

from . import config
from .analytical_reader import AnalyticalError, connection
from .analytical_query import ColumnSelection, check_complete, check_scores, prepare, score_metadata, selection_metadata
from .writer import preview_value

MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_SCAN_BYTES = 64 * 1024 * 1024
SAMPLE_TIMEOUT_SECONDS = 5


async def preview(request):
    spec = request.design
    limit = min(request.preview_rows, spec.limit)
    is_sample = request.preview_mode == 'sample'
    execution_spec = spec
    if is_sample:
        participants_per_game = 2 if spec.players == 'both' else 1
        sample_games = min(spec.max_games, (limit+participants_per_game-1)//participants_per_game)
        # An execution-only copy: never modify the saved/caller's design. The
        # ordinary planner checks eligibility and duplicates for these few games,
        # not max_games games. Both sides are checked even for an odd row sample.
        execution_spec = spec.model_copy(update={'max_games': sample_games})
    count = scan_bytes = sample_bytes = 0
    sample = []
    complete = False
    try:
        async with connection() as conn:
            await conn.execute("SET LOCAL statement_timeout = '3s'" if is_sample else "SET LOCAL statement_timeout = '12s'")
            await conn.execute("SET LOCAL lock_timeout = '1s'")
            snapshot_at = await conn.fetchval('SELECT transaction_timestamp()')
            query = await prepare(conn, execution_spec)
            columns = ColumnSelection(query.schema, spec.exclude_null_columns)
            cursor = await query.statement.cursor(*query.parameters)
            while True:
                fetch_count = (limit-count if is_sample else
                               min(config.BATCH_ROWS, spec.limit-count+1) if spec.exclude_null_columns
                               else limit+1-count)
                rows = await cursor.fetch(fetch_count)
                if not rows:
                    complete = True
                    break
                count += len(rows)
                if count > spec.limit:
                    raise AnalyticalError('RESULT_ROWS_EXCEEDED', 'The final result exceeds max_result_rows. Narrow the design or increase its permitted row bound.')
                check_scores(spec,rows)
                columns.observe(rows)
                for row in rows:
                    values = [preview_value(row[name]) for name in query.schema.names]
                    size = len(json.dumps(values, allow_nan=False).encode('utf-8'))
                    scan_bytes += size
                    if len(sample) < limit:
                        sample.append(values)
                        sample_bytes += size
                    if scan_bytes > MAX_SCAN_BYTES or sample_bytes > MAX_RESPONSE_BYTES:
                        raise AnalyticalError('PREVIEW_SIZE_EXCEEDED', 'Preview data exceeds its size budget; select fewer/smaller columns or request a saved export.')
                if is_sample and count >= limit:
                    break
                if not is_sample and not spec.exclude_null_columns and count > limit:
                    break
            if complete:
                check_complete(query, count)
            schema = columns.finish()
            indexes = [query.schema.get_field_index(name) for name in schema.names]
            selection = selection_metadata(spec, query)
            if is_sample:
                # Never present the tiny execution selection as an export total.
                game_index = query.schema.get_field_index('game_id')
                selection.update({
                    'selected_games': None, 'selected_participants': None,
                    'counts_scope': 'sample_only', 'sample_game_limit': execution_spec.max_games,
                    'returned_games': len({row[game_index] for row in sample}),
                    'returned_participants': len(sample),
                    'sampling': 'random starting year, forward then backward until the sample is filled; final rows sorted by date; not a uniform random sample of games',
                    'insufficient_games': None, 'missing_games': None,
                    'available_games': None, 'message': None,
                })
            result = {
                'kind': 'analytical_sample' if is_sample else 'analytical_preview',
                'preview_mode': request.preview_mode, 'mode': spec.mode,
                'snapshot_at': snapshot_at.isoformat(), 'preview_rows': request.preview_rows,
                'columns': [{'name': field.name, 'type': str(field.type), 'nullable': field.nullable} for field in schema],
                'rows': [[row[index] for index in indexes] for row in sample],
                'row_count': len(sample), 'has_more': None if is_sample else count > len(sample),
                'total_row_count': count if complete and not is_sample else None,
                'entire_result_checked': complete and not is_sample, 'column_selection': columns.metadata(),
                'selection': selection, 'score_semantics': score_metadata(spec),
                'json_integer_encoding': 'decimal string when outside JavaScript safe range',
            }
            if len(json.dumps(result, allow_nan=False).encode('utf-8')) > MAX_RESPONSE_BYTES:
                raise AnalyticalError('PREVIEW_SIZE_EXCEEDED', 'Preview response exceeds 2 MiB; select fewer/smaller columns.')
            return result
    except asyncpg.QueryCanceledError:
        raise AnalyticalError('PREVIEW_TIMEOUT', 'Preview exceeded its query deadline. Narrow the design or submit an asynchronous export; no partial preview was returned.') from None
    except asyncpg.TooManyConnectionsError:
        raise AnalyticalError('ANALYTICS_BUSY', 'Analytical readers are busy. Retry shortly.') from None
    except (asyncpg.PostgresConnectionError, asyncpg.InvalidAuthorizationSpecificationError):
        raise AnalyticalError('ANALYTICS_UNAVAILABLE', 'The isolated analytical reader is unavailable.') from None
    except asyncpg.PostgresError as error:
        code = 'QUERY_RESOURCE_LIMIT' if error.sqlstate in ('53400', '53200', '53100', '53300') else 'QUERY_ERROR'
        raise AnalyticalError(code, 'Preview could not complete within its SQL/type/resource contract. Check catalog columns/types and narrow the design.') from None
