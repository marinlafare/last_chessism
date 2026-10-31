"""Finished analytical datasets, using only the isolated reader connection."""
import hashlib
import os
import time
from datetime import timedelta

import asyncpg

from . import config, files
from .analytical_reader import AnalyticalError, connection
from .analytical_query import ColumnSelection, check_complete, check_scores, prepare, score_metadata, selection_metadata
from .analytical_spec import MAX_ANALYTICAL_COLUMNS, MATRIX_CONTRACT_KEY, MATRIX_GAME_ORDER, MoveMatrixSpec
from .catalog import public_catalog
from .move_matrix import SCORE_SEMANTICS
from .service import now
from .sql_policy import SCHEMA, FUNCTIONS, TYPES, OPERATORS
from .writer import ParquetOutput


def catalog(client):
    base = public_catalog(client.allowed_sources, client.max_rows)
    for source in base['sources']:
        source['relation'] = SCHEMA+'.'+source['name']
    links = [
        ('games','white_player_id','players','player_id'), ('games','black_player_id','players','player_id'),
        ('moves','game_id','games','game_id'), ('moves','fen_id','positions','fen_id'),
        ('game_positions','game_id','games','game_id'), ('game_positions','fen_id','positions','fen_id'),
        ('player_games','game_id','games','game_id'), ('player_games','player_id','players','player_id')]
    return {'version':1, 'enabled':bool(getattr(client,'allow_analytics',False)), 'schema':SCHEMA,
        'relations':base['sources'], 'relationships':[{'from':f'{SCHEMA}.{a}.{b}', 'to':f'{SCHEMA}.{c}.{d}', 'cardinality':'many-to-one'}
                for a,b,c,d in links if a in client.allowed_sources and c in client.allowed_sources],
        'functions':sorted(FUNCTIONS), 'expressions':['CASE','COALESCE','NULLIF','GREATEST','LEAST'],
        'operators':sorted(OPERATORS), 'cast_types':sorted(TYPES),
        'limits':{'max_result_rows':min(client.max_rows,config.MAX_ROWS), 'max_columns':MAX_ANALYTICAL_COLUMNS,
            'preview_rows':100, 'preview_seconds':15, 'preview_response_bytes':2097152,
            'sample_preview_seconds':5, 'sample_preview_statement_seconds':3,
            'preview_scan_bytes':67108864,
            'max_games':100000, 'max_sql_bytes':32768, 'query_seconds':60, 'export_seconds':config.TIMEOUT,
            'max_file_bytes':config.MAX_BYTES, 'global_export_concurrency':1, 'query_role_connections':2,
            'work_mem':'16 MiB per operation, not a whole-query memory cap', 'temp_file_limit':'256 MiB per query connection'},
        'overflow':'fail; no partial export published', 'score_semantics':SCORE_SEMANTICS,
        'move_matrix_output': {
            'fixed_columns':['game_id','player_name','color','played_at'],
            'max_move_columns':MAX_ANALYTICAL_COLUMNS-4,
            'played_at':{'type':'timestamp[us, UTC]', 'nullable':True,
                'meaning':'Stored game start date/time, included by default before move columns. JSON previews use an ISO 8601 UTC timestamp; Parquet preserves the timestamp type. Missing dates remain null. Both player rows share the same game timestamp.'}},
        'move_matrix_ordering': {'game_order':MATRIX_GAME_ORDER,
            'configurable':False,
            'meaning':'Final rows are chronological, oldest first. Missing dates last; game_id breaks date ties; players=both returns White before Black within each game. No ordering parameter. Stored older jobs retain their original selection and ordering.'},
        'move_matrix_selection': {
            'strategy':'random_year_forward_then_backward', 'configurable':False,
            'meaning':'Choose a random populated year for the player and game mode. Fill with qualifying games from that year, then later years through the current UTC year, then earlier years in reverse from the starting year. Skip empty years. Apply result/completeness filters before max_games. Undated games are a final fallback. Return fewer games if history is exhausted and report selection.insufficient_games, missing_games and available_games.',
            'preview':'Same eligibility and year-search rule; a fresh random start per preview. Sample previews stop at sample size, do not decide whether full max_games is attainable, and report insufficient_games/missing_games/available_games as null.',
            'export':'Random start is seeded by the saved export identity; idempotent retrieval does not redraw it. Final rows are date-sorted.'},
        'move_matrix_options': {
            'player_win': {'type':'string', 'values':['any','win','loss','draw'], 'default':'any',
                'meaning':'Recorded game outcome relative to the named player, regardless of color. Applied before max_games; players=both still filters by the named player and returns both rows. Unknown/unfinished results match only any. Independent of Stockfish scores.'},
            'max_games': {'type':'integer', 'default':1000, 'max_requested':min(100000,client.max_rows,config.MAX_ROWS),
                'max_both':min(100000,client.max_rows//2,config.MAX_ROWS//2),
                'meaning':'Select up to this many qualifying games. The server derives the result row bound: max_games for requested, 2 * max_games for both. Token/resource limits still apply.'},
            'require_complete_scores': {'type':'boolean', 'default':False,
                'true_scope':'whole_game', 'false_scope':'none',
                'meaning':'True: every played move in the entire game, for BOTH players, must have a stored score, including moves outside the output range. The requested range must also exist for selected participants. Filter before max_games. False: missing scores/moves allowed.'},
            'columns':'Every requested move column is preserved, regardless of require_complete_scores. Fixed columns are game_id, player_name, color, played_at. Missing scores stay NULL when require_complete_scores is false; missing game dates stay NULL independently.',
            'deprecated_inputs':{'max_result_rows':'Omit. A legacy value is accepted only when at least the derived bound; it does not override the bound.',
                                 'exclude_null_columns':'Omit or false; true is rejected. Column dropping is available for SQL designs.'}},
        'preview':{'endpoint':'/exports/v1/analytical-preview', 'body':'{design: existing SQL or move_matrix design, preview_rows: 1..100, preview_mode: design|sample}',
            'default_mode':'design',
            'sample':'Move matrices only. Sample a few qualifying games with all columns; do not select/count full max_games. kind=analytical_sample; totals and has_more=null. Cannot use exclude_null_columns=true. Saving is unchanged.',
            'exclude_null_columns':'SQL designs only: checks the entire result in the same snapshot and drops columns containing any NULL. Fails if none remain or validation exceeds limits.'},
        'sql_subset':'One SELECT; non-recursive read-only CTEs, subqueries, joins, grouping, aggregates, CASE, window functions and set operations. Only qualified catalog relations and allowlisted functions/operators/casts.',
        'numeric_outputs':'Unbounded numeric/decimal results must be explicitly cast to a supported scalar, for example AVG(score_cp)::double precision. They are never silently converted.',
        'selection':'Raw SQL controls its own selection and ordering. max_result_rows is a failure guard, never an implicit LIMIT. Move matrices filter game eligibility before max_games, then return one row per requested participant.'}


async def construct(job, spec, document):
    from .jobs import checkpoint, disk_work
    from .auth import authorize_spec
    from .models import ExportClient
    from chessism_api.database.engine import AsyncDBSession
    async with AsyncDBSession() as session:
        authorize_spec(await session.get(ExportClient,job.client_id),spec)
    directory=files.job_dir(job.id); temporary=directory/'data.parquet.building'
    if temporary.exists(): raise AnalyticalError('INTERRUPTED_EXPORT','Submit a new key after an interrupted export.')
    await disk_work(files.ensure_space)
    writer=None; count=0; last_progress=time.monotonic()
    try:
        async with connection() as conn:
            snapshot_at,snapshot=await conn.fetchrow('SELECT transaction_timestamp(),pg_current_snapshot()::text')
            await checkpoint(job.id,phase='planning',snapshot_at=snapshot_at)
            query=await prepare(conn,spec,selection_seed=job.id)
            column_selection=ColumnSelection(query.schema,spec.exclude_null_columns)
            writer=ParquetOutput(temporary,schema=query.schema)
            await checkpoint(job.id,phase='reading')
            cursor=await query.statement.cursor(*query.parameters)
            while True:
                rows=await cursor.fetch(min(config.BATCH_ROWS,spec.limit-count+1))
                if not rows:break
                if count+len(rows)>spec.limit:
                    raise AnalyticalError('RESULT_ROWS_EXCEEDED','Final result exceeds max_result_rows; narrow the query or raise the permitted bound. No partial file was published.')
                check_scores(spec,rows)
                column_selection.observe(rows)
                await disk_work(writer.write,rows)
                count+=len(rows)
                progress=time.monotonic()-last_progress>=1
                await checkpoint(job.id,**({'rows_written':count} if progress else {}))
                if progress:last_progress=time.monotonic()
            check_complete(query,count)
            schema=column_selection.finish()
    except asyncpg.QueryCanceledError:
        raise AnalyticalError('QUERY_TIMEOUT','Analytical query exceeded its 60-second statement limit; no partial export was published.') from None
    except asyncpg.PostgresError as error:
        code='QUERY_RESOURCE_LIMIT' if error.sqlstate in ('53400','53200','53100','53300') else 'QUERY_ERROR'
        raise AnalyticalError(code,'Query could not complete within its SQL/type/resource contract; check the catalog and narrow the query. No partial file was published.') from None
    finally:
        if writer:await disk_work(writer.close)
    if spec.exclude_null_columns:
        # Project the bounded spool, not a second database query. This preserves
        # the exact snapshot/row order without keeping the result in memory.
        await project_columns(job.id,temporary,schema)
    await checkpoint(job.id,phase='finalizing',rows_written=count)
    size=temporary.stat().st_size
    if size>config.MAX_BYTES:raise AnalyticalError('FILE_SIZE_EXCEEDED','Final file exceeds the export size limit.')
    digest=await disk_work(files.sha256,temporary)
    completed=now();expires=completed+timedelta(seconds=config.RETENTION)
    columns=[{'name':field.name,'type':str(field.type),'nullable':field.nullable} for field in schema]
    source_definition={key:value for key,value in document.items() if key != MATRIX_CONTRACT_KEY}
    if isinstance(spec,MoveMatrixSpec):
        source_definition['player_win']=spec.player_win
    manifest={'format':'parquet','format_version':1,'catalog_version':1,'query_schema':SCHEMA,
        'export_id':job.id,'source':spec.source,
        'source_definition':source_definition,
        'request_sha256':job.request_sha256,'executed_sql_sha256':hashlib.sha256(query.sql.encode()).hexdigest(),
        'estimated_plan_cost':query.estimated_cost,
        'row_count':count,'columns':columns,'max_result_rows':spec.limit,'limit_reached':False,'complete_result':True,
        'selection':selection_metadata(spec,query), 'score_semantics':score_metadata(spec),
        'column_selection':column_selection.metadata(),
        'snapshot':{'isolation':'repeatable_read','started_at':snapshot_at.isoformat(),'identifier':snapshot},
        'created_at':job.created_at.isoformat(),'completed_at':completed.isoformat(),'expires_at':expires.isoformat(),
        'file':{'name':'data.parquet','size_bytes':size,'sha256':digest}}
    await disk_work(files.write_json,directory/'manifest.json',manifest)
    os.replace(temporary,directory/'data.parquet')
    await checkpoint(job.id,status='complete',phase='complete',rows_written=count,file_bytes=size,
                     file_sha256=digest,limit_reached=False,completed_at=completed,expires_at=expires)


async def project_columns(job_id,source,schema):
    from .jobs import checkpoint,disk_work
    import pyarrow.parquet as pq
    await checkpoint(job_id,phase='filtering_columns')
    target=source.with_name('data.parquet.filtered')
    reader=await disk_work(pq.ParquetFile,source)
    writer=None
    try:
        writer=ParquetOutput(target,schema=schema)
        batches=reader.iter_batches(batch_size=config.BATCH_ROWS,columns=schema.names,use_threads=False)
        while True:
            batch=await disk_work(next,batches,None)
            if batch is None:break
            await disk_work(writer.write_batch,batch)
            await checkpoint(job_id)
    finally:
        try:
            if writer:await disk_work(writer.close)
        finally:
            await disk_work(reader.close)
    os.replace(target,source)
