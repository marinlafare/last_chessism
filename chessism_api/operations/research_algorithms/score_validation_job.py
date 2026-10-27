"""Explicit one-player GPU validation. Reads sources; never publishes chart summaries.

One repeatable-read, read-only snapshot freezes the matrix, independent SQL
baseline and saved CPU summary together. Only compact comparison reports enter
algorithm_run. No engine execution, custom code, cloud calls or CPU fallback.
"""
import asyncio
import hashlib
import tempfile
import time

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..mate_distance import mate_distance_sql
from ..matrix_constructor.artifact_files import file_operation
from ..matrix_constructor.defaults import default_formula
from ..matrix_constructor.definitions import resolve_definition
from ..player_score_distribution import MODES, normalize, positions_sql, prepare
from ..player_stockfish_summary import read_saved
from .summary_validation import BATCH_ROWS, cuda_backend
from .score_stream import MAX_ROWS, DTYPE, Comparison, pack, read_batch
from .storage import WORK_ROOT, ensure_capacity

OPERATION = 'score_distribution_cuda_validation'
VERSION = 2


def configuration(player, *, benchmark=False):
    player, _ = normalize(player)
    recipe = resolve_definition(default_formula('score-distribution')['config'],
        {'filters': {'players': [player], 'modes': list(MODES), 'max_rows': MAX_ROWS}})
    config = {'operation': OPERATION, 'implementation_version': VERSION, 'backend': 'cuda_cupy',
            'player': player, 'matrix': recipe, 'columns': recipe['feature_columns'], 'max_rows': MAX_ROWS}
    if benchmark:
        from .score_benchmark import instructions
        config['benchmark'] = instructions()
    return config


def summary_groups(player, payload):
    return {mode: {'player': player, 'mode': mode,
        'counts': [*payload[mode]['counts'], payload[mode]['mate_positions'], payload[mode]['tablebase_positions']],
        'mates': payload[mode]['mates']} for mode in MODES}


def mate_map(records):
    result = {}
    for row in records:
        key = (row['side'], row['moves'])
        result[key] = result.get(key, 0) + int(row['positions'])
    return result


def compare_groups(expected, actual):
    differences = []
    for mode in MODES:
        left = expected.get(mode, {'counts': [0]*15, 'mates': []})
        right = actual.get(mode, {'counts': [0]*15, 'mates': []})
        for index, (a, b) in enumerate(zip(left['counts'], right['counts'])):
            if a != b:
                differences.append({'mode': mode, 'counter': index, 'expected': a, 'actual': b})
        a, b = mate_map(left['mates']), mate_map(right['mates'])
        for side, moves in sorted(a.keys() | b.keys(), key=lambda key: (key[0], -1 if key[1] is None else key[1])):
            if a.get((side, moves), 0) != b.get((side, moves), 0):
                differences.append({'mode': mode, 'side': side, 'moves': moves,
                    'expected': a.get((side, moves), 0), 'actual': b.get((side, moves), 0)})
    return {'status': 'failed' if differences else 'passed', 'mismatches': len(differences), 'examples': differences[:20]}


def baseline_sql(session):
    # This is the existing production SQL classifier, not SQL generated from
    # CUDA decisions. It reads the whole player in the same frozen transaction.
    return positions_sql(session) + f'''
        SELECT mode,score_kind,bin_id,
            CASE WHEN score_kind='mate' THEN {mate_distance_sql()} END AS mate_moves,
            CASE WHEN score_kind='mate' THEN CASE WHEN player_score>0 THEN 'for' ELSE 'against' END END AS mate_side,
            COUNT(*)::bigint AS positions FROM classified WHERE score_kind<>'unscored'
        GROUP BY mode,score_kind,bin_id,mate_moves,mate_side'''


def input_sql(session):
    # Same five-column matrix contract, but the player is constant and category
    # dictionaries are fixed. Native indexed relations avoid compatibility-view
    # expansion, textual row keys and a full-history ORDER BY/materialization.
    return positions_sql(session) + f'''
        SELECT CASE mode WHEN 'bullet' THEN 0 WHEN 'blitz' THEN 1 WHEN 'rapid' THEN 2 ELSE -1 END,
            player_score::float8,
            CASE score_kind WHEN 'cp' THEN 0 WHEN 'mate' THEN 1 WHEN 'tablebase' THEN 2 ELSE -1 END,
            COALESCE(CASE WHEN score_kind='mate' THEN {mate_distance_sql()} END,-1)::bigint
        FROM scored WHERE score_kind<>'unscored' LIMIT :row_limit'''


def spool_batch(stream, records):
    batch = pack(records)
    stream.write(batch.tobytes())


async def freeze_source(config, reporter, stream, *, session_factory=None):
    if config != configuration(config.get('player'), benchmark='benchmark' in config):
        raise ValueError('Unsupported or modified CUDA validation instructions.')
    player = config['player']
    await reporter.progress('freezing_input', detail='Freezing one player’s scored positions and CPU references; source data is read-only.')
    factory = session_factory or AsyncDBSession
    async with factory() as session:
        await prepare(session)
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        if await session.scalar(text("""SELECT EXISTS(SELECT 1 FROM cloud_analysis_job
            WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))""")):
            raise ValueError('Wait for the active cloud workflow to finish before starting this validation.')
        if not await session.scalar(text('SELECT 1 FROM player WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'), {'player': player}):
            raise ValueError('Choose an existing player with a complete profile.')
        checkpoint = time.perf_counter()
        saved = await read_saved(session, player)
        if saved['summary_pending'] or saved['stale']:
            raise ValueError('This player’s CPU summary is still updating. Wait for it to become current.')
        expected = summary_groups(player, saved['by_mode'])
        total = sum(sum(group['counts']) for group in expected.values())
        if not 1 <= total <= MAX_ROWS:
            raise ValueError(f'This validation supports a whole player with 1–{MAX_ROWS:,} scored occurrences; this player has {total:,}. No rows were truncated.')
        summary_read = time.perf_counter() - checkpoint
        taken_at = (await session.scalar(text('SELECT transaction_timestamp()'))).isoformat()
        query = input_sql(session)
        params = {'player': player, 'modes': list(MODES), 'row_limit': MAX_ROWS + 1}
        checkpoint = time.perf_counter()
        count = 0
        cursor = await session.stream(text(query), params, execution_options={'yield_per': BATCH_ROWS})
        try:
            async for records in cursor.partitions(BATCH_ROWS):
                count += len(records)
                if count > MAX_ROWS or count > total:
                    raise ValueError('Input exceeded its bounded/current summary count; no truncation is allowed.')
                await file_operation(spool_batch, stream, records)
                await reporter.progress('freezing_input', count, total, 'Streaming scored positions into bounded numerical batches.')
        finally:
            await cursor.close()
        extraction = time.perf_counter() - checkpoint
        if count != total:
            raise ValueError('Scored occurrence count disagrees with the saved summary; refusing partial validation.')
        await reporter.progress('sql_reference', count, count, 'Checking the existing SQL classifier on the same database snapshot.')
        checkpoint = time.perf_counter()
        sql = {mode: {'player': player, 'mode': mode, 'counts': [0]*15, 'mates': []} for mode in MODES}
        statement = baseline_sql(session)
        for row in (await session.execute(text(statement), {'player': player, 'modes': list(MODES)})).mappings():
            group = sql[row['mode']]
            category = int(row['bin_id']) if row['score_kind'] == 'cp' else {'mate': 13, 'tablebase': 14}[row['score_kind']]
            group['counts'][category] += int(row['positions'])
            if row['score_kind'] == 'mate':
                group['mates'].append({'moves': row['mate_moves'], 'side': row['mate_side'], 'positions': int(row['positions'])})
        sql_seconds = time.perf_counter() - checkpoint
    # The database transaction closes before CUDA. The private temporary file
    # bounds RAM usage, disappears on cancellation/crash, and is not an artifact.
    source = {'player': player, 'scope': 'whole_player_all_modes', 'taken_at': taken_at,
        'summary_snapshot': saved['snapshot'], 'summary_generated_at': saved['generated_at'],
        'expected_rows': total, 'rows': count, 'row_limit': MAX_ROWS, 'truncated': False,
        'matrix_default': 'score-distribution', 'matrix_version': 2,
        'sql_sha256': hashlib.sha256(statement.encode()).hexdigest(),
        'extraction_sql_sha256': hashlib.sha256(query.encode()).hexdigest(),
        'extraction_version': VERSION, 'temporary_input_bytes': count * DTYPE.itemsize}
    return sql, expected, source, {'extraction_seconds': extraction,
        'sql_reference_seconds': sql_seconds, 'summary_read_seconds': summary_read}


async def execute(config, reporter):
    started = time.perf_counter()
    await reporter.progress('checking_gpu', detail='Checking the assigned CUDA GPU. No CPU fallback is permitted.')
    # Fail before touching the source DB if the worker has no GPU.
    checkpoint = time.perf_counter()
    await file_operation(cuda_backend, 0)
    gpu_preflight = time.perf_counter() - checkpoint
    ensure_capacity(config)
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(dir=WORK_ROOT) as stream:
        sql, saved, source, timings = await asyncio.wait_for(freeze_source(config, reporter, stream), timeout=180)
        await reporter.check()
        await reporter.progress('cuda_comparison', 0, source['rows'], 'Running scored-only CUDA classification in bounded batches.')
        comparison = await file_operation(Comparison, config['player'], device=0)
        stream.seek(0)
        checkpoint = time.perf_counter()
        while True:
            batch = await file_operation(read_batch, stream)
            if not len(batch):
                break
            await reporter.check()
            await file_operation(comparison.consume, batch)
            await reporter.progress('cuda_comparison', comparison.rows, source['rows'], 'Comparing every scored occurrence and accumulating exact counters.')
        if comparison.rows != source['rows']:
            raise ValueError('Numerical input stream changed; refusing partial validation.')
        report = comparison.report()
        timings['stream_comparison_seconds'] = time.perf_counter() - checkpoint
        if 'benchmark' in config:
            from .score_benchmark import benchmark
            actual = {row['mode']: {'counts': row['cuda'], 'mates': row['mates_cuda']} for row in report['groups']}
            if any(compare_groups(reference, actual)['status'] != 'passed' for reference in (sql, saved)):
                raise ValueError('SQL/summary references disagree; benchmarking was not started.')
            report['batch_benchmark'] = await asyncio.wait_for(benchmark(stream, report, source, reporter), timeout=180)
    await reporter.check()
    actual = {row['mode']: {'counts': row['cuda'], 'mates': row['mates_cuda']} for row in report['groups']}
    checks = {'existing_sql': compare_groups(sql, actual), 'saved_cpu_summary': compare_groups(saved, actual)}
    if any(item['status'] != 'passed' for item in checks.values()):
        report['status'] = 'failed'
    report.update(input_source='frozen_export', validation_scope='live_player', source=source,
        reference_checks=checks, reference_groups={'existing_sql': sql, 'saved_cpu_summary': saved},
        input_rows=source['rows'], included_rows=source['rows'])
    report['timings'].update(timings, gpu_preflight_seconds=gpu_preflight, validation_wall_seconds=time.perf_counter() - started)
    return report
