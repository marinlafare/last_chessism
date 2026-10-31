"""Explicit, read-only all-player score-distribution CUDA benchmark.

Run in the idle GPU research worker. Does not enqueue work, publish summaries,
change canonical algorithms, or call cloud services. Each player is frozen in
one read-only snapshot, with bounded 1,000-game SQL queries. Anonymous temporary
input files are deleted on exit; only compact JSONL evidence is retained.

Example: python benchmark_all_score_distributions.py --output /path/report.jsonl
"""
import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from statistics import median
import tempfile
import time

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from chessism_api.operations.player_score_distribution import MODES, games_sql, positions_sql, prepare
from chessism_api.operations.player_score_histogram import BATCH_GAMES, BATCH_SELECTION
from chessism_api.operations.player_stockfish_summary import read_saved
from chessism_api.operations.research_algorithms.score_benchmark import summary_pass, signature
from chessism_api.operations.research_algorithms.score_stream import Comparison, DTYPE, MAX_ROWS, read_batch
from chessism_api.operations.research_algorithms.score_validation_job import (
    baseline_sql, compare_groups, input_sql, spool_batch, summary_groups)
from chessism_api.operations.research_algorithms.storage import WORK_ROOT, ensure_capacity
from chessism_api.operations.research_algorithms.summary_validation import BATCH_ROWS, cuda_backend

MAX_PLAYER_ROWS = 20_000_000
MAX_COHORT_ROWS = 100_000_000
MAX_PLAYERS = 100
GPU_BATCH = 262144
REPEATS = 3


def empty_groups(player):
    return {mode: {'player': player, 'mode': mode, 'counts': [0] * 15, 'mates': []} for mode in MODES}


def merge_groups(target, addition):
    """Preserve multiplicity and all known/unknown mate distances and sides."""
    for mode in MODES:
        left, right = target[mode], addition[mode]
        left['counts'] = [a + b for a, b in zip(left['counts'], right['counts'])]
        mates = {}
        for row in [*left['mates'], *right['mates']]:
            key = row['side'], row['moves']
            mates[key] = mates.get(key, 0) + row['positions']
        left['mates'] = [{'side': side, 'moves': moves, 'positions': count}
            for (side, moves), count in sorted(mates.items(),
                key=lambda item: (item[0][0], -1 if item[0][1] is None else item[0][1]))]


class Window:
    """Seekable bounded view: existing validators keep their 2M-row limit."""
    def __init__(self, stream, offset, rows):
        if not 0 <= rows <= MAX_ROWS or offset < 0:
            raise ValueError('Invalid numerical input window.')
        self.stream, self.start, self.length = stream, offset * DTYPE.itemsize, rows * DTYPE.itemsize
        self.position = 0

    def seek(self, offset):
        if not 0 <= offset <= self.length:
            raise ValueError('Seek outside the bounded input window.')
        self.position = offset

    def read(self, size):
        if size < 0:
            raise ValueError('Unbounded reads are not permitted.')
        self.stream.seek(self.start + self.position)
        data = self.stream.read(min(size, self.length - self.position))
        self.position += len(data)
        return data


async def guard(session):
    if await session.scalar(text("""SELECT EXISTS(SELECT 1 FROM cloud_analysis_job
        WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))""")):
        raise ValueError('Active cloud workflow: benchmark refused.')
    if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM algorithm_run WHERE status IN ('queued','running'))")):
        raise ValueError('Research queue must remain idle during this explicit benchmark.')


def checked_inventory(rows):
    records = []
    if not 1 <= len(rows) <= MAX_PLAYERS:
        raise ValueError('Empty or oversized eligible-player cohort.')
    for raw in rows:
        row = dict(raw)
        row['scored'] = int(row['scored'])
        if (row['revision'] is None or row['revision'] != row['published_revision']
                or row['revision'] != row['snapshot_revision'] or row['generated_at'] is None):
            raise ValueError(f"Current saved summary required for {row['player_name']}.")
        if not 0 <= row['scored'] <= MAX_PLAYER_ROWS:
            raise ValueError('Player exceeds the explicit benchmark bound; no truncation is allowed.')
        row['generated_at'] = row['generated_at'].isoformat()
        row['snapshot'] = hashlib.sha256(f"stockfish-v1:{row['player_name']}:{row['revision']}".encode()).hexdigest()
        records.append(row)
    if sum(row['scored'] for row in records) > MAX_COHORT_ROWS:
        raise ValueError('Cohort exceeds the explicit benchmark bound; no players are skipped.')
    return sorted(records, key=lambda row: (row['scored'], row['player_name']))


async def inventory(session_factory=AsyncDBSession):
    async with session_factory() as session:
        await prepare(session)
        await guard(session)
        table = lambda name: summary_table(storage_for(session), name)
        rows = (await session.execute(text(f'''SELECT p.player_name,st.revision,st.published_revision,
            sn.revision AS snapshot_revision,sn.generated_at,
            COALESCE((SELECT SUM((m.value->>'cp_positions')::bigint+
                (m.value->>'mate_positions')::bigint+(m.value->>'tablebase_positions')::bigint)
                FROM jsonb_each(sn.payload) m),0) AS scored
            FROM player p LEFT JOIN {table('state')} st ON st.player_name=p.player_name
            LEFT JOIN {table('snapshot')} sn ON sn.player_name=p.player_name
            WHERE p.joined>0 AND p.deleted_at IS NULL'''))).mappings().all()
        return checked_inventory(rows)


def chunk_queries(session):
    # Reuse the independent production classifier and v2 extraction projection;
    # change ONLY game selection to the production writer's bounded selection.
    original = positions_sql(session)
    bounded = positions_sql(session, selected_games=BATCH_SELECTION)
    queries = input_sql(session), baseline_sql(session)
    if not all(query.startswith(original) for query in queries):
        raise ValueError('SQL contract changed; benchmark must be reviewed.')
    return tuple(bounded + query[len(original):] for query in queries)


async def freeze_player(expected, stream, emit, *, session_factory=AsyncDBSession):
    player, total = expected['player_name'], expected['scored']
    timings = {'game_selection_seconds': 0.0, 'extraction_seconds': 0.0, 'sql_seconds': 0.0}
    sql = empty_groups(player)
    count = batches = 0
    async with session_factory() as session:
        await prepare(session)
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        await guard(session)
        saved = await read_saved(session, player)
        if (saved['summary_pending'] or saved['stale'] or saved['snapshot'] != expected['snapshot']
                or saved['generated_at'] != expected['generated_at']):
            raise ValueError('Summary changed since cohort selection; refusing mixed revisions.')
        saved_groups = summary_groups(player, saved['by_mode'])
        if sum(sum(group['counts']) for group in saved_groups.values()) != total:
            raise ValueError('Saved summary count changed.')
        extract_query, sql_query = chunk_queries(session)
        started = time.perf_counter()
        cursor = await session.stream(text(f'SELECT game_link,is_white,mode FROM ({games_sql(session)}) selected'),
            {'player': player, 'modes': list(MODES)}, execution_options={'yield_per': BATCH_GAMES})
        partitions = cursor.mappings().partitions(BATCH_GAMES).__aiter__()
        timings['game_selection_seconds'] += time.perf_counter() - started
        try:
            while True:
                started = time.perf_counter()
                try:
                    games = await anext(partitions)
                except StopAsyncIteration:
                    timings['game_selection_seconds'] += time.perf_counter() - started
                    break
                timings['game_selection_seconds'] += time.perf_counter() - started
                params = {'game_links': [g['game_link'] for g in games],
                    'player_sides': [g['is_white'] for g in games], 'game_modes': [g['mode'] for g in games],
                    'row_limit': MAX_ROWS + 1}
                # Alternate which query first warms the same source chunk.
                for method in (('extraction', 'sql') if batches % 2 == 0 else ('sql', 'extraction')):
                    started = time.perf_counter()
                    if method == 'sql':
                        part = empty_groups(player)
                        for row in (await session.execute(text(sql_query), params)).mappings():
                            group = part[row['mode']]
                            category = int(row['bin_id']) if row['score_kind'] == 'cp' else {'mate': 13, 'tablebase': 14}[row['score_kind']]
                            group['counts'][category] += int(row['positions'])
                            if row['score_kind'] == 'mate':
                                group['mates'].append({'moves': row['mate_moves'], 'side': row['mate_side'], 'positions': int(row['positions'])})
                        merge_groups(sql, part)
                    else:
                        batch_count = 0
                        rows = await session.stream(text(extract_query), params, execution_options={'yield_per': BATCH_ROWS})
                        try:
                            async for records in rows.partitions(BATCH_ROWS):
                                count += len(records)
                                batch_count += len(records)
                                if count > total or batch_count > MAX_ROWS:
                                    raise ValueError('Source exceeded its bounded expected count; refusing truncation.')
                                spool_batch(stream, records)
                        finally:
                            await rows.close()
                    timings[method + '_seconds'] += time.perf_counter() - started
                batches += 1
                if batches % 20 == 0:
                    emit('progress', {'player': player, 'phase': 'freezing', 'rows': count, 'total': total})
        finally:
            await cursor.close()
        taken_at = (await session.scalar(text('SELECT transaction_timestamp()'))).isoformat()
    # Close the DB snapshot before running CUDA. Neither path writes summaries.
    if count != total or stream.tell() != count * DTYPE.itemsize:
        raise ValueError('Incomplete numerical extraction; refusing partial comparison.')
    if compare_groups(saved_groups, sql)['status'] != 'passed':
        raise ValueError('Saved summary differs from the independent SQL reference.')
    return sql, saved_groups, timings, {'taken_at': taken_at, 'game_batches': batches,
        'rows': count, 'temporary_input_bytes': count * DTYPE.itemsize,
        'extraction_sql_sha256': hashlib.sha256(extract_query.encode()).hexdigest(),
        'reference_sql_sha256': hashlib.sha256(sql_query.encode()).hexdigest()}


def measure_player(stream, player, rows):
    if not 0 <= rows <= MAX_PLAYER_ROWS:
        raise ValueError('Player exceeds benchmark limit.')
    stream.seek(0, 2)
    if stream.tell() != rows * DTYPE.itemsize:
        raise ValueError('Numerical input length changed; refusing partial comparison.')
    actual = empty_groups(player)
    samples = {backend: [0.0] * REPEATS for backend in ('cuda_cupy', 'numpy_cpu')}
    blocks, implementation, hardware = [], None, None
    for offset in range(0, rows, MAX_ROWS):
        size = min(MAX_ROWS, rows - offset)
        window = Window(stream, offset, size)
        comparison = Comparison(player, batch_rows=GPU_BATCH)
        while True:
            batch = read_batch(window, GPU_BATCH)
            if not len(batch):
                break
            comparison.consume(batch)
        proof = comparison.report()
        if proof['rows'] != size or proof['status'] != 'passed':
            raise ValueError('Per-occurrence CUDA validation failed or input was truncated.')
        expected = signature(proof)
        implementation, hardware = proof['implementation_sha256'], proof['hardware']
        merge_groups(actual, {row['mode']: {'counts': row['cuda'], 'mates': row['mates_cuda']} for row in proof['groups']})
        for repeat in range(REPEATS):
            order = ('cuda_cupy', 'numpy_cpu') if repeat % 2 else ('numpy_cpu', 'cuda_cupy')
            for backend in order:
                result = summary_pass(window, player, backend=backend, batch_rows=GPU_BATCH)
                if (result['rows'] != size or result['input_sha256'] != proof['input_sha256']
                        or result['groups'] != expected):
                    raise ValueError('Timed pass disagrees with validated input/counters.')
                samples[backend][repeat] += result['seconds']
        blocks.append({'offset': offset, 'rows': size, 'input_sha256': proof['input_sha256'],
            'row_mismatches': proof['row_mismatches'], 'counter_mismatches': proof['counter_mismatches']})
    return {'groups': actual, 'blocks': blocks, 'samples_seconds': samples,
        'medians_seconds': {backend: median(values) for backend, values in samples.items()},
        'hardware': hardware, 'implementation_sha256': implementation}


async def run(emit):
    started = time.perf_counter()
    _, hardware = cuda_backend(0)  # Fail before DB reads; no CPU fallback.
    initial = await inventory()
    ensure_capacity({'max_rows': max(row['scored'] for row in initial), 'columns': ['mode', 'score', 'kind', 'mate']})
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    emit('start', {'format': 'score_distribution_cohort_benchmark_v1', 'canonical': False,
        'hardware': hardware, 'players': len(initial), 'rows': sum(row['scored'] for row in initial),
        'gpu_batch_rows': GPU_BATCH, 'validation_window_rows': MAX_ROWS,
        'source_query_game_batch': BATCH_GAMES, 'inventory': initial})
    results = []
    for index, entry in enumerate(initial):
        player = entry['player_name']
        player_started = time.perf_counter()
        emit('progress', {'player': player, 'index': index + 1, 'total_players': len(initial), 'phase': 'starting'})
        with tempfile.TemporaryFile(dir=WORK_ROOT) as stream:
            sql, saved, timings, source = await asyncio.wait_for(freeze_player(entry, stream, emit), timeout=900)
            measured = measure_player(stream, player, entry['scored'])
        checks = {key: compare_groups(expected, measured['groups']) for key, expected in (('sql', sql), ('saved', saved))}
        if any(check['status'] != 'passed' for check in checks.values()):
            raise ValueError('CUDA did not match SQL and saved summaries; benchmark failed.')
        result = {'player': player, 'status': 'passed', 'rows': entry['scored'], 'summary_snapshot': entry['snapshot'],
            'source': source, 'checks': checks, **measured, 'timings': timings}
        timings['fresh_gpu_seconds'] = timings['game_selection_seconds'] + timings['extraction_seconds'] + measured['medians_seconds']['cuda_cupy']
        timings['fresh_sql_seconds'] = timings['game_selection_seconds'] + timings['sql_seconds']
        timings['validation_wall_seconds'] = time.perf_counter() - player_started
        results.append(result)
        emit('player', result)
    final = await inventory()
    if final != initial:
        raise ValueError('Eligible players/source summary revisions changed during the run; cohort evidence is not current.')
    total = {key: sum(result['timings'][key] for result in results) for key in
        ('game_selection_seconds', 'extraction_seconds', 'sql_seconds', 'fresh_gpu_seconds', 'fresh_sql_seconds')}
    total.update({backend + '_seconds': sum(result['medians_seconds'][backend] for result in results)
        for backend in ('cuda_cupy', 'numpy_cpu')})
    emit('complete', {'status': 'passed', 'canonical': False, 'players': len(results),
        'rows': sum(result['rows'] for result in results), 'row_mismatches': 0, 'counter_mismatches': 0,
        'summary_versions_unchanged': True, 'timings': total, 'wall_seconds': time.perf_counter() - started,
        'notes': ['Full eligible-player cohort; no sampling, truncation or unscored matrix rows.',
            'Per-player read-only snapshots; all summary revisions/timestamps checked again at end.',
            'Alternating SQL/extraction query order per 1,000-game chunk; database caches remain warm/order-sensitive.',
            'Fresh GPU = game selection + numerical extraction/packing/spooling + warm GPU aggregate median.',
            'Prepared-input medians include file reads, packing, hashing and reduction; GPU also includes transfers.',
            'Full validation wall time includes independent SQL/CPU checks and three timed passes per backend.',
            'No production publication, daily partition scheduling, endings or unscored coverage calculation is timed.',
            'No persistent shared-input cache, source writes, canonical promotion or production worker changes.']})


async def main(output):
    from constants import CONN_STRING
    from chessism_api.database.runtime import bind_runtime_engine
    # Exclusive creation: never overwrite an earlier report.
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x') as evidence:
        def emit(kind, data):
            line = json.dumps({'event': kind, 'at': datetime.now(timezone.utc).isoformat(), **data})
            evidence.write(line + '\n')
            evidence.flush()
            concise = {key: value for key, value in data.items() if key in
                ('player', 'players', 'rows', 'total', 'total_players', 'index', 'phase', 'status', 'timings', 'wall_seconds')}
            print(json.dumps({'event': kind, **concise}), flush=True)
        engine = await bind_runtime_engine(CONN_STRING)
        try:
            await asyncio.wait_for(run(emit), timeout=3600)
        except BaseException as error:
            emit('failed', {'status': 'failed', 'error_type': type(error).__name__, 'error': str(error)})
            raise
        finally:
            await engine.dispose()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    asyncio.run(main(parser.parse_args().output))
