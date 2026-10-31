"""All-profile, full-history .15 CUDA shadow summaries. Never canonical writes."""
import asyncio
from copy import deepcopy
import gzip
import hashlib
import inspect
import io
import json
from pathlib import Path
import time
import uuid

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_cohort_source as source, blunder_cohort_compute as compute
from . import blunder_shadow_source as extraction, blunder_v2_model as model, blunder_fixed
from .blunder_development_reuse import digest
from .blunder_threshold_compare import FORMAT as PARENT_FORMAT
from .blunder_cohort_batch import SharedBatch, TARGET_ROWS, MAX_GAMES
from .blunder_training import game_blocks
from .blunder_v2_job import cancellation_check
from .cuda_runtime import cuda_backend
from .repository import RunCancelled
from .storage import WORK_ROOT, ensure_capacity, workspace

OPERATION = 'rating_blunder_cohort_shadow_v1'
CHECKPOINT = 'chessism_blunder_cohort_checkpoint_v1'
FORMAT = 'chessism_blunder_cohort_report_v1'


def configuration(parent_id, row, resume_id=None):
    r = (row or {}).get('result') or {}
    if ((row or {}).get('status') != 'complete' or r.get('format') != PARENT_FORMAT or r.get('status') != 'passed'
            or r.get('canonical') is not False or r.get('production_changed') is not False or r.get('refitted') is not False
            or any(r.get('checks', {'missing': 1}).values()) or any(r.get('parent_reference_checks', {'missing': 1}).values())):
        raise ValueError('A completed, verified frozen .15/.20 threshold comparison is required.')
    return {'operation': OPERATION, 'implementation_version': 1, 'parent_run_id': str(uuid.UUID(str(parent_id))),
        'parent_report_sha256': digest(r), 'model_id': r['model_id'], 'fits_sha256': digest(r['fits']),
        'threshold': .15, 'k0': .00368208, 'refitted': False, 'canonical': False,
        'scope': 'all_complete_profiles_all_analyzed_standard_games_with_valid_ratings',
        'max_rows': source.MAX_PLAYER_POSITIONS, 'max_cohort_positions': source.MAX_COHORT_POSITIONS,
        'columns': ['before', 'after', 'player_rating', 'opponent_rating', 'mode'],
        'gpu_batch_rows': TARGET_ROWS, 'gpu_batch_max_games': MAX_GAMES,
        'resume_run_id': str(uuid.UUID(str(resume_id))) if resume_id else None}


def file_sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def write_records(stream, records, hasher):
    for record in records:
        raw = (json.dumps(record, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()
        hasher.update(raw); stream.write(raw)


async def checkpoint(reporter, payload, expected_players, *, session_factory=None, before_commit=None):
    await reporter.check()
    async with (session_factory or AsyncDBSession)() as session, session.begin():
        row = (await session.execute(text('SELECT status,cancel_requested,result FROM algorithm_run WHERE id=:id FOR UPDATE'),
            {'id': reporter.run_id})).mappings().one_or_none()
        if row is None or row['status'] != 'running' or row['cancel_requested']:
            raise RunCancelled('Stopped before shadow checkpoint; previous completed players remain saved.')
        previous = row['result'] or {}
        if len(previous.get('players', [])) != expected_players or previous.get('format', CHECKPOINT) != CHECKPOINT:
            raise ValueError('Stale cohort checkpoint; reload instead of overwriting completed players.')
        names = [p['player'] for p in payload['players']]
        if len(names) != len(set(names)) or len(names) > len(payload['inventory']):
            raise ValueError('Duplicate or unexpected player checkpoint.')
        if previous and (previous['config'] != payload['config'] or previous['players'] != payload['players'][:expected_players]):
            raise ValueError('Completed shadow players or frozen instructions changed.')
        await session.execute(text('UPDATE algorithm_run SET result=CAST(:payload AS jsonb) WHERE id=:id'),
            {'id': reporter.run_id, 'payload': json.dumps(payload, allow_nan=False)})
        if before_commit:
            await before_commit()


class CohortRunner:
    def __init__(self, xp, fits, config, reporter, folder, result):
        self.config, self.reporter, self.folder, self.result = config, reporter, folder, result
        self.batch = SharedBatch(xp, fits, cancellation_check(reporter, asyncio.get_running_loop()))
        self.states = {}
        self.batches = list(result.get('gpu_batches', []))
        self.timings = dict(result.get('comparison_timings', {}))
        self.checks = dict(result.get('checks', {}))

    async def flush(self):
        result = await file_operation(self.batch.flush)
        if result is None: return
        self.batches.append(result['batch'])
        for k, v in result['checks'].items(): self.checks[k] = self.checks.get(k, 0)+v
        for k, v in result['timings'].items(): self.timings[k] = self.timings.get(k, 0.)+v
        by_player = {}
        for r in result['records']: by_player.setdefault(r['player'], []).append(r)
        for player, records in by_player.items():
            state = self.states[player]
            state['groups'].add(records); state['processed_games'] += len(records)
            await file_operation(write_records, state['output'], records, state['artifact_hash'])
        await self.publish_ready()

    async def publish_ready(self):
        for player, s in self.states.items():
            if not s['done'] or s['published'] or s['processed_games'] != s['games']: continue
            for mode, group in s['groups'].modes.items():
                saved = s['paired'][mode]
                if group['saved_scope_histogram'] != saved['counts'] or group['old15'] != saved['blunders']:
                    raise ValueError(f'{player}/{mode}: independently grouped saved CPU histogram differs.')
            await file_operation(s['output'].close)
            directory = WORK_ROOT.parent/'blunder-cohorts'/self.reporter.run_id
            directory.mkdir(parents=True, exist_ok=True); target = directory/s['temporary'].name
            if target.exists(): raise ValueError('Refusing to overwrite an existing research artifact.')
            await file_operation(s['temporary'].rename, target)
            expected = s['expected']
            report = {'player': player, 'revision': expected['revision'], 'scope': expected['scope'], 'taken_at': s['taken_at'],
                'status': 'passed', 'games': s['games'], 'source_positions': s['positions'], 'moves': s['moves'],
                'source_sha256': s['source_hash'].hexdigest(), 'saved_histogram_agreement': True,
                **s['groups'].report(), 'current_ui_by_mode': s['current'], 'paired_saved_by_mode': s['paired'],
                'artifact': {'relative_path': str(target.relative_to(WORK_ROOT.parent)), 'sha256': await file_operation(file_sha, target),
                    'decoded_sha256': s['artifact_hash'].hexdigest(), 'bytes': target.stat().st_size, 'records': s['games']},
                'timings': {**s['timings'], 'player_span_seconds': time.perf_counter()-s['started']}}
            prior_count = len(self.result['players']); self.result['players'].append(report)
            self.result['included_rows'] = sum(p['moves'] for p in self.result['players'])
            self.result['checks'] = dict(self.checks); self.result['gpu_batches'] = list(self.batches)
            self.result['comparison_timings'] = dict(self.timings)
            await checkpoint(self.reporter, self.result, prior_count)
            s['published'] = True

    async def read_player(self, expected):
        player = expected['player_name']; previous_link = None
        temporary = self.folder/(hashlib.sha256(player.encode()).hexdigest()+'.jsonl.gz')
        s = {'expected': expected, 'started': time.perf_counter(), 'temporary': temporary,
            'output': gzip.open(temporary, 'xb', compresslevel=3), 'groups': compute.Groups(),
            'source_hash': hashlib.sha256(), 'artifact_hash': hashlib.sha256(), 'processed_games': 0,
            'games': 0, 'positions': 0, 'moves': 0, 'timings': {}, 'done': False, 'published': False}
        self.states[player] = s
        write_records(s['output'], [{'format': 'chessism_blunder_cohort_games_v1', 'player': player,
            'model_id': self.config['model_id'], 'threshold': .15, 'revision': expected['revision'],
            'parent_report_sha256': self.config['parent_report_sha256']}], s['artifact_hash'])
        async with AsyncDBSession() as session:
            await source.prepare(session)
            if await source.current_revision(session, player) != expected['revision'] or await source.scope(session, player) != expected['scope']:
                raise ValueError(f'{player}: source revision/scope changed since cohort selection.')
            s['current'] = await source.saved_histograms(session, player)
            s['paired'] = await source.saved_histograms(session, player, paired=True)
            s['taken_at'] = str(await session.scalar(text('SELECT transaction_timestamp()')))
            cursor = await session.stream(text(f'''SELECT * FROM ({source.raw_sql(session)}) s
                WHERE {source.ELIGIBLE} ORDER BY link,is_white'''), {'player': player, 'modes': list(source.MODES)},
                execution_options={'yield_per': 128})
            try:
                async for page in cursor.mappings().partitions(128):
                    games = []
                    for raw in page:
                        g = dict(raw)
                        if previous_link is not None and g['link'] <= previous_link:
                            raise ValueError('Duplicate/out-of-order player game; refusing double counting.')
                        previous_link = g['link']; g['played_at'] = g['played_at'].isoformat() if g['played_at'] else None
                        games.append(g)
                    for block in game_blocks(games):
                        await self.reporter.check(); stream = io.BytesIO()
                        count, timings = await extraction.extract(session, block, SourceReporter(self.reporter), stream)
                        s['source_hash'].update(digest(block).encode())
                        s['moves'] += count; s['games'] += len(block); s['positions'] += sum(g['total_positions'] for g in block)
                        for k, v in timings.items(): s['timings'][k] = s['timings'].get(k, 0.)+v
                        self.batch.append(np.frombuffer(stream.getvalue(), dtype=blunder_fixed.DTYPE), block)
                        if self.batch.ready:
                            await self.reporter.progress('shared_gpu_batch', s['positions'], expected['source_positions'],
                                f'{player}: calculating a combined {self.batch.rows:,}-move GPU batch across {len({g["player"] for g in self.batch.games})} player(s).')
                            await self.flush()
                        await self.reporter.progress('reading_cohort_inputs', s['positions'], expected['source_positions'],
                            f'{player}: {s["games"]:,} games read; {len(self.result["players"])}/{len(self.result["inventory"])} player summaries saved.')
            finally:
                await cursor.close()
        if s['positions'] != expected['source_positions'] or s['games'] != sum(g['eligible_games'] for g in expected['scope']):
            raise ValueError('Full-player extraction was incomplete; refusing a truncated summary.')
        s['done'] = True
        await self.publish_ready()

    def close(self):
        for state in self.states.values(): state['output'].close()


class SourceReporter:
    """Keep cancellation checks, but let the outer stream throttle progress writes."""
    def __init__(self, reporter): self.reporter = reporter
    async def check(self): await self.reporter.check()
    async def progress(self, *args, **kwargs): pass


def aggregate_players(players):
    totals = {m: compute.empty_group() for m in source.MODES}
    ratings = {}
    for p in players:
        for mode, g in p['by_mode'].items(): compute.add_groups(totals[mode], g)
        for g in p['rating_groups']:
            key = g['rating_band'], g['mode']
            ratings.setdefault(key, compute.empty_group())
            compute.add_groups(ratings[key], {k: v for k, v in g.items() if k not in ('rating_band', 'mode')})
    return {'by_mode': totals, 'rating_groups': [{'rating_band': b, 'mode': m, **v} for (b, m), v in sorted(ratings.items())]}


async def execute(config, reporter):
    await reporter.check(); ensure_capacity(config); started = time.perf_counter()
    if model.K0 != config['k0'] or config['threshold'] != .15:
        raise ValueError('Frozen inference constants changed.')
    xp, hardware = await file_operation(cuda_backend, 0)
    # Warm inference kernels before the per-batch timing totals.
    await file_operation(model.decisions, xp, xp.asarray([0.]), xp.asarray([-200.]), xp.asarray([0]), xp.asarray([0]), 0., 0., 0., 0., threshold=.15)
    async with AsyncDBSession() as session:
        await source.prepare(session)
        row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'), {'id': config['parent_run_id']})).mappings().one_or_none()
        if config != configuration(config['parent_run_id'], row, config['resume_run_id']):
            raise ValueError('Parent model report or cohort instructions changed.')
        parent = row['result']
        await reporter.progress('inventory_cohort', detail='All joined profiles; explicit incomplete-game/rating exclusions, no sampling.')
        inventory = await source.inventory(session)
        resumed = []; prior_payload = {}
        if config['resume_run_id']:
            prior = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'), {'id': config['resume_run_id']})).mappings().one_or_none()
            payload = (prior or {}).get('result') or {}
            if ((prior or {}).get('status') not in ('failed', 'cancelled') or payload.get('format') != CHECKPOINT
                    or payload['config']['parent_report_sha256'] != config['parent_report_sha256']
                    or payload['config']['threshold'] != .15 or payload['inventory'] != inventory):
                raise ValueError('Only an interrupted identical-revision cohort can be resumed.')
            resumed = deepcopy(payload['players'])
            prior_payload = payload
    for player in resumed:
        artifact = (WORK_ROOT.parent/player['artifact']['relative_path']).resolve()
        if not artifact.is_relative_to((WORK_ROOT.parent/'blunder-cohorts').resolve()) or not artifact.is_file():
            raise ValueError('A resumed research artifact is missing or outside its owned directory.')
        if await file_operation(file_sha, artifact) != player['artifact']['sha256']:
            raise ValueError('A resumed artifact checksum changed.')
    result = {'format': CHECKPOINT, 'status': 'running', 'config': config, 'canonical': False, 'production_changed': False,
        'promotion_ready': False, 'refitted': False, 'independent_calibration_test': False, 'model_id': parent['model_id'],
        'fits': parent['fits'], 'original_quality_gates': parent['original_quality_gates'], 'hardware': hardware,
        'inventory': inventory, 'players': resumed, 'resumed_players': len(resumed), 'included_rows': 0,
        'gpu_batches': list(prior_payload.get('gpu_batches', [])), 'checks': dict(prior_payload.get('checks', {})),
        'comparison_timings': dict(prior_payload.get('comparison_timings', {})),
        'notes': ['Full eligible histories of all complete, non-deleted profiles, not a sample. Game counts are player perspectives: shared games appear in each owner summary.',
            'Only fully analyzed standard games with both historical ratings in 1..4000 are inferred. Other games are explicitly counted, never assigned fake ratings or zero blunders.',
            'Current UI histograms are retained separately. Paired comparisons use the identical eligible game set; zero/wholly unassessed games are excluded from both paired histograms.',
            'Source revisions are pinned per player and checked after the cohort. Every player uses a read-only repeatable snapshot, not one long database-wide snapshot.',
            'One combined GPU stream accumulates bounded SQL blocks across player boundaries, targeting 262,144 moves or 8,192 games per batch. Results are grouped back into player summaries.',
            'Per-game GPU counts and input hashes are retained in checksummed compressed research artifacts; compact summaries/checkpoints are in the research database report.',
            'CUDA .15 is checked against scalar/vector CPU; the current rule is checked against production scalar, independent SQL, saved game counts and saved histograms.',
            'Timing includes validation overhead. GPU candidate timing includes transfers and decision flags for checking, not just production histogram reduction; no end-to-end production speedup is claimed.',
            'No refitting, independent calibration test or live canonical publication. Previous model-quality failures remain visible.']}
    await checkpoint(reporter, result, 0)
    with workspace(reporter.run_id) as folder:
        runner = CohortRunner(xp, parent['fits'], config, reporter, folder, result)
        try:
            for expected in inventory:
                if expected['player_name'] in {p['player'] for p in result['players']}:
                    continue
                await reporter.progress('starting_player', len(result['players']), len(inventory),
                    f"{expected['player_name']}: reading eligible history into the combined GPU stream.")
                await runner.read_player(expected)
            await runner.flush()
            if len(result['players']) != len(inventory):
                raise ValueError('The combined GPU stream did not publish every player.')
        finally:
            runner.close()
    async with AsyncDBSession() as session:
        await source.prepare(session)
        current = {p['player_name']: p['revision'] for p in await source.profiles(session)}
    result.update(format=FORMAT, status='passed' if current == {p['player_name']: p['revision'] for p in inventory} else 'stale',
        stale_players=[p['player_name'] for p in inventory if current.get(p['player_name']) != p['revision']],
        cohort_membership_changed=set(current) != {p['player_name'] for p in inventory},
        **aggregate_players(result['players']),
        implementation_sha256=hashlib.sha256(''.join(inspect.getsource(m) for m in
            (inspect.getmodule(execute), source, compute, extraction, model, blunder_fixed)).encode()).hexdigest(),
        timings={'cohort_wall_seconds': time.perf_counter()-started, **runner.timings,
            **{k: sum(p['timings'].get(k, 0.) for p in result['players']) for k in
                ('source_read_pack_seconds', 'existing_sql_reference_seconds')}})
    json.dumps(result, allow_nan=False)
    return result
