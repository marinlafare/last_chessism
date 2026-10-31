"""Research-only, atomic shadow snapshots inside AlgorithmRun.result.

No canonical table or live summary is touched. Upserts replace one sampled
game, retries cannot double-count, and failed transactions retain the last
complete generation. This is a pilot store, not the production scheduler.
"""
from collections import defaultdict
from copy import deepcopy
import json
import time

from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from .repository import RunCancelled

FORMAT = 'chessism_blunder_shadow_checkpoint_v1'
FIELDS = ('old15', 'baseline20', 'candidate20')


def aggregate(records):
    groups = defaultdict(lambda: {'games': 0, 'assessed_games': 0, 'no_assessed_player_moves': 0, 'moves': 0, 'unassessed': 0,
        **{k: 0 for k in FIELDS}, 'histograms': {k: [0]*7 for k in FIELDS}})
    for row in records:
        for key in ((row['player'], row['mode']), ('*', row['mode'])):
            group = groups[key]; group['games'] += 1
            assessed = row['moves'] > row['unassessed']
            group['assessed_games'] += assessed
            group['no_assessed_player_moves'] += not assessed
            group['moves'] += row['moves']; group['unassessed'] += row['unassessed']
            for field in FIELDS:
                group[field] += row[field]
                if assessed:
                    group['histograms'][field][min(6, row[field])] += 1
    return [{'player': k[0], 'mode': k[1], **v} for k, v in sorted(groups.items())]


def merge(previous, updates, model_id):
    if previous and (previous.get('format') != FORMAT or previous.get('model_id') != model_id or previous.get('summary_version') != 2):
        raise ValueError('Shadow model/format changed; do not mix summary versions.')
    records = {r['game']: r for r in (previous or {}).get('records', [])}
    if len(records) != len((previous or {}).get('records', [])):
        raise ValueError('Duplicate stored shadow games.')
    seen, changed = set(), 0
    for row in updates:
        if type(row['game']) is not int or row['game'] <= 0 or row['game'] in seen:
            raise ValueError('Invalid/duplicate shadow update identity.')
        seen.add(row['game'])
        if any(type(row[k]) is not int or row[k] < 0 for k in (*FIELDS, 'moves', 'unassessed')):
            raise ValueError('Invalid shadow counts.')
        if any(row[k] > row['moves'] for k in (*FIELDS, 'unassessed')):
            raise ValueError('Shadow counts exceed the move count.')
        if row['game'] in records and records[row['game']]['player'] != row['player']:
            raise ValueError('A sampled game cannot change assigned owner during an update.')
        if records.get(row['game']) != row:
            records[row['game']] = deepcopy(row); changed += 1
    if len(records) > 6000:
        raise ValueError('Shadow summary exceeds the six-thousand-game pilot budget.')
    ordered = [records[k] for k in sorted(records)]
    return {'format': FORMAT, 'summary_version': 2, 'model_id': model_id, 'generation': (previous or {}).get('generation', 0)+bool(changed),
        'canonical': False, 'production_changed': False, 'sample_only': True,
        'records': ordered, 'groups': aggregate(ordered)}, changed


async def publish(reporter, model_id, updates, expected_generation, *, session_factory=None, before_commit=None):
    await reporter.check()
    started = time.perf_counter()
    async with (session_factory or AsyncDBSession)() as session, session.begin():
        row = (await session.execute(text('SELECT status,cancel_requested,result FROM algorithm_run WHERE id=:id FOR UPDATE'),
            {'id': reporter.run_id})).mappings().one_or_none()
        if row is None or row['status'] != 'running' or row['cancel_requested']:
            raise RunCancelled('Cancelled before shadow publication; the previous complete generation is retained.')
        previous = row['result']
        if (previous or {}).get('generation', 0) != expected_generation:
            raise ValueError('Stale shadow generation; reload before retrying.')
        saved, changed = merge(previous, updates, model_id)
        if changed:
            await session.execute(text('UPDATE algorithm_run SET result=CAST(:payload AS jsonb) WHERE id=:id'),
                {'id': reporter.run_id, 'payload': json.dumps(saved, allow_nan=False)})
        if before_commit is not None:  # Failure injection for isolated database tests only.
            await before_commit()
    return saved, {'changed_games': changed, 'generation': saved['generation'], 'publication_seconds': time.perf_counter()-started}
