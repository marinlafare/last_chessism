"""Outcome-blind, bounded multi-player sample for rating-strength exploration."""
from collections import Counter, defaultdict, deque
from datetime import datetime
import gzip
import hashlib
import heapq
import json

from sqlalchemy import text

from chessism_api.database.relational_storage import storage_for
from ..matrix_constructor.artifact_files import file_operation
from . import blunder_cohort_source as cohort, blunder_development_reuse as history
from .blunder_cohort_job import file_sha
from .blunder_threshold_compare import BANDS, band
from .blunder_v2_source import held_player
from .storage import WORK_ROOT

OPERATION = 'rating_blunder_strength_comparison_v1'
RESERVATION = 'chessism_blunder_strength_reservation_v1'
FORMAT = 'chessism_blunder_strength_comparison_v1'
PER_GROUP = 250
PER_OWNER = 50
POOL_PER_OWNER_GAP = 64
MAX_GAMES = PER_GROUP * len(BANDS) * len(cohort.MODES)
MAX_POSITIONS = 1_000_000
SEED = 'blunder-strength-v1:20261026'


async def exploration_ids(session):
    """Explored outcomes cannot later become supposedly fresh test observations."""
    rows = (await session.execute(text('''SELECT result FROM algorithm_run
        WHERE config->>'operation'=:operation AND result IS NOT NULL
        ORDER BY created_at,id LIMIT 101'''), {'operation': OPERATION})).scalars().all()
    if len(rows) > 100:
        raise ValueError('Strength-exploration history exceeds its bounded scan.')
    known = set()
    for r in rows:
        if r.get('format') not in (RESERVATION, FORMAT):
            raise ValueError('Unrecognized strength-exploration identity reservation.')
        source = r['source']; values = source['inspected_game_ids']
        if history.digest(values) != source['inspected_game_ids_sha256']:
            raise ValueError('Exploration identity checksum changed.')
        known.update(history.ids(values))
    if len(known) > 100_000:
        raise ValueError('Exploration identity budget exceeded.')
    return known


class Pool:
    def __init__(self, excluded, test_from):
        self.excluded = set(excluded)
        self.test_from = datetime.fromisoformat(test_from)
        self.buckets = defaultdict(list)
        self.skipped = Counter()

    def add(self, r):
        if r['game'] in self.excluded:
            self.skipped['previous_research'] += 1; return
        if not r['played_at'] or datetime.fromisoformat(r['played_at']) >= self.test_from:
            self.skipped['reserved_later_dates_or_missing_date'] += 1; return
        if held_player(r['player']):
            self.skipped['held_out_owner'] += 1; return
        if r['mode'] not in cohort.MODES or not all(1 <= r[k] <= 4000 for k in ('player_rating', 'opponent_rating')):
            raise ValueError('Unexpected cohort rating or mode.')
        gap = abs(r['player_rating']-r['opponent_rating'])
        key = band(r['player_rating']), r['mode'], r['player'], 0 if gap <= 200 else 1 if gap <= 400 else 2
        rank = int.from_bytes(hashlib.sha256(f"{SEED}:{r['game']}:{r['player']}".encode()).digest(), 'big')
        item = (-rank, -r['game'], r)
        bucket = self.buckets[key]
        if len(bucket) < POOL_PER_OWNER_GAP:
            heapq.heappush(bucket, item)
        elif item[:2] > bucket[0][:2]:
            heapq.heapreplace(bucket, item)

    def choose(self):
        buckets = {k: deque(x[2] for x in sorted(v, key=lambda x: (-x[0], -x[1]))) for k, v in self.buckets.items()}
        selected, seen, cells = [], set(), []
        # Round-robin across cells, owners and rating gaps, not player-by-player.
        groups = [(b, m) for b in BANDS for m in cohort.MODES]
        counts, owner_counts = Counter(), Counter()
        while True:
            added = 0
            for cell in groups:
                keys = sorted(k for k in buckets if k[:2] == cell)
                for k in keys:
                    owner = (*cell, k[2]); q = buckets[k]
                    while q and q[0]['game'] in seen: q.popleft()
                    if q and counts[cell] < PER_GROUP and owner_counts[owner] < PER_OWNER:
                        r = q.popleft(); selected.append(r); seen.add(r['game'])
                        counts[cell] += 1; owner_counts[owner] += 1; added += 1
            if not added: break
        for b, m in groups:
            cells.append({'rating_band': b, 'mode': m, 'target': PER_GROUP, 'selected': counts[b, m],
                'owners': sum(n > 0 for (band_, mode_, _), n in owner_counts.items() if (band_, mode_) == (b, m))})
        return selected, {'cells_before_metadata_checks': cells, 'skipped_perspectives': dict(self.skipped),
            'per_owner_cell_cap': PER_OWNER, 'selected_unique_games': len(selected)}


def read_artifact(player, parent, pool, check):
    artifact = player['artifact']; path = (WORK_ROOT.parent/artifact['relative_path']).resolve()
    if not path.is_relative_to((WORK_ROOT.parent/'blunder-cohorts').resolve()) or not path.is_file():
        raise ValueError('Cohort artifact is missing or outside its owned directory.')
    if path.stat().st_size != artifact['bytes'] or file_sha(path) != artifact['sha256']:
        raise ValueError('Cohort artifact checksum changed.')
    decoded = hashlib.sha256(); count = 0; previous = None
    with gzip.open(path, 'rb') as f:
        raw = f.readline(); decoded.update(raw); header = json.loads(raw)
        if (header.get('player') != player['player'] or header.get('model_id') != parent['model_id']
                or header.get('threshold') != .15 or header.get('revision') != player['revision']
                or header.get('parent_report_sha256') != parent['config']['parent_report_sha256']):
            raise ValueError('Cohort artifact header changed.')
        for raw in f:
            if count % 4096 == 0: check()
            decoded.update(raw); r = json.loads(raw)
            if r['player'] != player['player'] or (previous is not None and r['game'] <= previous):
                raise ValueError('Duplicate or unordered cohort artifact game.')
            previous = r['game']; pool.add(r); count += 1
    if count != artifact['records'] or decoded.hexdigest() != artifact['decoded_sha256']:
        raise ValueError('Decoded cohort artifact is incomplete or changed.')


def metadata_sql(session):
    layout = storage_for(session); t = layout.table if layout else lambda n: n
    raw = cohort.raw_sql(session).replace('p.player_name=:player', '(p.player_name,g.link) IN (SELECT player,link FROM requested)')
    if layout:
        names, joins = 'w.player_name AS white_name,b.player_name AS black_name', f'JOIN {t("player")} w ON w.id=g.white_player_id JOIN {t("player")} b ON b.id=g.black_player_id'
    else:
        names, joins = 'g.white AS white_name,g.black AS black_name', ''
    return f'''WITH requested AS (SELECT * FROM UNNEST(CAST(:players AS text[]),CAST(:links AS bigint[])) r(player,link))
        SELECT s.*,g.white_result,g.black_result,{names} FROM ({raw}) s JOIN {t('game')} g ON g.link=s.link {joins}'''


async def load_metadata(session, selected, reporter):
    lookup = {}; inspected = set(); dropped = Counter()
    for offset in range(0, len(selected), 128):
        await reporter.check(); block = selected[offset:offset+128]
        rows = (await session.execute(text(metadata_sql(session)), {'players': [r['player'] for r in block],
            'links': [r['game'] for r in block], 'modes': list(cohort.MODES)})).mappings().all()
        if len(rows) != len(block) or len({(r['player'], r['link']) for r in rows}) != len(block):
            raise ValueError('Selected sample metadata is missing or duplicated.')
        lookup.update({(r['player'], r['link']): dict(r) for r in rows})
    games = []
    for old in selected:
        g = lookup[old['player'], old['game']]; inspected.add(g['link'])
        if not (g['analyzed'] and g['rated'] and g['standard']):
            raise ValueError('Selected game is no longer eligible.')
        for k in ('is_white', 'mode', 'player_rating', 'opponent_rating'):
            if g[k] != old[k]: raise ValueError('Selected game metadata changed since cohort.')
        if g['saved_moves'] != old['moves'] or g['saved_blunders'] != old['old15']:
            raise ValueError('Saved CPU summary changed since cohort.')
        if held_player(g['white_name']) or held_player(g['black_name']):
            dropped['held_out_opponent'] += 1; continue
        if g['white_result'] not in (0, .5, 1) or g['black_result'] not in (0, .5, 1) or g['white_result']+g['black_result'] != 1:
            dropped['invalid_outcome'] += 1; continue
        g['outcome'] = float(g['white_result'] if g['is_white'] else g['black_result'])
        g['played_at'] = g['played_at'].isoformat()
        g['cohort_candidate15'] = old['candidate15']
        games.append(g)
    if not games or len(games) > MAX_GAMES or sum(g['total_positions'] for g in games) > MAX_POSITIONS:
        raise ValueError('Sample empty or exceeds whole-game budget; no truncation permitted.')
    return games, sorted(inspected), dict(dropped)


async def reserve(source, reporter):
    from chessism_api.database.engine import AsyncDBSession
    from .repository import RunCancelled
    await reporter.check()
    payload = {'format': RESERVATION, 'source': source, 'canonical': False, 'production_changed': False}
    async with AsyncDBSession() as session, session.begin():
        saved = await session.scalar(text('''UPDATE algorithm_run SET result=CAST(:value AS jsonb)
            WHERE id=:id AND status='running' AND NOT cancel_requested RETURNING id'''),
            {'id': reporter.run_id, 'value': json.dumps(payload, allow_nan=False)})
        if saved is None: raise RunCancelled('Cancelled before strength-comparison reservation.')
