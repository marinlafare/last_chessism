"""Read-only full-population metadata audit, before fitting or consuming tests.

Run inside the API environment. No predictions, scores, outcome values, training,
reservations, or live summary writes. Fully-analyzed metadata is only an upper
bound on CP-supported training/evaluation coverage.
"""
import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
import time

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.runtime import bind_runtime_engine
from chessism_api.operations.research_algorithms import blunder_balanced_sampling as balance
from chessism_api.operations.research_algorithms import blunder_development_reuse as history
from chessism_api.operations.research_algorithms import blunder_v2_source as source

MAX_GAMES = 2_000_000
MAX_SECONDS = 180
MODES = ('bullet', 'blitz', 'rapid')
SPLITS = ('training', 'validation', 'fresh_test', 'held_player_test')
BANDS = ('<1200', '1200–1399', '1400–1599', '1600–1799', '1800–1999',
         '2000–2199', '2200–2399', '2400–2599', '2600+')


class Connections:
    def __init__(self):
        self.parents = {}
        self.games = Counter()

    def root(self, name):
        self.parents.setdefault(name, name)
        while name != self.parents[name]:
            self.parents[name] = self.parents[self.parents[name]]
            name = self.parents[name]
        return name

    def add(self, names):
        a, b = sorted(self.root(n) for n in names)
        if a != b:
            self.parents[b] = a
            self.games[a] += self.games.pop(b, 0)
        self.games[a] += 1

    def report(self):
        total = sum(self.games.values())
        largest = max(self.games.values(), default=0)
        return {'components': len(self.games), 'games': total,
                'largest_component_games': largest,
                'largest_component_share': largest / total if total else None,
                'current_independence_requirement_met': bool(total and len(self.games) >= 20
                                                              and largest * 5 <= total)}


class Audit:
    def __init__(self, policy, roles, known):
        self.policy, self.roles, self.known = policy, {k: set(v) for k, v in roles.items()}, set(known)
        if set().union(*self.roles.values()) != self.known or sum(map(len, self.roles.values())) != len(self.known):
            raise ValueError('Historical roles must be disjoint and cover all known games.')
        self.counts, self.positions, self.cells, self.excluded = (Counter() for _ in range(4))
        self.people = defaultdict(Counter)
        self.connections = defaultdict(Connections)
        self.scanned, self.previous = 0, 0
        self.digest = hashlib.sha256()

    def add(self, row):
        g = dict(row)
        if not self.previous < g['link'] or self.scanned >= MAX_GAMES:
            raise ValueError('Duplicate/unordered game or audit budget exceeded; no partial report.')
        names = balance.names(g)
        if (len(names) != 2 or not all(names) or g['mode'] not in MODES
                or not 0 <= g['rating_band'] < len(BANDS) or g['total_positions'] <= 0):
            raise ValueError('Invalid game metadata.')
        self.previous = g['link']; self.scanned += 1
        self.digest.update((json.dumps(g, sort_keys=True, default=str) + '\n').encode())
        split = balance.partition(g, self.policy)
        link = g['link']
        if link in self.roles['protected']:
            self.excluded['previous_test_exploration_or_unknown'] += 1; return
        if split < 0:
            self.excluded['held_player_before_test_date'] += 1; return
        if ((link in self.roles['training'] and split != 0)
                or (link in self.roles['validation'] and split != 1)):
            self.excluded['historical_role_conflict'] += 1; return
        if split >= 2 and link in self.known:
            self.excluded['previously_seen_test'] += 1; return
        key = split, g['mode']
        self.counts[key] += 1
        self.positions[key] += g['total_positions']
        self.cells[split, g['mode'], g['rating_band']] += 1
        self.people[key].update(names)
        if split >= 2:
            self.connections[split, 'all'].add(names)
            self.connections[key].add(names)

    def report(self):
        groups = []
        for split in range(4):
            for mode in MODES:
                key = split, mode
                n = self.counts[key]
                largest = max(self.people[key].values(), default=0)
                # Necessary upper bound from every player's <=10% count cap:
                # retained <= (all games - largest player's games) / .9.
                # Other players can tighten it further. This is NOT a selection.
                upper = min(n, (n-largest)*10//9)
                groups.append({'split': SPLITS[split], 'mode': mode, 'potential_games': n,
                    'positions_upper_bound': self.positions[key],
                    'unique_players_both_sides': len(self.people[key]),
                    'largest_player_games': largest, 'largest_player_share': largest/n if n else None,
                    'player_cap_count_upper_bound': upper,
                    'rating_bands': dict(zip(BANDS, [self.cells[split, mode, b] for b in range(9)]))})
        gaps = []
        for mode in MODES:
            for b, label in enumerate(BANDS):
                shortfalls = {SPLITS[s]: {'available_upper_bound': self.cells[s, mode, b], 'minimum': minimum}
                              for s, minimum in enumerate((500, 100, 100)) if self.cells[s, mode, b] < minimum}
                if shortfalls:
                    gaps.append({'mode': mode, 'average_rating': label, 'shortfalls': shortfalls})
        return {'format': 'chessism_blunder_data_capacity_audit_v1', 'status': 'metadata_audit_complete',
            'metadata_sha256': self.digest.hexdigest(), 'eligible_unique_games': self.scanned,
            'excluded': dict(self.excluded), 'groups': groups, 'coverage_shortfalls': gaps,
            'rating_mode_groups_meeting_raw_minimums': 27-len(gaps),
            'all_group_coverage_possible_in_current_pool': not gaps,
            'player_connections': [{'split': SPLITS[s], 'mode': mode, **c.report()}
                                   for (s, mode), c in sorted(self.connections.items())],
            'training_games_upper_bound': sum(self.counts[0, m] for m in MODES),
            'training_positions_upper_bound': sum(self.positions[0, m] for m in MODES),
            'scores_read': False, 'outcome_values_read': False, 'test_predictions_evaluated': False,
            'training_started': False, 'canonical': False, 'production_changed': False,
            'limitations': ['Counts precede CP-availability and player-cap checks; they are upper bounds, not selected training sizes.',
                'The eligibility predicate requires valid outcomes but no outcome values or scores are returned.',
                'Player-cap upper bounds are necessary conditions, not a claimed maximum feasible subset.',
                'Connectivity is for the full available pool; selecting a disjoint subset can change it but requires a separate representativeness audit.',
                'Metadata counting does not consume fresh test outcomes. Historical roles and dates are unchanged.']}


class Reporter:
    async def check(self):
        pass

    async def progress(self, *args, **kwargs):
        # Full-history replay would read position scores. Fail closed instead.
        raise ValueError('Metadata audit cannot replay historical positions; verified identity manifests are required.')


async def run():
    started = time.monotonic()
    engine = await bind_runtime_engine(os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+asyncpg://', 1))
    try:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            for setting in ("statement_timeout='60s'", "lock_timeout='1s'", "idle_in_transaction_session_timeout='60s'",
                            'max_parallel_workers_per_gather=0', "work_mem='32MB'", 'jit=off'):
                await session.execute(text('SET LOCAL ' + setting))
            if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
                raise ValueError('Wait for the active cloud workflow before the full metadata audit.')
            policy = await history.reference_plan(session)
            roles, known, proof = await history.history(session, policy, Reporter())
            audit = Audit(policy, roles, known)
            query = source.eligible_sql(session) + ''' SELECT link,mode,played_at,white_name,black_name,
                rating_band,total_positions FROM eligible ORDER BY link'''
            rows = await session.stream(text(query), execution_options={'yield_per': 5000})
            async for row in rows.mappings():
                if time.monotonic()-started >= MAX_SECONDS:
                    raise TimeoutError('Metadata audit exceeded its three-minute budget; no partial report.')
                audit.add(row)
            result = audit.report()
            result.update(created_at=datetime.now(timezone.utc).isoformat(), elapsed_seconds=time.monotonic()-started,
                source_snapshot=str(await session.scalar(text('SELECT transaction_timestamp()'))),
                policy=policy, historical_roles={k: len(v) for k, v in roles.items()}, verified_history=proof,
                query_sha256=hashlib.sha256(query.encode()).hexdigest())
            return result
    finally:
        await engine.dispose()


if __name__ == '__main__':
    print(json.dumps(asyncio.run(run()), indent=2, allow_nan=False))
