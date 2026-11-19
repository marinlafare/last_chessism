"""Exact game-rating counts, cached briefly; no FEN reads or schema changes.

PPD is a percentage histogram, not a cumulative distribution or a forecast.
The cache contains exact ratings for all three modes so mode switches reuse
the same snapshot. Game changes become visible within CACHE_SECONDS.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

from redis.exceptions import RedisError
from sqlalchemy.exc import DBAPIError

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql import player_catalog_reads as storage
from chessism_api.database.postgresql.transactions import read_transaction

CACHE_SECONDS = 60
BIN_WIDTH = 10
MAX_BINS = 10_000
MODES = ('bullet', 'blitz', 'rapid')
READ_SLOTS = asyncio.Semaphore(2)
LOG = logging.getLogger(__name__)


class DistributionBusy(RuntimeError):
    pass


def normalize_selection(player_name, mode):
    player = str(player_name or '').strip().lower()
    mode = str(mode or 'all').strip().lower()
    if not player or len(player) > 128:
        raise ValueError('A player name of 1–128 characters is required.')
    if mode not in ('all', *MODES):
        raise ValueError('Mode must be all, bullet, blitz or rapid.')
    return player, mode


def percentage_distribution(player, mode, snapshot, *, cache_hit=False):
    counts = {}
    excluded = 0
    for row in snapshot['counts']:
        if row['mode'] not in MODES or (mode != 'all' and row['mode'] != mode):
            continue
        count = int(row['games'])
        rating = row['rating']
        if rating is None or rating <= 0:
            excluded += count
            continue
        lower = int(rating) // BIN_WIDTH * BIN_WIDTH
        counts[lower] = counts.get(lower, 0) + count
    total = sum(counts.values())
    x = []
    if counts:
        start, end = min(counts), max(counts)
        if (end - start) // BIN_WIDTH + 1 > MAX_BINS:
            raise ValueError('Rating range exceeds the supported distribution size; check source ratings.')
        x = list(range(start, end + BIN_WIDTH, BIN_WIDTH))
    return {
        'player_name': player, 'mode': mode, 'distribution': 'percentage',
        'bin_width': BIN_WIDTH, 'bin_interval': '[x, x + 10)', 'y_unit': 'percent',
        'x': x, 'y': [round(100 * counts.get(value, 0) / total, 8) for value in x],
        'games_count': total, 'excluded_games_count': excluded,
        'total_games_count': total + excluded,
        'generated_at': snapshot['generated_at'],
        'cache_hit': cache_hit, 'max_age_seconds': CACHE_SECONDS,
    }


async def _cached_snapshot(redis, key):
    try:
        raw = await redis.get(key)
        if raw:
            snapshot = json.loads(raw)
            if (isinstance(snapshot, dict) and snapshot.get('version') == 1
                    and isinstance(snapshot.get('counts'), list)
                    and isinstance(snapshot.get('generated_at'), str)
                    and all(isinstance(row, dict) and row.get('mode') in MODES
                            and type(row.get('games')) is int and row['games'] >= 0
                            and (row.get('rating') is None or type(row['rating']) is int)
                            for row in snapshot['counts'])):
                return snapshot
    except (RedisError, ValueError, TypeError):
        LOG.warning('Player rating cache unavailable or invalid; reading game counts.')
    return None


async def get_player_rating_distribution(player_name, mode, redis, *, session_factory=None):
    player, mode = normalize_selection(player_name, mode)
    try:
        async with read_transaction(session_factory or AsyncDBSession) as session:
            await storage.read_limits(session)
            profile = await storage.active_profile(session, player)
            if not profile:
                raise LookupError(f'Player {player!r} was not found.')
            from .player_analytics_summary import read_snapshot
            saved = await read_snapshot(session, player, 'distribution')
            if saved is not None:
                return {**percentage_distribution(player, mode, saved, cache_hit=True),
                        'summary_revision':saved['summary_revision'], 'summary_stale':saved['summary_stale']}
            key = 'chessism:rating-ppd:v2:' + str(profile['id'])
            snapshot = await _cached_snapshot(redis,key)
            if snapshot is not None:
                return percentage_distribution(player, mode, snapshot, cache_hit=True)
            try:
                await asyncio.wait_for(READ_SLOTS.acquire(), timeout=2)
            except asyncio.TimeoutError as error:
                raise DistributionBusy('Rating calculations are busy. Please retry shortly.') from error
            try:
                snapshot = await _cached_snapshot(redis,key)
                if snapshot is not None:
                    return percentage_distribution(player, mode, snapshot, cache_hit=True)
                rows = await storage.rating_counts(session,profile['id'])
            finally:
                READ_SLOTS.release()
        snapshot = {'version':1, 'generated_at':datetime.now(timezone.utc).isoformat(),
            'counts':[{'mode':row['mode'], 'rating':row['rating'], 'games':int(row['games'])} for row in rows]}
        payload = percentage_distribution(player,mode,snapshot)
        try:
            await redis.set(key,json.dumps(snapshot),ex=CACHE_SECONDS)
        except RedisError:
            LOG.warning('Could not retain player rating counts in the cache.')
        return payload
    except DBAPIError as error:
        if getattr(error.orig,'sqlstate',None) in ('57014','55P03'):
            raise DistributionBusy('Rating calculation timed out. Please retry shortly.') from error
        raise
