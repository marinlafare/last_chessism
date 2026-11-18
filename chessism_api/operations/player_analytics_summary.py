"""Bounded daily metadata rebuilding and atomic publication of complete snapshots."""
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import logging
import os
import re

from chessism_api.database.postgresql import player_metadata as storage
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations.player_timezone import resolve_player_timezone

LOG = logging.getLogger(__name__)
MODES = ('bullet', 'blitz', 'rapid')


def enabled():
    return os.environ.get('CHESSISM_PLAYER_SUMMARIES', '0') == '1'


async def read_snapshot(session, player, section):
    if not enabled():
        return None
    row = await storage.snapshot(session, player, section)
    manual = await storage.manual_metadata(session)
    if not row:
        if not manual:
            return None
        payload = {'modes':{}, 'latest_played_at':None} if section == 'hours' else {'counts':[]} if section == 'distribution' else {'rows':[]}
        profile = await storage.profile(session, player)
        return {**payload, 'time_context':resolve_player_timezone(dict(profile or {})).payload(),
            'summary_revision':0, 'summary_stale':True, 'summary_pending':True, 'manual':True,
            '_player_id':profile['id'] if profile else None, 'generated_at':None}
    return {**row['payload'], 'summary_revision':int(row['revision']),
            'summary_stale':row['revision'] != row['current_revision'],
            **({'manual':True} if manual else {}), '_player_id':row['player_id'],
            'generated_at':row['generated_at'].isoformat()}


def counts():
    return {'games': 0, 'wins': 0, 'draws': 0, 'losses': 0}


def played_hours(seconds):
    """Match PostgreSQL ROUND(numeric, 2), including half-centihour ties."""
    return float((Decimal(str(seconds)) / Decimal(3600)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def add_result(bucket, result):
    bucket['games'] += 1
    if result in (1, 0.5, 0):
        bucket[{1: 'wins', 0.5: 'draws', 0: 'losses'}[result]] += 1


def last_key(row):
    # Same tie-break as existing daily rating SQL: black before white.
    return (row['played_at'], row['link'], row['color'] == 'black')


def summarize_day(rows, resolved):
    hours = defaultdict(lambda: {'games': 0, 'seconds': 0})
    activity, ratings, histogram, catalogs = {}, {}, defaultdict(int), {}
    latest = None
    for row in rows:
        mode = row['mode']
        hours[mode]['games'] += 1
        hours[mode]['seconds'] += float(row['time_elapsed'] or 0)
        stamp = row['played_at']
        if stamp:
            latest = max(latest, stamp) if latest else stamp
            local = resolved.local_datetime(stamp)
            key = (mode, local.date().isoformat(), local.isoweekday(), local.hour)
            add_result(activity.setdefault(key, counts()), row['result'])
            if mode in MODES:
                key = (mode, local.date().isoformat())
                candidate = {'mode': mode, 'date': key[1], 'rating': row['rating'],
                             'played_at': stamp.isoformat(), 'played_at_local': local.isoformat(),
                             'link': int(row['link']), 'color': row['color']}
                if key not in ratings or last_key(candidate) > last_key(ratings[key]):
                    ratings[key] = candidate
        if mode not in MODES:
            continue
        histogram[(mode, row['rating'])] += 1
        opening = re.sub(r'^https?://(www\.)?chess\.com/openings/', '',
                         str(row['eco'] or '').strip(), flags=re.I)
        opening = '' if opening == 'no_eco' else opening
        for kind, entry in (('opponents', row['opponent_name'] or ''), ('openings', opening)):
            key = (kind, mode, row['color'], entry)
            add_result(catalogs.setdefault(key, counts()), row['result'])
    return {
        'hours': [{'mode': mode, **value} for mode, value in hours.items()],
        'latest': latest.isoformat() if latest else None,
        'activity': [{'mode': key[0], 'date': key[1], 'weekday': key[2], 'hour': key[3], **value}
                     for key, value in activity.items()],
        'ratings': list(ratings.values()),
        'distribution': [{'mode': key[0], 'rating': key[1], 'games': value} for key, value in histogram.items()],
        'catalogs': [{'kind': key[0], 'mode': key[1], 'color': key[2], 'key': key[3], **value}
                     for key, value in catalogs.items()],
    }


def combine_partitions(partitions):
    hours = defaultdict(lambda: {'games': 0, 'seconds': 0})
    activity, daily, ratings, histogram, catalogs = {}, {}, {}, defaultdict(int), {}
    latest = None
    for payload in partitions:
        if payload['latest']:
            latest = max(latest, payload['latest']) if latest else payload['latest']
        for row in payload['hours']:
            for field in ('games', 'seconds'):
                hours[row['mode']][field] += row[field]
        for row in payload['activity']:
            for target, key in ((activity, (row['mode'], row['weekday'], row['hour'])),
                                (daily, (row['mode'], row['date'], row['weekday'], row['hour']))):
                bucket = target.setdefault(key, counts())
                for field in bucket:
                    bucket[field] += row[field]
        for row in payload['ratings']:
            key = row['mode'], row['date']
            if key not in ratings or last_key(row) > last_key(ratings[key]):
                ratings[key] = row
        for row in payload['distribution']:
            histogram[(row['mode'], row['rating'])] += row['games']
        for row in payload['catalogs']:
            key = row['kind'], row['mode'], row['color'], row['key']
            bucket = catalogs.setdefault(key, counts())
            for field in bucket:
                bucket[field] += row[field]
    return {
        'hours': {'modes': dict(hours), 'latest_played_at': latest},
        'activity': {'rows': [{'mode': key[0], 'weekday': key[1], 'hour': key[2], **value}
                              for key, value in activity.items()]},
        'activity_daily': {'rows': [{'mode': key[0], 'date': key[1], 'weekday': key[2], 'hour': key[3], **value}
                                    for key, value in daily.items()]},
        'ratings': {'rows': sorted(ratings.values(), key=lambda row: (row['mode'], row['date']))},
        'distribution': {'counts': [{'mode': key[0], 'rating': key[1], 'games': value}
                                    for key, value in histogram.items()]},
        **{kind: {'rows': [{'mode': key[1], 'color': key[2], 'key': key[3], **value}
                          for key, value in catalogs.items() if key[0] == kind]}
           for kind in ('opponents', 'openings')},
    }


async def get_latest_game(player, *, session_factory=None):
    async with read_transaction(session_factory or AsyncDBSession) as session:
        saved = await read_snapshot(session, player, 'hours')
        if saved is not None:
            return {'player_name':player, 'played_at':saved['latest_played_at'],
                    'summary_revision':saved['summary_revision'], 'summary_stale':saved['summary_stale']}
        stamp = await storage.latest_game(session, player)
        return {'player_name':player, 'played_at':stamp.isoformat() if stamp else None}


async def get_player_hours_played(player_name, *, session_factory=None):
    async with read_transaction(session_factory or AsyncDBSession) as session:
        saved = await read_snapshot(session, player_name, 'hours')
        payload = saved if saved is not None else await storage.live_hours(session, player_name)
    modes = payload['modes']
    return {'player_name':player_name,
        'total_hours':played_hours(sum(row['seconds'] for row in modes.values())),
        **{f'{mode}_games':modes.get(mode, {}).get('games',0) for mode in MODES},
        **{f'{mode}_hours':played_hours(modes.get(mode, {}).get('seconds',0)) for mode in MODES},
        **({key:saved[key] for key in ('summary_revision','summary_stale')} if saved is not None else {})}


async def refresh_player(player, *, batch_days=32, session_factory=None):
    if type(batch_days) is not int or batch_days<=0:
        raise ValueError('batch_days must be a positive integer')
    async with write_transaction(session_factory or AsyncDBSession) as session:
        await storage.begin_refresh(session)
        if await storage.manual_metadata(session, lock=True):
            return {'status':'awaiting_cuda'}
        profile = await storage.profile(session, player)
        if not profile:
            return {'status':'ready'}
        owner = profile['id']
        attempt = await storage.acquire_refresh(session, owner)
        if not attempt['acquired']:
            return {'status':'busy'}
        state = await storage.state(session, owner)
        if not state:
            if profile['joined'] and profile['joined']>0 and not profile['deleted_at']:
                raise RuntimeError('Required player analytics state is missing')
            return {'status':'ready'}
        if state['revision'] == state['published_revision']:
            return {'status':'ready'}
        if not profile['joined'] or profile['joined']<=0 or profile['deleted_at']:
            await storage.lock_source(session, owner)
            await storage.remove_ineligible(session, owner)
            return {'status':'ineligible'}
        rebuild = await storage.rebuild_days(session, owner) if state['full_rebuild'] else None
        dirty = rebuild[:batch_days] if rebuild is not None else await storage.dirty_days(session, owner, batch_days)
        resolved = resolve_player_timezone(dict(profile))
        calculated = []
        for day in dirty:
            rows = await storage.day_rows(session, owner, day)
            calculated.append((day, summarize_day(rows, resolved)))
        # Compute before taking producer locks. Both writers lock state before
        # dirty days; RR rejects a concurrent invalidation before any mutation.
        await storage.lock_source(session, owner)
        if rebuild is not None:
            await storage.queue_full_rebuild(session, owner, rebuild)
        for day, payload in calculated:
            await storage.replace_day(session, owner, day, payload)
        remaining = await storage.remaining_days(session, owner)
        if not remaining:
            sections = combine_partitions(await storage.partitions(session, owner))
            for payload in sections.values():
                payload['time_context'] = resolved.payload()
            await storage.publish(session, owner, state['revision'], sections)
        await storage.finish_batch(session, owner, state['revision'], not remaining, attempt['attempted_at'])
        return {'status':'building' if remaining else 'ready', 'days':len(dirty), 'remaining':remaining}


async def refresh_pending(*, session_factory=None):
    factory = session_factory or AsyncDBSession
    async with read_transaction(factory) as session:
        if await storage.manual_metadata(session):
            return None
        pending = await storage.next_pending(session)
    if not pending:
        return None
    player = pending['player_name']
    try:
        return {'player':player, **await refresh_player(player, session_factory=factory)}
    except Exception:
        LOG.exception('Player summaries retained; refresh will retry for %s', player)
        try:
            async with write_transaction(factory) as session:
                await storage.record_failure(session, pending)
        except Exception:
            LOG.exception('Could not record refresh failure for %s', player)
        return {'player':player, 'status':'retrying'}
