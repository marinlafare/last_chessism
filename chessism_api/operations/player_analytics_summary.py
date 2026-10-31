"""Incremental, durable summaries of game metadata (no engine/FEN scans).

Rebuild changed UTC-day partitions, then atomically publish all sections.
Retries replace partitions, never add a game twice. API reads never rebuild.
"""
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
import os
import re

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.player_analytics_schema import analytics_table
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.player_timezone import resolve_player_timezone

LOG = logging.getLogger(__name__)
MODES = ('bullet', 'blitz', 'rapid')
NULL_DAY = date(1, 1, 1)


def enabled():
    return os.environ.get('CHESSISM_PLAYER_SUMMARIES', '0') == '1'


async def read_snapshot(session, player, section):
    if not enabled():
        return None
    table = lambda name: analytics_table(storage_for(session), name)
    row = (await session.execute(text(f'''SELECT s.payload,s.revision,s.generated_at,
        state.revision AS current_revision FROM {table('snapshot')} s
        JOIN {table('state')} state USING(player_name)
        WHERE s.player_name=:player AND s.section=:section'''),
        {'player': player, 'section': section})).mappings().first()
    from .cuda_summaries.metadata_store import enabled as cuda_metadata
    manual=await cuda_metadata(session)
    if not row:
        if not manual:return None
        payload={'modes':{},'latest_played_at':None} if section=='hours' else {'counts':[]} if section=='distribution' else {'rows':[]}
        profile=(await session.execute(text('SELECT country,location,timezone,timezone_source FROM player WHERE player_name=:p'),{'p':player})).mappings().first()
        return {**payload,'time_context':resolve_player_timezone(dict(profile or {})).payload(),
            'summary_revision':0,'summary_stale':True,'summary_pending':True,'manual':True,'generated_at':None}
    return {**row['payload'], 'summary_revision': int(row['revision']),
            'summary_stale': row['revision'] != row['current_revision'],
            **({'manual':True} if manual else {}),
            'generated_at': row['generated_at'].isoformat()}


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


def source_rows_sql(session, *, dates_only=False):
    layout = storage_for(session)
    bound = '''(:all_days OR (:null_day AND played_at IS NULL) OR
        (NOT :null_day AND played_at>=:start AND played_at<:end))'''
    if not layout:
        return text(f'''SELECT {'DISTINCT played_at' if dates_only else '*'}
            FROM game_player WHERE player_name=:player AND {bound}''')
    branches = []
    for side, other, white in (('white', 'black', 'true'), ('black', 'white', 'false')):
        fields = 'g.played_at' if dates_only else f'''g.link,'{side}'::text AS color,g.played_at,m.name AS mode,
            g.{side}_elo AS rating,g.{side}_result AS result,g.time_elapsed,g.eco,
            opponent.player_name AS opponent_name'''
        joins = '' if dates_only else f'''LEFT JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
            LEFT JOIN {layout.table('player')} opponent ON opponent.id=g.{other}_player_id'''
        branches.append(f'''SELECT {fields} FROM {layout.table('game')} g {joins}
            WHERE g.{side}_player_id=(SELECT id FROM owner)
            AND {bound.replace('played_at', 'g.played_at')}
            AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp
                WHERE gp.game_link=g.link AND gp.player_id=(SELECT id FROM owner) AND gp.is_white={white})''')
    return text(f'''WITH owner AS (SELECT id FROM {layout.table('player')} WHERE player_name=:player)
        {' UNION ALL '.join(branches)}''')


def day_params(player, day=NULL_DAY, *, all_days=False):
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return {'player': player, 'all_days': all_days, 'null_day': day == NULL_DAY,
            'start': start, 'end': start + timedelta(days=1)}


async def get_latest_game(player):
    async with AsyncDBSession() as session:
        saved = await read_snapshot(session, player, 'hours')
        if saved is not None:
            return {'player_name': player, 'played_at': saved['latest_played_at'],
                    'summary_revision': saved['summary_revision'], 'summary_stale': saved['summary_stale']}
        from chessism_api.database.relational_reads import player_rows_sql
        stamp = await session.scalar(player_rows_sql(session, '''SELECT gp.played_at
            FROM game_player gp WHERE gp.player_name=:player AND gp.played_at IS NOT NULL
            ORDER BY gp.played_at DESC LIMIT 1''', ('played_at',)), {'player': player})
        return {'player_name': player, 'played_at': stamp.isoformat() if stamp else None}


async def refresh_player(player, *, batch_days=32, session_factory=None):
    factory = session_factory or AsyncDBSession
    async with factory() as session:
        from .cuda_summaries.metadata_store import enabled as cuda_metadata
        if await cuda_metadata(session):return {'status':'awaiting_cuda'}
        await session.rollback()
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
        from .cuda_summaries.schema import CONTROL
        if await session.scalar(text('SELECT to_regclass(:t) IS NOT NULL'),{'t':CONTROL}):
            manual=await session.scalar(text(f"SELECT COALESCE((to_jsonb(c)->>'metadata')::boolean,false) AND manual FROM {CONTROL} c WHERE id FOR SHARE"))
            if manual:return {'status':'awaiting_cuda'}
        await session.execute(text("SET LOCAL statement_timeout='30s'"))
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        if not await session.scalar(text('SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))'),
                                    {'key': 'player-analytics:' + player}):
            return {'status': 'busy'}
        table = lambda name: analytics_table(storage_for(session), name)
        state = (await session.execute(text(f'SELECT * FROM {table("state")} WHERE player_name=:player'),
                                       {'player': player})).mappings().first()
        if not state or state['revision'] == state['published_revision']:
            return {'status': 'ready'}
        profile = (await session.execute(text('''SELECT player_name,joined,deleted_at,country,location,
            timezone,timezone_source FROM player WHERE player_name=:player'''), {'player': player})).mappings().first()
        if not profile or not profile['joined'] or profile['joined'] <= 0 or profile['deleted_at']:
            for name in ('dirty', 'partition', 'snapshot'):
                await session.execute(text(f'DELETE FROM {table(name)} WHERE player_name=:player'), {'player': player})
            await session.execute(text(f'''UPDATE {table('state')} SET published_revision=revision,
                full_rebuild=true,attempted_at=clock_timestamp(),error=NULL WHERE player_name=:player'''), {'player': player})
            await session.commit()
            return {'status': 'ineligible'}
        if state['full_rebuild']:
            # Initial build / timezone change only. Ordinary ingestion skips
            # this history enumeration and reads only queued days.
            rows = (await session.execute(source_rows_sql(session, dates_only=True), day_params(player, all_days=True))).mappings()
            days = {row['played_at'].astimezone(timezone.utc).date() if row['played_at'] else NULL_DAY for row in rows}
            days.update((await session.execute(text(f'SELECT day FROM {table("partition")} WHERE player_name=:player'),
                                                {'player': player})).scalars())
            for day in sorted(days or {NULL_DAY}):
                await session.execute(text(f'''INSERT INTO {table('dirty')}(player_name,day)
                    VALUES(:player,:day) ON CONFLICT DO NOTHING'''), {'player': player, 'day': day})
            await session.execute(text(f'UPDATE {table("state")} SET full_rebuild=false WHERE player_name=:player'), {'player': player})
        dirty = (await session.execute(text(f'''SELECT day FROM {table('dirty')} WHERE player_name=:player
            ORDER BY day LIMIT :limit'''), {'player': player, 'limit': batch_days})).scalars().all()
        resolved = resolve_player_timezone(dict(profile))
        for day in dirty:
            rows = (await session.execute(source_rows_sql(session), day_params(player, day))).mappings().all()
            payload = summarize_day(rows, resolved)
            await session.execute(text(f'''INSERT INTO {table('partition')}(player_name,day,payload)
                VALUES(:player,:day,CAST(:payload AS jsonb)) ON CONFLICT(player_name,day)
                DO UPDATE SET payload=EXCLUDED.payload'''), {'player': player, 'day': day, 'payload': json.dumps(payload)})
            await session.execute(text(f'DELETE FROM {table("dirty")} WHERE player_name=:player AND day=:day'),
                                  {'player': player, 'day': day})
        remaining = await session.scalar(text(f'SELECT count(*) FROM {table("dirty")} WHERE player_name=:player'), {'player': player})
        if not remaining:
            partitions = (await session.execute(text(f'SELECT payload FROM {table("partition")} WHERE player_name=:player ORDER BY day'),
                                                {'player': player})).scalars()
            for section, payload in combine_partitions(partitions).items():
                payload['time_context'] = resolved.payload()
                await session.execute(text(f'''INSERT INTO {table('snapshot')}(player_name,section,revision,payload)
                    VALUES(:player,:section,:revision,CAST(:payload AS jsonb)) ON CONFLICT(player_name,section)
                    DO UPDATE SET revision=EXCLUDED.revision,payload=EXCLUDED.payload,generated_at=clock_timestamp()'''),
                    {'player': player, 'section': section, 'revision': state['revision'], 'payload': json.dumps(payload)})
        # Updating the source row under REPEATABLE READ detects a concurrent
        # invalidation (serialization failure); nothing stale is published.
        await session.execute(text(f'''UPDATE {table('state')} SET attempted_at=clock_timestamp(),error=NULL,
            published_revision=CASE WHEN :ready THEN :revision ELSE published_revision END WHERE player_name=:player'''),
            {'player': player, 'ready': not remaining, 'revision': state['revision']})
        await session.commit()
        return {'status': 'ready' if not remaining else 'building', 'days': len(dirty), 'remaining': remaining}


async def refresh_pending(*, session_factory=None):
    factory = session_factory or AsyncDBSession
    async with factory() as session:
        from .cuda_summaries.metadata_store import enabled as cuda_metadata
        if await cuda_metadata(session):return None
        table = analytics_table(storage_for(session), 'state')
        player = await session.scalar(text(f'''SELECT player_name FROM {table}
            WHERE revision<>published_revision AND (error IS NULL OR attempted_at<now()-interval '30 seconds')
            ORDER BY attempted_at NULLS FIRST,player_name LIMIT 1'''))
    if not player:
        return None
    try:
        return {'player': player, **await refresh_player(player, session_factory=factory)}
    except Exception:
        LOG.exception('Player summaries retained; refresh will retry for %s', player)
        async with factory.begin() as session:
            await session.execute(text(f'''UPDATE {table} SET attempted_at=clock_timestamp(),
                error='Refresh failed; retained previous complete summary. Will retry.' WHERE player_name=:player'''), {'player': player})
        return {'player': player, 'status': 'retrying'}
