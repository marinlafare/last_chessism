"""Canonical player metadata persistence; the operation owns its transaction."""
from datetime import datetime, time, timedelta, timezone
import json

from sqlalchemy import text


async def execute(session, sql, **params):
    if not session.in_transaction():
        raise ValueError('Player metadata requires a caller-owned transaction')
    return await session.execute(text(sql), params)


async def manual_metadata(session, *, lock=False):
    exists = await execute(session, "SELECT to_regclass('public.cuda_summary_control') IS NOT NULL")
    if not exists.scalar_one():
        return False
    result = await execute(session, '''SELECT manual AND
        coalesce((to_jsonb(c)->>'metadata')::boolean,false)
        FROM public.cuda_summary_control c WHERE id''' + (' FOR SHARE' if lock else ''))
    return bool(result.scalar())


async def snapshot(session, player, section):
    result = await execute(session, '''SELECT s.player_id,s.payload,s.revision,s.generated_at,
        state.revision AS current_revision FROM public.player_analytics_snapshot s
        JOIN public.player_analytics_state state USING(player_id)
        JOIN public.player p ON p.id=s.player_id
        WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL AND s.section=:section''',
        player=player, section=section)
    return result.mappings().first()


async def profile(session, player):
    result = await execute(session, '''SELECT id,player_name,joined,deleted_at,country,location,
        timezone,timezone_source FROM public.player WHERE player_name=:player''', player=player)
    return result.mappings().first()


async def begin_refresh(session):
    await execute(session, 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    await execute(session, "SET LOCAL TIME ZONE 'UTC'")
    await execute(session, "SET LOCAL statement_timeout='30s'")
    await execute(session, "SET LOCAL lock_timeout='1s'")
    await execute(session, 'SET LOCAL max_parallel_workers_per_gather=0')


async def acquire_refresh(session, player_id):
    result = await execute(session, '''SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))
        AS acquired,clock_timestamp() AS attempted_at''', key='player-analytics:'+str(player_id))
    return result.mappings().one()


async def state(session, player_id):
    result = await execute(session, 'SELECT * FROM public.player_analytics_state WHERE player_id=:id', id=player_id)
    return result.mappings().first()


async def remove_ineligible(session, player_id):
    for table in ('dirty', 'partition', 'snapshot'):
        await execute(session, f'DELETE FROM public.player_analytics_{table} WHERE player_id=:id', id=player_id)
    await execute(session, '''UPDATE public.player_analytics_state SET published_revision=revision,
        full_rebuild=true,attempted_at=clock_timestamp(),error=NULL WHERE player_id=:id''', id=player_id)


async def rebuild_days(session, player_id):
    # Include old days so moving/deleting the last game removes its old counts.
    result = await execute(session, '''SELECT day FROM (
            SELECT (g.played_at AT TIME ZONE 'UTC')::date AS day
            FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
            WHERE gp.player_id=:id AND g.is_standard AND g.n_moves>0
            UNION SELECT day FROM public.player_analytics_partition WHERE player_id=:id
            UNION SELECT day FROM public.player_analytics_dirty WHERE player_id=:id
        ) days ORDER BY day''', id=player_id)
    return result.scalars().all()


async def lock_source(session, player_id):
    result = await execute(session, '''SELECT player_id FROM public.player_analytics_state
        WHERE player_id=:id FOR UPDATE''', id=player_id)
    result.scalar_one()


async def queue_full_rebuild(session, player_id, days):
    await execute(session, '''INSERT INTO public.player_analytics_dirty(player_id,day)
        SELECT :id,day FROM unnest(CAST(:days AS date[])) day ON CONFLICT DO NOTHING''', id=player_id, days=days)
    await execute(session, 'UPDATE public.player_analytics_state SET full_rebuild=false WHERE player_id=:id', id=player_id)


async def dirty_days(session, player_id, limit):
    result = await execute(session, '''SELECT day FROM public.player_analytics_dirty
        WHERE player_id=:id ORDER BY day LIMIT :limit''', id=player_id, limit=limit)
    return result.scalars().all()


async def day_rows(session, player_id, day):
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    result = await execute(session, '''SELECT g.link,g.played_at,m.name AS mode,
        CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
        g.time_elapsed,g.eco,opponent.player_name AS opponent_name
        FROM public.game_player gp JOIN public.game g ON g.link=gp.game_link
        LEFT JOIN public.game_mode m ON m.id=g.mode_id
        JOIN public.player opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
        WHERE gp.player_id=:id AND g.is_standard AND g.n_moves>0
            AND g.played_at>=:start AND g.played_at<:end ORDER BY g.played_at,g.link,gp.is_white''',
        id=player_id, start=start, end=start+timedelta(days=1))
    return result.mappings().all()


async def replace_day(session, player_id, day, payload):
    await execute(session, '''INSERT INTO public.player_analytics_partition(player_id,day,payload)
        VALUES(:id,:day,CAST(:payload AS jsonb)) ON CONFLICT(player_id,day)
        DO UPDATE SET payload=EXCLUDED.payload''', id=player_id, day=day, payload=json.dumps(payload))
    await execute(session, 'DELETE FROM public.player_analytics_dirty WHERE player_id=:id AND day=:day', id=player_id, day=day)


async def remaining_days(session, player_id):
    result = await execute(session, 'SELECT count(*) FROM public.player_analytics_dirty WHERE player_id=:id', id=player_id)
    return result.scalar_one()


async def partitions(session, player_id):
    result = await execute(session, 'SELECT payload FROM public.player_analytics_partition WHERE player_id=:id ORDER BY day', id=player_id)
    return result.scalars().all()


async def publish(session, player_id, revision, sections):
    for section, payload in sections.items():
        await execute(session, '''INSERT INTO public.player_analytics_snapshot(player_id,section,revision,payload)
            VALUES(:id,:section,:revision,CAST(:payload AS jsonb)) ON CONFLICT(player_id,section)
            DO UPDATE SET revision=EXCLUDED.revision,payload=EXCLUDED.payload,generated_at=clock_timestamp()''',
            id=player_id, section=section, revision=revision, payload=json.dumps(payload))


async def finish_batch(session, player_id, revision, ready, attempted_at):
    # A concurrent source invalidation raises a serialization failure under RR;
    # every partition/snapshot write in this batch then rolls back with it.
    result = await execute(session, '''UPDATE public.player_analytics_state SET attempted_at=:attempted_at,error=NULL,
        published_revision=CASE WHEN :ready THEN :revision ELSE published_revision END
        WHERE player_id=:id RETURNING player_id''',
        id=player_id, revision=revision, ready=ready, attempted_at=attempted_at)
    result.scalar_one()


async def next_pending(session):
    result = await execute(session, '''SELECT p.player_name,s.player_id,s.revision,
        clock_timestamp() AS attempted_at FROM public.player_analytics_state s
        JOIN public.player p ON p.id=s.player_id WHERE revision<>published_revision
        AND (error IS NULL OR attempted_at<now()-interval '30 seconds')
        ORDER BY s.attempted_at NULLS FIRST,p.player_name LIMIT 1''')
    return result.mappings().first()


async def record_failure(session, pending):
    await execute(session, '''UPDATE public.player_analytics_state SET attempted_at=:at,
        error='Refresh failed; retained previous complete summary. Will retry.'
        WHERE player_id=:id AND revision=:revision AND revision<>published_revision
            AND (attempted_at IS NULL OR attempted_at<:at)''',
        id=pending['player_id'], revision=pending['revision'], at=pending['attempted_at'])


async def live_hours(session, player):
    result = await execute(session, '''SELECT m.name AS mode,count(*) AS games,
        coalesce(sum(g.time_elapsed),0) AS seconds,max(g.played_at) AS latest
        FROM public.player p JOIN public.game_player gp ON gp.player_id=p.id
        JOIN public.game g ON g.link=gp.game_link LEFT JOIN public.game_mode m ON m.id=g.mode_id
        WHERE p.player_name=:player AND g.is_standard AND g.n_moves>0 GROUP BY m.name''', player=player)
    rows = result.mappings().all()
    latest = max((row['latest'] for row in rows), default=None)
    return {'modes': {row['mode']: {'games':row['games'], 'seconds':row['seconds']} for row in rows},
            'latest_played_at':latest.isoformat() if latest else None}


async def latest_game(session, player):
    result = await execute(session, '''SELECT g.played_at FROM public.player p
        JOIN public.game_player gp ON gp.player_id=p.id JOIN public.game g ON g.link=gp.game_link
        WHERE p.player_name=:player AND g.is_standard AND g.n_moves>0
        ORDER BY g.played_at DESC LIMIT 1''', player=player)
    return result.scalar()
