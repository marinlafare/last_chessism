"""Canonical public profiles and remote statistics; caller owns the transaction."""
from sqlalchemy import text


PROFILE_FIELDS = ('player_name', 'name', 'url', 'title', 'avatar', 'followers', 'country',
                  'location', 'timezone', 'timezone_source', 'joined', 'status',
                  'is_streamer', 'twitch_url', 'verified', 'league')
PROFILE_COLUMNS = ','.join((*PROFILE_FIELDS, 'deleted_at'))
STATS_FIELDS = tuple(f'chess_{mode}_{field}' for mode in ('rapid', 'blitz', 'bullet')
                    for field in ('last_rating', 'best_rating', 'games', 'wins', 'losses', 'draws', 'last_percentile')) + (
                        'fide', 'puzzle_rush_best_score', 'tactics_highest_rating', 'tactics_lowest_rating')


async def execute(session, sql, **params):
    if not session.in_transaction():
        raise ValueError('Player persistence requires a caller-owned transaction')
    return await session.execute(text(sql), params)


async def read_profile(session, player):
    row = (await execute(session, f'SELECT {PROFILE_COLUMNS} FROM public.player WHERE player_name=:player',
                         player=player)).mappings().first()
    return dict(row) if row is not None else None


async def primary_profiles(session):
    rows = (await execute(session, f'''SELECT {PROFILE_COLUMNS} FROM public.player
        WHERE joined>0 AND deleted_at IS NULL ORDER BY player_name''')).mappings()
    return [dict(row) for row in rows]


async def ensure_shell(session, player):
    await execute(session, '''INSERT INTO public.player(player_name,joined) VALUES(:player,0)
        ON CONFLICT(player_name) DO NOTHING''', player=player)
    row = await read_profile(session, player)
    if row is None:
        raise LookupError('Player was removed while creating its profile')
    return row


async def save_profile(session, profile):
    values = {key: profile[key] for key in PROFILE_FIELDS}
    # Use the current stored timezone, including edits made while the remote
    # request was in flight. A successful explicit refresh restores the profile.
    updates = [f'{key}=EXCLUDED.{key}' for key in PROFILE_FIELDS
               if key not in ('player_name', 'timezone', 'timezone_source')]
    updates += ["timezone=COALESCE(NULLIF(EXCLUDED.timezone,''),player.timezone)",
                "timezone_source=CASE WHEN NULLIF(EXCLUDED.timezone,'') IS NULL THEN player.timezone_source ELSE EXCLUDED.timezone_source END",
                'deleted_at=NULL']
    row = (await execute(session, f'''INSERT INTO public.player({','.join(PROFILE_FIELDS)})
        VALUES({','.join(':'+key for key in PROFILE_FIELDS)})
        ON CONFLICT(player_name) DO UPDATE SET {','.join(updates)}
        RETURNING {PROFILE_COLUMNS}''', **values)).mappings().one()
    return dict(row)


async def read_stats(session, player):
    row = (await execute(session, f'''SELECT p.player_name,s.last_updated,{','.join('s.'+k for k in STATS_FIELDS)}
        FROM public.player_stats s JOIN public.player p ON p.id=s.player_id
        WHERE p.player_name=:player''', player=player)).mappings().first()
    return dict(row) if row is not None else None


async def save_stats(session, stats):
    owner = (await execute(session, '''SELECT id FROM public.player
        WHERE player_name=:player FOR KEY SHARE''', player=stats['player_name'])).scalar_one_or_none()
    if owner is None:
        raise LookupError('Stats owner no longer exists')
    values = {key: stats[key] for key in STATS_FIELDS}
    row = (await execute(session, f'''INSERT INTO public.player_stats(player_id,{','.join(STATS_FIELDS)})
        VALUES(:owner,{','.join(':'+key for key in STATS_FIELDS)})
        ON CONFLICT(player_id) DO UPDATE SET {','.join(k+'=EXCLUDED.'+k for k in STATS_FIELDS)},
            last_updated=clock_timestamp()
        RETURNING last_updated,{','.join(STATS_FIELDS)}''', owner=owner, **values)).mappings().one()
    return {'player_name': stats['player_name'], **dict(row)}
