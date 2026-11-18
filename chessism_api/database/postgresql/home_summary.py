"""Canonical homepage snapshot SQL; callers own transactions and queue policy."""
import json

from sqlalchemy import text

from chessism_api.player_titles import OFFICIAL_PLAYER_TITLES


def require_transaction(session):
    if not session.in_transaction():
        raise ValueError('Homepage summaries require a caller-owned transaction')


async def read_snapshot(session):
    require_transaction(session)
    return (await session.execute(text('''
        SELECT m.total_fens,m.titled_players,m.titled_fens,m.titled_games,
            m.source_revision,m.refreshed_at,m.attempted_at,m.last_error,
            s.revision AS current_revision,
            coalesce((SELECT jsonb_agg(jsonb_build_object('player_name',r.player_name,
                'title',r.title,'games',r.games,'game_half_units',r.game_half_units,'avatar',p.avatar)
                ORDER BY r.game_half_units DESC,r.player_name)
                FROM jsonb_to_recordset(m.players) r(player_id bigint,player_name text,title text,
                    games bigint,game_half_units bigint)
                LEFT JOIN public.player p ON p.id=r.player_id AND p.deleted_at IS NULL),'[]'::jsonb) AS players
        FROM public.home_titled_summary m JOIN public.home_summary_source s USING(id) WHERE m.id=1
    '''))).mappings().one()


async def untitled_players(session):
    require_transaction(session)
    return (await session.execute(text('''
        SELECT p.player_name,p.avatar,sum(s.n_games) AS games
        FROM public.main_character_mode_summary s JOIN public.player p ON p.id=s.player_id
        WHERE p.deleted_at IS NULL
            AND NOT(upper(trim(coalesce(p.title,'')))=ANY(CAST(:titles AS text[])))
        GROUP BY p.player_name,p.avatar HAVING sum(s.n_games)>0 ORDER BY games DESC,p.player_name
    '''),{'titles':list(OFFICIAL_PLAYER_TITLES)})).mappings().all()


async def game_players(session):
    require_transaction(session)
    return (await session.execute(text('''
        SELECT p.player_name,upper(trim(coalesce(p.title,''))) AS title,p.avatar,counts.games
        FROM public.player p CROSS JOIN LATERAL (
            SELECT count(*) AS games FROM public.game_player gp
            JOIN public.game g ON g.link=gp.game_link
            WHERE gp.player_id=p.id AND g.is_standard AND g.n_moves>0
        ) counts
        WHERE p.deleted_at IS NULL AND p.joined>0 AND counts.games>0
        ORDER BY counts.games DESC,p.player_name
    '''))).mappings().all()


async def begin_refresh(session, lock_id):
    require_transaction(session)
    await session.execute(text("SET LOCAL statement_timeout='900s'"))
    await session.execute(text("SET LOCAL lock_timeout='5s'"))
    await session.execute(text("SET LOCAL work_mem='64MB'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=2'))
    row=(await session.execute(text('''SELECT pg_try_advisory_xact_lock(:key) AS acquired,
        clock_timestamp() AS attempted_at'''),{'key':lock_id})).mappings().one()
    return row['attempted_at'] if row['acquired'] else None


async def build_snapshot(session):
    """One statement snapshot for source revision, distinct unions and roster."""
    require_transaction(session)
    return dict((await session.execute(text('''
        WITH titles AS MATERIALIZED (
            SELECT id,player_name,upper(trim(title)) AS title FROM public.player
            WHERE deleted_at IS NULL AND upper(trim(title))=ANY(CAST(:titles AS text[]))
        ), games AS MATERIALIZED (
            SELECT g.link,w.id AS white,b.id AS black FROM public.game g
            LEFT JOIN titles w ON w.id=g.white_player_id LEFT JOIN titles b ON b.id=g.black_player_id
            WHERE g.is_standard AND g.n_moves>0 AND (w.id IS NOT NULL OR b.id IS NOT NULL)
        ), credits AS (
            SELECT white AS player_id,CASE WHEN black IS NULL THEN 2 ELSE 1 END AS units
            FROM games WHERE white IS NOT NULL
            UNION ALL
            SELECT black,CASE WHEN white IS NULL THEN 2 ELSE 1 END FROM games WHERE black IS NOT NULL
        ), roster AS (
            SELECT p.id AS player_id,p.player_name,p.title,count(c.player_id) AS games,
                coalesce(sum(c.units),0) AS game_half_units
            FROM titles p LEFT JOIN credits c ON c.player_id=p.id GROUP BY p.id,p.player_name,p.title
        )
        SELECT s.revision AS source_revision,(SELECT count(*) FROM public.fen) AS total_fens,
            (SELECT count(*) FROM titles) AS titled_players,(SELECT count(*) FROM games) AS titled_games,
            (SELECT count(DISTINCT a.fen_id) FROM public.game_fen_association a
             JOIN games g ON g.link=a.game_link) AS titled_fens,
            coalesce((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.game_half_units DESC,r.player_name)
                      FROM roster r),'[]'::jsonb) AS players
        FROM public.home_summary_source s WHERE s.id=1
    '''),{'titles':list(OFFICIAL_PLAYER_TITLES)})).mappings().one())


async def publish(session, snapshot, attempted_at):
    require_transaction(session)
    result=await session.execute(text('''UPDATE public.home_titled_summary SET
        total_fens=:total_fens,titled_players=:titled_players,titled_games=:titled_games,
        titled_fens=:titled_fens,source_revision=:source_revision,players=CAST(:players AS jsonb),
        refreshed_at=clock_timestamp(),attempted_at=:attempted_at,last_error=NULL
        WHERE id=1 RETURNING id'''),{**snapshot,'players':json.dumps(snapshot['players']),
                                    'attempted_at':attempted_at})
    result.scalar_one()


async def record_failure(session, attempted_at):
    require_transaction(session)
    await session.execute(text('''UPDATE public.home_titled_summary SET attempted_at=:attempted_at,
        last_error='Summary refresh failed; the previous snapshot is retained.'
        WHERE id=1 AND (attempted_at IS NULL OR attempted_at<:attempted_at)'''),
        {'attempted_at':attempted_at})
