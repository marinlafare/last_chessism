"""Canonical per-player mode projections shared by ingestion and explicit refresh."""
from .game_imports import INGESTION_COMMIT_LOCK


async def refresh_modes(connection, names=None):
    """Replace selected projections atomically; None means all, [] means none.

    Use the ingestion lock before reading to serialize with complete game imports.
    Callers need READ COMMITTED so a waiter reads the newly committed source.
    """
    if not connection.is_in_transaction():
        raise ValueError('Mode summary requires a caller-owned transaction')
    if names is not None and not names:
        return {'bullet':0,'blitz':0,'rapid':0,'total':0}
    await connection.execute('SELECT pg_advisory_xact_lock($1)', INGESTION_COMMIT_LOCK)
    await connection.execute('''DELETE FROM public.main_character_mode_summary s
        WHERE $1::text[] IS NULL OR s.player_id IN
            (SELECT id FROM public.player WHERE player_name=ANY($1::text[]))''', names)
    # Mode statistics retain player-perspective semantics: self-play has two
    # sides here. Month/global game totals count the game only once.
    rows = await connection.fetch('''
        INSERT INTO public.main_character_mode_summary
          (mode,player_id,n_games,rating,avg_game_rating,last_rating,
           wins,draws,losses,as_white,as_black,last_game_at)
        WITH source AS MATERIALIZED (
            SELECT gp.player_id,gp.is_white,g.link,g.played_at,m.name AS mode,
              CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
              CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result
            FROM public.player p JOIN public.game_player gp ON gp.player_id=p.id
            JOIN public.game g ON g.link=gp.game_link JOIN public.game_mode m ON m.id=g.mode_id
            WHERE ($1::text[] IS NULL OR p.player_name=ANY($1::text[])) AND p.joined<>0
              AND g.is_standard AND g.n_moves>0 AND m.name IN ('bullet','blitz','rapid')
        ), totals AS (
            SELECT mode,player_id,count(*)::int AS n_games,round(avg(rating))::int AS average,
              count(*) FILTER(WHERE result=1)::int AS wins,
              count(*) FILTER(WHERE result=0.5)::int AS draws,
              count(*) FILTER(WHERE result=0)::int AS losses,
              count(*) FILTER(WHERE is_white)::int AS whites,
              count(*) FILTER(WHERE NOT is_white)::int AS blacks
            FROM source GROUP BY mode,player_id
        ), latest AS (
            SELECT DISTINCT ON(mode,player_id) mode,player_id,rating,played_at FROM source
            ORDER BY mode,player_id,played_at DESC,link DESC,is_white DESC
        ) SELECT t.mode,t.player_id,t.n_games,l.rating,t.average,l.rating,
            t.wins,t.draws,t.losses,t.whites,t.blacks,l.played_at
          FROM totals t JOIN latest l USING(mode,player_id) RETURNING mode
    ''', names)
    counts = {mode:0 for mode in ('bullet','blitz','rapid')}
    for row in rows: counts[row['mode']] += 1
    return {**counts, 'total':len(rows)}
