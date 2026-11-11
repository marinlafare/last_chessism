"""Canonical dashboard queries in an application-owned read transaction."""
from sqlalchemy import text

from chessism_api.player_titles import OFFICIAL_PLAYER_TITLES


def _require_transaction(session):
    if not session.in_transaction():
        raise ValueError('A caller-owned transaction is required')


async def database_totals(session):
    _require_transaction(session)
    return dict((await session.execute(text('''SELECT n_games_in_db,main_characters,
        secondary_characters,n_positions,scored_fens,bullet_games,blitz_games,rapid_games
        FROM public.database_summary WHERE id=1'''))).mappings().one())


async def pipeline_totals(session):
    _require_transaction(session)
    return dict((await session.execute(text('''SELECT parsed_games,analyzable_games,
        excluded_games,fen_extracted_games,tablebase_marked_games,refreshed_at
        FROM public.fen_pipeline_summary WHERE id=1'''))).mappings().one())


async def remaining_extraction(session):
    _require_transaction(session)
    return await session.scalar(text('''SELECT count(*) FROM public.game
        WHERE is_standard AND n_moves>0 AND NOT fens_done'''))


async def main_game_counts(session):
    _require_transaction(session)
    return (await session.execute(text('''SELECT m.name AS mode,count(*) AS total
        FROM public.game g JOIN public.game_mode m ON m.id=g.mode_id
        WHERE g.is_standard AND g.n_moves>0 AND m.name IN ('bullet','blitz','rapid')
          AND EXISTS (SELECT 1 FROM public.player p
                      WHERE p.id IN (g.white_player_id,g.black_player_id) AND p.joined<>0)
        GROUP BY m.name'''))).mappings().all()


async def ranked_players(session, mode, limit, title_status):
    _require_transaction(session)
    return (await session.execute(text('''SELECT p.player_name,s.n_games,s.rating,
        s.avg_game_rating,s.last_rating,s.wins,s.draws,s.losses,s.as_white,s.as_black,
        TO_CHAR(s.last_game_at,'YYYY-MON-DD') AS last_game_date,
        p.name AS full_name,p.title,p.avatar,p.url AS profile_url
        FROM public.main_character_mode_summary s JOIN public.player p ON p.id=s.player_id
        WHERE s.mode=:mode AND p.deleted_at IS NULL AND p.joined>0
          AND (:title_status='all'
            OR (:title_status='titled' AND UPPER(TRIM(COALESCE(p.title,'')))=ANY(CAST(:titles AS text[])))
            OR (:title_status='untitled' AND NOT (UPPER(TRIM(COALESCE(p.title,'')))=ANY(CAST(:titles AS text[])))))
        ORDER BY s.rating DESC NULLS LAST,s.n_games DESC,p.player_name ASC LIMIT :limit'''),
        {'mode':mode,'limit':limit,'title_status':title_status,'titles':list(OFFICIAL_PLAYER_TITLES)})).mappings().all()
