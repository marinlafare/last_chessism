"""Game-import projections, written within the same caller-owned transaction."""
from .game_coverage import refresh_pipeline_summary
from .mode_summary import refresh_modes
from .summary_writes import refresh_game_counts


async def refresh_import_summaries(connection, player_names, new_analyzable_links):
    if not connection.is_in_transaction():
        raise ValueError('Import summaries require a caller-owned transaction')
    for table in ('database_summary','fen_pipeline_summary'):
        if await connection.fetchval(f'SELECT id FROM public.{table} WHERE id=1 FOR UPDATE') != 1:
            raise RuntimeError('Required import summaries are missing; initialization or repair is needed')
    names = sorted(set(player_names))
    await _mark_pending(connection, new_analyzable_links)
    await refresh_modes(connection, names)
    await refresh_game_counts(connection)
    await refresh_pipeline_summary(connection)


async def _mark_pending(connection, links):
    if not links:
        return
    await connection.execute('''
        INSERT INTO public.player_salience_summary(player_id,status)
        SELECT DISTINCT p.id,'stale' FROM public.game_player gp
        JOIN public.player p ON p.id=gp.player_id
        JOIN public.game g ON g.link=gp.game_link
        WHERE gp.game_link=ANY($1::bigint[]) AND g.is_standard AND g.n_moves>0
          AND p.joined>0 AND p.deleted_at IS NULL
        ORDER BY p.id ON CONFLICT(player_id) DO UPDATE SET status='stale',error=NULL
    ''', list(links))
    await connection.execute('''
        INSERT INTO public.player_salience_pending_game(game_link,is_white)
        SELECT gp.game_link,gp.is_white FROM public.game_player gp
        JOIN public.player p ON p.id=gp.player_id
        JOIN public.game g ON g.link=gp.game_link
        WHERE gp.game_link=ANY($1::bigint[]) AND g.is_standard AND g.n_moves>0
          AND p.joined>0 AND p.deleted_at IS NULL ORDER BY gp.game_link,gp.is_white
        ON CONFLICT(game_link,is_white) DO NOTHING
    ''', list(links))
