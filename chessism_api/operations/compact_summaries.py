"""Numeric-ID summary writes; legacy response names are supplied by read views."""
from sqlalchemy import text

from chessism_api.database.compact_storage import compact_for


async def refresh_mode_summary(session, players=None):
    layout = compact_for(session)
    if not layout:
        raise RuntimeError('Numeric summaries require compact storage')
    params = {'all_players':players is None, 'players':list(players or ())}
    summary, player, game, ownership, modes = (layout.table(name) for name in (
        'main_character_mode_summary','player','game','game_player','game_mode'))
    await session.execute(text(f'''DELETE FROM {summary} s WHERE :all_players OR s.player_id IN
        (SELECT id FROM {player} WHERE player_name=ANY(CAST(:players AS text[])))'''), params)
    rows = (await session.execute(text(f'''
        INSERT INTO {summary}(mode,player_id,n_games,rating,avg_game_rating,last_rating,
                              wins,draws,losses,as_white,as_black,last_game_at,refreshed_at)
        WITH selected AS MATERIALIZED (
            SELECT id FROM {player} WHERE joined<>0
              AND (:all_players OR player_name=ANY(CAST(:players AS text[])))
        ), source AS MATERIALIZED (
            SELECT gp.player_id,gp.is_white,g.played_at,m.name AS mode,
                   CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
                   CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result
            FROM selected p JOIN {ownership} gp ON gp.player_id=p.id
            JOIN {game} g ON g.link=gp.game_link JOIN {modes} m ON m.id=g.mode_id
            WHERE m.name IN ('bullet','blitz','rapid')
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
            ORDER BY mode,player_id,played_at DESC
        ) SELECT t.mode,t.player_id,t.n_games,l.rating,t.average,l.rating,
            t.wins,t.draws,t.losses,t.whites,t.blacks,l.played_at,CURRENT_TIMESTAMP
          FROM totals t JOIN latest l USING(mode,player_id) RETURNING mode
    '''), params)).mappings().all()
    counts = {key:0 for key in ('bullet','blitz','rapid')}
    for row in rows:
        counts[row['mode']] += 1
    return {**counts, 'total':len(rows)}
