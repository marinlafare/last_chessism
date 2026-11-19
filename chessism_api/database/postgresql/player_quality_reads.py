"""Player-scoped native source for engine quality and position drill-downs.

This projection is bounded to the requested owner. Both self-play sides remain
separate, and metadata-only variant/empty games do not enter analysis charts.
"""

SIDES = '''WITH quality_games AS NOT MATERIALIZED (
    SELECT g.link,g.played_at,g.n_moves,g.eco,gp.is_white,gp.player_id,p.player_name,
        opponent.player_name AS opponent_name,m.name AS mode,
        CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
        CASE WHEN gp.is_white THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result
    FROM public.player p JOIN public.game_player gp ON gp.player_id=p.id
    JOIN public.game g ON g.link=gp.game_link
    JOIN public.player opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
    LEFT JOIN public.game_mode m ON m.id=g.mode_id
    WHERE p.player_name=:player AND g.is_standard AND g.n_moves>0
)'''
