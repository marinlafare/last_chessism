"""Native table relationships shared by matrix query builders."""

GAME = '''public.game g
    JOIN public.player wp ON wp.id=g.white_player_id
    JOIN public.player bp ON bp.id=g.black_player_id
    LEFT JOIN public.game_mode gm ON gm.id=g.mode_id'''
PARTICIPANTS = '''public.game_player gp
    JOIN public.game g ON g.link=gp.game_link
    JOIN public.player p ON p.id=gp.player_id
    JOIN public.player opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
    LEFT JOIN public.game_mode gm ON gm.id=g.mode_id'''
COLOR = "CASE WHEN gp.is_white THEN 'white' ELSE 'black' END"
RATING = 'CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END'
OPPONENT_RATING = 'CASE WHEN gp.is_white THEN g.black_elo ELSE g.white_elo END'
RESULT = 'CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END'
