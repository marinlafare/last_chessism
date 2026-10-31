"""Indexed ownership/date selection before saved engine-summary lookups."""
from chessism_api.database.relational_storage import storage_for


def normalized_candidates(session, *, date_window, selection_sql, minimum_moves_filter):
    layout = storage_for(session)
    if layout is None:
        return None
    t = layout.table
    branches = []
    for side, other, flag in [('white', 'black', 'true'), ('black', 'white', 'false')]:
        branches.append(f'''SELECT g.link, {flag} AS is_white, g.played_at, g.n_moves,
            g.{side}_result AS result, g.{side}_elo AS rating, g.{other}_elo AS opponent_rating,
            g.{other}_player_id AS opponent_id, g.mode_id
            FROM {t('game')} g WHERE g.{side}_player_id=(SELECT id FROM owner)
            {date_window.replace('gp.', 'g.')}''')
    return f'''WITH owner AS MATERIALIZED (
        SELECT id,player_name FROM {t('player')} WHERE player_name=:player
    ), owned_games AS MATERIALIZED ({' UNION ALL '.join(branches)}),
    selected_games AS MATERIALIZED (
        SELECT gp.* FROM (
            SELECT g.*, member.id AS game_player_id, owner.player_name,
                CASE WHEN g.is_white THEN 'white' ELSE 'black' END AS color,
                mode.name AS mode, opponent.player_name AS opponent_name
            FROM owned_games g CROSS JOIN owner
            JOIN {t('game_player')} member ON member.game_link=g.link
                AND member.is_white=g.is_white AND member.player_id=owner.id
            JOIN {t('game_mode')} mode ON mode.id=g.mode_id
            LEFT JOIN {t('player')} opponent ON opponent.id=g.opponent_id
        ) gp WHERE gp.mode=ANY(CAST(:modes AS text[])) AND gp.played_at IS NOT NULL
            {minimum_moves_filter} AND {selection_sql}
    ) '''


def engine_lookup(session):
    layout = storage_for(session)
    if layout:
        return f'''LEFT JOIN LATERAL (
            SELECT saved.analyzed_player_moves, saved.own_move_cp_gain,
                saved.own_move_cp_loss, saved.game_efficiency, saved.mean_win_percent_loss,
                saved.median_win_percent_loss, saved.final_player_cp, saved.end_by,
                CASE WHEN gp.is_white AND b.white_complete THEN b.white_count
                     WHEN NOT gp.is_white AND b.black_complete THEN b.black_count END AS blunder_count
            FROM {layout.table('game_player_engine_summary')} saved
            LEFT JOIN LATERAL (
                SELECT white_complete,black_complete,white_count,black_count
                FROM {layout.table('canonical_blunder_assessment')} assessment
                WHERE assessment.game_link=gp.link AND assessment.model_id=(
                    SELECT active_model FROM {layout.table('canonical_blunder_control')} WHERE id)
                OFFSET 0
            ) b ON true
            WHERE saved.game_player_id=gp.game_player_id OFFSET 0
        ) engine ON true'''
    return '''LEFT JOIN LATERAL (
        SELECT saved.* FROM game_player_engine_summary saved
        WHERE saved.game_link=gp.link AND saved.player_color=gp.color
          AND saved.player_name=:player OFFSET 0
    ) engine ON true'''
