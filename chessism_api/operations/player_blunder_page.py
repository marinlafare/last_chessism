"""Player-scoped published CUDA counts, never a full canonical-engine join."""
from chessism_api.database.relational_storage import storage_for


def blunder_rows_sql(session, clause):
    layout = storage_for(session)
    if not layout:
        return None
    t = layout.table
    branches = []
    for side, flag in (('white', 'true'), ('black', 'false')):
        # OFFSET is a planner fence: select this owner's GPU rows BEFORE joining
        # engine summaries. The old compatibility view expanded all owners.
        branches.append(f"""SELECT b.game_link, '{side}'::text AS player_color,
            g.played_at, m.name AS mode, saved.game_efficiency AS accuracy,
            b.blunder_count
            FROM (
                SELECT game_link,{side}_count AS blunder_count
                FROM {t('canonical_blunder_assessment')}
                WHERE model_id=:model AND {side}_owner=:player AND {side}_complete
                  AND {clause.replace('blunder_count', side + '_count', 1)}
                OFFSET 0
            ) b
            JOIN {t('game_player')} gp ON gp.game_link=b.game_link AND gp.is_white={flag}
                AND gp.player_id=(SELECT id FROM {t('player')} WHERE player_name=:player)
            JOIN {t('game')} g ON g.link=b.game_link
            JOIN {t('game_mode')} m ON m.id=g.mode_id
            JOIN {t('game_analysis_coverage')} coverage ON coverage.link=b.game_link
            JOIN LATERAL (
                SELECT game_efficiency FROM {t('game_player_engine_summary')}
                WHERE game_player_id=gp.id AND analyzed_player_moves>0 OFFSET 0
            ) saved ON true
            WHERE m.name=ANY(CAST(:modes AS text[]))
              AND coverage.is_fully_analyzed AND coverage.total_positions>0""")
    return ' UNION ALL '.join(branches)
