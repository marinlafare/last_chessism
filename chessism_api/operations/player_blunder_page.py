"""Player-scoped published CUDA counts, never a full canonical-engine join."""


def blunder_rows_sql(session, clause):

    branches = []
    for side, flag in (('white', 'true'), ('black', 'false')):
        # OFFSET is a planner fence: select this owner's GPU rows BEFORE joining
        # engine summaries. The old compatibility view expanded all owners.
        branches.append(f"""SELECT b.game_link, '{side}'::text AS player_color,
            g.played_at, m.name AS mode, saved.game_efficiency AS accuracy,
            b.blunder_count
            FROM (
                SELECT game_link,{side}_count AS blunder_count
                FROM public.canonical_blunder_assessment
                WHERE model_id=:model AND {side}_owner=:player AND {side}_complete
                  AND {clause.replace('blunder_count', side + '_count', 1)}
                OFFSET 0
            ) b
            JOIN public.game_player gp ON gp.game_link=b.game_link AND gp.is_white={flag}
                AND gp.player_id=(SELECT id FROM public.player WHERE player_name=:player)
            JOIN public.game g ON g.link=b.game_link
            JOIN public.game_mode m ON m.id=g.mode_id
            JOIN public.game_analysis_coverage coverage ON coverage.link=b.game_link
            JOIN LATERAL (
                SELECT game_efficiency FROM public.game_player_engine_summary
                WHERE game_link=gp.game_link AND is_white=gp.is_white AND analyzed_player_moves>0 OFFSET 0
            ) saved ON true
            WHERE g.is_standard AND g.n_moves>0 AND m.name=ANY(CAST(:modes AS text[]))
              AND coverage.is_fully_analyzed AND coverage.total_positions>0""")
    return ' UNION ALL '.join(branches)
