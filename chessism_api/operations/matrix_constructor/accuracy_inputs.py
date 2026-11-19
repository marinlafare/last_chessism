"""Reviewable ordered accuracy inputs, not calculated accuracy or a CUDA job."""
from datetime import date

from chessism_api.evaluations import kind_sql

OWNER = """EXISTS (SELECT 1 FROM public.game_player gp JOIN public.player p ON p.id=gp.player_id
    WHERE gp.game_link=g.link AND p.joined>0 AND p.deleted_at IS NULL {selection})"""


def row_type(Column, RowType):
    return RowType(
        key='accuracy_positions', label='Accuracy — ordered game evaluations',
        description=('One row per move, both colors, White-perspective evaluations. Each fully scored '
                     'game appears once. Whole-game selection; repeated positions retained. No accuracy is calculated.'),
        sources=('game', 'game_player', 'player', 'game_analysis_summary', 'game_fen_association', 'fen'),
        row_key_expression="CONCAT(g.link, ':', ply)",
        from_sql='public.game g LEFT JOIN public.game_mode gm ON gm.id=g.mode_id JOIN public.game_analysis_coverage coverage ON coverage.link=g.link',
        order_sql='g.played_at DESC NULLS LAST, g.link, ply',
        columns=(
            Column('game', 'Game', 'input.game', 'game.link', data_type='category', default=True,
                   label_allowed=False, description='Structural game identity, not a predictive feature. One shared sequence for both players.'),
            Column('ply', 'Ply', 'input.ply', 'game_fen_association', storage_dtype='int32', default=True,
                   label_allowed=False, description='Structural half-move order: 1 = first White move, 2 = first Black move. No gaps.'),
            Column('score', 'Score · White perspective', 'input.score', 'fen.score', storage_dtype='float64', default=True,
                   description='Stored evaluation after the move; positive favors White. Inspect score_kind before interpreting as CP.'),
            Column('score_kind', 'Evaluation type', 'input.score_kind', 'fen', data_type='category', default=True,
                   description='cp, mate, tablebase or unknown. Unknown provenance is retained, not invented.'),
        ),
        player_filter_sql=OWNER.format(selection='AND p.player_name=ANY(CAST(:players AS text[]))'),
        mode_filter_sql='gm.name=ANY(CAST(:modes AS text[]))', date_filter_sql='g.played_at',
        min_moves_filter_sql='g.n_moves>=:min_moves', analyzed_filter_sql='coverage.is_fully_analyzed',
    )


def accuracy_sql(config, row_type, where_parts, params, *, preview_rows=None):
    params = {key: date.fromisoformat(value) if key in ('date_from', 'date_to') and isinstance(value,str) else value
              for key, value in params.items()}
    cap = config['filters']['max_rows']
    # Preview bounds candidates separately from the whole-game output budget.
    params['candidate_games'] = min(preview_rows, cap) if preview_rows and cap else preview_rows or cap
    params['preview_rows'] = preview_rows
    conditions = [*where_parts, 'coverage.is_fully_analyzed', 'coverage.total_positions>0',
                  "gm.name IN ('bullet','blitz','rapid')", OWNER.format(selection='')]
    common = f'''WITH candidates AS MATERIALIZED (
        SELECT g.link,g.played_at,coverage.total_positions
        FROM {row_type.from_sql} WHERE {' AND '.join(conditions)}
        ORDER BY g.played_at DESC NULLS LAST,g.link LIMIT :candidate_games
    ), budgeted AS (
        SELECT *,sum(total_positions) OVER(ORDER BY played_at DESC NULLS LAST,link
            ROWS UNBOUNDED PRECEDING) AS end_row FROM candidates
    ), selected_games AS MATERIALIZED (
        SELECT * FROM budgeted WHERE CAST(:max_rows AS bigint) IS NULL OR end_row<=:max_rows
    ), positions AS MATERIALIZED (
        SELECT g.link,g.played_at,g.total_positions,
            ((a.n_move-1)*2+CASE WHEN a.is_white THEN 1 ELSE 2 END)::int AS ply,
            CASE WHEN a.is_white THEN 'white' ELSE 'black' END AS move_color,f.score::float8 AS score,{kind_sql('f.score::bigint', 'f.analysis_source')} AS score_kind
        FROM selected_games g JOIN public.game_fen_association a ON a.game_link=g.link
        LEFT JOIN public.fen f ON f.fen_id=a.fen_id
    ), valid_games AS (
        SELECT link FROM positions GROUP BY link HAVING count(*)=max(total_positions)
          AND count(score)=count(*) AND count(DISTINCT ply)=count(*)
          AND min(ply)=1 AND max(ply)=count(*)
          AND bool_and(COALESCE(move_color IN ('white','black'),false))
          AND bool_and(score NOT IN ('NaN'::float8,'Infinity'::float8,'-Infinity'::float8))
    ), input AS (
        SELECT CONCAT(p.link,':',p.ply) AS row_key,p.link::text AS game,p.ply,p.score,p.score_kind,p.played_at
        FROM positions p JOIN valid_games v USING(link)
    )'''
    selected = [*config['feature_columns'], *config['label_columns']]
    projection = ''.join(f', input."{key}"' for key in selected)
    query = common + f''' SELECT input.row_key{projection} FROM input
        ORDER BY input.played_at DESC NULLS LAST,input.game::bigint,input.ply LIMIT :preview_rows'''
    count = common + ' SELECT count(*)::bigint FROM (SELECT 1 FROM input LIMIT :count_limit) counted'
    return count, query, params
