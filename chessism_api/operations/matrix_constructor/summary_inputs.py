"""Research input contracts for summary algorithms, not saved summary values.

These two row units deliberately do not reuse game_position (mover-only).
Queries limit occurrence identities before loading evaluations. A blunder's
predecessor is the immediately preceding ply, never a previous sampled row.
"""

from datetime import date
from dataclasses import replace

from ..mate_distance import mate_distance_sql


def row_types(Column, RowType):
    def column(key, label, kind='number', description=''):
        return Column(key, label, f'input."{key}"', 'summary input',
                      data_type=kind, storage_dtype=None if kind == 'category' else 'float64',
                      description=description, default=True)

    common = (
        column('player', 'Player', 'category'),
        column('game', 'Game ID', 'category', 'Identity only; group/split by whole game.'),
        column('player_color', 'Player color', 'category'),
        column('mode', 'Game mode', 'category'),
        column('started_at', 'Game timestamp UTC'),
        column('ply', 'Half-move number'),
        column('player_score', 'Evaluation after the move', description='Player perspective; inspect score_kind before using as CP.'),
        column('score_kind', 'Evaluation type', 'category'),
        column('analysis_source', 'Evaluation source', 'category'),
        column('mate_moves', 'Mate distance', description='Known moves to mate; null means unavailable or not a mate score.'),
    )
    extra = (
        column('previous_player_score', 'Evaluation before the move'),
        column('previous_score_kind', 'Previous evaluation type', 'category'),
        column('player_rating', 'Player rating at game time'),
        column('opponent_rating', 'Opponent rating at game time'),
        column('initial_prior', 'Initial evaluation uses an assumed +15 CP',
               description='1 only for the first White move: the legacy before-evaluation is +15 CP (+0.15 pawns), not a stored engine evaluation. Otherwise 0.'),
        column('result', 'Final result (training label only)', description='1 / 0.5 / 0; never an inference feature.'),
    )
    return {key: RowType(
        key=key, label=label, description=description,
        sources=('game_player', 'game_fen_association', 'fen', 'game_analysis_summary'),
        row_key_expression="CONCAT(gp.link, ':', gp.color, ':', gfa.n_move, ':', gfa.move_color)",
        from_sql='game_player gp',
        order_sql="gp.played_at DESC NULLS LAST, gp.link, gp.color, gfa.n_move, gfa.move_color DESC",
        columns=tuple(replace(item, default=item.key in (
            {'player', 'mode', 'player_score', 'score_kind', 'mate_moves'} if key == 'score_distribution' else
            {'player', 'game', 'mode', 'player_score', 'score_kind', 'analysis_source',
             'previous_player_score', 'previous_score_kind', 'player_rating', 'opponent_rating', 'initial_prior'}
        )) for item in common + (extra if key == 'blunder_moves' else ())),
        player_filter_sql='gp.player_name = ANY(CAST(:players AS text[]))',
        mode_filter_sql='gp.mode = ANY(CAST(:modes AS text[]))', date_filter_sql='gp.played_at',
        min_moves_filter_sql='gp.n_moves >= :min_moves',
        analyzed_filter_sql='EXISTS (SELECT 1 FROM fen scored WHERE scored.fen_id=gfa.fen_id AND scored.score IS NOT NULL)',
    ) for key, label, description in (
        ('score_distribution', 'Score distribution occurrences',
         'Scored position occurrences for each player, after either color moves. No unscored rows; repeated positions remain repeated.'),
        ('blunder_moves', 'Blunder move transitions',
         'Own moves in fully analyzed games, with preceding evaluations and game-time ratings. No blunder model is applied.'),
    )}


def kind_sql(score, source):
    return f"CASE WHEN {score} IS NULL THEN 'unscored' WHEN {source}='tablebase' THEN 'tablebase' WHEN ABS({score})>=9000 THEN 'mate' ELSE 'cp' END"


def summary_input_sql(config, row_type, where_parts, params):
    params = {key: date.fromisoformat(value) if key in ('date_from', 'date_to') else value
              for key, value in params.items()}
    blunders = row_type.key == 'blunder_moves'
    conditions = list(where_parts)
    if not blunders and config.get('summary_input_version', 1) >= 2:
        # Apply eligibility BEFORE the limit/count; unscored positions must not
        # consume the requested sample or enter the CUDA matrix at all.
        eligibility = row_type.analyzed_filter_sql
        if eligibility not in conditions:
            conditions.append(eligibility)
    if blunders:
        conditions.extend([
            'gfa.move_color=gp.color',
            'EXISTS (SELECT 1 FROM game_analysis_summary coverage WHERE coverage.link=gp.link AND coverage.is_fully_analyzed AND coverage.total_positions>0)',
        ])
    where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
    base = 'FROM game_player gp JOIN game_fen_association gfa ON gfa.game_link=gp.link'
    count = f'SELECT COUNT(*)::bigint FROM (SELECT 1 {base} {where} LIMIT :count_limit) counted'
    sign = "CASE WHEN s.color='white' THEN 1 ELSE -1 END"
    score_kind = kind_sql('f.score', 'f.analysis_source')
    previous_kind = kind_sql('previous.score', 'previous.analysis_source')
    selected_keys = [*config['feature_columns'], *config['label_columns']]
    include_result = blunders and 'result' in selected_keys
    extra_projection = (',gp.rating,gp.opponent_rating' + (',gp.result' if include_result else '')) if blunders else ''
    extra = f""",
        CASE WHEN s.n_move=1 AND s.move_color='white' THEN 15.0
             ELSE ({sign}) * previous.score END AS previous_player_score,
        CASE WHEN s.n_move=1 AND s.move_color='white' THEN 'cp'
             ELSE {previous_kind} END AS previous_score_kind,
        NULLIF(s.rating,0)::float8 AS player_rating,
        NULLIF(s.opponent_rating,0)::float8 AS opponent_rating,
        CASE WHEN s.n_move=1 AND s.move_color='white' THEN 1.0 ELSE 0.0 END AS initial_prior""" if blunders else ''
    if include_result:
        extra += ', s.result::float8 AS result'
    predecessor = """LEFT JOIN game_fen_association before_move
        ON before_move.game_link=s.link
        AND before_move.n_move=CASE WHEN s.move_color='white' THEN s.n_move-1 ELSE s.n_move END
        AND before_move.move_color=CASE WHEN s.move_color='white' THEN 'black' ELSE 'white' END
        LEFT JOIN fen previous ON previous.fen_id=before_move.fen_id""" if blunders else ''
    selected = ', '.join(f'input."{key}"' for key in selected_keys)
    query = f"""WITH scope AS MATERIALIZED (
        SELECT gp.link,gp.player_name,gp.color,gp.mode,gp.played_at,
            gfa.n_move,gfa.move_color,gfa.fen_id {extra_projection}
        {base} {where} ORDER BY {row_type.order_sql} LIMIT :max_rows
    ), input AS (
        SELECT CONCAT(s.link,':',s.color,':',s.n_move,':',s.move_color) AS row_key,
            s.player_name AS player,s.link::text AS game,s.color AS player_color,s.mode,
            EXTRACT(EPOCH FROM s.played_at)::float8 AS started_at,
            ((s.n_move-1)*2+CASE WHEN s.move_color='white' THEN 1 ELSE 2 END)::float8 AS ply,
            ({sign}) * f.score AS player_score, {score_kind} AS score_kind,
            f.analysis_source,
            CASE WHEN {score_kind}='mate' THEN {mate_distance_sql('f.score', 'f.analysis_source')} END AS mate_moves
            {extra}
        FROM scope s LEFT JOIN fen f ON f.fen_id=s.fen_id {predecessor}
    ) SELECT input.row_key{', ' + selected if selected else ''} FROM input
      ORDER BY input.started_at DESC NULLS LAST,input.game::bigint,input.player_color,input.ply"""
    return count, query, params
