"""Canonical whole-game research selection and verified evaluation projections.

The operation owns the read snapshot and limits. These queries never publish
models, repair coverage or infer missing evaluations.
"""
from chessism_api.evaluations import kind_sql


TRAINING_GAMES = '''WITH eligible AS (
    SELECT g.link,m.name AS mode,g.white_elo,g.black_elo,g.white_result,
        LEAST(8,GREATEST(0,FLOOR(((g.white_elo::bigint+g.black_elo)/2.0-1000)/200)))::int AS rating_band,
        c.total_positions,hashtextextended(g.link::text,:sample_seed) AS sample_order
    FROM game g JOIN game_mode m ON m.id=g.mode_id
    JOIN game_analysis_coverage c ON c.link=g.link
    WHERE g.is_standard AND g.n_moves>10 AND c.is_fully_analyzed AND c.total_positions>0
      AND m.name IN ('bullet','blitz','rapid')
      AND g.white_elo>0 AND g.black_elo>0 AND ABS(g.white_elo::bigint-g.black_elo)<=200
      AND g.white_result+g.black_result=1
      AND LOWER(g.white_str_result) NOT IN ('timeout','abandoned','timevsinsufficient')
      AND LOWER(g.black_str_result) NOT IN ('timeout','abandoned','timevsinsufficient')
      AND EXISTS(SELECT 1 FROM player p WHERE p.id IN(g.white_player_id,g.black_player_id)
          AND p.joined>0 AND p.deleted_at IS NULL)
), ranked AS (
    SELECT *,ROW_NUMBER() OVER(PARTITION BY rating_band,mode ORDER BY sample_order,link) AS rank
    FROM eligible
) SELECT link,mode,white_elo,black_elo,white_result,total_positions
  FROM ranked WHERE rank<=:games_per_group ORDER BY link'''


def eligible_games(*, owners=False):
    owner_columns = '''(w.joined>0 AND w.deleted_at IS NULL) AS white_complete,
        (b.joined>0 AND b.deleted_at IS NULL) AS black_complete,''' if owners else ''
    return f'''WITH eligible AS (
        SELECT {owner_columns}g.link,m.name AS mode,g.played_at,g.white_elo,g.black_elo,g.white_result,
            w.player_name AS white_name,b.player_name AS black_name,c.total_positions,
            LOWER(g.white_str_result) AS white_end,LOWER(g.black_str_result) AS black_end,
            LEAST(8,GREATEST(0,FLOOR(((g.white_elo::bigint+g.black_elo)/2.0-1000)/200)))::int AS rating_band,
            hashtextextended(g.link::text,20261028) AS sample_order
        FROM game g JOIN game_mode m ON m.id=g.mode_id
        JOIN player w ON w.id=g.white_player_id JOIN player b ON b.id=g.black_player_id
        JOIN game_analysis_coverage c ON c.link=g.link
        WHERE g.is_standard AND g.n_moves>0 AND c.is_fully_analyzed AND c.total_positions>0
          AND m.name IN ('bullet','blitz','rapid')
          AND g.white_elo BETWEEN 1 AND 4000 AND g.black_elo BETWEEN 1 AND 4000
          AND g.white_result+g.black_result=1
          AND ((w.joined>0 AND w.deleted_at IS NULL) OR (b.joined>0 AND b.deleted_at IS NULL))
    )'''


def v2_selection():
    return eligible_games() + ''', ranked AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY mode,rating_band ORDER BY sample_order,link) AS rank
        FROM eligible
    ) SELECT * FROM ranked WHERE rank<=:games_per_group ORDER BY played_at,link'''


PLY = '((a.n_move::bigint-1)*2+CASE WHEN a.is_white THEN 1 ELSE 2 END)'
VERIFIED_KIND = kind_sql('f.score::bigint', 'f.analysis_source', codes=True)
POSITION_SOURCE = '''FROM game_fen_association a LEFT JOIN fen f ON f.fen_id=a.fen_id'''


def positions(*, fen_ids=False):
    return f'''SELECT a.game_link,{PLY} AS ply,f.score::float8,
        {'a.fen_id,' if fen_ids else ''}{VERIFIED_KIND} AS kind
        {POSITION_SOURCE}
        WHERE a.game_link=ANY(CAST(:links AS bigint[]))
        ORDER BY a.game_link,ply LIMIT :row_limit'''


# Explicit int4 ply preserves the verified binary wire contract. An out-of-range
# occurrence fails instead of overflowing into a seemingly valid predecessor.
BLUNDER_COPY = f'''SELECT a.game_link::bigint,{PLY}::integer AS ply,
    COALESCE(f.score::float8,'NaN'::float8),({VERIFIED_KIND})::smallint
    {POSITION_SOURCE}
    WHERE a.game_link=ANY($1::bigint[]) ORDER BY a.game_link,ply LIMIT $2::bigint'''
