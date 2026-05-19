\timing on

CREATE TABLE IF NOT EXISTS game_analysis_summary (
    link BIGINT PRIMARY KEY REFERENCES game(link) ON DELETE CASCADE,
    total_positions INTEGER NOT NULL DEFAULT 0,
    analyzed_positions INTEGER NOT NULL DEFAULT 0,
    unscored_positions INTEGER NOT NULL DEFAULT 0,
    is_fully_analyzed BOOLEAN NOT NULL DEFAULT FALSE,
    score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_score DOUBLE PRECISION,
    avg_abs_score DOUBLE PRECISION,
    max_abs_score DOUBLE PRECISION,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE game_analysis_summary
    ADD COLUMN IF NOT EXISTS total_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS analyzed_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unscored_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS is_fully_analyzed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS avg_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS avg_abs_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS max_abs_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;

INSERT INTO game_analysis_summary (
    link,
    total_positions,
    analyzed_positions,
    unscored_positions,
    is_fully_analyzed,
    score_sum,
    abs_score_sum,
    avg_score,
    avg_abs_score,
    max_abs_score,
    refreshed_at
)
SELECT
    gfa.game_link AS link,
    COUNT(*)::int AS total_positions,
    COUNT(f.score)::int AS analyzed_positions,
    (COUNT(*) - COUNT(f.score))::int AS unscored_positions,
    (COUNT(*) > 0 AND COUNT(*) = COUNT(f.score)) AS is_fully_analyzed,
    COALESCE(SUM(f.score) FILTER (WHERE f.score IS NOT NULL), 0)::double precision AS score_sum,
    COALESCE(SUM(ABS(f.score)) FILTER (WHERE f.score IS NOT NULL), 0)::double precision AS abs_score_sum,
    (AVG(f.score) FILTER (WHERE f.score IS NOT NULL))::double precision AS avg_score,
    (AVG(ABS(f.score)) FILTER (WHERE f.score IS NOT NULL))::double precision AS avg_abs_score,
    (MAX(ABS(f.score)) FILTER (WHERE f.score IS NOT NULL))::double precision AS max_abs_score,
    CURRENT_TIMESTAMP AS refreshed_at
FROM game_fen_association gfa
JOIN fen f ON f.fen = gfa.fen_fen
GROUP BY gfa.game_link
ON CONFLICT (link) DO UPDATE SET
    total_positions = EXCLUDED.total_positions,
    analyzed_positions = EXCLUDED.analyzed_positions,
    unscored_positions = EXCLUDED.unscored_positions,
    is_fully_analyzed = EXCLUDED.is_fully_analyzed,
    score_sum = EXCLUDED.score_sum,
    abs_score_sum = EXCLUDED.abs_score_sum,
    avg_score = EXCLUDED.avg_score,
    avg_abs_score = EXCLUDED.avg_abs_score,
    max_abs_score = EXCLUDED.max_abs_score,
    refreshed_at = EXCLUDED.refreshed_at;

DROP INDEX CONCURRENTLY IF EXISTS ix_game_analysis_summary_fully_positions;
DROP INDEX CONCURRENTLY IF EXISTS ix_game_analysis_summary_incomplete;
DROP INDEX CONCURRENTLY IF EXISTS ix_game_analysis_summary_all_positions;

CREATE INDEX CONCURRENTLY ix_game_analysis_summary_fully_positions
    ON game_analysis_summary (total_positions DESC, max_abs_score DESC NULLS LAST, link DESC)
    WHERE is_fully_analyzed = true AND total_positions > 0;

CREATE INDEX CONCURRENTLY ix_game_analysis_summary_incomplete
    ON game_analysis_summary (unscored_positions DESC, total_positions DESC, link DESC)
    WHERE is_fully_analyzed = false AND total_positions > 0;

CREATE INDEX CONCURRENTLY ix_game_analysis_summary_all_positions
    ON game_analysis_summary (is_fully_analyzed DESC, total_positions DESC, link DESC)
    WHERE total_positions > 0;

ANALYZE game_analysis_summary;
