\timing on

ALTER TABLE game_analysis_summary
    ADD COLUMN IF NOT EXISTS equal_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS small_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS clear_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS decisive_positions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS mate_positions INTEGER NOT NULL DEFAULT 0;

WITH bucket_counts AS (
    SELECT
        gfa.game_link AS link,
        COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) < 50)::int AS equal_positions,
        COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 50 AND ABS(f.score) < 150)::int AS small_positions,
        COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 150 AND ABS(f.score) < 300)::int AS clear_positions,
        COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 300 AND ABS(f.score) < 9000)::int AS decisive_positions,
        COUNT(*) FILTER (WHERE f.score IS NOT NULL AND ABS(f.score) >= 9000)::int AS mate_positions
    FROM game_fen_association gfa
    JOIN fen f ON f.fen = gfa.fen_fen
    WHERE f.score IS NOT NULL
    GROUP BY gfa.game_link
)
UPDATE game_analysis_summary gas
SET
    equal_positions = bucket_counts.equal_positions,
    small_positions = bucket_counts.small_positions,
    clear_positions = bucket_counts.clear_positions,
    decisive_positions = bucket_counts.decisive_positions,
    mate_positions = bucket_counts.mate_positions
FROM bucket_counts
WHERE gas.link = bucket_counts.link;

ANALYZE game_analysis_summary;
