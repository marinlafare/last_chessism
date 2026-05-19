\timing on

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_scored_n_games_desc
    ON fen (n_games DESC)
    WHERE score IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_scored_abs_score_n_games
    ON fen ((ABS(score)) DESC, n_games DESC)
    WHERE score IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_scored_impact_desc
    ON fen (((n_games::double precision * ABS(score))) DESC, n_games DESC)
    WHERE score IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_scored_score
    ON fen (score)
    WHERE score IS NOT NULL;

ANALYZE fen;
