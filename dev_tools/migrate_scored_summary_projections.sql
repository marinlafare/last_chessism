\timing on

CREATE TABLE IF NOT EXISTS scored_position_summary (
    id INTEGER PRIMARY KEY DEFAULT 1,
    total_positions BIGINT NOT NULL DEFAULT 0,
    analyzed_fens BIGINT NOT NULL DEFAULT 0,
    scored_positions BIGINT NOT NULL DEFAULT 0,
    nonzero_scored_fens BIGINT NOT NULL DEFAULT 0,
    unscored_fens BIGINT NOT NULL DEFAULT 0,
    equal_positions BIGINT NOT NULL DEFAULT 0,
    small_positions BIGINT NOT NULL DEFAULT 0,
    clear_positions BIGINT NOT NULL DEFAULT 0,
    decisive_positions BIGINT NOT NULL DEFAULT 0,
    mate_positions BIGINT NOT NULL DEFAULT 0,
    equal_appearances BIGINT NOT NULL DEFAULT 0,
    small_appearances BIGINT NOT NULL DEFAULT 0,
    clear_appearances BIGINT NOT NULL DEFAULT 0,
    decisive_appearances BIGINT NOT NULL DEFAULT 0,
    mate_appearances BIGINT NOT NULL DEFAULT 0,
    equal_abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    small_abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    clear_abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    decisive_abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    mate_abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    white_better BIGINT NOT NULL DEFAULT 0,
    black_better BIGINT NOT NULL DEFAULT 0,
    balanced BIGINT NOT NULL DEFAULT 0,
    score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    abs_score_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    wdl_win_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    wdl_draw_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    wdl_loss_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    wdl_positions BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT scored_position_summary_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS scored_rating_summary (
    id INTEGER PRIMARY KEY DEFAULT 1,
    rating_basis VARCHAR(32) NOT NULL DEFAULT 'avg_elo',
    source_full_games BIGINT NOT NULL DEFAULT 0,
    source_distinct_ratings BIGINT NOT NULL DEFAULT 0,
    groups_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    ratings_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT scored_rating_summary_singleton CHECK (id = 1)
);
