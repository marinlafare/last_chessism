-- Canonical projections required by ingestion. Application operations own refreshes.
CREATE TABLE player_salience_summary (
    player_id bigint PRIMARY KEY REFERENCES player(id) ON DELETE CASCADE,
    status varchar(16) NOT NULL DEFAULT 'stale'
        CHECK (status IN ('stale','queued','running','ready','failed')),
    source_game_count bigint NOT NULL DEFAULT 0,
    source_position_count bigint NOT NULL DEFAULT 0,
    effective_game_count double precision NOT NULL DEFAULT 0,
    error text
);
CREATE INDEX ix_player_salience_status ON player_salience_summary(status,player_id);

-- Each game has exactly two mandatory sides. Ownership is derived through
-- game_player; storing a second player_id here could contradict that owner.
CREATE TABLE player_salience_pending_game (
    game_link bigint NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    is_white boolean NOT NULL,
    PRIMARY KEY (game_link,is_white)
);

CREATE TABLE main_character_mode_summary (
    mode varchar(16) NOT NULL,
    player_id bigint NOT NULL REFERENCES player(id) ON DELETE CASCADE,
    n_games integer NOT NULL DEFAULT 0,
    rating integer, avg_game_rating integer, last_rating integer,
    wins integer NOT NULL DEFAULT 0, draws integer NOT NULL DEFAULT 0,
    losses integer NOT NULL DEFAULT 0, as_white integer NOT NULL DEFAULT 0,
    as_black integer NOT NULL DEFAULT 0,
    last_game_at timestamptz, refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (mode,player_id)
);
CREATE INDEX ix_main_character_mode_rating ON main_character_mode_summary(mode,rating);
CREATE INDEX ix_main_character_mode_games ON main_character_mode_summary(mode,n_games);

CREATE TABLE game_analysis_coverage (
    link bigint PRIMARY KEY REFERENCES game(link) ON DELETE CASCADE,
    total_positions integer NOT NULL,
    analyzed_positions integer NOT NULL,
    unscored_positions integer NOT NULL,
    is_fully_analyzed boolean NOT NULL,
    equal_positions integer NOT NULL,
    small_positions integer NOT NULL,
    clear_positions integer NOT NULL,
    decisive_positions integer NOT NULL,
    mate_positions integer NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    CHECK (total_positions > 0 AND analyzed_positions >= 0 AND unscored_positions >= 0),
    CHECK (total_positions = analyzed_positions + unscored_positions),
    CHECK (is_fully_analyzed = (unscored_positions = 0))
);

CREATE TABLE database_summary (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    n_games_in_db bigint NOT NULL DEFAULT 0,
    main_characters bigint NOT NULL DEFAULT 0,
    secondary_characters bigint NOT NULL DEFAULT 0,
    n_positions bigint NOT NULL DEFAULT 0,
    analyzed_fens bigint NOT NULL DEFAULT 0,
    unscored_fens bigint NOT NULL DEFAULT 0,
    scored_fens bigint NOT NULL DEFAULT 0,
    nonzero_scored_fens bigint NOT NULL DEFAULT 0,
    bullet_games bigint NOT NULL DEFAULT 0,
    blitz_games bigint NOT NULL DEFAULT 0,
    rapid_games bigint NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE fen_pipeline_summary (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    parsed_games bigint NOT NULL DEFAULT 0,
    analyzable_games bigint NOT NULL DEFAULT 0,
    excluded_games bigint NOT NULL DEFAULT 0,
    fen_extracted_games bigint NOT NULL DEFAULT 0,
    tablebase_marked_games bigint NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    CHECK (parsed_games >= 0 AND fen_extracted_games >= 0 AND tablebase_marked_games >= 0),
    CHECK (fen_extracted_games <= parsed_games),
    CHECK (tablebase_marked_games <= fen_extracted_games)
);

-- Empty database operation has valid zero totals; startup never creates caches.
INSERT INTO database_summary DEFAULT VALUES;
INSERT INTO fen_pipeline_summary DEFAULT VALUES;

CREATE TABLE scored_position_summary (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    total_positions bigint NOT NULL DEFAULT 0,
    analyzed_fens bigint NOT NULL DEFAULT 0,
    scored_positions bigint NOT NULL DEFAULT 0,
    nonzero_scored_fens bigint NOT NULL DEFAULT 0,
    unscored_fens bigint NOT NULL DEFAULT 0,
    equal_positions bigint NOT NULL DEFAULT 0,
    small_positions bigint NOT NULL DEFAULT 0,
    clear_positions bigint NOT NULL DEFAULT 0,
    decisive_positions bigint NOT NULL DEFAULT 0,
    mate_positions bigint NOT NULL DEFAULT 0,
    equal_appearances bigint NOT NULL DEFAULT 0,
    small_appearances bigint NOT NULL DEFAULT 0,
    clear_appearances bigint NOT NULL DEFAULT 0,
    decisive_appearances bigint NOT NULL DEFAULT 0,
    mate_appearances bigint NOT NULL DEFAULT 0,
    equal_abs_score_sum double precision NOT NULL DEFAULT 0,
    small_abs_score_sum double precision NOT NULL DEFAULT 0,
    clear_abs_score_sum double precision NOT NULL DEFAULT 0,
    decisive_abs_score_sum double precision NOT NULL DEFAULT 0,
    mate_abs_score_sum double precision NOT NULL DEFAULT 0,
    white_better bigint NOT NULL DEFAULT 0,
    black_better bigint NOT NULL DEFAULT 0,
    balanced bigint NOT NULL DEFAULT 0,
    score_sum double precision NOT NULL DEFAULT 0,
    abs_score_sum double precision NOT NULL DEFAULT 0,
    wdl_win_sum double precision NOT NULL DEFAULT 0,
    wdl_draw_sum double precision NOT NULL DEFAULT 0,
    wdl_loss_sum double precision NOT NULL DEFAULT 0,
    wdl_positions bigint NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO scored_position_summary DEFAULT VALUES;
