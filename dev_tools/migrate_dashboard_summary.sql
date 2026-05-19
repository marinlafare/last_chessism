\timing on

CREATE TABLE IF NOT EXISTS database_summary (
    id INTEGER PRIMARY KEY DEFAULT 1,
    n_games_in_db BIGINT NOT NULL DEFAULT 0,
    main_characters BIGINT NOT NULL DEFAULT 0,
    secondary_characters BIGINT NOT NULL DEFAULT 0,
    n_positions BIGINT NOT NULL DEFAULT 0,
    analyzed_fens BIGINT NOT NULL DEFAULT 0,
    unscored_fens BIGINT NOT NULL DEFAULT 0,
    scored_fens BIGINT NOT NULL DEFAULT 0,
    nonzero_scored_fens BIGINT NOT NULL DEFAULT 0,
    bullet_games BIGINT NOT NULL DEFAULT 0,
    blitz_games BIGINT NOT NULL DEFAULT 0,
    rapid_games BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT database_summary_singleton CHECK (id = 1)
);

ALTER TABLE database_summary
    ADD COLUMN IF NOT EXISTS n_games_in_db BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS main_characters BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS secondary_characters BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS n_positions BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS analyzed_fens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unscored_fens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS scored_fens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS nonzero_scored_fens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS bullet_games BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS blitz_games BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rapid_games BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;

INSERT INTO database_summary (
    id,
    n_games_in_db,
    main_characters,
    secondary_characters,
    n_positions,
    analyzed_fens,
    unscored_fens,
    scored_fens,
    nonzero_scored_fens,
    bullet_games,
    blitz_games,
    rapid_games,
    refreshed_at
)
SELECT
    1,
    (SELECT COUNT(*)::bigint FROM game),
    (SELECT COUNT(*)::bigint FROM player WHERE joined IS NOT NULL AND joined <> 0),
    (SELECT COUNT(*)::bigint FROM player WHERE joined IS NULL OR joined = 0),
    (SELECT COUNT(*)::bigint FROM fen),
    (SELECT COUNT(*)::bigint FROM fen WHERE score IS NOT NULL),
    (SELECT COUNT(*)::bigint FROM fen WHERE score IS NULL),
    (SELECT COUNT(*)::bigint FROM fen WHERE score IS NOT NULL AND score <> 0),
    (SELECT COUNT(*)::bigint FROM fen WHERE score IS NOT NULL AND score <> 0),
    (SELECT COUNT(*)::bigint FROM game WHERE mode = 'bullet'),
    (SELECT COUNT(*)::bigint FROM game WHERE mode = 'blitz'),
    (SELECT COUNT(*)::bigint FROM game WHERE mode = 'rapid'),
    CURRENT_TIMESTAMP
ON CONFLICT (id) DO UPDATE SET
    n_games_in_db = EXCLUDED.n_games_in_db,
    main_characters = EXCLUDED.main_characters,
    secondary_characters = EXCLUDED.secondary_characters,
    n_positions = EXCLUDED.n_positions,
    analyzed_fens = EXCLUDED.analyzed_fens,
    unscored_fens = EXCLUDED.unscored_fens,
    scored_fens = EXCLUDED.scored_fens,
    nonzero_scored_fens = EXCLUDED.nonzero_scored_fens,
    bullet_games = EXCLUDED.bullet_games,
    blitz_games = EXCLUDED.blitz_games,
    rapid_games = EXCLUDED.rapid_games,
    refreshed_at = EXCLUDED.refreshed_at;

ANALYZE database_summary;
