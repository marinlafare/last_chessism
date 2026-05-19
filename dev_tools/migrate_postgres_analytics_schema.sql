\timing on

ALTER TABLE game
    ADD COLUMN IF NOT EXISTS mode VARCHAR(16),
    ADD COLUMN IF NOT EXISTS played_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS avg_elo DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS game_player (
    link BIGINT NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    color VARCHAR(5) NOT NULL,
    player_name VARCHAR NOT NULL REFERENCES player(player_name),
    opponent_name VARCHAR NOT NULL REFERENCES player(player_name),
    result DOUBLE PRECISION NOT NULL,
    rating INTEGER NOT NULL,
    opponent_rating INTEGER NOT NULL,
    mode VARCHAR(16),
    played_at TIMESTAMPTZ,
    eco VARCHAR NOT NULL,
    n_moves INTEGER NOT NULL,
    time_elapsed DOUBLE PRECISION NOT NULL,
    avg_elo DOUBLE PRECISION,
    PRIMARY KEY (link, color)
);

CREATE TABLE IF NOT EXISTS game_opening (
    link BIGINT NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    n_moves INTEGER NOT NULL,
    opening VARCHAR NOT NULL,
    mode VARCHAR(16),
    avg_elo DOUBLE PRECISION,
    played_at TIMESTAMPTZ,
    PRIMARY KEY (link, n_moves)
);

UPDATE game
SET
    mode = CASE
        WHEN time_control LIKE '%/%' THEN 'daily'
        WHEN split_part(time_control, '+', 1) ~ '^[0-9]+$' THEN
            CASE
                WHEN CAST(split_part(time_control, '+', 1) AS INTEGER) < 180 THEN 'bullet'
                WHEN CAST(split_part(time_control, '+', 1) AS INTEGER) < 600 THEN 'blitz'
                WHEN CAST(split_part(time_control, '+', 1) AS INTEGER) <= 1800 THEN 'rapid'
                ELSE 'classical'
            END
        ELSE 'unknown'
    END,
    played_at = make_timestamp(
        year::int,
        month::int,
        day::int,
        hour::int,
        minute::int,
        second::double precision
    ) AT TIME ZONE 'UTC',
    avg_elo = (white_elo + black_elo) / 2.0
WHERE mode IS NULL
   OR played_at IS NULL
   OR avg_elo IS NULL;

INSERT INTO game_player (
    link,
    color,
    player_name,
    opponent_name,
    result,
    rating,
    opponent_rating,
    mode,
    played_at,
    eco,
    n_moves,
    time_elapsed,
    avg_elo
)
SELECT
    link,
    'white',
    white,
    black,
    white_result,
    white_elo,
    black_elo,
    mode,
    played_at,
    eco,
    n_moves,
    time_elapsed,
    avg_elo
FROM game
UNION ALL
SELECT
    link,
    'black',
    black,
    white,
    black_result,
    black_elo,
    white_elo,
    mode,
    played_at,
    eco,
    n_moves,
    time_elapsed,
    avg_elo
FROM game
ON CONFLICT (link, color) DO UPDATE SET
    player_name = EXCLUDED.player_name,
    opponent_name = EXCLUDED.opponent_name,
    result = EXCLUDED.result,
    rating = EXCLUDED.rating,
    opponent_rating = EXCLUDED.opponent_rating,
    mode = EXCLUDED.mode,
    played_at = EXCLUDED.played_at,
    eco = EXCLUDED.eco,
    n_moves = EXCLUDED.n_moves,
    time_elapsed = EXCLUDED.time_elapsed,
    avg_elo = EXCLUDED.avg_elo;

WITH requested AS (
    SELECT n_moves, n_moves * 2 AS required_half_moves
    FROM generate_series(3, 10) AS supported(n_moves)
),
opening_moves AS (
    SELECT
        m.link,
        r.n_moves,
        m.n_move,
        m.white_move,
        m.black_move
    FROM moves m
    JOIN requested r ON m.n_move BETWEEN 1 AND r.n_moves
),
complete_games AS (
    SELECT link, n_moves
    FROM opening_moves
    GROUP BY link, n_moves
    HAVING COUNT(DISTINCT n_move) = n_moves
),
ply_moves AS (
    SELECT
        link,
        n_moves,
        (n_move * 2 - 1) AS ply,
        white_move AS san
    FROM opening_moves
    UNION ALL
    SELECT
        link,
        n_moves,
        (n_move * 2) AS ply,
        black_move AS san
    FROM opening_moves
),
openings AS (
    SELECT
        pm.link,
        pm.n_moves,
        string_agg(pm.san, ' ' ORDER BY pm.ply) AS opening
    FROM ply_moves pm
    JOIN complete_games cg
      ON cg.link = pm.link
     AND cg.n_moves = pm.n_moves
    WHERE pm.san IS NOT NULL
      AND pm.san <> ''
      AND pm.san <> '--'
    GROUP BY pm.link, pm.n_moves
    HAVING COUNT(*) = (pm.n_moves * 2)
)
INSERT INTO game_opening (
    link,
    n_moves,
    opening,
    mode,
    avg_elo,
    played_at
)
SELECT
    o.link,
    o.n_moves,
    o.opening,
    g.mode,
    g.avg_elo,
    g.played_at
FROM openings o
JOIN game g ON g.link = o.link
ON CONFLICT (link, n_moves) DO UPDATE SET
    opening = EXCLUDED.opening,
    mode = EXCLUDED.mode,
    avg_elo = EXCLUDED.avg_elo,
    played_at = EXCLUDED.played_at;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_mode ON game(mode);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_played_at ON game(played_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_mode_avg_elo ON game(mode, avg_elo);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_white_played_at ON game(white, played_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_black_played_at ON game(black, played_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_fens_remaining ON game(link) WHERE fens_done = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_moves_link_n_move ON moves(link, n_move);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_moves_n_move_link ON moves(n_move, link);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_n_games_desc ON fen(n_games DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fen_unscored_n_games_desc ON fen(n_games DESC) WHERE score IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_player_main_characters ON player(player_name) WHERE joined IS NOT NULL AND joined <> 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_months_player_year_month ON months(player_name, year DESC, month DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_player_player_played_at ON game_player(player_name, played_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_player_player_mode_played_at ON game_player(player_name, mode, played_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_player_mode_rating ON game_player(mode, rating);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_player_link ON game_player(link);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_opening_mode_n_moves_opening ON game_opening(mode, n_moves, opening);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_game_opening_n_moves_mode_avg_elo ON game_opening(n_moves, mode, avg_elo);

ANALYZE game;
ANALYZE moves;
ANALYZE fen;
ANALYZE player;
ANALYZE months;
ANALYZE game_player;
ANALYZE game_opening;
