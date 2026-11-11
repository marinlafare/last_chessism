-- Canonical foundation. No legacy schema, archive copy, read adapter or data import.
CREATE TABLE player (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_name text NOT NULL UNIQUE CHECK (player_name <> '' AND player_name = lower(btrim(player_name))),
    name text, url text, title text, avatar text, followers integer,
    country text, location text, timezone varchar(64), timezone_source varchar(32),
    joined bigint, status text, is_streamer boolean, twitch_url text,
    verified boolean, league text, deleted_at timestamptz
);
CREATE TABLE game_mode (
    id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name varchar(16) NOT NULL UNIQUE
);
CREATE TABLE game (
    link bigint PRIMARY KEY CHECK (link > 0),
    white_player_id bigint NOT NULL REFERENCES player(id),
    black_player_id bigint NOT NULL REFERENCES player(id),
    mode_id smallint REFERENCES game_mode(id),
    played_at timestamptz NOT NULL,
    white_elo integer NOT NULL, black_elo integer NOT NULL,
    white_result double precision NOT NULL CHECK (white_result IN (0, 0.5, 1)),
    black_result double precision NOT NULL CHECK (black_result IN (0, 0.5, 1)),
    white_str_result text NOT NULL, black_str_result text NOT NULL,
    time_control text NOT NULL, eco text NOT NULL,
    time_elapsed double precision NOT NULL CHECK (time_elapsed >= 0 AND time_elapsed < 'Infinity'::float8),
    n_moves integer NOT NULL CHECK (n_moves >= 0),
    rules varchar(32) NOT NULL CHECK (rules <> ''),
    initial_setup varchar(128),
    -- Same standard-game predicate as relational_repository.standard_game.
    is_standard boolean GENERATED ALWAYS AS (
        rules = 'chess' AND regexp_replace(btrim(coalesce(initial_setup, '')), '\s+', ' ', 'g') IN (
            '', 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -',
            'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'
        )
    ) STORED,
    fens_done boolean NOT NULL DEFAULT false,
    fens_processing boolean NOT NULL DEFAULT false,
    CHECK (NOT fens_processing OR (is_standard AND n_moves > 0 AND NOT fens_done)),
    CHECK (NOT fens_done OR (is_standard AND n_moves > 0))
);
CREATE INDEX ix_game_played_at ON game(played_at);
CREATE INDEX ix_game_white_played_at ON game(white_player_id, played_at);
CREATE INDEX ix_game_black_played_at ON game(black_player_id, played_at);
CREATE INDEX ix_game_mode_played_at ON game(mode_id, played_at);
CREATE INDEX ix_game_extraction_pending ON game(link)
    WHERE is_standard AND n_moves > 0 AND NOT fens_done AND NOT fens_processing;
-- A semantic projection, not a compatibility adapter: exactly two sides, even in self-play.
-- game is the sole owner of participation; no separately writable duplicate rows.
CREATE VIEW game_player AS
    SELECT link AS game_link, white_player_id AS player_id, true AS is_white FROM game
    UNION ALL
    SELECT link, black_player_id, false FROM game;
CREATE TABLE moves (
    link bigint NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    n_move integer NOT NULL CHECK (n_move > 0),
    white_move text NOT NULL, black_move text NOT NULL,
    white_reaction_time double precision NOT NULL, black_reaction_time double precision NOT NULL,
    white_time_left double precision NOT NULL, black_time_left double precision NOT NULL,
    PRIMARY KEY (link, n_move)
);
CREATE TABLE opening_sequence (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    opening text NOT NULL UNIQUE
);
CREATE TABLE game_opening (
    link bigint NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    n_moves integer NOT NULL CHECK (n_moves >= 0),
    opening_id bigint NOT NULL REFERENCES opening_sequence(id),
    PRIMARY KEY (link, n_moves)
);
CREATE TABLE months (
    player_id bigint NOT NULL REFERENCES player(id),
    year integer NOT NULL CHECK (year BETWEEN 1 AND 9999),
    month smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    n_games integer NOT NULL CHECK (n_games >= 0),
    PRIMARY KEY (player_id, year, month)
);
CREATE TABLE fen (
    fen_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fen text NOT NULL UNIQUE,
    -- Four-field keys retain the accepted identity; chess legality belongs to the parser.
    CHECK (fen = btrim(fen) AND fen ~ '^[^ ]+ [wb] [KQkq-]+ (-|[a-h][36])$'),
    n_games bigint NOT NULL DEFAULT 0 CHECK (n_games >= 0),
    moves_counter text NOT NULL DEFAULT '', next_moves text,
    score integer,
    wdl_win smallint, wdl_draw smallint, wdl_loss smallint,
    piece_count smallint CHECK (piece_count BETWEEN 2 AND 32),
    analysis_source varchar(32),
    tablebase_wdl smallint CHECK (tablebase_wdl BETWEEN -2 AND 2),
    tablebase_dtz integer, analyzed_at timestamptz,
    CHECK ((wdl_win IS NULL AND wdl_draw IS NULL AND wdl_loss IS NULL) OR
           (wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL
            AND wdl_win BETWEEN 0 AND 1000 AND wdl_draw BETWEEN 0 AND 1000
            AND wdl_loss BETWEEN 0 AND 1000 AND wdl_win + wdl_draw + wdl_loss = 1000)),
    CHECK ((score IS NULL AND analysis_source IS NULL AND analyzed_at IS NULL
            AND tablebase_wdl IS NULL AND tablebase_dtz IS NULL AND wdl_win IS NULL)
        OR (score IS NULL AND analysis_source = 'tablebase_unavailable'
            AND tablebase_wdl IS NULL AND tablebase_dtz IS NULL AND wdl_win IS NULL)
        OR (score IS NOT NULL AND analysis_source IS NOT NULL AND analyzed_at IS NOT NULL AND
            ((analysis_source = 'stockfish' AND tablebase_wdl IS NULL AND tablebase_dtz IS NULL)
             OR (analysis_source = 'tablebase' AND tablebase_wdl IS NOT NULL))))
);
CREATE INDEX ix_fen_unscored ON fen(n_games DESC, fen) WHERE score IS NULL;
CREATE INDEX ix_fen_backup_changed ON fen(analyzed_at, fen) WHERE score IS NOT NULL;
CREATE INDEX ix_fen_pending_tablebase ON fen(n_games DESC, fen)
    WHERE score IS NULL AND coalesce(analysis_source, '') <> 'tablebase_unavailable'
    AND coalesce(piece_count, char_length(translate(split_part(fen, ' ', 1), '12345678/', ''))) <= 5;
CREATE TABLE game_fen_association (
    game_link bigint NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    fen_id bigint NOT NULL REFERENCES fen(fen_id),
    n_move integer NOT NULL CHECK (n_move >= 0),
    is_white boolean NOT NULL,
    PRIMARY KEY (game_link, n_move, is_white)
);
CREATE INDEX ix_game_fen_fen ON game_fen_association(fen_id);
CREATE TABLE fen_continuation (
    fen_id bigint NOT NULL REFERENCES fen(fen_id) ON DELETE CASCADE,
    rank smallint NOT NULL CHECK (rank >= 1),
    move text NOT NULL CHECK (move <> ''), score integer NOT NULL,
    PRIMARY KEY (fen_id, rank)
);
-- Prevent analysis references on variants/zero-move games in either update direction.
CREATE FUNCTION require_analyzable_game() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    IF TG_TABLE_NAME = 'game_fen_association' THEN
        PERFORM 1 FROM game WHERE link = NEW.game_link AND is_standard AND n_moves > 0 FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'FEN association requires a standard nonempty game' USING ERRCODE = '23514';
        END IF;
    ELSE
        IF (NOT NEW.is_standard OR NEW.n_moves = 0)
           AND EXISTS (SELECT 1 FROM game_fen_association WHERE game_link = NEW.link) THEN
            RAISE EXCEPTION 'Cannot invalidate a game with FEN associations' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER check_association_eligibility BEFORE INSERT OR UPDATE ON game_fen_association
    FOR EACH ROW EXECUTE FUNCTION require_analyzable_game();
-- AFTER observes the computed is_standard value.
CREATE TRIGGER check_game_eligibility AFTER UPDATE ON game
    FOR EACH ROW EXECUTE FUNCTION require_analyzable_game();
