-- Derived snapshots deliberately retain old identities after source deletion.
-- Readers may show the previous complete snapshot while its refresh is pending.
CREATE TABLE fen_appearance_summary (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    source_updated_at timestamptz, refreshed_at timestamptz, attempted_at timestamptz,
    last_error varchar(240)
);
CREATE TABLE fen_appearance_bin (
    appearances bigint PRIMARY KEY CHECK (appearances>=0),
    total_fens bigint NOT NULL CHECK (total_fens>=0),
    unscored_fens bigint NOT NULL CHECK (unscored_fens>=0 AND unscored_fens<=total_fens)
);
CREATE TABLE fen_commonness_summary (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    source_updated_at timestamptz, refreshed_at timestamptz, attempted_at timestamptz,
    last_error varchar(240)
);
CREATE TABLE fen_commonness_position (
    rank smallint PRIMARY KEY CHECK (rank BETWEEN 1 AND 10),
    fen text NOT NULL UNIQUE, appearances bigint NOT NULL CHECK (appearances>1),
    n_move integer, move_color varchar(5), san varchar(32),
    label_occurrences bigint NOT NULL DEFAULT 0 CHECK (label_occurrences>=0),
    example_game_link bigint, example_line text
);
INSERT INTO fen_appearance_summary DEFAULT VALUES;
INSERT INTO fen_commonness_summary DEFAULT VALUES;
CREATE INDEX ix_fen_commonness ON fen(n_games DESC,fen) WHERE n_games>1;
