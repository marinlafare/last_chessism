-- Derived homepage snapshot. The JSON roster belongs to the same revision as
-- its totals, even after a source profile is physically deleted.
CREATE TABLE home_summary_source (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision>0)
);
CREATE TABLE home_titled_summary (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id=1),
    source_revision bigint,
    total_fens bigint NOT NULL DEFAULT 0,
    titled_players bigint NOT NULL DEFAULT 0,
    titled_fens bigint NOT NULL DEFAULT 0,
    titled_games bigint NOT NULL DEFAULT 0,
    players jsonb NOT NULL DEFAULT '[]'::jsonb,
    refreshed_at timestamptz,
    attempted_at timestamptz,
    last_error varchar(240),
    CHECK (total_fens>=titled_fens AND titled_fens>=0 AND titled_games>=0 AND titled_players>=0),
    CHECK (jsonb_typeof(players)='array' AND jsonb_array_length(players)=titled_players)
);
INSERT INTO home_summary_source DEFAULT VALUES;
INSERT INTO home_titled_summary DEFAULT VALUES;

CREATE FUNCTION invalidate_home_summary() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    UPDATE public.home_summary_source SET revision=revision+1 WHERE id=1;
    IF NOT FOUND THEN RAISE EXCEPTION 'Required home summary source is missing'; END IF;
    RETURN NULL;
END $$;

CREATE TRIGGER home_player_rows AFTER INSERT OR DELETE OR TRUNCATE ON player
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_player_values AFTER UPDATE OF title,deleted_at,player_name ON player
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_game_rows AFTER INSERT OR DELETE OR TRUNCATE ON game
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_game_values AFTER UPDATE OF white_player_id,black_player_id,rules,initial_setup,n_moves,link ON game
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_fen_rows AFTER INSERT OR DELETE OR TRUNCATE ON fen
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_fen_values AFTER UPDATE OF fen ON fen
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_association_rows AFTER INSERT OR DELETE OR TRUNCATE ON game_fen_association
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
CREATE TRIGGER home_association_values AFTER UPDATE OF game_link,fen_id ON game_fen_association
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_home_summary();
