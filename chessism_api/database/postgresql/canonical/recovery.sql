-- Durable refresh identities are committed with their source changes.
-- No game FK: deletion must remain recoverable after the game has disappeared.
CREATE TABLE fen_summary_refresh (
    game_link bigint PRIMARY KEY CHECK (game_link > 0),
    revision bigserial NOT NULL,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retry_after timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    error text
);
CREATE INDEX fen_summary_refresh_ready ON fen_summary_refresh(retry_after,queued_at,game_link);

-- Compare evaluation fields as sets before touching the association table. This
-- avoids cached empty-transition join plans and ignores ingestion counter writes.
-- Canonical scored provenance is always known; unavailable/unscored labels have
-- the same evaluation semantics and need no historical source-only routing.
CREATE FUNCTION queue_fen_summary_refresh() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE changed_ids bigint[];
BEGIN
    SELECT array_agg(fen_id) INTO changed_ids FROM (
        SELECT fen_id,score,COALESCE(analysis_source='tablebase',false) FROM new_fens
        EXCEPT
        SELECT fen_id,score,COALESCE(analysis_source='tablebase',false) FROM old_fens
    ) changed;
    IF changed_ids IS NULL THEN RETURN NULL; END IF;
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT a.game_link
        FROM unnest(changed_ids) changed(fen_id)
        JOIN public.fen f USING(fen_id)
        JOIN public.game_fen_association a USING(fen_id)
        -- Cloud batches already commit durable receipts and projection recovery.
        WHERE NOT EXISTS(SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=f.fen)
        ORDER BY a.game_link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_refresh AFTER UPDATE ON fen
REFERENCING OLD TABLE AS old_fens NEW TABLE AS new_fens
FOR EACH STATEMENT EXECUTE FUNCTION queue_fen_summary_refresh();

-- Associations can reuse previously scored FENs without any FEN update.
CREATE FUNCTION fen_summary_game_fen_association_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT game_link FROM (SELECT game_link FROM new_rows) changed ORDER BY game_link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_game_fen_association_insert AFTER INSERT ON game_fen_association
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION fen_summary_game_fen_association_insert();

CREATE FUNCTION fen_summary_game_fen_association_update() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT game_link FROM (SELECT game_link FROM old_rows UNION SELECT game_link FROM new_rows) changed ORDER BY game_link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_game_fen_association_update AFTER UPDATE ON game_fen_association
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION fen_summary_game_fen_association_update();

CREATE FUNCTION fen_summary_game_fen_association_delete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT game_link FROM (SELECT game_link FROM old_rows) changed ORDER BY game_link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_game_fen_association_delete AFTER DELETE ON game_fen_association
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION fen_summary_game_fen_association_delete();

-- Participants live on game; the derived game_player view has no write triggers.
CREATE FUNCTION fen_summary_game_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT game_link FROM (SELECT link AS game_link FROM new_rows WHERE is_standard AND n_moves>0) changed ORDER BY game_link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_game_insert AFTER INSERT ON game
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION fen_summary_game_insert();

CREATE FUNCTION fen_summary_game_update() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    WITH previous AS MATERIALIZED (
        SELECT link,white_player_id,black_player_id FROM old_rows
        WHERE is_standard AND n_moves>0
    ), incoming AS MATERIALIZED (
        SELECT link,white_player_id,black_player_id FROM new_rows
        WHERE is_standard AND n_moves>0
    )
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT link FROM (
            (SELECT * FROM previous EXCEPT SELECT * FROM incoming)
            UNION
            (SELECT * FROM incoming EXCEPT SELECT * FROM previous)
        ) changed ORDER BY link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_game_update AFTER UPDATE ON game
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION fen_summary_game_update();

CREATE FUNCTION fen_summary_game_delete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    INSERT INTO public.fen_summary_refresh(game_link)
        SELECT DISTINCT game_link FROM (SELECT link AS game_link FROM old_rows WHERE is_standard AND n_moves>0) changed ORDER BY game_link
    ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision,
        retry_after=clock_timestamp(),attempts=0,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER fen_summary_game_delete AFTER DELETE ON game
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION fen_summary_game_delete();

