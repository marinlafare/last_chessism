-- Published salience uses the authoritative native side identity.
CREATE TABLE game_player_salience (
    game_link bigint NOT NULL REFERENCES game(link) ON DELETE CASCADE,
    is_white boolean NOT NULL,
    salience float8 NOT NULL CHECK(salience>0 AND salience<=1),
    weighted_numerator float8 NOT NULL CHECK(weighted_numerator>0),
    depth_weight_sum float8 NOT NULL CHECK(depth_weight_sum>0),
    position_occurrence_count integer NOT NULL CHECK(position_occurrence_count>0),
    unique_position_count integer NOT NULL CHECK(unique_position_count>0 AND unique_position_count<=position_occurrence_count),
    repeated_position_count integer GENERATED ALWAYS AS (position_occurrence_count-unique_position_count) STORED,
    PRIMARY KEY(game_link,is_white)
);
CREATE TABLE player_position_frequency (
    player_id bigint NOT NULL REFERENCES player(id) ON DELETE CASCADE,
    is_white boolean NOT NULL, fen_id bigint NOT NULL REFERENCES fen(fen_id) ON DELETE CASCADE,
    games_with_position bigint NOT NULL CHECK(games_with_position>1),
    total_occurrences bigint NOT NULL CHECK(total_occurrences>=games_with_position),
    PRIMARY KEY(player_id,is_white,fen_id)
);

-- CUDA publication state. Installed only by the empty canonical initializer.

CREATE TABLE public.canonical_blunder_model (id text PRIMARY KEY,source_run text NOT NULL,config jsonb NOT NULL,
            evidence jsonb NOT NULL,approved_by text NOT NULL,approved_at timestamptz NOT NULL DEFAULT now(),
            approval text NOT NULL CHECK(approval='provisional_user_approved'));

CREATE TABLE public.canonical_blunder_control (id boolean PRIMARY KEY DEFAULT true CHECK(id),target_model text REFERENCES public.canonical_blunder_model(id),active_model text REFERENCES public.canonical_blunder_model(id),
            previous_model text REFERENCES public.canonical_blunder_model(id),status text NOT NULL DEFAULT 'idle',progress jsonb NOT NULL DEFAULT '{}',
            error text,updated_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE public.canonical_blunder_dirty (game_link bigint PRIMARY KEY,revision bigserial NOT NULL);

CREATE TABLE public.canonical_blunder_game (model_id text NOT NULL REFERENCES public.canonical_blunder_model(id),game_link bigint NOT NULL,revision bigint NOT NULL,
            mode text,played_at timestamptz,white_owner text,black_owner text,
            status text NOT NULL,white_count int,black_count int,white_assessed int,black_assessed int,
            flags jsonb NOT NULL,source_sha256 text NOT NULL,
            PRIMARY KEY(model_id,game_link),CHECK(white_count>=0 AND black_count>=0));

CREATE TABLE public.canonical_blunder_snapshot (model_id text NOT NULL REFERENCES public.canonical_blunder_model(id),player_name text NOT NULL,payload jsonb NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(model_id,player_name));

CREATE INDEX canonical_blunder_game_white_owner ON public.canonical_blunder_game(model_id,white_owner);

CREATE INDEX canonical_blunder_game_black_owner ON public.canonical_blunder_game(model_id,black_owner);

INSERT INTO public.canonical_blunder_control(id) VALUES(true) ON CONFLICT DO NOTHING;

CREATE FUNCTION public.protect_canonical_blunder_model()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Canonical model records are immutable'; END $$;

CREATE TRIGGER canonical_model_immutable BEFORE UPDATE OR DELETE ON public.canonical_blunder_model
        FOR EACH ROW EXECUTE FUNCTION public.protect_canonical_blunder_model();

CREATE FUNCTION public.canonical_blunder_truncate() RETURNS trigger
            LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
            INSERT INTO public.canonical_blunder_dirty(game_link)
                SELECT game_link FROM public.canonical_blunder_game WHERE EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                UNION SELECT link FROM public.game WHERE EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                ORDER BY 1 ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
            RETURN NULL; END $$;

CREATE FUNCTION public.canonical_blunder_game_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_insert AFTER INSERT ON public.game
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_insert();

CREATE FUNCTION public.canonical_blunder_game_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM old_rows UNION SELECT link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_update AFTER UPDATE ON public.game
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_update();

CREATE FUNCTION public.canonical_blunder_game_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM old_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_delete AFTER DELETE ON public.game
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_delete();



CREATE TRIGGER canonical_blunder_truncate AFTER TRUNCATE ON public.game
            FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_truncate();

CREATE FUNCTION public.canonical_blunder_player_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT g.link FROM new_rows r JOIN public.game g
                        ON r.id IN(g.white_player_id,g.black_player_id) ) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_player_insert AFTER INSERT ON public.player
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_player_insert();

CREATE FUNCTION public.canonical_blunder_player_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT g.link FROM old_rows r JOIN public.game g
                        ON r.id IN(g.white_player_id,g.black_player_id) WHERE EXISTS(SELECT 1 FROM old_rows o FULL JOIN new_rows n USING(id)
                        WHERE r.id=COALESCE(o.id,n.id) AND (o.joined,o.deleted_at,o.player_name) IS DISTINCT FROM (n.joined,n.deleted_at,n.player_name)) UNION SELECT g.link FROM new_rows r JOIN public.game g
                        ON r.id IN(g.white_player_id,g.black_player_id) WHERE EXISTS(SELECT 1 FROM old_rows o FULL JOIN new_rows n USING(id)
                        WHERE r.id=COALESCE(o.id,n.id) AND (o.joined,o.deleted_at,o.player_name) IS DISTINCT FROM (n.joined,n.deleted_at,n.player_name))) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_player_update AFTER UPDATE ON public.player
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_player_update();

CREATE FUNCTION public.canonical_blunder_player_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT g.link FROM old_rows r JOIN public.game g
                        ON r.id IN(g.white_player_id,g.black_player_id) ) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_player_delete AFTER DELETE ON public.player
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_player_delete();



CREATE TRIGGER canonical_blunder_truncate AFTER TRUNCATE ON public.player
            FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_truncate();

CREATE FUNCTION public.canonical_blunder_game_fen_association_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT game_link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_fen_association_insert AFTER INSERT ON public.game_fen_association
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_fen_association_insert();

CREATE FUNCTION public.canonical_blunder_game_fen_association_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT game_link AS link FROM old_rows UNION SELECT game_link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_fen_association_update AFTER UPDATE ON public.game_fen_association
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_fen_association_update();

CREATE FUNCTION public.canonical_blunder_game_fen_association_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT game_link AS link FROM old_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_fen_association_delete AFTER DELETE ON public.game_fen_association
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_fen_association_delete();



CREATE TRIGGER canonical_blunder_truncate AFTER TRUNCATE ON public.game_fen_association
            FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_truncate();

CREATE FUNCTION public.canonical_blunder_moves_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_moves_insert AFTER INSERT ON public.moves
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_moves_insert();

CREATE FUNCTION public.canonical_blunder_moves_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM old_rows UNION SELECT link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_moves_update AFTER UPDATE ON public.moves
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_moves_update();

CREATE FUNCTION public.canonical_blunder_moves_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM old_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_moves_delete AFTER DELETE ON public.moves
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_moves_delete();



CREATE TRIGGER canonical_blunder_truncate AFTER TRUNCATE ON public.moves
            FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_truncate();

CREATE FUNCTION public.canonical_blunder_game_analysis_coverage_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_analysis_coverage_insert AFTER INSERT ON public.game_analysis_coverage
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_analysis_coverage_insert();

CREATE FUNCTION public.canonical_blunder_game_analysis_coverage_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM old_rows UNION SELECT link AS link FROM new_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_analysis_coverage_update AFTER UPDATE ON public.game_analysis_coverage
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_analysis_coverage_update();

CREATE FUNCTION public.canonical_blunder_game_analysis_coverage_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                INSERT INTO public.canonical_blunder_dirty(game_link) SELECT DISTINCT link FROM (SELECT link AS link FROM old_rows) changed
                    WHERE link IS NOT NULL AND EXISTS(SELECT 1 FROM public.canonical_blunder_control WHERE target_model IS NOT NULL)
                    ORDER BY link ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision;
                RETURN NULL; END $$;

CREATE TRIGGER canonical_blunder_game_analysis_coverage_delete AFTER DELETE ON public.game_analysis_coverage
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_game_analysis_coverage_delete();



CREATE TRIGGER canonical_blunder_truncate AFTER TRUNCATE ON public.game_analysis_coverage
            FOR EACH STATEMENT EXECUTE FUNCTION public.canonical_blunder_truncate();

CREATE VIEW public.canonical_blunder_assessment AS
        WITH totals AS (
            SELECT b.*, white_assessed + black_assessed +
                jsonb_array_length(flags->'unassessed') AS total_plies
            FROM public.canonical_blunder_game b
        ), coverage AS (
            SELECT totals.*,
                CASE WHEN status='assessed' THEN (total_plies+1)/2-white_assessed END AS white_unassessed,
                CASE WHEN status='assessed' THEN total_plies/2-black_assessed END AS black_unassessed
            FROM totals
        ) SELECT coverage.*,
            COALESCE(status='assessed' AND white_assessed>0 AND white_unassessed=0, false) AS white_complete,
            COALESCE(status='assessed' AND black_assessed>0 AND black_unassessed=0, false) AS black_complete
        FROM coverage;

-- Names here belong to derived snapshots/captured work, not authoritative player
-- ownership. Retain retired names until publication can clear their old snapshots.
CREATE TABLE public.player_stockfish_state (player_name text PRIMARY KEY, revision bigint NOT NULL DEFAULT 1,
            published_revision bigint NOT NULL DEFAULT 0, full_rebuild boolean NOT NULL DEFAULT true,
            attempted_at timestamptz, error text);

CREATE TABLE public.player_stockfish_dirty (player_name text NOT NULL, day date NOT NULL, PRIMARY KEY(player_name,day));

CREATE TABLE public.player_stockfish_partition (player_name text NOT NULL, day date NOT NULL, payload jsonb NOT NULL, PRIMARY KEY(player_name,day));

CREATE TABLE public.player_stockfish_snapshot (player_name text PRIMARY KEY, revision bigint NOT NULL, payload jsonb NOT NULL,
            generated_at timestamptz NOT NULL DEFAULT clock_timestamp());

CREATE FUNCTION public.queue_player_stockfish(
        owner_name text, changed_at timestamptz, rebuild boolean DEFAULT false)
        RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF owner_name IS NULL THEN RETURN; END IF;
        INSERT INTO public.player_stockfish_state AS current(player_name) VALUES(owner_name)
            ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                full_rebuild=current.full_rebuild OR rebuild,error=NULL;
        INSERT INTO public.player_stockfish_dirty(player_name,day)
            VALUES(owner_name,COALESCE((changed_at AT TIME ZONE 'UTC')::date,DATE '-infinity'))
            ON CONFLICT DO NOTHING;
        END $$;

CREATE FUNCTION public.stockfish_summary_truncate()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        UPDATE public.player_stockfish_state SET revision=revision+1,full_rebuild=true,error=NULL;
        RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_truncate AFTER TRUNCATE ON public.player
            FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_truncate();

CREATE FUNCTION public.stockfish_summary_player_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT r.player_name,NULL::timestamptz AS played_at FROM new_rows r ) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR true,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_player_insert AFTER INSERT ON public.player
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_player_insert();

CREATE FUNCTION public.stockfish_summary_player_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT r.player_name,NULL::timestamptz AS played_at FROM old_rows r WHERE EXISTS(SELECT 1 FROM old_rows o
                        FULL JOIN new_rows n USING(player_name) WHERE r.player_name=COALESCE(o.player_name,n.player_name)
                        AND (o.joined,o.deleted_at,o.player_name) IS DISTINCT FROM (n.joined,n.deleted_at,n.player_name)) UNION SELECT r.player_name,NULL::timestamptz AS played_at FROM new_rows r WHERE EXISTS(SELECT 1 FROM old_rows o
                        FULL JOIN new_rows n USING(player_name) WHERE r.player_name=COALESCE(o.player_name,n.player_name)
                        AND (o.joined,o.deleted_at,o.player_name) IS DISTINCT FROM (n.joined,n.deleted_at,n.player_name))) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR true,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_player_update AFTER UPDATE ON public.player
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_player_update();

CREATE FUNCTION public.stockfish_summary_player_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT r.player_name,NULL::timestamptz AS played_at FROM old_rows r ) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR true,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_player_delete AFTER DELETE ON public.player
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_player_delete();

CREATE TRIGGER stockfish_summary_truncate AFTER TRUNCATE ON public.game
            FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_truncate();

CREATE FUNCTION public.stockfish_summary_game_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,r.played_at FROM new_rows r
                            JOIN public.player p ON p.id IN(r.white_player_id,r.black_player_id)) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_insert AFTER INSERT ON public.game
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_insert();

CREATE FUNCTION public.stockfish_summary_game_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,r.played_at FROM old_rows r
                            JOIN public.player p ON p.id IN(r.white_player_id,r.black_player_id) UNION SELECT p.player_name,r.played_at FROM new_rows r
                            JOIN public.player p ON p.id IN(r.white_player_id,r.black_player_id)) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_update AFTER UPDATE ON public.game
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_update();

CREATE FUNCTION public.stockfish_summary_game_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,r.played_at FROM old_rows r
                            JOIN public.player p ON p.id IN(r.white_player_id,r.black_player_id)) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_delete AFTER DELETE ON public.game
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_delete();

CREATE TRIGGER stockfish_summary_truncate AFTER TRUNCATE ON public.game_fen_association
            FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_truncate();

CREATE FUNCTION public.stockfish_summary_game_fen_association_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT game_link AS link FROM new_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_fen_association_insert AFTER INSERT ON public.game_fen_association
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_fen_association_insert();

CREATE FUNCTION public.stockfish_summary_game_fen_association_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT game_link AS link FROM old_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link UNION SELECT p.player_name,g.played_at FROM (SELECT DISTINCT game_link AS link FROM new_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_fen_association_update AFTER UPDATE ON public.game_fen_association
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_fen_association_update();

CREATE FUNCTION public.stockfish_summary_game_fen_association_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT game_link AS link FROM old_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_fen_association_delete AFTER DELETE ON public.game_fen_association
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_fen_association_delete();

CREATE TRIGGER stockfish_summary_truncate AFTER TRUNCATE ON public.moves
            FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_truncate();

CREATE FUNCTION public.stockfish_summary_moves_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM new_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_moves_insert AFTER INSERT ON public.moves
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_moves_insert();

CREATE FUNCTION public.stockfish_summary_moves_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM old_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link UNION SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM new_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_moves_update AFTER UPDATE ON public.moves
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_moves_update();

CREATE FUNCTION public.stockfish_summary_moves_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM old_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_moves_delete AFTER DELETE ON public.moves
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_moves_delete();

CREATE TRIGGER stockfish_summary_truncate AFTER TRUNCATE ON public.game_analysis_coverage
            FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_truncate();

CREATE FUNCTION public.stockfish_summary_game_analysis_coverage_insert() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM new_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_analysis_coverage_insert AFTER INSERT ON public.game_analysis_coverage
                REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_analysis_coverage_insert();

CREATE FUNCTION public.stockfish_summary_game_analysis_coverage_update() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM old_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link UNION SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM new_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_analysis_coverage_update AFTER UPDATE ON public.game_analysis_coverage
                REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_analysis_coverage_update();

CREATE FUNCTION public.stockfish_summary_game_analysis_coverage_delete() RETURNS trigger
                LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
                WITH changes AS MATERIALIZED (
                    SELECT DISTINCT changed.player_name,
                        COALESCE((played_at AT TIME ZONE 'UTC')::date,DATE '-infinity') AS day
                    FROM (SELECT p.player_name,g.played_at FROM (SELECT DISTINCT link AS link FROM old_rows) r
                            JOIN public.game_player gp ON gp.game_link=r.link
                            JOIN public.player p ON p.id=gp.player_id
                            JOIN public.game g ON g.link=r.link) changed WHERE
                    EXISTS(SELECT 1 FROM public.player p WHERE p.player_name=changed.player_name AND p.joined>0 AND p.deleted_at IS NULL)
                    OR EXISTS(SELECT 1 FROM public.player_stockfish_state s WHERE s.player_name=changed.player_name)
                ), queued AS (
                    INSERT INTO public.player_stockfish_state AS current(player_name)
                    SELECT DISTINCT player_name FROM changes ORDER BY player_name
                    ON CONFLICT(player_name) DO UPDATE SET revision=current.revision+1,
                        full_rebuild=current.full_rebuild OR false,error=NULL
                    RETURNING player_name
                )
                INSERT INTO public.player_stockfish_dirty(player_name,day)
                    SELECT c.player_name,c.day FROM changes c JOIN queued USING(player_name)
                    ORDER BY c.player_name,c.day ON CONFLICT DO NOTHING;
                RETURN NULL; END $$;

CREATE TRIGGER stockfish_summary_game_analysis_coverage_delete AFTER DELETE ON public.game_analysis_coverage
                REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION public.stockfish_summary_game_analysis_coverage_delete();

INSERT INTO public.player_stockfish_state(player_name)
        SELECT player_name FROM public.player WHERE joined>0 AND deleted_at IS NULL ON CONFLICT DO NOTHING;

CREATE TABLE public.cuda_summary_pending (
        metric text NOT NULL CHECK(metric IN ('blunders','accuracy','score_distribution')),
        game_link bigint NOT NULL,revision bigint NOT NULL,queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY(metric,game_link));

CREATE INDEX cuda_summary_pending_game ON public.cuda_summary_pending(game_link);

CREATE TABLE public.cuda_summary_run (
        id uuid PRIMARY KEY, requested_by text NOT NULL,model_id text NOT NULL,
        metrics text[] NOT NULL DEFAULT ARRAY['blunders'],
        status text NOT NULL CHECK(status IN ('queued','running','retrying','complete','failed')),
        phase text NOT NULL DEFAULT 'queued',created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        started_at timestamptz,finished_at timestamptz,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        total_games bigint NOT NULL DEFAULT 0,processed_games bigint NOT NULL DEFAULT 0,
        superseded_games bigint NOT NULL DEFAULT 0,attempts int NOT NULL DEFAULT 0,
        retry_after timestamptz,error text,timings jsonb NOT NULL DEFAULT '{}');

CREATE UNIQUE INDEX cuda_summary_one_active
        ON public.cuda_summary_run((true)) WHERE status IN ('queued','running','retrying');

CREATE TABLE public.cuda_summary_item (
        run_id uuid NOT NULL REFERENCES public.cuda_summary_run(id),game_link bigint NOT NULL,revision bigint NOT NULL,
        status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','published','superseded')),
        PRIMARY KEY(run_id,game_link));

CREATE INDEX cuda_summary_item_pending
        ON public.cuda_summary_item(run_id,game_link) WHERE status='pending';

CREATE FUNCTION public.queue_cuda_summaries()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF NOT (SELECT manual FROM public.cuda_summary_control WHERE id) THEN RETURN NULL; END IF;
        INSERT INTO public.cuda_summary_pending(metric,game_link,revision)
            SELECT m.metric,r.game_link,r.revision FROM new_rows r CROSS JOIN
                unnest(ARRAY['blunders','accuracy','score_distribution']) m(metric)
            ORDER BY m.metric,r.game_link
        ON CONFLICT(metric,game_link) DO UPDATE SET revision=EXCLUDED.revision;
        RETURN NULL; END $$;

CREATE TRIGGER queue_cuda_summaries_insert AFTER INSERT ON public.canonical_blunder_dirty
            REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.queue_cuda_summaries();

CREATE TRIGGER queue_cuda_summaries_update AFTER UPDATE ON public.canonical_blunder_dirty
            REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.queue_cuda_summaries();

ALTER TABLE public.cuda_summary_control ADD COLUMN scores boolean NOT NULL DEFAULT false;

ALTER TABLE public.cuda_summary_item ADD COLUMN blunders boolean NOT NULL DEFAULT true;

CREATE TABLE public.cuda_score_scope(run_id uuid REFERENCES public.cuda_summary_run(id),player_name text, revision bigint NOT NULL,full_rebuild boolean NOT NULL,
            planned boolean NOT NULL DEFAULT false,status text NOT NULL DEFAULT 'pending'
            CHECK(status IN ('pending','published','superseded')),PRIMARY KEY(run_id,player_name));

CREATE TABLE public.cuda_score_day(run_id uuid REFERENCES public.cuda_summary_run(id),player_name text,day date,PRIMARY KEY(run_id,player_name,day));

CREATE TABLE public.cuda_score_owner(run_id uuid REFERENCES public.cuda_summary_run(id),game_link bigint,player_name text,day date,is_white boolean NOT NULL,
            PRIMARY KEY(run_id,game_link,is_white));

CREATE TABLE public.cuda_score_part(run_id uuid REFERENCES public.cuda_summary_run(id),player_name text,day date,batch bigint,payload jsonb NOT NULL,PRIMARY KEY(run_id,player_name,day,batch));

CREATE TABLE public.cuda_score_pending_capture(run_id uuid REFERENCES public.cuda_summary_run(id),game_link bigint,revision bigint NOT NULL,PRIMARY KEY(run_id,game_link));

CREATE INDEX cuda_score_owner_player ON public.cuda_score_owner(run_id,player_name,day);

ALTER TABLE public.cuda_summary_control ADD COLUMN accuracy boolean NOT NULL DEFAULT false;

CREATE TABLE public.cuda_accuracy_state(player_name text PRIMARY KEY,revision bigint NOT NULL,version text NOT NULL,
            timezone jsonb NOT NULL,generated_at timestamptz NOT NULL DEFAULT clock_timestamp());

CREATE TABLE public.cuda_accuracy_game(player_name text,game_link bigint,day date NOT NULL,played_at timestamptz,
            mode text NOT NULL,accuracy float8,moves int NOT NULL,is_white boolean NOT NULL,PRIMARY KEY(player_name,game_link,is_white));

CREATE TABLE public.cuda_accuracy_daily(player_name text,day date,mode_mask smallint,payload jsonb NOT NULL,PRIMARY KEY(player_name,day,mode_mask));

CREATE TABLE public.cuda_accuracy_stage(run_id uuid REFERENCES public.cuda_summary_run(id),player_name text,game_link bigint,
            day date NOT NULL,played_at timestamptz,mode text NOT NULL,is_white boolean NOT NULL,
            payload jsonb NOT NULL,PRIMARY KEY(run_id,player_name,game_link,is_white));

CREATE TABLE public.cuda_accuracy_capture(run_id uuid REFERENCES public.cuda_summary_run(id),game_link bigint,revision bigint NOT NULL,PRIMARY KEY(run_id,game_link));

CREATE INDEX cuda_accuracy_game_day ON public.cuda_accuracy_game(player_name,day);

ALTER TABLE public.cuda_summary_control ADD COLUMN metadata boolean NOT NULL DEFAULT false;

CREATE TABLE public.cuda_metadata_state(player_name text PRIMARY KEY,behavior_revision bigint NOT NULL,score_revision bigint NOT NULL,
            behavior_version text NOT NULL,ending_version text NOT NULL,payload jsonb NOT NULL,
            generated_at timestamptz NOT NULL DEFAULT clock_timestamp());

CREATE TABLE public.cuda_metadata_scope(run_id uuid REFERENCES public.cuda_summary_run(id),player_id bigint NOT NULL,player_name text,behavior_revision bigint NOT NULL,
            score_revision bigint NOT NULL,behavior boolean NOT NULL,endings boolean NOT NULL,
            full_behavior boolean NOT NULL,full_endings boolean NOT NULL,profile jsonb NOT NULL,
            status text NOT NULL DEFAULT 'pending',result jsonb,PRIMARY KEY(run_id,player_name));

CREATE TABLE public.cuda_metadata_day(run_id uuid REFERENCES public.cuda_summary_run(id),player_name text,day date,
            behavior boolean NOT NULL DEFAULT false,endings boolean NOT NULL DEFAULT false,
            PRIMARY KEY(run_id,player_name,day));

CREATE TABLE public.cuda_metadata_part(run_id uuid REFERENCES public.cuda_summary_run(id),batch bigint,player_name text,day date,
            behavior jsonb,endings jsonb,PRIMARY KEY(run_id,batch,player_name,day));

CREATE TABLE public.cuda_ending_partition(player_name text,day date,payload jsonb NOT NULL,PRIMARY KEY(player_name,day));

CREATE TABLE public.cuda_metadata_baseline(run_id uuid REFERENCES public.cuda_summary_run(id),player_name text,section text,payload jsonb NOT NULL,PRIMARY KEY(run_id,player_name,section));

ALTER TABLE public.cuda_summary_control ADD COLUMN salience boolean NOT NULL DEFAULT false;

CREATE TABLE public.cuda_salience_state(player_id bigint PRIMARY KEY REFERENCES public.player(id) ON DELETE CASCADE, revision bigint NOT NULL DEFAULT 0,
            published_revision bigint NOT NULL DEFAULT -1, version text, generated_at timestamptz);

CREATE TABLE public.cuda_salience_scope(run_id uuid REFERENCES public.cuda_summary_run(id),player_id bigint,player_name text NOT NULL,
            revision bigint NOT NULL,score_revision bigint, status text NOT NULL DEFAULT 'pending',
            PRIMARY KEY(run_id,player_id));

-- Dense run-local matrix indexes never encode or replace the native game-side key.
CREATE TABLE public.cuda_salience_input (
    run_id uuid NOT NULL REFERENCES public.cuda_summary_run(id),
    input_index bigint NOT NULL CHECK(input_index>0 AND input_index<9007199254740992),
    game_link bigint NOT NULL, is_white boolean NOT NULL,
    PRIMARY KEY(run_id,input_index), UNIQUE(run_id,game_link,is_white)
);

CREATE TABLE public.cuda_salience_part(run_id uuid REFERENCES public.cuda_summary_run(id),batch int,input_index bigint,
            numerator float8 NOT NULL,positions int NOT NULL,unique_positions int NOT NULL,
            PRIMARY KEY(run_id,input_index,batch),FOREIGN KEY(run_id,input_index) REFERENCES public.cuda_salience_input(run_id,input_index));

CREATE TABLE public.cuda_salience_frequency(run_id uuid REFERENCES public.cuda_summary_run(id),owner bigint,fen_id bigint,
            games bigint NOT NULL,occurrences bigint NOT NULL,PRIMARY KEY(run_id,owner,fen_id));

CREATE TABLE public.cuda_salience_game_stage(run_id uuid REFERENCES public.cuda_summary_run(id),game_link bigint,is_white boolean,
            numerator float8 NOT NULL,positions int NOT NULL,unique_positions int NOT NULL,
            salience float8 NOT NULL CHECK(salience>0 AND salience<=1),PRIMARY KEY(run_id,game_link,is_white));

CREATE TABLE public.cuda_salience_daily(player_id bigint REFERENCES public.player(id) ON DELETE CASCADE,day date,mode_mask smallint,payload jsonb NOT NULL,
            PRIMARY KEY(player_id,day,mode_mask));

ALTER TABLE public.cuda_salience_state ADD COLUMN daily_version text;

ALTER TABLE public.cuda_salience_state ADD COLUMN timezone jsonb NOT NULL DEFAULT '{}';

INSERT INTO public.cuda_salience_state(player_id) SELECT id FROM public.player
        WHERE joined>0 AND deleted_at IS NULL ON CONFLICT DO NOTHING;

CREATE FUNCTION public.invalidate_cuda_salience()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF NEW.status='stale' THEN
            INSERT INTO public.cuda_salience_state(player_id,revision) VALUES(NEW.player_id,1)
            ON CONFLICT(player_id) DO UPDATE SET revision=cuda_salience_state.revision+1;
        END IF; RETURN NEW; END $$;

CREATE TRIGGER invalidate_cuda_salience AFTER INSERT OR UPDATE OF status
        ON public.player_salience_summary FOR EACH ROW EXECUTE FUNCTION public.invalidate_cuda_salience();

ALTER TABLE public.cuda_summary_control ADD COLUMN frequencies boolean NOT NULL DEFAULT false;

ALTER TABLE public.cuda_salience_state ADD COLUMN frequency_revision bigint;

ALTER TABLE public.cuda_salience_state ADD COLUMN frequency_behavior_revision bigint;

ALTER TABLE public.cuda_salience_state ADD COLUMN frequency_version text;

ALTER TABLE public.cuda_salience_state ADD COLUMN frequency_generation uuid;

ALTER TABLE public.cuda_salience_state ADD COLUMN frequency_generated_at timestamptz;

CREATE TABLE public.player_fen_frequency (
        player_id bigint REFERENCES public.player(id) ON DELETE CASCADE,
        is_white boolean NOT NULL,mode_id integer REFERENCES public.game_mode(id),
        fen_id bigint REFERENCES public.fen(fen_id) ON DELETE CASCADE,appearances bigint NOT NULL CHECK(appearances>0),
        PRIMARY KEY(player_id,is_white,mode_id,fen_id));

CREATE INDEX player_fen_frequency_rank ON public.player_fen_frequency(player_id,appearances DESC,fen_id,is_white) INCLUDE(mode_id);

-- Derived projection, not a second engine storage implementation. A missing or
-- partially assessed GPU count is unknown, never the retired CPU count or zero.
CREATE VIEW canonical_blunder_engine AS
SELECT e.*,CASE WHEN e.is_white AND b.white_complete THEN b.white_count
    WHEN NOT e.is_white AND b.black_complete THEN b.black_count END::integer AS blunder_count
FROM game_player_engine_summary e CROSS JOIN canonical_blunder_control c
LEFT JOIN canonical_blunder_assessment b ON b.model_id=c.active_model AND b.game_link=e.game_link;
