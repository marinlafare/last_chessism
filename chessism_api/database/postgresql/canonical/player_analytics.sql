-- Derived game-metadata summaries. Ownership is the canonical player identity.
CREATE TABLE player_analytics_state (
    player_id bigint PRIMARY KEY REFERENCES player(id) ON DELETE CASCADE,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision>0),
    full_rebuild boolean NOT NULL DEFAULT true,
    published_revision bigint NOT NULL DEFAULT 0 CHECK (published_revision>=0 AND published_revision<=revision),
    attempted_at timestamptz, error text
);
CREATE TABLE player_analytics_dirty (
    player_id bigint NOT NULL REFERENCES player_analytics_state(player_id) ON DELETE CASCADE,
    day date NOT NULL, PRIMARY KEY(player_id,day)
);
CREATE TABLE player_analytics_partition (
    player_id bigint NOT NULL REFERENCES player_analytics_state(player_id) ON DELETE CASCADE,
    day date NOT NULL, payload jsonb NOT NULL, PRIMARY KEY(player_id,day)
);
CREATE TABLE player_analytics_snapshot (
    player_id bigint NOT NULL REFERENCES player_analytics_state(player_id) ON DELETE CASCADE,
    section text NOT NULL CHECK (section IN ('hours','activity','activity_daily','ratings','distribution','opponents','openings')),
    revision bigint NOT NULL CHECK (revision>0),
    generated_at timestamptz NOT NULL DEFAULT clock_timestamp(), payload jsonb NOT NULL,
    PRIMARY KEY(player_id,section)
);

CREATE FUNCTION queue_player_analytics(owner_id bigint, changed_day date, rebuild boolean DEFAULT false)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.player WHERE id=owner_id AND joined>0 AND deleted_at IS NULL)
       AND NOT EXISTS(SELECT 1 FROM public.player_analytics_state WHERE player_id=owner_id) THEN RETURN; END IF;
    INSERT INTO public.player_analytics_state AS current(player_id) VALUES(owner_id)
        ON CONFLICT(player_id) DO UPDATE SET revision=current.revision+1,
            full_rebuild=current.full_rebuild OR rebuild,error=NULL;
    IF changed_day IS NOT NULL THEN
        INSERT INTO public.player_analytics_dirty(player_id,day) VALUES(owner_id,changed_day)
            ON CONFLICT DO NOTHING;
    END IF;
END $$;

CREATE FUNCTION player_analytics_game_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
    FOR item IN SELECT DISTINCT owner.id,(g.played_at AT TIME ZONE 'UTC')::date AS day
        FROM new_rows g CROSS JOIN LATERAL (VALUES(g.white_player_id),(g.black_player_id)) owner(id)
        WHERE g.is_standard AND g.n_moves>0 ORDER BY owner.id,day LOOP
        PERFORM public.queue_player_analytics(item.id,item.day);
    END LOOP; RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_game_insert AFTER INSERT ON game
REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_game_insert();

CREATE FUNCTION player_analytics_game_delete() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
    FOR item IN SELECT DISTINCT owner.id,(g.played_at AT TIME ZONE 'UTC')::date AS day
        FROM old_rows g CROSS JOIN LATERAL (VALUES(g.white_player_id),(g.black_player_id)) owner(id)
        WHERE g.is_standard AND g.n_moves>0 ORDER BY owner.id,day LOOP
        PERFORM public.queue_player_analytics(item.id,item.day);
    END LOOP; RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_game_delete AFTER DELETE ON game
REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_game_delete();

CREATE FUNCTION player_analytics_game_update() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
    FOR item IN WITH changed AS (
        SELECT o.link AS old_link,n.link AS new_link FROM old_rows o FULL JOIN new_rows n USING(link)
        WHERE (o.white_player_id,o.black_player_id,o.mode_id,o.played_at,o.white_elo,o.black_elo,
               o.white_result,o.black_result,o.time_elapsed,o.eco,o.is_standard,o.n_moves>0)
          IS DISTINCT FROM
              (n.white_player_id,n.black_player_id,n.mode_id,n.played_at,n.white_elo,n.black_elo,
               n.white_result,n.black_result,n.time_elapsed,n.eco,n.is_standard,n.n_moves>0)
    ), games AS (
        SELECT g.* FROM old_rows g JOIN changed c ON c.old_link=g.link
        UNION ALL SELECT g.* FROM new_rows g JOIN changed c ON c.new_link=g.link
    ) SELECT DISTINCT owner.id,(g.played_at AT TIME ZONE 'UTC')::date AS day
        FROM games g CROSS JOIN LATERAL (VALUES(g.white_player_id),(g.black_player_id)) owner(id)
        WHERE g.is_standard AND g.n_moves>0 ORDER BY owner.id,day LOOP
        PERFORM public.queue_player_analytics(item.id,item.day);
    END LOOP; RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_game_update AFTER UPDATE ON game
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_game_update();

CREATE FUNCTION player_analytics_player_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
    FOR item IN SELECT id FROM new_rows WHERE joined>0 AND deleted_at IS NULL ORDER BY id LOOP
        PERFORM public.queue_player_analytics(item.id,NULL,true);
    END LOOP; RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_player_insert AFTER INSERT ON player
REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_player_insert();

CREATE FUNCTION player_analytics_player_update() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
    FOR item IN WITH changed AS (
        SELECT n.id,o.player_name IS DISTINCT FROM n.player_name AS renamed
        FROM old_rows o JOIN new_rows n USING(id)
        WHERE (o.player_name,o.timezone,o.timezone_source,o.country,o.location,o.joined,o.deleted_at)
            IS DISTINCT FROM (n.player_name,n.timezone,n.timezone_source,n.country,n.location,n.joined,n.deleted_at)
    ), affected AS (
        SELECT id FROM changed
        UNION SELECT owner.id FROM changed p JOIN public.game g
            ON p.renamed AND p.id IN(g.white_player_id,g.black_player_id)
            CROSS JOIN LATERAL (VALUES(g.white_player_id),(g.black_player_id)) owner(id)
            WHERE g.is_standard AND g.n_moves>0
    ) SELECT id FROM affected ORDER BY id LOOP
        PERFORM public.queue_player_analytics(item.id,NULL,true);
    END LOOP; RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_player_update AFTER UPDATE ON player
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_player_update();

CREATE FUNCTION player_analytics_mode_update() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE item record; BEGIN
    FOR item IN SELECT DISTINCT owner.id,(g.played_at AT TIME ZONE 'UTC')::date AS day
        FROM old_rows o JOIN new_rows n USING(id) JOIN public.game g ON g.mode_id=n.id
        CROSS JOIN LATERAL (VALUES(g.white_player_id),(g.black_player_id)) owner(id)
        WHERE o.name IS DISTINCT FROM n.name AND g.is_standard AND g.n_moves>0 ORDER BY owner.id,day LOOP
        PERFORM public.queue_player_analytics(item.id,item.day);
    END LOOP; RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_mode_update AFTER UPDATE ON game_mode
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_mode_update();

CREATE FUNCTION player_analytics_game_truncate() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
    UPDATE public.player_analytics_state SET revision=revision+1,full_rebuild=true,error=NULL;
    RETURN NULL;
END $$;
CREATE TRIGGER player_analytics_game_truncate AFTER TRUNCATE ON game
FOR EACH STATEMENT EXECUTE FUNCTION player_analytics_game_truncate();
