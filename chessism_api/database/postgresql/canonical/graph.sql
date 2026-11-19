-- Canonical graph outbox and immutable map projections. No graph service is contacted.

CREATE SCHEMA graph_sync;
        CREATE TABLE graph_sync.control (
            id boolean PRIMARY KEY DEFAULT true CHECK(id), enabled boolean NOT NULL DEFAULT false,
            version text NOT NULL, heartbeat timestamptz, phase text NOT NULL DEFAULT 'disabled',
            detail text, games bigint NOT NULL DEFAULT 0, positions bigint NOT NULL DEFAULT 0,
            occurrences bigint NOT NULL DEFAULT 0, moves bigint NOT NULL DEFAULT 0,
            processed_events bigint NOT NULL DEFAULT 0, last_batch_seconds double precision,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
        INSERT INTO graph_sync.control(id,version) VALUES(true,'chess_graph_v1');
        CREATE TABLE graph_sync.event (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            kind text NOT NULL CHECK(kind IN ('game','score','player')),
            ids bigint[] NOT NULL CHECK(cardinality(ids) BETWEEN 1 AND 1000),
            offset_items integer NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            completed_at timestamptz, attempts integer NOT NULL DEFAULT 0,
            retry_at timestamptz, error text, batch_id uuid);
        CREATE INDEX graph_sync_pending ON graph_sync.event(id) WHERE completed_at IS NULL;
        CREATE INDEX graph_sync_event_created ON graph_sync.event(created_at);
        ALTER TABLE graph_sync.event ADD COLUMN source_job_id text;
        ALTER TABLE graph_sync.event ADD COLUMN matched_ids bigint[];
        ALTER TABLE graph_sync.event ADD COLUMN started_at timestamptz;
        ALTER TABLE graph_sync.control ADD COLUMN cloud_tracking_since timestamptz;
        CREATE INDEX graph_sync_event_cloud ON graph_sync.event(source_job_id)
            WHERE source_job_id IS NOT NULL;
        CREATE TABLE graph_sync.backfill (
            id uuid PRIMARY KEY, player_id bigint NOT NULL, player_name text NOT NULL,
            game_ids bigint[] NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
        ALTER TABLE graph_sync.backfill ADD COLUMN verification jsonb;
        CREATE FUNCTION graph_sync.enqueue(event_kind text, entity_ids bigint[]) RETURNS void
        LANGUAGE plpgsql AS $$ BEGIN
            IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN; END IF;
            INSERT INTO graph_sync.event(kind,ids)
            SELECT event_kind,array_agg(value ORDER BY value) FROM (
                SELECT value,(row_number() OVER(ORDER BY value)-1)/1000 AS chunk
                FROM (SELECT DISTINCT unnest(entity_ids) AS value) d WHERE value IS NOT NULL
            ) x GROUP BY chunk;
        END $$;

CREATE FUNCTION graph_sync.capture_game_insert() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(link) FROM new_rows WHERE is_standard AND n_moves>0 AND fens_done INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_game_insert AFTER INSERT ON public.game
            REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_game_insert();

CREATE FUNCTION graph_sync.capture_game_delete() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(link) FROM old_rows INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_game_delete AFTER DELETE ON public.game
            REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_game_delete();

CREATE FUNCTION graph_sync.capture_game_update() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(n.link)
            FROM new_rows n JOIN old_rows o USING(link) WHERE (n.fens_done OR o.fens_done) AND
            (n.fens_done,n.is_standard,n.white_player_id,n.black_player_id,n.mode_id,n.played_at,n.white_result,
             n.black_result,n.white_str_result,n.black_str_result,n.n_moves)
            IS DISTINCT FROM
            (o.fens_done,o.is_standard,o.white_player_id,o.black_player_id,o.mode_id,o.played_at,o.white_result,
             o.black_result,o.white_str_result,o.black_str_result,o.n_moves) INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_game_update AFTER UPDATE ON public.game
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_game_update();

CREATE FUNCTION graph_sync.capture_player_update() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(n.id)
            FROM new_rows n JOIN old_rows o USING(id) WHERE
            (n.player_name,n.joined,n.deleted_at) IS DISTINCT FROM (o.player_name,o.joined,o.deleted_at) INTO affected;
                PERFORM graph_sync.enqueue('player',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_player_update AFTER UPDATE ON public.player
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_player_update();

CREATE FUNCTION graph_sync.capture_player_delete() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(id) FROM old_rows INTO affected;
                PERFORM graph_sync.enqueue('player',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_player_delete AFTER DELETE ON public.player
            REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_player_delete();

CREATE FUNCTION graph_sync.capture_moves_update() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(g.link) FROM public.game g
                JOIN (SELECT link AS id FROM old_rows UNION SELECT link AS id FROM new_rows) changed ON changed.id=g.link WHERE g.fens_done INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_moves_update AFTER UPDATE ON public.moves
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_moves_update();

CREATE FUNCTION graph_sync.capture_moves_delete() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(g.link) FROM public.game g
                JOIN (SELECT link AS id FROM old_rows) changed ON changed.id=g.link WHERE g.fens_done INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_moves_delete AFTER DELETE ON public.moves
            REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_moves_delete();

CREATE FUNCTION graph_sync.capture_game_fen_association_update() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(g.link) FROM public.game g
                JOIN (SELECT game_link AS id FROM old_rows UNION SELECT game_link AS id FROM new_rows) changed ON changed.id=g.link WHERE g.fens_done INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_game_fen_association_update AFTER UPDATE ON public.game_fen_association
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_game_fen_association_update();

CREATE FUNCTION graph_sync.capture_game_fen_association_delete() RETURNS trigger
            LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
                IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
                SELECT array_agg(g.link) FROM public.game g
                JOIN (SELECT game_link AS id FROM old_rows) changed ON changed.id=g.link WHERE g.fens_done INTO affected;
                PERFORM graph_sync.enqueue('game',affected); RETURN NULL;
            END $$;
            CREATE TRIGGER chessism_capture_game_fen_association_delete AFTER DELETE ON public.game_fen_association
            REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_game_fen_association_delete();

CREATE FUNCTION graph_sync.capture_fen_update() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
          IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
          SELECT array_agg(fen_id) INTO affected FROM (
            SELECT fen_id,score,analysis_source,tablebase_wdl,tablebase_dtz FROM new_rows
            EXCEPT
            SELECT fen_id,score,analysis_source,tablebase_wdl,tablebase_dtz FROM old_rows
          ) changed;
          IF affected IS NULL THEN RETURN NULL; END IF;
          
          INSERT INTO graph_sync.event(kind,ids,source_job_id)
          SELECT 'score',array_agg(fen_id ORDER BY fen_id),job_id FROM (
            SELECT n.fen_id,r.job_id,
              (row_number() OVER(PARTITION BY r.job_id ORDER BY n.fen_id)-1)/1000 AS chunk
            FROM unnest(affected) changed(fen_id)
            JOIN public.fen n USING(fen_id)
            LEFT JOIN public.cloud_fen_claim c ON c.fen=n.fen
            LEFT JOIN public.cloud_analysis_run r ON r.id=c.run_id
          ) grouped GROUP BY job_id,chunk ORDER BY job_id NULLS LAST,chunk;
        
          RETURN NULL;
        END $$;

CREATE TRIGGER chessism_capture_fen_update
        AFTER UPDATE ON public.fen
        REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
        FOR EACH STATEMENT EXECUTE FUNCTION graph_sync.capture_fen_update();

UPDATE graph_sync.control
            SET cloud_tracking_since=COALESCE(cloud_tracking_since,clock_timestamp()) WHERE id;

CREATE SCHEMA graph_map;
        CREATE TABLE graph_map.control (
            id boolean PRIMARY KEY DEFAULT true CHECK(id),revision bigint NOT NULL DEFAULT 1,
            active_snapshot uuid,updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
        INSERT INTO graph_map.control(id) VALUES(true);
        CREATE TABLE graph_map.snapshot (
            id uuid PRIMARY KEY,revision bigint NOT NULL,version text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            games integer NOT NULL,nodes integer NOT NULL,connections integer NOT NULL,max_ply integer NOT NULL);
        CREATE TABLE graph_map.game (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,ordinal integer NOT NULL,
            game_id bigint NOT NULL,source_hash text NOT NULL,white_id bigint,black_id bigint,
            white_name text,black_name text,played_at text,PRIMARY KEY(snapshot,ordinal),UNIQUE(snapshot,game_id));
        CREATE TABLE graph_map.node (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,key text NOT NULL,
            ply integer NOT NULL,kind text NOT NULL,fen_key text,label text,
            games integer NOT NULL,x integer NOT NULL,y integer NOT NULL,members integer[] NOT NULL,
            PRIMARY KEY(snapshot,key));
        CREATE INDEX graph_map_node_view ON graph_map.node(snapshot,ply,y);
        CREATE TABLE graph_map.connection (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,key text NOT NULL,
            source text NOT NULL,target text NOT NULL,kind text NOT NULL,hidden integer NOT NULL,
            reason text NOT NULL,label text,games integer NOT NULL,members integer[] NOT NULL,
            PRIMARY KEY(snapshot,key));
        CREATE INDEX graph_map_connection_source ON graph_map.connection(snapshot,source);
        CREATE INDEX graph_map_connection_target ON graph_map.connection(snapshot,target);
        CREATE TABLE graph_map.segment (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,connection text NOT NULL,
            ordinal integer NOT NULL,start_ply integer NOT NULL,end_ply integer NOT NULL,reason text NOT NULL,
            PRIMARY KEY(snapshot,connection,ordinal));
        CREATE TABLE graph_map.player (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,player_id bigint NOT NULL,
            name text NOT NULL,white_games integer[] NOT NULL,black_games integer[] NOT NULL,
            PRIMARY KEY(snapshot,player_id));
        CREATE INDEX graph_map_player_name ON graph_map.player(snapshot,name);
        CREATE TABLE graph_map.input (
            run_id uuid NOT NULL,game_id bigint NOT NULL,payload jsonb NOT NULL,
            PRIMARY KEY(run_id,game_id));
        CREATE FUNCTION graph_map.invalidate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF EXISTS(SELECT 1 FROM added WHERE kind IN ('game','player')) THEN
                UPDATE graph_map.control SET revision=revision+1,updated_at=clock_timestamp() WHERE id;
            END IF;
            RETURN NULL;
        END $$;
        CREATE TRIGGER chessism_ply_map_dirty AFTER INSERT ON graph_sync.event
            REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION graph_map.invalidate();
