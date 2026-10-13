"""Rebuild compact ownership/opening projections without legacy wide writes."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for, INGESTION_COMMIT_LOCK


async def sync_projection(session, links):
    layout = storage_for(session)
    params={'links':list(links),'all_games':not links}
    # Share ingestion's bounded-commit ordering when repairing ownership.
    await session.execute(text('SELECT pg_advisory_xact_lock(:key)'),{'key':INGESTION_COMMIT_LOCK})
    await session.execute(text(f"""CREATE TEMP TABLE relational_projection_games ON COMMIT DROP AS
        SELECT g.link,CASE WHEN g.time_control LIKE '%/%' THEN 'daily'
            WHEN split_part(g.time_control,'+',1) ~ '^[0-9]+$' THEN
                CASE WHEN split_part(g.time_control,'+',1)::integer<180 THEN 'bullet'
                     WHEN split_part(g.time_control,'+',1)::integer<600 THEN 'blitz'
                     WHEN split_part(g.time_control,'+',1)::integer<=1800 THEN 'rapid' ELSE 'classical' END
            ELSE 'unknown' END AS mode FROM {layout.table('game')} g
        WHERE :all_games OR g.link=ANY(CAST(:links AS bigint[]))"""),params)
    await session.execute(text(f"""INSERT INTO {layout.table('game_mode')}(name)
        SELECT DISTINCT mode FROM relational_projection_games ORDER BY mode ON CONFLICT(name) DO NOTHING"""))
    await session.execute(text(f"""UPDATE {layout.table('game')} g
        SET mode_id=COALESCE(g.mode_id,m.id),avg_elo=COALESCE(g.avg_elo,(g.white_elo+g.black_elo)/2.0),
            played_at=COALESCE(g.played_at,make_timestamp(g.year,g.month,g.day,g.hour,g.minute,g.second::float8) AT TIME ZONE 'UTC')
        FROM relational_projection_games p,{layout.table('game_mode')} m
        WHERE p.link=g.link AND m.name=p.mode AND (g.mode_id IS NULL OR g.avg_elo IS NULL OR g.played_at IS NULL)"""))
    await session.execute(text(f"""INSERT INTO {layout.table('game_player')}(game_link,player_id,is_white)
        SELECT g.link,CASE WHEN side.is_white THEN g.white_player_id ELSE g.black_player_id END,side.is_white
        FROM {layout.table('game')} g JOIN relational_projection_games p ON p.link=g.link
        CROSS JOIN (VALUES(false),(true)) side(is_white) ORDER BY g.link,side.is_white
        ON CONFLICT(game_link,is_white) DO NOTHING"""))
    await session.execute(text(f"""CREATE TEMP TABLE relational_projection_openings ON COMMIT DROP AS
        SELECT m.link,r.n_moves,string_agg(ply.san,' ' ORDER BY m.n_move,ply.side) AS opening
        FROM {layout.table('moves')} m JOIN relational_projection_games g ON g.link=m.link
        JOIN generate_series(3,10) r(n_moves) ON m.n_move BETWEEN 1 AND r.n_moves
        CROSS JOIN LATERAL (VALUES(0,m.white_move),(1,m.black_move)) ply(side,san)
        WHERE ply.san IS NOT NULL AND ply.san NOT IN ('','--')
        GROUP BY m.link,r.n_moves HAVING count(*)=r.n_moves*2 AND count(DISTINCT m.n_move)=r.n_moves"""))
    await session.execute(text(f"""INSERT INTO {layout.table('opening_sequence')}(opening)
        SELECT DISTINCT opening COLLATE "C" FROM relational_projection_openings ORDER BY 1
        ON CONFLICT(opening) DO NOTHING"""))
    await session.execute(text(f"""INSERT INTO {layout.table('game_opening')}(link,n_moves,opening_id)
        SELECT p.link,p.n_moves,d.id FROM relational_projection_openings p
        JOIN {layout.table('opening_sequence')} d ON d.opening=p.opening
        ON CONFLICT(link,n_moves) DO UPDATE SET opening_id=EXCLUDED.opening_id"""))
