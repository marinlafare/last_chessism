"""Metadata-only inputs; one final FEN lookup, never every game position."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from .metadata_store import SCOPE,DAY


def query(layout):
    t=layout.table
    day="COALESCE((g.played_at AT TIME ZONE 'UTC')::date,DATE '-infinity')"
    return f'''WITH selected AS MATERIALIZED (
        SELECT gp.id AS cursor,p.player_name,g.link,m.name AS mode,g.played_at,g.time_elapsed,g.eco,g.n_moves,
        CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
        CASE WHEN gp.is_white THEN g.white_elo ELSE g.black_elo END AS rating,
        CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
        opponent.player_name AS opponent_name,
        CASE WHEN gp.is_white THEN g.white_str_result ELSE g.black_str_result END AS own_reason,
        CASE WHEN gp.is_white THEN g.black_str_result ELSE g.white_str_result END AS opponent_reason,
        CASE WHEN gp.is_white THEN g.black_result ELSE g.white_result END AS opponent_result,
        c.total_positions,c.analyzed_positions,c.unscored_positions,c.is_fully_analyzed AS fully_analyzed,
        (s.behavior AND (s.full_behavior OR COALESCE(d.behavior,false))) AS take_behavior,
        (s.endings AND (s.full_endings OR COALESCE(d.endings,false))) AS take_endings
        FROM {SCOPE} s JOIN {t('player')} p USING(player_name)
        JOIN {t('game_player')} gp ON gp.player_id=p.id
        JOIN {t('game')} g ON g.link=gp.game_link LEFT JOIN {t('game_mode')} m ON m.id=g.mode_id
        LEFT JOIN {DAY} d ON d.run_id=s.run_id AND d.player_name=s.player_name AND d.day={day}
        LEFT JOIN {t('player')} opponent ON opponent.id=CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
        LEFT JOIN {t('game_analysis_coverage')} c ON c.link=g.link
        WHERE s.run_id=:id AND s.status='pending' AND p.joined>0 AND p.deleted_at IS NULL
            AND gp.id>:after AND ((gp.is_white AND g.white_player_id=p.id) OR (NOT gp.is_white AND g.black_player_id=p.id))
            AND ((s.behavior AND (s.full_behavior OR COALESCE(d.behavior,false)))
                 OR (s.endings AND (s.full_endings OR COALESCE(d.endings,false))))
        ORDER BY gp.id LIMIT :limit
        ) SELECT s.*,f.fen_id IS NOT NULL AS final_exists,f.score AS final_score,
            (f.analysis_source='tablebase' OR f.tablebase_wdl IS NOT NULL) AS final_tablebase
        FROM selected s
        LEFT JOIN LATERAL (
            SELECT mv.n_move,CASE WHEN LOWER(BTRIM(mv.white_move)) NOT IN ('','--','none','null') THEN
                CASE WHEN LOWER(BTRIM(mv.black_move)) NOT IN ('','--','none','null') THEN false
                     WHEN BTRIM(mv.black_move)='--' THEN true END END AS is_white
            FROM {t('moves')} mv WHERE mv.link=s.link AND s.take_endings AND s.mode IN ('bullet','blitz','rapid')
                AND (LOWER(BTRIM(s.own_reason)) IN ('timeout','resigned','resignation')
                    OR LOWER(BTRIM(s.opponent_reason)) IN ('timeout','resigned','resignation'))
            ORDER BY mv.n_move DESC LIMIT 1
        ) last_move ON last_move.n_move=s.n_moves AND s.n_moves>0
        LEFT JOIN LATERAL (
            SELECT max(a.fen_id) AS fen_id FROM {t('game_fen_association')} a
            WHERE a.game_link=s.link AND a.n_move=last_move.n_move AND a.is_white=last_move.is_white
            HAVING count(*)=1
        ) final_position ON true LEFT JOIN {t('fen')} f ON f.fen_id=final_position.fen_id
        ORDER BY s.cursor'''


async def read(session,run_id,after,limit=65536):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='45s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    return [dict(r) for r in (await session.execute(text(query(storage_for(session))),
        {'id':run_id,'after':after,'limit':limit})).mappings()]
