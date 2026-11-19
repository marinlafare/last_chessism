"""Exact full and incremental calculations for player corpus salience."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.database.postgresql.analysis_cleanup import finish_analysis_cleanup
from chessism_api.operations.salience_staging import stage_game_positions


INCREMENTAL_MAX_GAMES = 5_000
INCREMENTAL_MAX_CORPUS_RATIO = 0.05


logger = logging.getLogger(__name__)


async def _prepare_session_memory(session) -> None:
    """Prevent large salience sorts from using PostgreSQL's 4 MB default."""
    await session.execute(text("SET LOCAL work_mem = '256MB'"))
    await session.execute(text("SET LOCAL maintenance_work_mem = '512MB'"))


async def _full_rebuild(session, player: str) -> dict[str, Any]:
    """Build a canonical baseline from every FEN-ready game for one player."""
    await session.execute(text("""
        CREATE TEMP TABLE salience_snapshot_games
        ON COMMIT DROP AS
        SELECT gp.game_link, CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS player_color
        FROM game_player gp
        JOIN game game_row ON game_row.link = gp.game_link
        JOIN player owner ON owner.id=gp.player_id
        WHERE gp.player_id=(SELECT id FROM player WHERE player_name=:player)
          AND game_row.is_standard AND game_row.n_moves>0 AND game_row.fens_done
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_snapshot_games_key
        ON salience_snapshot_games (game_link, player_color)
    """))

    await stage_game_positions(session, incremental=False)

    await session.execute(text("""
        CREATE TEMP TABLE salience_frequency_stage
        ON COMMIT DROP AS
        SELECT
            CAST(:player AS text) AS player_name,
            player_color,
            fen_id,
            COUNT(*)::bigint AS games_with_position,
            SUM(occurrence_count)::bigint AS total_occurrences
        FROM salience_game_position_stage
        GROUP BY player_color, fen_id
        HAVING COUNT(*) > 1
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_frequency_stage_key
        ON salience_frequency_stage (player_color, fen_id)
    """))
    await session.execute(text("ANALYZE salience_frequency_stage"))

    await session.execute(text("""
        CREATE TEMP TABLE salience_game_stage
        ON COMMIT DROP AS
        SELECT
            game_position.game_link,
            CAST(:player AS text) AS player_name,
            game_position.player_color,
            SUM(
                game_position.weighted_occurrence_mass
                / COALESCE(frequency.games_with_position, 1)
            )::double precision AS weighted_numerator,
            SUM(game_position.depth_weight_sum)::double precision AS depth_weight_sum,
            SUM(
                game_position.weighted_occurrence_mass
                / COALESCE(frequency.games_with_position, 1)
            )::double precision
                / NULLIF(SUM(game_position.depth_weight_sum), 0) AS salience,
            SUM(game_position.occurrence_count)::integer
                AS position_occurrence_count,
            COUNT(*)::integer AS unique_position_count,
            SUM(game_position.occurrence_count - 1)::integer
                AS repeated_position_count
        FROM salience_game_position_stage game_position
        LEFT JOIN salience_frequency_stage frequency
          ON frequency.player_color = game_position.player_color
         AND frequency.fen_id = game_position.fen_id
        GROUP BY game_position.game_link, game_position.player_color
    """), {"player": player})

    stats = (await session.execute(text("""
        SELECT
            COUNT(*)::bigint AS source_game_count,
            COALESCE(SUM(position_occurrence_count), 0)::bigint
                AS source_position_count,
            COALESCE(SUM(salience), 0)::double precision AS effective_game_count
        FROM salience_game_stage
    """))).mappings().one()

    await persist_full(session,player)
    return {**dict(stats), "calculation": "full"}


async def _incremental_update(session, player: str) -> dict[str, Any]:
    """Apply exact frequency deltas for newly ingested FEN-ready games."""
    await session.execute(text("""
        CREATE TEMP TABLE salience_new_games
        ON COMMIT DROP AS
        SELECT pending.game_link, CASE WHEN pending.is_white THEN 'white' ELSE 'black' END AS player_color
        FROM player_salience_pending_game pending
        JOIN game game_row ON game_row.link = pending.game_link
        LEFT JOIN game_player_salience existing
          ON existing.game_link = pending.game_link
         AND existing.is_white = pending.is_white
        JOIN game_player gp ON gp.game_link=pending.game_link AND gp.is_white=pending.is_white
        WHERE gp.player_id=(SELECT id FROM player WHERE player_name=:player)
          AND game_row.is_standard AND game_row.n_moves>0 AND game_row.fens_done
          AND existing.game_link IS NULL
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_new_games_key
        ON salience_new_games (game_link, player_color)
    """))

    await stage_game_positions(session, incremental=True)

    # Materializing the player's existing game keys first is important: joining
    # changed opening FENs directly against the global association table makes
    # PostgreSQL visit millions of other players' opening occurrences.
    await session.execute(text("""
        CREATE TEMP TABLE salience_existing_game
        ON COMMIT DROP AS
        SELECT s.game_link, CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS player_color
        FROM game_player_salience s JOIN game_player gp
          ON gp.game_link=s.game_link AND gp.is_white=s.is_white
        WHERE gp.player_id=(SELECT id FROM player WHERE player_name=:player)
    """), {"player": player})
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_existing_game_key
        ON salience_existing_game (game_link, player_color)
    """))
    await session.execute(text("ANALYZE salience_existing_game"))

    await session.execute(text("""
        CREATE TEMP TABLE salience_changed_fen
        ON COMMIT DROP AS
        SELECT DISTINCT player_color, fen_id
        FROM salience_new_game_position
    """))
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_changed_fen_key
        ON salience_changed_fen (player_color, fen_id)
    """))
    await session.execute(text("ANALYZE salience_changed_fen"))

    # This compact stage serves both frequency discovery and historical score
    # adjustment. Persisting singleton frequencies would duplicate almost every
    # game position, so an absent persistent row deliberately means frequency 1.
    await session.execute(text("""
        CREATE TEMP TABLE salience_existing_game_position
        ON COMMIT DROP AS
        WITH ordered_occurrences AS (
            SELECT
                existing.game_link,
                existing.player_color,
                association.fen_id,
                (
                    association.n_move * 2
                    - CASE WHEN association.move_color = 'white' THEN 1 ELSE 0 END
                )::integer AS ply,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        existing.game_link,
                        existing.player_color,
                        association.fen_id
                    ORDER BY
                        association.n_move,
                        CASE WHEN association.move_color = 'white' THEN 1 ELSE 2 END
                )::integer AS occurrence_index
            FROM salience_existing_game existing
            CROSS JOIN LATERAL (
                SELECT
                    game_fen_association.fen_id,
                    game_fen_association.n_move,
                    CASE WHEN game_fen_association.is_white THEN 'white' ELSE 'black' END AS move_color
                FROM game_fen_association
                WHERE game_fen_association.game_link = existing.game_link AND game_fen_association.n_move>0
                OFFSET 0
            ) association
            JOIN salience_changed_fen changed
              ON changed.player_color = existing.player_color
             AND changed.fen_id = association.fen_id
        )
        SELECT
            game_link,
            player_color,
            fen_id,
            COUNT(*)::integer AS occurrence_count,
            SUM(
                (
                    0.25 + 0.75 * LEAST(ply::double precision / 16.0, 1.0)
                ) / occurrence_index
            )::double precision AS weighted_occurrence_mass
        FROM ordered_occurrences
        GROUP BY game_link, player_color, fen_id
    """))
    await session.execute(text("""
        CREATE INDEX salience_existing_game_position_frequency_idx
        ON salience_existing_game_position (player_color, fen_id)
    """))
    await session.execute(text("ANALYZE salience_existing_game_position"))

    await session.execute(text("""
        CREATE TEMP TABLE salience_frequency_change
        ON COMMIT DROP AS
        WITH existing_frequency AS (
            SELECT
                player_color,
                fen_id,
                COUNT(*)::bigint AS game_frequency,
                SUM(occurrence_count)::bigint AS total_occurrences
            FROM salience_existing_game_position
            GROUP BY player_color, fen_id
        ),
        new_frequency AS (
            SELECT
                player_color,
                fen_id,
                COUNT(*)::bigint AS game_frequency,
                SUM(occurrence_count)::bigint AS total_occurrences
            FROM salience_new_game_position
            GROUP BY player_color, fen_id
        )
        SELECT
            new_frequency.player_color,
            new_frequency.fen_id,
            COALESCE(existing_frequency.game_frequency, 0)::bigint
                AS old_game_frequency,
            (
                COALESCE(existing_frequency.game_frequency, 0)
                + new_frequency.game_frequency
            )::bigint AS new_game_frequency,
            (
                COALESCE(existing_frequency.total_occurrences, 0)
                + new_frequency.total_occurrences
            )::bigint AS new_total_occurrences
        FROM new_frequency
        LEFT JOIN existing_frequency
          ON existing_frequency.player_color = new_frequency.player_color
         AND existing_frequency.fen_id = new_frequency.fen_id
    """))
    await session.execute(text("""
        CREATE UNIQUE INDEX salience_frequency_change_key
        ON salience_frequency_change (player_color, fen_id)
    """))
    await session.execute(text("ANALYZE salience_frequency_change"))

    # Only historical games containing a changed FEN need a score adjustment.
    # The exact delta is A(game,FEN) * (1/new_frequency - 1/old_frequency).
    await session.execute(text("""
        CREATE TEMP TABLE salience_existing_delta
        ON COMMIT DROP AS
        SELECT
            existing.game_link,
            existing.player_color,
            SUM(
                existing.weighted_occurrence_mass
                * (
                    1.0 / frequency.new_game_frequency
                    - 1.0 / frequency.old_game_frequency
                )
            )::double precision AS numerator_delta
        FROM salience_existing_game_position existing
        JOIN salience_frequency_change frequency
          ON frequency.player_color = existing.player_color
         AND frequency.fen_id = existing.fen_id
        WHERE frequency.old_game_frequency > 0
        GROUP BY existing.game_link, existing.player_color
    """))
    return await persist_incremental(session,player)


async def calculate_player_salience(player_name: str) -> dict[str, Any]:
    """Refresh one player with an exact incremental or canonical full pass."""
    player = str(player_name).strip().lower()
    if not player:
        raise ValueError("Player name is required.")
    return await calculate(AsyncDBSession, player)


async def persist_full(session, player):
    owner = f"(SELECT id FROM player WHERE player_name=:player)"
    params = {'player': player}
    await session.execute(text(f"DELETE FROM player_position_frequency WHERE player_id={owner}"),params)
    await session.execute(text(f"""INSERT INTO player_position_frequency
        (player_id,is_white,fen_id,games_with_position,total_occurrences)
        SELECT {owner},player_color='white',fen_id,games_with_position,total_occurrences
        FROM salience_frequency_stage WHERE games_with_position>1"""),params)
    await session.execute(text(f"""DELETE FROM game_player_salience s
        USING game_player gp WHERE gp.game_link=s.game_link AND gp.is_white=s.is_white AND gp.player_id={owner}"""),params)
    await session.execute(text(f"""INSERT INTO game_player_salience
        (game_link,is_white,salience,weighted_numerator,depth_weight_sum,position_occurrence_count,unique_position_count)
        SELECT gp.game_link,gp.is_white,s.salience,s.weighted_numerator,s.depth_weight_sum,s.position_occurrence_count,s.unique_position_count
        FROM salience_game_stage s JOIN game_player gp
          ON gp.game_link=s.game_link AND gp.is_white=(s.player_color='white') AND gp.player_id={owner}"""),params)
    await clear_pending(session,player,'salience_snapshot_games')


async def clear_pending(session, player, snapshot):
    if snapshot not in ('salience_snapshot_games','salience_new_games'):
        raise ValueError('Invalid salience stage')
    await session.execute(text(f"""DELETE FROM player_salience_pending_game pending
        USING game_player gp,player p,{snapshot} snapshot
        WHERE pending.game_link=gp.game_link AND pending.is_white=gp.is_white AND gp.player_id=p.id AND p.player_name=:player
          AND gp.game_link=snapshot.game_link AND gp.is_white=(snapshot.player_color='white')"""),{'player':player})


async def persist_incremental(session, player):
    owner = f"(SELECT id FROM player WHERE player_name=:player)"
    params = {'player':player}
    await session.execute(text(f"""UPDATE game_player_salience s
        SET weighted_numerator=s.weighted_numerator+d.numerator_delta,
            salience=(s.weighted_numerator+d.numerator_delta)/NULLIF(s.depth_weight_sum,0)
        FROM salience_existing_delta d,game_player gp
        WHERE s.game_link=gp.game_link AND s.is_white=gp.is_white AND gp.player_id={owner}
          AND gp.game_link=d.game_link AND gp.is_white=(d.player_color='white')"""),params)
    await session.execute(text(f"""INSERT INTO game_player_salience
        (game_link,is_white,salience,weighted_numerator,depth_weight_sum,position_occurrence_count,unique_position_count)
        SELECT gp.game_link,gp.is_white,
            SUM(n.weighted_occurrence_mass/f.new_game_frequency)/NULLIF(SUM(n.depth_weight_sum),0),
            SUM(n.weighted_occurrence_mass/f.new_game_frequency)::float8,
            SUM(n.depth_weight_sum)::float8,SUM(n.occurrence_count)::integer,COUNT(*)::integer
        FROM salience_new_game_position n JOIN salience_frequency_change f
          ON f.player_color=n.player_color AND f.fen_id=n.fen_id
        JOIN game_player gp
          ON gp.game_link=n.game_link AND gp.is_white=(n.player_color='white') AND gp.player_id={owner}
        GROUP BY gp.game_link,gp.is_white"""),params)
    await session.execute(text(f"""INSERT INTO player_position_frequency
        (player_id,is_white,fen_id,games_with_position,total_occurrences)
        SELECT {owner},player_color='white',fen_id,new_game_frequency,new_total_occurrences
        FROM salience_frequency_change WHERE new_game_frequency>1
        ON CONFLICT(player_id,is_white,fen_id) DO UPDATE SET
            games_with_position=EXCLUDED.games_with_position,total_occurrences=EXCLUDED.total_occurrences"""),params)
    await clear_pending(session,player,'salience_new_games')
    stats = (await session.execute(text(f"""SELECT COUNT(*)::bigint AS source_game_count,
        COALESCE(SUM(s.position_occurrence_count),0)::bigint AS source_position_count,
        COALESCE(SUM(s.salience),0)::float8 AS effective_game_count
        FROM game_player_salience s JOIN game_player gp ON gp.game_link=s.game_link AND gp.is_white=s.is_white
        WHERE gp.player_id={owner}"""),params)).mappings().one()
    return {**dict(stats),'calculation':'incremental'}


async def calculate(factory, player):
    async with write_transaction(factory) as session:
        from .cuda_summaries.salience_store import enabled
        if await enabled(session):
            raise ValueError('Salience is CUDA-only; request the manual summary pipeline.')
        summary = 'player_salience_summary'
        owner = await session.scalar(text(f"""SELECT id FROM player
            WHERE player_name=:player AND COALESCE(joined,0)>0 AND deleted_at IS NULL"""),{'player':player})
        if owner is None:
            raise ValueError(f'Tracked player {player!r} was not found.')
        await session.execute(text(f"""INSERT INTO {summary}(player_id,status,source_game_count,
            source_position_count,effective_game_count,error) VALUES(:id,'running',0,0,0,NULL)
            ON CONFLICT(player_id) DO UPDATE SET status='running',error=NULL"""),{'id':owner})
    try:
        async with write_transaction(factory) as session:
            # Serialize one player's refresh and block stale notifications
            # until commit; new ingested games then leave it stale, not lost.
            baseline = await session.scalar(text(f"SELECT source_game_count FROM {summary} WHERE player_id=:id FOR UPDATE"),{'id':owner})
            pending = await session.scalar(text(f"""SELECT count(*) FROM player_salience_pending_game p
                JOIN game_player gp ON gp.game_link=p.game_link AND gp.is_white=p.is_white
                JOIN game g ON g.link=gp.game_link
                LEFT JOIN game_player_salience s ON s.game_link=gp.game_link AND s.is_white=gp.is_white
                WHERE gp.player_id=:id AND g.is_standard AND g.n_moves>0 AND g.fens_done AND s.game_link IS NULL"""),{'id':owner})
            incremental = baseline > 0 and 0 < pending <= min(5000,max(1,int(baseline*0.05)))
            await _prepare_session_memory(session)
            stats = await (_incremental_update(session,player) if incremental else _full_rebuild(session,player))
            # Pending extraction work cannot be labelled ready.
            status = await session.scalar(text(f"""UPDATE {summary} SET
                status=CASE WHEN EXISTS(SELECT 1 FROM player_salience_pending_game p
                    JOIN game_player gp ON gp.game_link=p.game_link AND gp.is_white=p.is_white WHERE gp.player_id=:id)
                    THEN 'stale' ELSE 'ready' END,
                source_game_count=:source_game_count,source_position_count=:source_position_count,
                effective_game_count=:effective_game_count,error=NULL WHERE player_id=:id RETURNING status"""),dict(stats,id=owner))
        return {'player_name':player,'status':status,'pending_games':int(pending),
                **stats,'effective_game_count':round(float(stats['effective_game_count']),6)}
    except BaseException:
        async def record_failure():
            async with write_transaction(factory) as session:
                await session.execute(text("""UPDATE player_salience_summary
                    SET status='failed',error='Salience calculation failed'
                    WHERE player_id=:id AND status<>'stale'"""),{'id':owner})
        try:
            await finish_analysis_cleanup(record_failure())
        except BaseException:
            logger.exception('Could not record failed salience; preserving original failure')
        raise
