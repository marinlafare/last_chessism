"""Explicit numeric-key deletion; never removes canonical FENs or the archive."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for


async def delete_exclusive_batch(session, player, candidates):
    layout = storage_for(session)
    result = await session.execute(text(f"""SELECT gp.game_link FROM {layout.table('game_player')} gp
        JOIN {layout.table('player')} owner ON owner.id=gp.player_id
        JOIN {layout.table('game')} g ON g.link=gp.game_link
        JOIN {layout.table('player')} opponent ON opponent.id=
            CASE WHEN gp.is_white THEN g.black_player_id ELSE g.white_player_id END
        WHERE owner.player_name=:player AND gp.game_link=ANY(CAST(:links AS bigint[]))
          AND COALESCE(opponent.joined,0)=0
        ORDER BY gp.game_link FOR UPDATE OF gp,g,opponent SKIP LOCKED"""),{'player':player,'links':candidates})
    links = [int(row[0]) for row in result]
    if not links:
        return 0
    params = {'links':links}
    await session.execute(text(f"""WITH removed AS (
        SELECT fen_id,count(*)::bigint AS occurrences FROM {layout.table('game_fen_association')}
        WHERE game_link=ANY(CAST(:links AS bigint[])) GROUP BY fen_id
    ) UPDATE fen SET n_games=GREATEST(0,fen.n_games-removed.occurrences)
      FROM removed WHERE fen.fen_id=removed.fen_id"""),params)
    await session.execute(text(f"DELETE FROM {layout.table('game_fen_association')} WHERE game_link=ANY(CAST(:links AS bigint[]))"),params)
    await session.execute(text(f"DELETE FROM {layout.table('moves')} WHERE link=ANY(CAST(:links AS bigint[]))"),params)
    deleted = await session.execute(text(f"DELETE FROM {layout.table('game')} WHERE link=ANY(CAST(:links AS bigint[]))"),params)
    return int(deleted.rowcount)


async def clear_player_projections(session, player):
    layout = storage_for(session)
    owner = f"(SELECT id FROM {layout.table('player')} WHERE player_name=:player)"
    for table in ('game_player_salience','player_salience_pending_game'):
        await session.execute(text(f"""DELETE FROM {layout.table(table)} s USING {layout.table('game_player')} gp
            WHERE gp.id=s.game_player_id AND gp.player_id={owner}"""),{'player':player})
    for table in ('player_position_frequency','player_salience_summary'):
        await session.execute(text(f"DELETE FROM {layout.table(table)} WHERE player_id={owner}"),{'player':player})
