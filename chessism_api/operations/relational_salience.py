"""Numeric persistence for the existing full/incremental salience calculation."""
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for


async def persist_full(session, player):
    layout = storage_for(session)
    owner = f"(SELECT id FROM {layout.table('player')} WHERE player_name=:player)"
    params = {'player': player}
    await session.execute(text(f"DELETE FROM {layout.table('player_position_frequency')} WHERE player_id={owner}"),params)
    await session.execute(text(f"""INSERT INTO {layout.table('player_position_frequency')}
        (player_id,is_white,fen_id,games_with_position,total_occurrences)
        SELECT {owner},player_color='white',fen_id,games_with_position,total_occurrences
        FROM salience_frequency_stage WHERE games_with_position>1"""),params)
    await session.execute(text(f"""DELETE FROM {layout.table('game_player_salience')} s
        USING {layout.table('game_player')} gp WHERE gp.id=s.game_player_id AND gp.player_id={owner}"""),params)
    await session.execute(text(f"""INSERT INTO {layout.table('game_player_salience')}
        (game_player_id,salience,weighted_numerator,depth_weight_sum,position_occurrence_count,unique_position_count)
        SELECT gp.id,s.salience,s.weighted_numerator,s.depth_weight_sum,s.position_occurrence_count,s.unique_position_count
        FROM salience_game_stage s JOIN {layout.table('game_player')} gp
          ON gp.game_link=s.game_link AND gp.is_white=(s.player_color='white') AND gp.player_id={owner}"""),params)
    await clear_pending(session,player,'salience_snapshot_games')


async def clear_pending(session, player, snapshot):
    if snapshot not in ('salience_snapshot_games','salience_new_games'):
        raise ValueError('Invalid salience stage')
    layout = storage_for(session)
    await session.execute(text(f"""DELETE FROM {layout.table('player_salience_pending_game')} pending
        USING {layout.table('game_player')} gp,{layout.table('player')} p,{snapshot} snapshot
        WHERE pending.game_player_id=gp.id AND gp.player_id=p.id AND p.player_name=:player
          AND gp.game_link=snapshot.game_link AND gp.is_white=(snapshot.player_color='white')"""),{'player':player})


async def persist_incremental(session, player):
    layout = storage_for(session)
    owner = f"(SELECT id FROM {layout.table('player')} WHERE player_name=:player)"
    params = {'player':player}
    await session.execute(text(f"""UPDATE {layout.table('game_player_salience')} s
        SET weighted_numerator=s.weighted_numerator+d.numerator_delta,
            salience=(s.weighted_numerator+d.numerator_delta)/NULLIF(s.depth_weight_sum,0)
        FROM salience_existing_delta d,{layout.table('game_player')} gp
        WHERE s.game_player_id=gp.id AND gp.player_id={owner}
          AND gp.game_link=d.game_link AND gp.is_white=(d.player_color='white')"""),params)
    await session.execute(text(f"""INSERT INTO {layout.table('game_player_salience')}
        (game_player_id,salience,weighted_numerator,depth_weight_sum,position_occurrence_count,unique_position_count)
        SELECT gp.id,
            SUM(n.weighted_occurrence_mass/f.new_game_frequency)/NULLIF(SUM(n.depth_weight_sum),0),
            SUM(n.weighted_occurrence_mass/f.new_game_frequency)::float8,
            SUM(n.depth_weight_sum)::float8,SUM(n.occurrence_count)::integer,COUNT(*)::integer
        FROM salience_new_game_position n JOIN salience_frequency_change f
          ON f.player_color=n.player_color AND f.fen_id=n.fen_id
        JOIN {layout.table('game_player')} gp
          ON gp.game_link=n.game_link AND gp.is_white=(n.player_color='white') AND gp.player_id={owner}
        GROUP BY gp.id"""),params)
    await session.execute(text(f"""INSERT INTO {layout.table('player_position_frequency')}
        (player_id,is_white,fen_id,games_with_position,total_occurrences)
        SELECT {owner},player_color='white',fen_id,new_game_frequency,new_total_occurrences
        FROM salience_frequency_change WHERE new_game_frequency>1
        ON CONFLICT(player_id,is_white,fen_id) DO UPDATE SET
            games_with_position=EXCLUDED.games_with_position,total_occurrences=EXCLUDED.total_occurrences"""),params)
    await clear_pending(session,player,'salience_new_games')
    stats = (await session.execute(text(f"""SELECT COUNT(*)::bigint AS source_game_count,
        COALESCE(SUM(s.position_occurrence_count),0)::bigint AS source_position_count,
        COALESCE(SUM(s.salience),0)::float8 AS effective_game_count
        FROM {layout.table('game_player_salience')} s JOIN {layout.table('game_player')} gp ON gp.id=s.game_player_id
        WHERE gp.player_id={owner}"""),params)).mappings().one()
    return {**dict(stats),'calculation':'incremental'}


async def calculate(factory, player):
    from .player_salience_calculation import _full_rebuild, _incremental_update, _prepare_session_memory
    async with factory() as session:
        layout = storage_for(session)
        summary = layout.table('player_salience_summary')
        owner = await session.scalar(text(f"""SELECT id FROM {layout.table('player')}
            WHERE player_name=:player AND COALESCE(joined,0)>0 AND deleted_at IS NULL"""),{'player':player})
        if owner is None:
            raise ValueError(f'Tracked player {player!r} was not found.')
        await session.execute(text(f"""INSERT INTO {summary}(player_id,status,source_game_count,
            source_position_count,effective_game_count,error) VALUES(:id,'running',0,0,0,NULL)
            ON CONFLICT(player_id) DO UPDATE SET status='running',error=NULL"""),{'id':owner})
        await session.commit()
    try:
        async with factory() as session:
            async with session.begin():
                # Serialize one player's refresh and block stale notifications
                # until commit; new ingested games then leave it stale, not lost.
                baseline = await session.scalar(text(f"SELECT source_game_count FROM {summary} WHERE player_id=:id FOR UPDATE"),{'id':owner})
                pending = await session.scalar(text(f"""SELECT count(*) FROM {layout.table('player_salience_pending_game')} p
                    JOIN {layout.table('game_player')} gp ON gp.id=p.game_player_id
                    JOIN {layout.table('game')} g ON g.link=gp.game_link
                    LEFT JOIN {layout.table('game_player_salience')} s ON s.game_player_id=gp.id
                    WHERE gp.player_id=:id AND g.fens_done AND s.game_player_id IS NULL"""),{'id':owner})
                incremental = baseline > 0 and 0 < pending <= min(5000,max(1,int(baseline*0.05)))
                await _prepare_session_memory(session)
                stats = await (_incremental_update(session,player) if incremental else _full_rebuild(session,player))
                # Pending extraction work cannot be labelled ready.
                status = await session.scalar(text(f"""UPDATE {summary} SET
                    status=CASE WHEN EXISTS(SELECT 1 FROM {layout.table('player_salience_pending_game')} p
                        JOIN {layout.table('game_player')} gp ON gp.id=p.game_player_id WHERE gp.player_id=:id)
                        THEN 'stale' ELSE 'ready' END,
                    source_game_count=:source_game_count,source_position_count=:source_position_count,
                    effective_game_count=:effective_game_count,error=NULL WHERE player_id=:id RETURNING status"""),dict(stats,id=owner))
        return {'player_name':player,'status':status,'pending_games':int(pending),
                **stats,'effective_game_count':round(float(stats['effective_game_count']),6)}
    except BaseException:
        async with factory() as session:
            await session.execute(text(f"""UPDATE {summary} SET status='failed',error='Salience calculation failed'
                WHERE player_id=:id AND status<>'stale'"""),{'id':owner})
            await session.commit()
        raise
