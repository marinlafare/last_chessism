"""Rehearsal-only sink adapter around the existing exact salience algorithms.

The established full/incremental math is shared, not reimplemented. Temporary
legacy-shaped working rows are limited to one player; durable writes use IDs.
This is not wired into production jobs and is not a throughput acceptance test.
"""
from sqlalchemy import text
from scripts.db_refactor_candidate import C, V


async def calculate(session, store, player, *, incremental=False):
    from chessism_api.operations.player_salience_calculation import _full_rebuild,_incremental_update
    conn = store.conn
    identity = await conn.fetchval(f'SELECT id FROM {C}.player WHERE player_name=$1',player)
    if identity is None:
        raise ValueError('Unknown candidate player')
    # Save the preexisting temp-table catalog so cleanup only removes objects
    # owned by this calculation, never an unrelated session-local table.
    before = set(await conn.fetch('SELECT relname FROM pg_class WHERE relnamespace=pg_my_temp_schema()'))
    before = {r['relname'] for r in before}
    tables = ('player_position_frequency','game_player_salience','player_salience_pending_game')
    if before.intersection(tables):
        raise RuntimeError('Salience shadow tables are already in use')
    await session.execute(text(f'SET LOCAL search_path={V},public'))
    for table in tables:
        await session.execute(text(f'''CREATE TEMP TABLE {table} ON COMMIT DROP AS
            SELECT * FROM {V}.{table} WHERE player_name=:player'''),{'player':player})
    await session.execute(text('''CREATE UNIQUE INDEX ON player_position_frequency(player_name,player_color,fen_id)'''))
    stats = await (_incremental_update(session,player) if incremental else _full_rebuild(session,player))
    await conn.execute(f'DELETE FROM {C}.player_position_frequency WHERE player_id=$1',identity)
    await conn.execute(f'''INSERT INTO {C}.player_position_frequency
        SELECT $1,player_color='white',fen_id,games_with_position,total_occurrences
        FROM pg_temp.player_position_frequency WHERE player_name=$2''',identity,player)
    await conn.execute(f'''DELETE FROM {C}.game_player_salience s USING {C}.game_player gp
        WHERE gp.id=s.game_player_id AND gp.player_id=$1''',identity)
    await conn.execute(f'''INSERT INTO {C}.game_player_salience(game_player_id,salience,
        weighted_numerator,depth_weight_sum,position_occurrence_count,unique_position_count)
        SELECT gp.id,s.salience,s.weighted_numerator,s.depth_weight_sum,
            s.position_occurrence_count,s.unique_position_count
        FROM pg_temp.game_player_salience s JOIN {C}.game_player gp
          ON gp.game_link=s.game_link AND gp.is_white=(s.player_color='white') AND gp.player_id=$1
        WHERE s.player_name=$2''',identity,player)
    await conn.execute(f'''DELETE FROM {C}.player_salience_pending_game p USING {C}.game_player gp
        WHERE gp.id=p.game_player_id AND gp.player_id=$1 AND NOT EXISTS(
            SELECT 1 FROM pg_temp.player_salience_pending_game t
            WHERE t.game_link=gp.game_link AND (t.player_color='white')=gp.is_white AND t.player_name=$2)''',identity,player)
    await conn.execute(f'''INSERT INTO {C}.player_salience_summary(player_id,status,source_game_count,
            source_position_count,effective_game_count,error)
        VALUES ($1,'ready',$2,$3,$4,NULL) ON CONFLICT(player_id) DO UPDATE SET
            status='ready',source_game_count=EXCLUDED.source_game_count,
            source_position_count=EXCLUDED.source_position_count,
            effective_game_count=EXCLUDED.effective_game_count,error=NULL''',identity,
        stats['source_game_count'],stats['source_position_count'],stats['effective_game_count'])
    created = await conn.fetch("SELECT relname FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relkind='r'")
    for row in created:
        name = row['relname']
        if name not in before and (name in tables or name.startswith('salience_')):
            # Names come only from the current session catalog; escape anyway.
            await conn.execute('DROP TABLE pg_temp."'+name.replace('"','""')+'"')
    await session.execute(text('SET LOCAL search_path=public'))
    return stats
