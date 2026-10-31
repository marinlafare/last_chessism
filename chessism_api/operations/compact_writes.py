"""Physical v3 writes behind the explicit session storage contract."""
from sqlalchemy import delete, func, insert, select, text, update

from chessism_api.database.compact_storage import compact_for, compact_table
from chessism_api.database.relational_storage import driver_connection


async def insert_associations(session, rows):
    layout = compact_for(session)
    if not layout:
        raise RuntimeError('Compact association writer requires v3')
    unique = {}
    for row in rows:
        if row['move_color'] not in ('white', 'black'):
            raise ValueError('Invalid move side')
        key = (row['game_link'], row['n_move'], row['move_color'] == 'white')
        value = row['fen_id']
        if key in unique and unique[key] != value:
            raise ValueError('Conflicting FENs for one game/move/side')
        unique[key] = value
    conn = await driver_connection(session)
    await conn.execute('''CREATE TEMP TABLE IF NOT EXISTS compact_association_input
        (game_link bigint,n_move integer,is_white boolean,fen_id bigint) ON COMMIT DROP;
        TRUNCATE pg_temp.compact_association_input''')
    await conn.copy_records_to_table('compact_association_input',
        columns=['game_link','n_move','is_white','fen_id'],
        records=[(*key,value) for key,value in sorted(unique.items())])
    await conn.execute(f'''INSERT INTO {layout.table('game_fen_association')}(game_link,n_move,is_white,fen_id)
        SELECT game_link,n_move,is_white,fen_id FROM compact_association_input ORDER BY game_link,n_move,is_white
        ON CONFLICT(game_link,n_move,is_white) DO NOTHING''')
    # A retry of the same position is harmless; a different position must fail,
    # including when another writer won the unique-key race while INSERT waited.
    conflict = await conn.fetchval(f'''SELECT EXISTS(SELECT 1 FROM compact_association_input i
        JOIN {layout.table('game_fen_association')} a USING(game_link,n_move,is_white)
        WHERE a.fen_id<>i.fen_id)''')
    if conflict:
        raise ValueError('Existing game/move/side points to a different FEN; refusing to overwrite')


async def player_stats_write(session, model, operation, player, data=None):
    layout = compact_for(session)
    table = compact_table(model, layout.core)
    owner = await session.scalar(text(f'SELECT id FROM {layout.table("player")} WHERE player_name=:name'), {'name':player})
    if owner is None:
        if operation == 'create':
            raise ValueError('Stats owner does not exist')
        return None
    values = {key:value for key,value in (data or {}).items() if key in table.c and key != 'player_id'}
    if data and data.get('player_name',player) != player:
        raise ValueError('Cannot change stats ownership')
    if operation == 'update' and not values:
        row = (await session.execute(select(table).where(table.c.player_id == owner))).mappings().first()
        return None if row is None else {
            key:value for key,value in row.items() if key != 'player_id'
        } | {'player_name':player}
    if operation == 'create':
        query = insert(table).values(player_id=owner, **values)
    elif operation == 'update':
        # Core physical adapters must preserve PlayerStats' ORM onupdate behavior.
        values.setdefault('last_updated', func.now())
        query = update(table).where(table.c.player_id == owner).values(**values)
    elif operation == 'delete':
        query = delete(table).where(table.c.player_id == owner)
    else:
        raise ValueError('Invalid stats operation')
    row = (await session.execute(query.returning(*table.c))).mappings().first()
    if row is None:
        return None
    return {key:value for key,value in row.items() if key != 'player_id'} | {'player_name':player}
