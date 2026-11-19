"""Versioned derived summaries. Reads never rebuild; publication is atomic."""
import json
from sqlalchemy import text
from chessism_api.database.postgresql.transactions import driver_connection
from .ply_map_compute import VERSION, MAX_PLY

METRIC = 'ply_map'


async def status(session):
    row = dict((await session.execute(text('''SELECT c.revision,c.active_snapshot,s.version,
        (SELECT enabled FROM graph_sync.control WHERE id) AS enabled,
        s.created_at,s.games,s.nodes,s.connections,s.max_ply,
        ((SELECT enabled FROM graph_sync.control WHERE id) AND
         (s.id IS NULL OR s.revision<>c.revision OR s.version<>:version)) AS pending,
        EXISTS(SELECT 1 FROM graph_sync.event WHERE kind IN ('game','player') AND completed_at IS NULL) AS syncing
        FROM graph_map.control c LEFT JOIN graph_map.snapshot s ON s.id=c.active_snapshot WHERE c.id'''),
        {'version':VERSION})).mappings().one())
    return dict(installed=True,**row)


async def capture(session, run_id):
    from chessism_api.operations.cuda_summaries.schema import RUN
    state = await status(session)
    if not state['pending'] or state.get('syncing'):
        return 0
    await session.execute(text(f"UPDATE {RUN} SET timings=jsonb_set(timings,'{{ply_map}}',CAST(:state AS jsonb)) WHERE id=:id"),
        {'id':run_id,'state':json.dumps(dict(version=VERSION,revision=state['revision'],cursor=-1,
                                           captured_games=0,prepared=False,complete=False))})
    return 1


async def current(session, revision, *, lock=False):
    if lock:
        await session.execute(text('SELECT id FROM graph_map.control WHERE id FOR UPDATE'))
    state = await status(session)
    return state['revision'] == revision and not state['syncing']


async def publish(session, run_id, revision, result):
    """COPY immutable generation then switch pointer in the same transaction."""
    if not await current(session, revision, lock=True):
        return False
    if result['version'] != VERSION:
        raise ValueError('Ply-map contract changed during preparation.')
    # Retry of a committed publication is idempotent.
    exists = await session.scalar(text('SELECT EXISTS(SELECT 1 FROM graph_map.snapshot WHERE id=:id)'), {'id':run_id})
    if exists:
        return True
    await session.execute(text('''INSERT INTO graph_map.snapshot(id,revision,version,games,nodes,connections,max_ply)
        VALUES(:id,:revision,:version,:games,:nodes,:connections,:max_ply)'''),
        dict(id=run_id,revision=revision,version=VERSION,games=len(result['games']),
             nodes=len(result['nodes']),connections=len(result['connections']),max_ply=MAX_PLY))
    connection = await driver_connection(session)
    async def copy(table, columns, rows):
        await connection.copy_records_to_table(table,schema_name='graph_map',columns=('snapshot',*columns),
            records=((run_id,*row) for row in rows),timeout=120)
    columns = ('key','ply','kind','fen_key','label','games','x','y','members')
    await copy('node',columns,([n[c] for c in columns] for n in result['nodes']))
    columns = ('key','source','target','kind','hidden','reason','label','games','members')
    await copy('connection',columns,([e[c] for c in columns] for e in result['connections']))
    await copy('segment',('connection','ordinal','start_ply','end_ply','reason'),result['segments'])
    players = {}
    for game in result['games']:
        for color in ('white','black'):
            pid = game[f'{color}_id']
            if pid is None: continue
            player = players.setdefault(pid,dict(name=game[f'{color}_name'],white=[],black=[]))
            player[color].append(game['ordinal'])
    await copy('player',('player_id','name','white_games','black_games'),
               ((pid,p['name'],p['white'],p['black']) for pid,p in players.items()))
    columns = ('ordinal','id','source_hash','white_id','black_id','white_name','black_name','played_at')
    await copy('game',('ordinal','game_id',*columns[2:]),([g.get(c) for c in columns] for g in result['games']))
    await session.execute(text('UPDATE graph_map.control SET active_snapshot=:id WHERE id'),{'id':run_id})
    await session.execute(text('DELETE FROM graph_map.input WHERE run_id=:id'),{'id':run_id})
    # Retain the previous generation for open tabs; older derived generations
    # can be discarded. No source games or graph records are deleted.
    await session.execute(text('''DELETE FROM graph_map.snapshot WHERE id NOT IN (
        SELECT id FROM graph_map.snapshot ORDER BY created_at DESC LIMIT 2)'''))
    return True
