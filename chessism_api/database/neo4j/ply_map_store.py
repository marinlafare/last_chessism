"""Versioned derived summaries. Reads never rebuild; publication is atomic."""
import json
from sqlalchemy import text
from chessism_api.database.postgresql.relational_storage import driver_connection
from .ply_map_compute import VERSION, MAX_PLY

METRIC = 'ply_map'


async def installed(session):
    return bool(await session.scalar(text("SELECT to_regclass('graph_map.control') IS NOT NULL")))


async def install(session):
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    if not await session.scalar(text("SELECT to_regclass('graph_sync.event') IS NOT NULL")):
        raise ValueError('Install Neo4j synchronization first.')
    connection = await driver_connection(session)
    await connection.execute('''
        CREATE SCHEMA IF NOT EXISTS graph_map;
        CREATE TABLE IF NOT EXISTS graph_map.control (
            id boolean PRIMARY KEY DEFAULT true CHECK(id),revision bigint NOT NULL DEFAULT 1,
            active_snapshot uuid,updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
        INSERT INTO graph_map.control(id) VALUES(true) ON CONFLICT DO NOTHING;
        CREATE TABLE IF NOT EXISTS graph_map.snapshot (
            id uuid PRIMARY KEY,revision bigint NOT NULL,version text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            games integer NOT NULL,nodes integer NOT NULL,connections integer NOT NULL,max_ply integer NOT NULL);
        CREATE TABLE IF NOT EXISTS graph_map.game (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,ordinal integer NOT NULL,
            game_id bigint NOT NULL,source_hash text NOT NULL,white_id bigint,black_id bigint,
            white_name text,black_name text,played_at text,PRIMARY KEY(snapshot,ordinal),UNIQUE(snapshot,game_id));
        CREATE TABLE IF NOT EXISTS graph_map.node (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,key text NOT NULL,
            ply integer NOT NULL,kind text NOT NULL,fen_key text,label text,
            games integer NOT NULL,x integer NOT NULL,y integer NOT NULL,members integer[] NOT NULL,
            PRIMARY KEY(snapshot,key));
        CREATE INDEX IF NOT EXISTS graph_map_node_view ON graph_map.node(snapshot,ply,y);
        CREATE TABLE IF NOT EXISTS graph_map.connection (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,key text NOT NULL,
            source text NOT NULL,target text NOT NULL,kind text NOT NULL,hidden integer NOT NULL,
            reason text NOT NULL,label text,games integer NOT NULL,members integer[] NOT NULL,
            PRIMARY KEY(snapshot,key));
        CREATE INDEX IF NOT EXISTS graph_map_connection_source ON graph_map.connection(snapshot,source);
        CREATE INDEX IF NOT EXISTS graph_map_connection_target ON graph_map.connection(snapshot,target);
        CREATE TABLE IF NOT EXISTS graph_map.segment (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,connection text NOT NULL,
            ordinal integer NOT NULL,start_ply integer NOT NULL,end_ply integer NOT NULL,reason text NOT NULL,
            PRIMARY KEY(snapshot,connection,ordinal));
        CREATE TABLE IF NOT EXISTS graph_map.player (
            snapshot uuid REFERENCES graph_map.snapshot ON DELETE CASCADE,player_id bigint NOT NULL,
            name text NOT NULL,white_games integer[] NOT NULL,black_games integer[] NOT NULL,
            PRIMARY KEY(snapshot,player_id));
        CREATE INDEX IF NOT EXISTS graph_map_player_name ON graph_map.player(snapshot,name);
        CREATE TABLE IF NOT EXISTS graph_map.input (
            run_id uuid NOT NULL,game_id bigint NOT NULL,payload jsonb NOT NULL,
            PRIMARY KEY(run_id,game_id));
        CREATE OR REPLACE FUNCTION graph_map.invalidate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF EXISTS(SELECT 1 FROM added WHERE kind IN ('game','player')) THEN
                UPDATE graph_map.control SET revision=revision+1,updated_at=clock_timestamp() WHERE id;
            END IF;
            RETURN NULL;
        END $$;
        CREATE OR REPLACE TRIGGER chessism_ply_map_dirty AFTER INSERT ON graph_sync.event
            REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION graph_map.invalidate();
    ''')


async def status(session):
    if not await installed(session):
        return dict(installed=False,pending=False)
    row = dict((await session.execute(text('''SELECT c.revision,c.active_snapshot,s.version,
        s.created_at,s.games,s.nodes,s.connections,s.max_ply,
        (s.id IS NULL OR s.revision<>c.revision OR s.version<>:version) AS pending,
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
