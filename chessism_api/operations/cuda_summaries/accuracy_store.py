"""Accuracy staging and publication under the shared player-revision guard."""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from ..player_timezone import resolve_player_timezone
from ..matrix_constructor.artifact_files import file_operation
from . import accuracy_compute as compute, score_runs
from .schema import CONTROL, RUN, PENDING

STATE = 'public.cuda_accuracy_state'
GAME = 'public.cuda_accuracy_game'
DAILY = 'public.cuda_accuracy_daily'
STAGE = 'public.cuda_accuracy_stage'
CAPTURE = 'public.cuda_accuracy_capture'


async def install(session):
    await session.execute(text(f'ALTER TABLE {CONTROL} ADD COLUMN IF NOT EXISTS accuracy boolean NOT NULL DEFAULT false'))
    for table, fields in (
        (STATE, '''player_name text PRIMARY KEY,revision bigint NOT NULL,version text NOT NULL,
            timezone jsonb NOT NULL,generated_at timestamptz NOT NULL DEFAULT clock_timestamp()'''),
        (GAME, '''player_name text,game_link bigint,day date NOT NULL,played_at timestamptz,
            mode text NOT NULL,accuracy float8,moves int NOT NULL,PRIMARY KEY(player_name,game_link)'''),
        (DAILY, 'player_name text,day date,mode_mask smallint,payload jsonb NOT NULL,PRIMARY KEY(player_name,day,mode_mask)'),
        (STAGE, f'''run_id uuid REFERENCES {RUN}(id),player_name text,game_link bigint,
            day date NOT NULL,played_at timestamptz,mode text NOT NULL,is_white boolean NOT NULL,
            payload jsonb NOT NULL,PRIMARY KEY(run_id,player_name,game_link)'''),
        (CAPTURE, f'run_id uuid REFERENCES {RUN}(id),game_link bigint,revision bigint NOT NULL,PRIMARY KEY(run_id,game_link)'),
    ):
        await session.execute(text(f'CREATE TABLE IF NOT EXISTS {table}({fields})'))
    await session.execute(text(f'CREATE INDEX IF NOT EXISTS cuda_accuracy_game_day ON {GAME}(player_name,day)'))


async def enabled(session):
    return bool(await session.scalar(text(f'SELECT accuracy FROM {CONTROL} WHERE id')))


def needs_work(layout):
    """Backfill absent v1 output only after a superuser explicitly requests a run."""
    state = summary_table(layout, 'state')
    return f'''SELECT s.player_name,s.revision FROM {state} s
        LEFT JOIN {STATE} a USING(player_name)
        WHERE (a.player_name IS NULL AND EXISTS(SELECT 1 FROM {layout.table('player')} p
            WHERE p.player_name=s.player_name AND p.joined>0 AND p.deleted_at IS NULL))
          OR a.revision<>s.revision OR a.version<>'{compute.VERSION}' '''


async def capture(session, run_id):
    layout = storage_for(session); state = summary_table(layout, 'state')
    await session.execute(text(f'''INSERT INTO {score_runs.SCOPE}(run_id,player_name,revision,full_rebuild)
        SELECT :id,w.player_name,w.revision,true FROM ({needs_work(layout)}) w
        ON CONFLICT(run_id,player_name) DO NOTHING'''), {'id': run_id})
    # Existing dirty-day scopes stay incremental only when both writers share the same baseline.
    await session.execute(text(f'''UPDATE {score_runs.SCOPE} q SET full_rebuild=true
        FROM {state} s WHERE q.run_id=:id AND q.player_name=s.player_name AND NOT EXISTS(
            SELECT 1 FROM {STATE} a WHERE a.player_name=s.player_name AND a.revision=s.published_revision
                AND a.version=:version)'''), {'id': run_id, 'version': compute.VERSION})
    await session.execute(text(f'''INSERT INTO {CAPTURE}(run_id,game_link,revision)
        SELECT :id,game_link,revision FROM {PENDING} WHERE metric='accuracy' '''), {'id': run_id})


async def stage(session, run, records):
    if not records:
        return
    from .bulk import temp_rows, json_rows
    columns = ('player_name','game_link','played_at','mode','is_white','payload')
    values = await json_rows(lambda: [(r['player_name'],r['game_link'],r['played_at'],r['mode'],r['is_white'],
                                      json.dumps(r['payload'],allow_nan=False)) for r in records])
    await temp_rows(session, 'cuda_accuracy_upload',
        'player_name text,game_link bigint,played_at timestamptz,mode text,is_white boolean,payload jsonb', columns, values)
    await session.execute(text(f'''INSERT INTO {STAGE}(run_id,player_name,game_link,day,played_at,mode,is_white,payload)
        SELECT :id,r.player_name,r.game_link,o.day,r.played_at,r.mode,r.is_white,r.payload
        FROM pg_temp.cuda_accuracy_upload r
        JOIN {score_runs.OWNER} o ON o.run_id=:id AND o.player_name=r.player_name AND o.game_link=r.game_link
        ON CONFLICT(run_id,player_name,game_link) DO UPDATE SET payload=EXCLUDED.payload,
            day=EXCLUDED.day,played_at=EXCLUDED.played_at,mode=EXCLUDED.mode,is_white=EXCLUDED.is_white'''),
        {'id': run['id']})


async def prepare(session, run, scope, xp):
    layout = storage_for(session); p = {'id': run['id'], 'player': scope['player_name']}
    profile = (await session.execute(text(f'SELECT * FROM {layout.table("player")} WHERE player_name=:player'), p)).mappings().first()
    zone = resolve_player_timezone(dict(profile or {}))
    previous = [] if scope['full_rebuild'] else [dict(r) for r in (await session.execute(text(f'''SELECT * FROM {GAME} g
        WHERE player_name=:player AND NOT EXISTS(SELECT 1 FROM {score_runs.DAY} d WHERE d.run_id=:id
            AND d.player_name=g.player_name AND d.day=g.day)'''), p)).mappings()]
    changed = [dict(r) for r in (await session.execute(text(f'''SELECT game_link,played_at,mode,
        (payload->>'game_efficiency')::float8 AS accuracy,(payload->>'analyzed_player_moves')::int AS moves
        FROM {STAGE} WHERE run_id=:id AND player_name=:player'''), p)).mappings()]
    salience = dict((await session.execute(text(f'''SELECT gp.game_link,s.salience
        FROM {layout.table('game_player_salience')} s JOIN {layout.table('game_player')} gp ON gp.id=s.game_player_id
        JOIN {layout.table('player')} p ON p.id=gp.player_id WHERE p.player_name=:player'''), p)).all())
    # Timestamp conversion is grouping metadata; band counts, modes and sums run on CUDA.
    rows = [{'day': zone.local_datetime(r['played_at']).date().isoformat(),
             'mode_bit': {'bullet': 1, 'blitz': 2, 'rapid': 4}[r['mode']],
             'accuracy': r['accuracy'], 'salience': salience.get(r['game_link'])}
            for r in [*previous, *changed] if r['played_at'] and r['moves'] > 10
            and r['accuracy'] is not None and 0 <= r['accuracy'] <= 100]
    daily = await file_operation(compute.daily, xp, rows)
    return {'timezone': zone.payload(), 'daily': daily}


async def publish(session, run, scope, prepared, eligible):
    """Caller already holds the shared state lock and checked exact revision."""
    layout = storage_for(session); p = {'id': run['id'], 'player': scope['player_name'], 'full': scope['full_rebuild']}
    engine = layout.table('game_player_engine_summary'); gp = layout.table('game_player'); player = layout.table('player')
    affected = f'''(:full OR EXISTS(SELECT 1 FROM {score_runs.OWNER} o WHERE o.run_id=:id
        AND o.player_name=:player AND o.game_link=gp.game_link) OR EXISTS(SELECT 1 FROM {GAME} a
        JOIN {score_runs.DAY} d ON d.run_id=:id AND d.player_name=a.player_name AND d.day=a.day
        WHERE a.player_name=:player AND a.game_link=gp.game_link))'''
    await session.execute(text(f'''DELETE FROM {engine} e USING {gp} gp,{player} p
        WHERE e.game_player_id=gp.id AND gp.player_id=p.id AND p.player_name=:player AND {affected}
        AND (NOT :eligible OR NOT EXISTS(SELECT 1 FROM {STAGE} s WHERE s.run_id=:id
            AND s.player_name=:player AND s.game_link=gp.game_link AND s.is_white=gp.is_white))'''), {**p, 'eligible': eligible})
    if eligible:
        # Do not calculate, copy or update any retired CPU blunder count.
        fields = compute.FIELDS
        types = {'result': 'text', 'end_by': 'text', 'analyzed_player_moves': 'int', 'mate_for_positions': 'int', 'mate_against_positions': 'int'}
        projection = ','.join(f"(s.payload->>'{f}')::{types.get(f,'float8')}" for f in fields)
        await session.execute(text(f'''INSERT INTO {engine}(game_player_id,{','.join(fields)})
            SELECT gp.id,{projection} FROM {STAGE} s JOIN {player} p ON p.player_name=s.player_name
            JOIN {gp} gp ON gp.player_id=p.id AND gp.game_link=s.game_link AND gp.is_white=s.is_white
            WHERE s.run_id=:id AND s.player_name=:player ORDER BY gp.id
            ON CONFLICT(game_player_id) DO UPDATE SET {','.join(f+'=EXCLUDED.'+f for f in fields)}'''), p)
    await session.execute(text(f'''DELETE FROM {GAME} g WHERE player_name=:player AND (:full OR NOT :eligible OR EXISTS(
        SELECT 1 FROM {score_runs.DAY} d WHERE d.run_id=:id AND d.player_name=g.player_name AND d.day=g.day))'''), {**p, 'eligible': eligible})
    if eligible:
        await session.execute(text(f'''INSERT INTO {GAME}(player_name,game_link,day,played_at,mode,accuracy,moves)
            SELECT player_name,game_link,day,played_at,mode,(payload->>'game_efficiency')::float8,
                (payload->>'analyzed_player_moves')::int FROM {STAGE} WHERE run_id=:id AND player_name=:player
            ON CONFLICT(player_name,game_link) DO UPDATE SET day=EXCLUDED.day,played_at=EXCLUDED.played_at,
                mode=EXCLUDED.mode,accuracy=EXCLUDED.accuracy,moves=EXCLUDED.moves'''), p)
    await session.execute(text(f'DELETE FROM {DAILY} WHERE player_name=:player'), p)
    if eligible and prepared['daily']:
        from .bulk import temp_rows,json_rows
        values = await json_rows(lambda: [(r['day'],r['mode_mask'],json.dumps(r)) for r in prepared['daily']])
        await temp_rows(session,'cuda_daily_upload','day text,mode_mask smallint,payload jsonb',
                        ('day','mode_mask','payload'),values)
        await session.execute(text(f'''INSERT INTO {DAILY}(player_name,day,mode_mask,payload)
            SELECT :player,day::date,mode_mask,payload FROM pg_temp.cuda_daily_upload'''),p)
    await session.execute(text(f'''INSERT INTO {STATE}(player_name,revision,version,timezone)
        VALUES(:player,:revision,:version,CAST(:timezone AS jsonb)) ON CONFLICT(player_name) DO UPDATE SET
        revision=EXCLUDED.revision,version=EXCLUDED.version,timezone=EXCLUDED.timezone,generated_at=clock_timestamp()'''),
        {**p, 'revision': scope['revision'], 'version': compute.VERSION, 'timezone': json.dumps(prepared['timezone'])})


async def complete(session, run):
    await session.execute(text(f'''DELETE FROM {PENDING} p USING {CAPTURE} c WHERE c.run_id=:id
        AND p.metric='accuracy' AND p.game_link=c.game_link AND p.revision=c.revision
        AND NOT EXISTS(SELECT 1 FROM {score_runs.SCOPE} WHERE run_id=:id AND status<>'published')'''), {'id': run['id']})
    for table in (STAGE, CAPTURE):
        await session.execute(text(f'DELETE FROM {table} WHERE run_id=:id'), {'id': run['id']})
