"""Run-owned, WAL-logged scratch tables; never touch published canonical state."""
import json
import time
import uuid
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from . import publication

NAMES={'canonical_blunder_control','canonical_blunder_dirty','canonical_blunder_game','canonical_blunder_snapshot','player'}
CURRENT_NAMES=NAMES|{'canonical_blunder_assessment','fen_summary_refresh'}
OPERATION='canonical_blunder_timing_v1'


class Scratch:
    def __init__(self,run_id,*,factory=None):
        self.name='cbbench_'+uuid.UUID(str(run_id)).hex
        self.marker=OPERATION+':'+str(uuid.UUID(str(run_id)))
        self.factory=factory or AsyncDBSession; self.created=False

    async def create(self,model_id,items,players):
        async with write_transaction(self.factory) as s:
            await s.execute(text("SET LOCAL lock_timeout='1s'"))
            await s.execute(text("SET LOCAL statement_timeout='30s'"))
            # No IF NOT EXISTS: never adopt or erase a pre-existing namespace.
            await s.execute(text(f'CREATE SCHEMA {self.name}'))
            await s.execute(text(f"COMMENT ON SCHEMA {self.name} IS '{self.marker}'"))
            for name in ('control','game','snapshot'):
                await s.execute(text(f'CREATE TABLE {self.name}.canonical_blunder_{name} (LIKE public.canonical_blunder_{name} INCLUDING ALL)'))
            await s.execute(text(f'CREATE TABLE {self.name}.canonical_blunder_dirty(game_link bigint PRIMARY KEY,revision bigint NOT NULL)'))
            await s.execute(text(f'CREATE TABLE {self.name}.player(player_name text PRIMARY KEY,joined bigint,deleted_at timestamptz)'))
            await s.execute(text(f'CREATE TABLE {self.name}.fen_summary_refresh(game_link bigint PRIMARY KEY)'))
            # Read the canonical view definition with all source names qualified;
            # its one input is replaced by this run's private game-result table.
            await s.execute(text('SET LOCAL search_path TO pg_catalog'))
            definition=await s.scalar(text("SELECT pg_get_viewdef('public.canonical_blunder_assessment'::regclass,true)"))
            if 'public.canonical_blunder_game' not in definition:
                raise ValueError('Unrecognized canonical assessment view')
            definition=definition.replace('public.canonical_blunder_game',self.name+'.canonical_blunder_game')
            await s.execute(text(f'CREATE VIEW {self.name}.canonical_blunder_assessment AS '+definition))
            await s.execute(text(f'''INSERT INTO {self.name}.player(player_name,joined)
                SELECT unnest(CAST(:players AS text[])),1'''),{'players':players})
            await s.execute(text(f'INSERT INTO {self.name}.canonical_blunder_control(target_model) VALUES(:model)'),{'model':model_id})
            await s.execute(text(f'''INSERT INTO {self.name}.canonical_blunder_dirty(game_link,revision)
                SELECT r.game_link,r.revision FROM jsonb_to_recordset(CAST(:rows AS jsonb)) r(game_link bigint,revision bigint)'''),
                {'rows':json.dumps(items)})
        self.created=True

    async def write(self,model_id,items,records):
        timings={}
        start=time.perf_counter()
        async with write_transaction(self.factory) as s:
            result=await publication.publish(s,model_id,items,records,{'benchmark':True},namespace=self.name)
            timings['publication_prepare_sql_seconds']=time.perf_counter()-start
            committing=time.perf_counter()
        timings['publication_commit_seconds']=time.perf_counter()-committing
        return result,timings

    async def snapshots(self,model_id):
        async with read_transaction(self.factory) as s:
            rows=await s.execute(text(f'SELECT player_name,payload FROM {self.name}.canonical_blunder_snapshot WHERE model_id=:id'),{'id':model_id})
            return dict(rows.all())

    async def cleanup(self):
        if not self.created:return
        async with write_transaction(self.factory) as s:
            await s.execute(text("SET LOCAL lock_timeout='2s'"))
            await s.execute(text("SET LOCAL statement_timeout='30s'"))
            owned=await s.scalar(text('''SELECT obj_description(oid,'pg_namespace')=:marker
                AND nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                FROM pg_namespace WHERE nspname=:name'''),{'name':self.name,'marker':self.marker})
            if not owned:raise ValueError('Scratch ownership marker changed; refusing cleanup.')
            names=set((await s.execute(text('SELECT table_name FROM information_schema.tables WHERE table_schema=:name'),{'name':self.name})).scalars())
            if names!=CURRENT_NAMES:raise ValueError('Scratch contents changed; refusing cleanup of unexpected objects.')
            if names==CURRENT_NAMES:
                await s.execute(text(f'DROP VIEW {self.name}.canonical_blunder_assessment RESTRICT'))
                await s.execute(text(f'DROP TABLE {self.name}.fen_summary_refresh RESTRICT'))
            # Drop only the five exact tables owned by this run, with RESTRICT.
            for name in sorted(NAMES):await s.execute(text(f'DROP TABLE {self.name}.{name} RESTRICT'))
            await s.execute(text(f'DROP SCHEMA {self.name} RESTRICT'))
        self.created=False


async def recover_interrupted():
    """Only the single worker calls this after marking interrupted runs failed."""
    async with read_transaction(AsyncDBSession) as s:
        runs=(await s.execute(text('''SELECT r.id FROM algorithm_run r JOIN pg_namespace n
            ON n.nspname='cbbench_'||replace(r.id::text,'-','')
            WHERE r.config->>'operation'=:op AND r.status IN ('failed','cancelled','complete')'''),
            {'op':OPERATION})).scalars().all()
    for run in runs:
        scratch=Scratch(run); scratch.created=True
        await scratch.cleanup()
    return len(runs)
