"""Run-owned, WAL-logged scratch tables; never touch published canonical state."""
import json
import time
import uuid
from contextlib import asynccontextmanager
from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import RelationalStorage,SESSION_KEY,identifier
from .schema import table
from . import publication
from .assessment import install_view

NAMES={'canonical_blunder_control','canonical_blunder_dirty','canonical_blunder_game','canonical_blunder_snapshot','player'}
CURRENT_NAMES=NAMES|{'canonical_blunder_assessment','fen_summary_refresh'}
OPERATION='canonical_blunder_timing_v1'


class Scratch:
    def __init__(self,run_id,layout,*,factory=None):
        self.name='cbbench_'+uuid.UUID(str(run_id)).hex
        self.marker=OPERATION+':'+str(uuid.UUID(str(run_id)))
        self.layout=RelationalStorage(self.name,layout.archive,layout.reads,layout.version)
        self.source=layout; self.factory=factory or AsyncDBSession; self.created=False

    @asynccontextmanager
    async def session(self):
        async with self.factory() as session:
            session.info[SESSION_KEY]=self.layout
            yield session

    async def create(self,model_id,items,players):
        async with self.session() as s,s.begin():
            await s.execute(text("SET LOCAL lock_timeout='1s'"))
            await s.execute(text("SET LOCAL statement_timeout='30s'"))
            # No IF NOT EXISTS: never adopt or erase a pre-existing namespace.
            await s.execute(text(f'CREATE SCHEMA {identifier(self.name)}'))
            await s.execute(text(f"COMMENT ON SCHEMA {identifier(self.name)} IS '{self.marker}'"))
            for name in ('control','game','snapshot'):
                await s.execute(text(f'CREATE TABLE {table(self.layout,name)} (LIKE {table(self.source,name)} INCLUDING ALL)'))
            await s.execute(text(f'CREATE TABLE {table(self.layout,"dirty")}(game_link bigint PRIMARY KEY,revision bigint NOT NULL)'))
            await s.execute(text(f'CREATE TABLE {self.layout.table("player")}(player_name text PRIMARY KEY,joined bigint,deleted_at timestamptz)'))
            await s.execute(text(f'CREATE TABLE {self.layout.table("fen_summary_refresh")}(game_link bigint PRIMARY KEY)'))
            await install_view(s,self.layout)
            await s.execute(text(f'''INSERT INTO {self.layout.table('player')}(player_name,joined)
                SELECT unnest(CAST(:players AS text[])),1'''),{'players':players})
            await s.execute(text(f'INSERT INTO {table(self.layout,"control")}(target_model) VALUES(:model)'),{'model':model_id})
            await s.execute(text(f'''INSERT INTO {table(self.layout,'dirty')}(game_link,revision)
                SELECT r.game_link,r.revision FROM jsonb_to_recordset(CAST(:rows AS jsonb)) r(game_link bigint,revision bigint)'''),
                {'rows':json.dumps(items)})
        self.created=True

    async def write(self,model_id,items,records):
        timings={}
        async with self.session() as s:
            tx=await s.begin()
            try:
                start=time.perf_counter()
                result=await publication.publish(s,model_id,items,records,{'benchmark':True})
                timings['publication_prepare_sql_seconds']=time.perf_counter()-start
                start=time.perf_counter(); await tx.commit()
                timings['publication_commit_seconds']=time.perf_counter()-start
            except BaseException:
                await tx.rollback(); raise
        return result,timings

    async def snapshots(self,model_id):
        async with self.session() as s:
            rows=await s.execute(text(f'SELECT player_name,payload FROM {table(self.layout,"snapshot")} WHERE model_id=:id'),{'id':model_id})
            return dict(rows.all())

    async def cleanup(self):
        if not self.created:return
        async with self.session() as s,s.begin():
            await s.execute(text("SET LOCAL lock_timeout='2s'"))
            await s.execute(text("SET LOCAL statement_timeout='30s'"))
            owned=await s.scalar(text('''SELECT obj_description(oid,'pg_namespace')=:marker
                AND nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                FROM pg_namespace WHERE nspname=:name'''),{'name':self.name,'marker':self.marker})
            if not owned:raise ValueError('Scratch ownership marker changed; refusing cleanup.')
            names=set((await s.execute(text('SELECT table_name FROM information_schema.tables WHERE table_schema=:name'),{'name':self.name})).scalars())
            if names not in (NAMES,CURRENT_NAMES):raise ValueError('Scratch contents changed; refusing cleanup of unexpected objects.')
            if names==CURRENT_NAMES:
                await s.execute(text(f'DROP VIEW {table(self.layout,"assessment")} RESTRICT'))
                await s.execute(text(f'DROP TABLE {self.layout.table("fen_summary_refresh")} RESTRICT'))
            # Drop only the five exact tables owned by this run, with RESTRICT.
            for name in sorted(NAMES):await s.execute(text(f'DROP TABLE {identifier(self.name)}.{identifier(name)} RESTRICT'))
            await s.execute(text(f'DROP SCHEMA {identifier(self.name)} RESTRICT'))
        self.created=False


async def recover_interrupted():
    """Only the single worker calls this after marking interrupted runs failed."""
    from chessism_api.database.relational_storage import storage_for
    async with AsyncDBSession() as s:
        layout=storage_for(s)
        runs=(await s.execute(text('''SELECT r.id FROM algorithm_run r JOIN pg_namespace n
            ON n.nspname='cbbench_'||replace(r.id::text,'-','')
            WHERE r.config->>'operation'=:op AND r.status IN ('failed','cancelled','complete')'''),
            {'op':OPERATION})).scalars().all()
    for run in runs:
        scratch=Scratch(run,layout); scratch.created=True
        await scratch.cleanup()
    return len(runs)
