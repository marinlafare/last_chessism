"""Real planner regression, exclusively in a disposable PostgreSQL schema."""
import hashlib
import json
import time
import unittest
from sqlalchemy import text
import test_cloud_analysis_database as existing
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.fen_claims import RECHECK_SQL, unclaimed_locked_fens
from chessism_api.database.postgresql.models import CloudAnalysisRun


@unittest.skipUnless(existing.TEST_URL, 'requires disposable PostgreSQL')
class RecheckDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = existing.CloudDatabaseTests.asyncSetUp
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown

    async def populate(self):
        async with AsyncDBSession() as session,session.begin():
            session.add(CloudAnalysisRun(id='b'*32,job_id=self.job.id,status='selecting',
                                        positions=[],launch={},receipts={}))
        async with self.engine.begin() as c:
            await c.execute(text('ALTER TABLE cloud_fen_claim SET (autovacuum_enabled=false)'))
            await c.execute(text("""INSERT INTO fen(fen,n_games,moves_counter,score)
                SELECT repeat(md5(i::text),2),1,'{}',CASE WHEN i%7=0 THEN 0 ELSE NULL END
                FROM generate_series(1,100000) i"""))
            # Empty statistics followed by a large committed reservation is the
            # production regression. Do not refresh statistics before the test.
            await c.execute(text('ANALYZE fen'))
            await c.execute(text('ANALYZE cloud_fen_claim'))
            await c.execute(text("""INSERT INTO cloud_fen_claim(fen,run_id)
                SELECT repeat(md5(i::text),2),:run FROM generate_series(1,50000) i"""),{'run':'b'*32})

    async def test_stale_statistics_and_generic_plan_stay_bounded_and_correct(self):
        await self.populate()
        def fen(i): return hashlib.md5(str(i).encode()).hexdigest()*2
        values=list(range(48001,58001))
        wanted=[fen(i) for i in values]+[fen(58001),'missing-position',fen(58001)]
        expected=[fen(i) for i in values+[58001,58001] if i>50000 and i%7]
        for mode in ('force_custom_plan','force_generic_plan'):
            async with AsyncDBSession() as session,session.begin():
                await session.execute(text("SET LOCAL statement_timeout='5s'"))
                await session.execute(text(f"SET LOCAL plan_cache_mode='{mode}'"))
                started=time.perf_counter()
                self.assertEqual(await unclaimed_locked_fens(session,wanted),expected)
                elapsed=time.perf_counter()-started
                plan='\n'.join((await session.scalars(text('EXPLAIN '+RECHECK_SQL),
                    {'fens':wanted[:5000]})).all())
                self.assertIn('SetOp',plan)
                self.assertNotIn('Anti Join',plan)
                print(json.dumps({'case':'stale_claim_statistics','plan_mode':mode,
                    'claims':50000,'selected':len(wanted),'seconds':round(elapsed,3),'correct':True}),flush=True)
            async with self.engine.connect() as c:
                self.assertEqual(await c.scalar(text("SELECT reltuples FROM pg_class WHERE oid='cloud_fen_claim'::regclass")),0)
