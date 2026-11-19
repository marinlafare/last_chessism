"""Supported admin tools use native identities; no real jobs or cloud clients."""
from sqlalchemy import text

from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations.cloud_analysis import input_validation
from tests import check_canonical_cuda_postgres as fixture
from scripts import rerun_cuda_summaries as launcher, benchmark_all_score_distributions as benchmark


class RuntimeToolsTests(fixture.CanonicalCudaTests):
    async def test_full_cohort_launcher_preserves_snapshots_and_native_sides(self):
        await fixture.CanonicalCudaTests.test_atomic_score_accuracy_and_blunder_publication_and_saved_readers(self)
        before = await self.db.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id)::text FROM fen f')
        scores = await self.db.fetchval('SELECT jsonb_agg(to_jsonb(s) ORDER BY player_name)::text FROM player_stockfish_snapshot s')
        inventory = await benchmark.inventory(self.factory)
        self.assertEqual([r['player_name'] for r in inventory], ['bob', 'alice'])
        run = await launcher.start(self.factory)
        async with read_transaction(self.factory) as session:
            saved = await launcher.runs.progress(session, run['id'])
        note = saved['timings']['comparison']; namespace = note['baseline_schema']
        self.assertEqual((saved['status'], note['players'], note['games']), ('queued', 2, 2))
        self.assertEqual(await self.db.fetchval(f'SELECT count(*) FROM {namespace}.score_snapshot'), 2)
        self.assertEqual(await self.db.fetchval(f'SELECT count(*) FROM {namespace}.engine_summary'), 4)
        self.assertEqual(await self.db.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id)::text FROM fen f'), before)
        self.assertEqual(await self.db.fetchval('SELECT jsonb_agg(to_jsonb(s) ORDER BY player_name)::text FROM player_stockfish_snapshot s'), scores)
        with self.assertRaisesRegex(ValueError, 'Existing work'): await launcher.start(self.factory)
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_cloud_claim_copy_requires_transaction_and_rolls_back_conflicts(self):
        await self.db.execute("INSERT INTO cloud_analysis_job(id,status,target,imported) VALUES('copy-job','running',5,0)")
        await self.db.execute("INSERT INTO cloud_analysis_run(id,job_id,status) VALUES('copy-run','copy-job','running')")
        fens = list(self.fens)[:2]
        session = self.factory()
        try:
            with self.assertRaisesRegex(ValueError, 'caller-owned transaction'):
                await input_validation.claim(session, 'copy-run', fens)
        finally:
            await session.close()
        with self.assertRaisesRegex(RuntimeError, 'abort'):
            async with write_transaction(self.factory) as session:
                await input_validation.claim(session, 'copy-run', fens)
                raise RuntimeError('abort')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'), 0)
        async with write_transaction(self.factory) as session:
            await input_validation.claim(session, 'copy-run', fens)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_fen_claim'), 2)
        self.assertEqual(self.engine.pool.checkedout(), 0)
