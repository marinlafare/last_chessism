"""Supported admin tools use native identities; no real jobs or cloud clients."""
from sqlalchemy import text
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations.cloud_analysis import input_validation
from tests import check_canonical_cuda_postgres as fixture
from scripts import rerun_cuda_summaries as launcher, benchmark_all_score_distributions as benchmark
from scripts import compare_cuda_summaries as comparison, manage_canonical_blunders as administration


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

    async def test_rerun_comparison_uses_native_sides_and_detects_real_differences(self):
        await self.test_full_cohort_launcher_preserves_snapshots_and_native_sides()
        identifier=await self.db.fetchval("SELECT id FROM cuda_summary_run WHERE status='queued'")
        async with write_transaction(self.factory) as session:
            run=await launcher.runs.progress(session,identifier)
            while await fixture.score_runs.plan(session,run):pass
            run=await launcher.runs.next_batch(session)
        await self.stage(run)
        for _ in range(3):await fixture.score_publication.finalize(self.factory,run,fixture.DEVICE)
        async with read_transaction(self.factory) as session:result=await comparison.compare(session,identifier)
        self.assertTrue(result['passed'],result)
        self.assertEqual(result['checks']['accuracy_games']['compared'],4)
        self.assertEqual(result['checks']['engine_statistics']['compared'],4)
        await self.db.execute('UPDATE game_player_engine_summary SET own_move_cp_gain=own_move_cp_gain+1 WHERE game_link=1 AND is_white')
        async with read_transaction(self.factory) as session:result=await comparison.compare(session,identifier)
        self.assertFalse(result['passed']);self.assertEqual(result['checks']['engine_statistics']['mismatches'],1)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_model_status_and_rollback_keep_explicit_transaction_boundary(self):
        args=SimpleNamespace(adopt_run=None,rollback_to=None,acknowledgement='')
        with patch.object(administration,'bind_runtime_engine',AsyncMock(return_value=self.engine)), \
             patch.object(administration,'AsyncDBSession',self.factory), \
             patch.dict('os.environ',{'DATABASE_URL':'postgresql://unused'}):
            await administration.main(args)
            args.rollback_to=self.model
            with self.assertRaisesRegex(ValueError,'acknowledgement'):await administration.main(args)
            args.acknowledgement='explicit_canonical_rollback'
            await administration.main(args)
            self.assertEqual(await self.db.fetchval('SELECT target_model FROM canonical_blunder_control'),self.model)
            args.rollback_to='legacy_cpu'
            with self.assertRaisesRegex(ValueError,'retired'):await administration.main(args)
        self.assertEqual(self.engine.pool.checkedout(),0)
