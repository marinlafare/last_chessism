"""Run only through the network-isolated, disposable PostgreSQL harness."""
from datetime import datetime, timezone
import unittest

from sqlalchemy import select, text
import test_cloud_analysis_database as cloud
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import Fen, FenContinuation, DatabaseSummary
from chessism_api.operations.fen_results import stage_fen_results, increment_new_fen_counts
from chessism_api.operations.fen_backup.database import restore_batch
from chessism_api.operations.tablebase import _save_tablebase_batch


@unittest.skipUnless(cloud.TEST_URL, 'requires explicitly isolated PostgreSQL')
class EvaluationPipelineTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = cloud.CloudDatabaseTests.asyncSetUp
    asyncTearDown = cloud.CloudDatabaseTests.asyncTearDown

    async def saved(self):
        async with AsyncDBSession() as s:
            row = await s.get(Fen, self.fens[0])
            totals = await s.get(DatabaseSummary, 1)
            return row.score, row.analysis_source, row.analyzed_at, totals.analyzed_fens, totals.unscored_fens

    async def test_local_scores_continuations_and_totals_commit_or_rollback_together(self):
        rows = [{'fen': self.fens[0], 'score': 0, 'next_moves': 'e2e4'}]
        moves = [{'fen_fen': self.fens[0], 'rank': 2, 'move': 'd2d4', 'score': 5}]
        with self.assertRaisesRegex(RuntimeError, 'before commit'):
            async with AsyncDBSession() as s, s.begin():
                await stage_fen_results(s, rows, moves, count_new=True)
                raise RuntimeError('interrupted before commit')
        self.assertEqual(await self.saved(), (None, None, None, 0, 3))
        async with AsyncDBSession() as s:
            self.assertIsNone(await s.scalar(select(FenContinuation)))
        async with AsyncDBSession() as s, s.begin():
            await stage_fen_results(s, rows, moves, count_new=True)
        score, source, stamp, analyzed, unscored = await self.saved()
        self.assertEqual((score, source, analyzed, unscored), (0, 'stockfish', 1, 2))
        self.assertIsNotNone(stamp)
        with self.assertRaisesRegex(ValueError, 'already scored'):
            async with AsyncDBSession() as s, s.begin():
                await stage_fen_results(s, rows, moves, count_new=True)
        self.assertEqual(await self.saved(), (score, source, stamp, analyzed, unscored))

    async def test_invalid_duplicate_or_missing_results_leave_no_partial_writes(self):
        good = {'fen': self.fens[0], 'score': 24, 'next_moves': None}
        for rows in ([good, good], [good, {**good, 'fen': self.fens[1], 'score': float('nan')}],
                     [good, {**good, 'fen': 'does-not-exist'}]):
            with self.assertRaises(ValueError):
                async with AsyncDBSession() as s, s.begin():
                    await stage_fen_results(s, rows, [], count_new=True)
            self.assertEqual(await self.saved(), (None, None, None, 0, 3))

    async def test_one_statement_per_batch_not_one_trigger_per_fen(self):
        async with AsyncDBSession() as s, s.begin():
            await s.execute(text('CREATE TABLE evaluation_statements(n int)'))
            await s.execute(text('''CREATE FUNCTION count_evaluation_statement() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN INSERT INTO evaluation_statements SELECT count(*) FROM changed;
                RETURN NULL; END $$'''))
            await s.execute(text('''CREATE TRIGGER count_evaluations AFTER UPDATE ON fen
                REFERENCING NEW TABLE AS changed FOR EACH STATEMENT EXECUTE FUNCTION count_evaluation_statement()'''))
            await stage_fen_results(s, [{'fen': fen, 'score': i, 'next_moves': None}
                for i, fen in enumerate(self.fens)], [], count_new=True)
            self.assertEqual((await s.execute(text('SELECT n FROM evaluation_statements'))).scalars().all(), [3])
            totals = await s.get(DatabaseSummary, 1)
            self.assertEqual((totals.analyzed_fens, totals.nonzero_scored_fens), (3, 2))

    async def test_missing_totals_roll_back_scores_instead_of_silently_losing_counts(self):
        async with AsyncDBSession() as s, s.begin():
            await s.execute(text('DELETE FROM database_summary WHERE id=1'))
        with self.assertRaisesRegex(ValueError, 'totals are missing'):
            async with AsyncDBSession() as s, s.begin():
                await stage_fen_results(s, [{'fen': self.fens[0], 'score': 42, 'next_moves': None}], [], count_new=True)
        async with AsyncDBSession() as s:
            self.assertIsNone((await s.get(Fen, self.fens[0])).score)

    async def test_large_writer_input_keeps_bounded_statements(self):
        rows = [{'fen': f'synthetic-batch-{i}', 'score': i, 'next_moves': None} for i in range(1001)]
        async with AsyncDBSession() as s, s.begin():
            s.add_all(Fen(fen=row['fen'], n_games=1, moves_counter='{}') for row in rows)
            await s.flush()
            await s.execute(text('UPDATE database_summary SET unscored_fens=1004 WHERE id=1'))
            await stage_fen_results(s, rows, [], count_new=True)
            totals = await s.get(DatabaseSummary, 1)
            self.assertEqual((totals.analyzed_fens, totals.unscored_fens, totals.nonzero_scored_fens), (1001, 3, 1000))

    async def test_restore_preserves_missing_and_known_metadata_exactly(self):
        stamp = datetime(2020, 1, 1, tzinfo=timezone.utc)
        rows = [{'fen': self.fens[0], 'score': 24, 'continuations': []},
                {'fen': self.fens[1], 'score': -9998, 'analysis_source': 'stockfish',
                 'analyzed_at': stamp.isoformat(), 'continuations': []}]
        await restore_batch(rows)
        async with AsyncDBSession() as s:
            legacy, known = await s.get(Fen, self.fens[0]), await s.get(Fen, self.fens[1])
            self.assertEqual((legacy.score, legacy.analysis_source, legacy.analyzed_at), (24, None, None))
            self.assertEqual((known.score, known.analysis_source, known.analyzed_at), (-9998, 'stockfish', stamp))

    async def test_tablebase_writes_preserve_type_and_atomic_counts(self):
        rows = [{'fen': self.fens[0], 'piece_count': 2, 'score': 0, 'next_moves': None,
                 'wdl_win': 0, 'wdl_draw': 1000, 'wdl_loss': 0, 'tablebase_wdl': 0, 'tablebase_dtz': 0}]
        async with AsyncDBSession() as s, s.begin():
            await _save_tablebase_batch(s, rows, [])
            await increment_new_fen_counts(s, rows)
        score, source, stamp, analyzed, unscored = await self.saved()
        self.assertEqual((score, source, analyzed, unscored), (0, 'tablebase', 1, 2))
        self.assertIsNotNone(stamp)


if __name__ == '__main__':
    unittest.main()
