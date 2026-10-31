"""Endgame lifecycle cleanup contracts; no database, Redis or cloud access."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from arq.jobs import JobStatus

from chessism_api.operations import tablebase
from chessism_api.operations.ingestion_pipeline.progress import PROGRESS_TTL_SECONDS
from chessism_api.operations.redis_ownership import RELEASE_OWNED_LUA


class TablebaseLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_result_writer_drops_a_position_claimed_during_probing(self):
        candidates=SimpleNamespace(count=2,discover=AsyncMock(),read=unittest.mock.Mock(side_effect=[[1,2],[]]),close=unittest.mock.Mock())
        rows=[{'fen':'first','piece_count':3},{'fen':'second','piece_count':4}]
        prober=SimpleNamespace(workers=2,probe=AsyncMock(return_value=(rows,[])),close=AsyncMock())
        session=SimpleNamespace(commit=AsyncMock(),rollback=AsyncMock(),close=AsyncMock())
        with (patch.object(tablebase.os.path,'isdir',return_value=True),
              patch.object(tablebase,'CandidateSpool',return_value=candidates),
              patch.object(tablebase,'TablebaseProber',return_value=prober),
              patch.object(tablebase,'_read_tablebase_batch',AsyncMock(return_value=rows)),
              patch.object(tablebase,'_lease_tablebase_batch',AsyncMock(return_value=(session,rows[1:]))),
              patch.object(tablebase,'_save_tablebase_batch',AsyncMock()) as save,
              patch('chessism_api.operations.fen_results.increment_new_fen_counts',AsyncMock()) as increment,
              patch.object(tablebase,'increment_scored_position_summary_for_scored_fens',AsyncMock()),
              patch.object(tablebase,'increment_game_analysis_summary_for_scored_fens',AsyncMock()),
              patch.object(tablebase,'count_tablebase_candidates',AsyncMock()) as count):
            result=await tablebase.analyze_tablebase_positions({},game_links=[1],refresh_projections=False)
        self.assertEqual(result,{'processed':2,'solved':1,'unavailable':0,'target':2,'skipped':1})
        save.assert_awaited_once_with(session,rows[1:],[])
        increment.assert_awaited_once_with(session,rows[1:])
        session.commit.assert_awaited_once();session.close.assert_awaited_once()
        count.assert_not_awaited()
        candidates.discover.assert_awaited_once()
        self.assertIsNone(candidates.discover.await_args.args[1])

    async def test_probe_failure_has_no_open_write_session(self):
        candidates=SimpleNamespace(count=1,discover=AsyncMock(),read=unittest.mock.Mock(return_value=[1]),close=unittest.mock.Mock())
        prober=SimpleNamespace(workers=2,probe=AsyncMock(side_effect=RuntimeError('probe failed')),close=AsyncMock())
        with (patch.object(tablebase.os.path,'isdir',return_value=True),
              patch.object(tablebase,'CandidateSpool',return_value=candidates),
              patch.object(tablebase,'TablebaseProber',return_value=prober),
              patch.object(tablebase,'_read_tablebase_batch',AsyncMock(return_value=[{'fen':'first'}])),
              patch.object(tablebase,'_lease_tablebase_batch',AsyncMock()) as lease):
            with self.assertRaisesRegex(RuntimeError,'probe failed'):
                await tablebase.analyze_tablebase_positions({},refresh_projections=False)
        lease.assert_not_awaited();prober.close.assert_awaited_once_with(abort=True)
        candidates.close.assert_called_once()

    async def test_shared_progress_preserves_schema_counters_and_expiry(self):
        redis = AsyncMock()
        with patch('chessism_api.operations.ingestion_pipeline.progress.time.time', return_value=123):
            await tablebase._write_progress(redis, 'job', total=8, processed=3,
                failed=-1, phase='tablebase', detail='Solving', ingestion_run_id='run')
        key, raw = redis.set.call_args.args
        self.assertEqual(key, 'chessism:job_progress:job')
        self.assertEqual(json.loads(raw), {
            'job_id': 'job', 'kind': 'tablebase_analysis', 'ingestion_run_id': 'run',
            'total': 8, 'processed': 3, 'failed': 0, 'phase': 'tablebase',
            'detail': 'Solving', 'updated_at': 123,
        })
        self.assertEqual(redis.set.call_args.kwargs, {'ex': PROGRESS_TTL_SECONDS})

    async def test_progress_without_a_redis_connection_remains_optional(self):
        await tablebase._write_progress(None, 'job', total=0, processed=0,
            failed=0, phase='complete', detail='No work')
        self.assertEqual(await tablebase._read_progress(None, 'job'), {})

    async def test_release_checks_owner_and_deletes_in_one_atomic_operation(self):
        redis = AsyncMock()
        await tablebase._release_coordination(redis, 'old-job')
        redis.eval.assert_awaited_once_with(
            RELEASE_OWNED_LUA, 1, tablebase.TABLEBASE_COORDINATION_KEY, 'old-job')
        redis.get.assert_not_awaited()
        redis.delete.assert_not_awaited()

    async def test_stale_job_cleanup_cannot_delete_a_replacement_owner(self):
        redis = AsyncMock()
        redis.get.return_value = b'old-job'
        redis.eval.return_value = 0  # Another owner replaced old-job before release.
        redis.set.return_value = False  # Its lease still prevents admission.
        with (patch.object(tablebase, 'Job') as job,
              patch.object(tablebase, 'has_tablebase_candidates', AsyncMock(return_value=True))):
            job.return_value.status = AsyncMock(return_value=JobStatus.complete)
            result = await tablebase.ensure_tablebase_analysis_enqueued(redis, game_links=[1])
        self.assertEqual(result['status'], 'already_active')
        redis.eval.assert_awaited_once_with(
            RELEASE_OWNED_LUA, 1, tablebase.TABLEBASE_COORDINATION_KEY, 'old-job')
        redis.delete.assert_not_awaited()
        redis.enqueue_job.assert_not_awaited()

    async def test_enqueue_failure_releases_only_the_matching_reservation(self):
        redis = AsyncMock()
        redis.get.return_value = None
        redis.set.return_value = True
        redis.enqueue_job.side_effect = RuntimeError('Synthetic queue failure')
        with patch.object(tablebase, 'has_tablebase_candidates', AsyncMock(return_value=True)):
            with self.assertRaisesRegex(RuntimeError, 'Synthetic queue failure'):
                await tablebase.ensure_tablebase_analysis_enqueued(redis, game_links=[1])
        redis.eval.assert_awaited_once_with(
            RELEASE_OWNED_LUA, 1, tablebase.TABLEBASE_COORDINATION_KEY, 'reserving')
        redis.delete.assert_not_awaited()

    async def test_failed_progress_reads_tolerate_missing_or_invalid_payloads(self):
        redis = AsyncMock()
        for value in (None, b'', b'{bad json', b'[]', b'null'):
            redis.get.return_value = value
            self.assertEqual(await tablebase._read_progress(redis, 'job'), {})
        redis.get.return_value = b'{"processed":3}'
        self.assertEqual(await tablebase._read_progress(redis, 'job'), {'processed': 3})


if __name__ == '__main__':
    unittest.main()
