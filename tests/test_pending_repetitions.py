import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations import pending_repetitions as cache


NOW = datetime(2026, 10, 11, 20, tzinfo=timezone.utc)


def context(session):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


class PendingRepetitionTests(unittest.IsolatedAsyncioTestCase):
    def test_freshness_debounces_changes_and_retries_failures(self):
        self.assertTrue(cache.needs_refresh({}, NOW, NOW))
        meta = {"refreshed_at": NOW, "attempted_at": NOW, "source_updated_at": NOW}
        self.assertFalse(cache.needs_refresh(meta, NOW, NOW + timedelta(days=1)))
        self.assertFalse(cache.needs_refresh(meta, NOW + timedelta(seconds=1), NOW + timedelta(seconds=299)))
        self.assertTrue(cache.needs_refresh(meta, NOW + timedelta(seconds=1), NOW + timedelta(seconds=300)))
        meta["last_error"] = "failure"
        self.assertTrue(cache.needs_refresh(meta, NOW, NOW + timedelta(seconds=300)))

    async def read(self, rows, stale=False, fail_queue=False):
        session = AsyncMock()
        result = MagicMock()
        result.mappings.return_value.all.return_value = rows
        session.execute.return_value = result
        redis = AsyncMock()
        if fail_queue:
            redis.enqueue_job.side_effect = RuntimeError("Redis unavailable")
        with patch.object(cache, "AsyncDBSession", return_value=context(session)), \
                patch("chessism_api.operations.common_positions.get_common_positions", AsyncMock(return_value={"rows": []})), \
                patch.object(cache, "needs_refresh", return_value=stale):
            payload = await cache.get_fen_appearances(redis)
        self.assertNotIn("FROM fen", str(session.execute.call_args.args[0]))
        return payload, redis

    async def test_read_is_small_and_does_not_scan_live_fens(self):
        payload, redis = await self.read([{"refreshed_at": NOW, "source_updated_at": NOW,
            "current_source_at": NOW, "appearances": 2, "total_fens": 5, "unscored_fens": 2}])
        self.assertEqual(payload["rows"], [{"appearances": 2, "total_fens": 5, "unscored_fens": 2}])
        self.assertEqual(payload["definition"], "all_fens")
        self.assertFalse(payload["stale"])
        redis.enqueue_job.assert_not_awaited()

    async def test_missing_snapshot_queues_unique_background_job(self):
        payload, redis = await self.read([{"refreshed_at": None, "appearances": None}], stale=True)
        self.assertEqual(payload["rows"], [])
        self.assertIsNone(payload["refreshed_at"])
        redis.enqueue_job.assert_awaited_once_with("run_pending_repetition_refresh",
            _job_id=cache.JOB_ID, _queue_name="analysis_queue")

    async def test_empty_snapshot_is_not_missing_and_redis_failure_keeps_old_bins(self):
        payload, _ = await self.read([{"refreshed_at": NOW, "appearances": None}])
        self.assertEqual(payload["rows"], [])
        self.assertIsNotNone(payload["refreshed_at"])
        with self.assertLogs(cache.LOG, level="ERROR"):
            payload, _ = await self.read([{"refreshed_at": NOW, "appearances": 1, "total_fens": 2000, "unscored_fens": 1500}],
                                        stale=True, fail_queue=True)
        self.assertEqual(payload["rows"][0]["total_fens"], 2000)
        self.assertIn("unavailable", payload["refresh_error"])

    async def test_worker_publishes_bins_and_timestamp_in_one_snapshot_transaction(self):
        mark, refresh = AsyncMock(), AsyncMock()
        result = MagicMock()
        result.scalar.return_value = NOW
        refresh.execute.return_value = result
        with patch.object(cache, "AsyncDBSession", side_effect=[context(mark), context(refresh)]):
            await cache.run_pending_repetition_refresh({})
        queries = [str(c.args[0]) for c in refresh.execute.call_args_list]
        self.assertIn("REPEATABLE READ", queries[0])
        aggregate = [q for q in queries if "FROM fen GROUP BY n_games" in q]
        self.assertEqual(len(aggregate), 1)
        self.assertIn("COUNT(*)::bigint", aggregate[0])
        self.assertIn("COUNT(*) FILTER (WHERE score IS NULL)::bigint", aggregate[0])
        self.assertTrue(any("statement_timeout = '180s'" in q for q in queries))
        self.assertIn("last_error = NULL", queries[-1])
        refresh.commit.assert_awaited_once()

    async def test_existing_unscored_endpoint_uses_shared_snapshot(self):
        snapshot = {"definition": "all_fens", "refreshed_at": NOW.isoformat(), "rows": [
            {"appearances": 0, "total_fens": 4, "unscored_fens": 2},
            {"appearances": 1, "total_fens": 30, "unscored_fens": 10},
            {"appearances": 1000000, "total_fens": 2, "unscored_fens": 0},
        ]}
        with patch.object(cache, "get_fen_appearances", AsyncMock(return_value=snapshot)):
            payload = await cache.get_pending_repetitions(AsyncMock())
        self.assertEqual(payload["definition"], "score_is_null")
        self.assertEqual(payload["refreshed_at"], NOW.isoformat())
        self.assertEqual(payload["rows"], [{"repetitions": 0, "fen_count": 2}, {"repetitions": 1, "fen_count": 10}])

    async def test_lock_contention_never_replaces_existing_snapshot(self):
        mark, refresh = AsyncMock(), AsyncMock()
        result = MagicMock()
        result.scalar.return_value = False
        refresh.execute.return_value = result
        with patch.object(cache, "AsyncDBSession", side_effect=[context(mark), context(refresh)]):
            result = await cache.run_pending_repetition_refresh({})
        self.assertEqual(result, {"status": "already_refreshing"})
        self.assertFalse(any("DELETE FROM" in str(c.args[0]) for c in refresh.execute.call_args_list))
        refresh.commit.assert_not_awaited()

    async def test_refresh_failure_does_not_commit_partial_bins(self):
        mark, refresh, failure = AsyncMock(), AsyncMock(), AsyncMock()
        refresh.execute.side_effect = RuntimeError("failure")
        with patch.object(cache, "AsyncDBSession", side_effect=[context(mark), context(refresh), context(failure)]), \
                self.assertLogs(cache.LOG, level="ERROR"), self.assertRaises(RuntimeError):
            await cache.run_pending_repetition_refresh({})
        refresh.commit.assert_not_awaited()
        failure.commit.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
