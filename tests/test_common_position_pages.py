import base64
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
from chessism_api.operations import common_position_pages as pages
from test_common_positions import E4, NOW, context, result


CURSOR = pages.page_cursor({"rank": 10, "fen": E4, "appearances": 50}, NOW.isoformat())


class CommonPositionPageTests(unittest.IsolatedAsyncioTestCase):
    def test_cursor_roundtrip_and_rejection(self):
        after = pages.parse_cursor(CURSOR)
        self.assertEqual(after, {"v": 1, "rank": 10, "fen": E4, "appearances": 50, "snapshot": NOW.isoformat()})
        self.assertEqual(pages.cache_key(CURSOR), pages.cache_key(CURSOR))
        for cursor in (None, "", "invalid", "a" * 769):
            with self.assertRaises(ValueError): pages.parse_cursor(cursor)
        for field, value in (("rank", 11), ("rank", -10), ("rank", True), ("appearances", 0),
                             ("fen", "invalid"), ("snapshot", "2026-10-11")):
            encoded = base64.urlsafe_b64encode(json.dumps({**after, field: value}).encode()).decode()
            with self.assertRaises(ValueError): pages.parse_cursor(encoded)

    async def read(self, redis, source=NOW, retry=False):
        session = AsyncMock()
        session.execute.return_value = result(scalar=source)
        with patch.object(pages, "AsyncDBSession", return_value=context(session)):
            return await pages.get_common_position_page(redis, CURSOR, retry)

    async def test_missing_page_enqueues_bounded_unique_work_then_reuses_cache(self):
        redis = AsyncMock()
        redis.get.return_value = None
        self.assertEqual((await self.read(redis))["status"], "pending")
        redis.enqueue_job.assert_awaited_once_with("run_commonness_page_refresh", CURSOR,
            _job_id=pages.cache_key(CURSOR), _queue_name="analysis_queue")
        redis.enqueue_job.reset_mock()
        redis.get.return_value = json.dumps({"status": "complete", "rows": [{"rank": 11}]}).encode()
        self.assertEqual((await self.read(redis))["rows"], [{"rank": 11}])
        redis.enqueue_job.assert_not_awaited()

    async def test_changed_source_never_serves_a_mixed_page_and_retry_is_explicit(self):
        redis = AsyncMock()
        self.assertEqual((await self.read(redis, source=None))["status"], "changed")
        redis.get.assert_not_awaited()
        redis.enqueue_job.assert_not_awaited()
        redis.get.return_value = json.dumps({"status": "error", "detail": "failed"})
        self.assertEqual((await self.read(redis))["status"], "error")
        redis.delete.assert_not_awaited()
        self.assertEqual((await self.read(redis, retry=True))["status"], "pending")
        redis.delete.assert_awaited_once_with(pages.cache_key(CURSOR))

    async def build(self, count):
        session = AsyncMock()
        session.execute.side_effect = [result() for _ in range(6)] + [result(scalar=NOW),
            result([{"fen": f"fen-{i}", "appearances": 40-i} for i in range(count)])]
        described = [{"rank": 11+i, "fen": E4, "appearances": 40-i, "n_move": 1,
            "move_color": "white", "san": "e4", "label_occurrences": 2, "example_line": "1. e4"} for i in range(min(10,count))]
        with patch.object(pages, "AsyncDBSession", return_value=context(session)), \
                patch.object(pages, "describe_positions", AsyncMock(return_value=described)) as describe:
            payload = await pages.build_page(CURSOR)
        self.assertIn("LIMIT 11", str(session.execute.call_args.args[0]))
        self.assertIn("fen > :fen", str(session.execute.call_args.args[0]))
        self.assertIn("n_games <= :appearances", str(session.execute.call_args.args[0]))
        self.assertNotIn("OFFSET", str(session.execute.call_args.args[0]))
        self.assertEqual(len(describe.call_args.args[1]), min(10,count))
        self.assertEqual(describe.call_args.args[2], 11)
        self.assertEqual(describe.call_args.kwargs['snapshot'], NOW.isoformat())
        self.assertTrue(any('force_custom_plan' in str(call.args[0]) for call in session.execute.call_args_list))
        return payload

    async def test_keyset_tie_breaker_lookahead_final_partial_and_empty_pages(self):
        payload = await self.build(11)
        self.assertEqual(len(payload["rows"]), 10)
        self.assertEqual(pages.parse_cursor(payload["next_cursor"])["rank"], 20)
        self.assertEqual(payload["rows"][0]["label"], "1. e4")
        for count in (10,3,0):
            payload = await self.build(count)
            self.assertEqual(len(payload["rows"]), count)
            self.assertIsNone(payload["next_cursor"])

    async def test_worker_caches_success_with_ttl_and_failure_without_partial_rows(self):
        redis = AsyncMock()
        with patch.object(pages, "build_page", AsyncMock(return_value={"status": "complete", "rows": []})):
            self.assertEqual(await pages.run_commonness_page_refresh({"redis": redis}, CURSOR), {"status": "complete"})
        self.assertEqual(redis.set.call_args.kwargs["ex"], 3600)
        with patch.object(pages, "build_page", AsyncMock(side_effect=RuntimeError("test"))), \
                self.assertLogs(pages.LOG, level="ERROR"), self.assertRaises(RuntimeError):
            await pages.run_commonness_page_refresh({"redis": redis}, CURSOR)
        error = json.loads(redis.set.call_args.args[1])
        self.assertEqual(error["status"], "error")
        self.assertNotIn("rows", error)


if __name__ == "__main__": unittest.main()
