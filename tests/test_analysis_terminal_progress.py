"""Execute the real progress Lua against explicitly isolated Redis."""
import json
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from chessism_api.operations import analysis
from analysis_failure_test_support import isolated_test_redis, stockfish_analysis_module


class TerminalProgressTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis = isolated_test_redis()
        self.addAsyncCleanup(self.redis.aclose)
        await self.redis.ping()
        self.job_id = "analysis-failure-test-" + uuid4().hex
        self.keys = [
            f"chessism:job_progress:{self.job_id}",
            f"chessism:job_progress_fens:{self.job_id}",
            f"chessism:job_progress_failed_fens:{self.job_id}",
        ]
        async def delete_progress():
            await self.redis.delete(*self.keys)

        self.addAsyncCleanup(delete_progress)
        self.service = stockfish_analysis_module()
        client = patch.object(self.service, "redis_client", self.redis)
        client.start()
        self.addCleanup(client.stop)
        self.request = self.service.AnalysisRequest(
            fens=["first", "second"], progress_job_id=self.job_id,
            progress_total=20, progress_detail_prefix="pass 1",
        )

    async def publish(self, phase, processed=0, failed=0):
        await analysis._write_job_progress(
            {"redis": self.redis}, self.job_id,
            total=20, processed=processed, failed=failed,
            phase=phase, detail="coordinator progress",
        )

    async def payload(self):
        return json.loads(await self.redis.get(self.keys[0]))

    async def snapshot(self):
        # Absolute expiry times avoid timing-dependent TTL assertions or sleeps.
        async with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.get(self.keys[0])
            pipeline.smembers(self.keys[1])
            pipeline.smembers(self.keys[2])
            for key in self.keys:
                pipeline.pexpiretime(key)
            return await pipeline.execute()

    async def assert_terminal_unchanged(self, phase):
        await self.redis.sadd(self.keys[1], "committed", "old-failure")
        await self.redis.sadd(self.keys[2], "old-failure")
        for key in self.keys[1:]:
            await self.redis.expire(key, 300)
        await self.publish(phase, processed=12, failed=2)
        before = await self.snapshot()

        await self.service._write_progress(self.request, "late-success", False, "late success")
        await self.service._write_progress(self.request, "late-failure", True, "late failure")

        self.assertEqual((await self.payload())["phase"], phase)
        self.assertEqual(await self.snapshot(), before)

    async def test_late_updates_preserve_failed_payload_sets_and_expirations(self):
        await self.assert_terminal_unchanged("failed")

    async def test_late_updates_preserve_complete_payload_sets_and_expirations(self):
        await self.assert_terminal_unchanged("complete")

    async def test_terminal_update_returns_existing_count_without_creating_sets(self):
        await self.publish("failed", processed=7, failed=1)
        before = await self.snapshot()
        count = await self.redis.eval(
            self.service.PROGRESS_UPDATE_SCRIPT, 3, *self.keys,
            "late-fen", "1", 20, self.job_id, "late result", 123, 86400,
        )
        self.assertEqual(count, 7)
        self.assertEqual(await self.redis.exists(*self.keys[1:]), 0)
        self.assertEqual(await self.snapshot(), before)

    async def test_ordinary_progress_deduplicates_and_preserves_payload_contract(self):
        await self.publish("queued")
        for fen, failed in (("first", False), ("first", False), ("second", True), ("second", True)):
            await self.service._write_progress(self.request, fen, failed, "batch result")
        payload = await self.payload()
        self.assertEqual(payload["job_id"], self.job_id)
        self.assertEqual(payload["total"], 20)
        self.assertEqual(payload["processed"], 2)
        self.assertEqual(payload["failed"], 1)
        self.assertEqual(payload["phase"], "analyzing")
        self.assertEqual(payload["detail"], "pass 1 | batch result")
        self.assertIsInstance(payload["updated_at"], (int, float))
        self.assertEqual(await self.redis.smembers(self.keys[1]), {"first", "second"})
        self.assertEqual(await self.redis.smembers(self.keys[2]), {"second"})
        for key in self.keys:
            ttl = await self.redis.ttl(key)
            self.assertGreater(ttl, 0)
            self.assertLessEqual(ttl, self.service.PROGRESS_TTL_SECONDS)

    async def test_ordinary_updates_preserve_monotonic_processed_and_failed_counts(self):
        await self.publish("committed", processed=8, failed=3)
        await self.service._write_progress(self.request, "first", True, "one result")
        payload = await self.payload()
        self.assertEqual((payload["processed"], payload["failed"]), (8, 3))
        self.assertEqual(payload["phase"], "analyzing")

    async def test_loop_complete_allows_the_next_pass_to_report_progress(self):
        await self.publish("loop_complete", processed=8, failed=1)
        await self.service._write_progress(self.request, "next-pass", False, "next pass")
        payload = await self.payload()
        self.assertEqual(payload["phase"], "analyzing")
        self.assertEqual((payload["processed"], payload["failed"]), (8, 1))
        self.assertEqual(await self.redis.smembers(self.keys[1]), {"next-pass"})

    async def assert_reset_allows_new_attempt(self, phase):
        await self.service._write_progress(self.request, "old-fen", True, "old attempt")
        await self.publish(phase, processed=12, failed=2)
        await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
        self.assertEqual(await self.redis.exists(*self.keys), 0)

        await self.publish("queued")
        await self.service._write_progress(self.request, "new-fen", False, "new attempt")
        payload = await self.payload()
        self.assertEqual(payload["phase"], "analyzing")
        self.assertEqual((payload["processed"], payload["failed"]), (1, 0))
        self.assertEqual(await self.redis.smembers(self.keys[1]), {"new-fen"})
        self.assertEqual(await self.redis.exists(self.keys[2]), 0)

    async def test_explicit_reset_allows_a_new_attempt_after_failure(self):
        await self.assert_reset_allows_new_attempt("failed")

    async def test_explicit_reset_allows_a_new_attempt_after_completion(self):
        await self.assert_reset_allows_new_attempt("complete")
