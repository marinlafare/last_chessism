"""Execute the real progress Lua against explicitly isolated Redis."""
import json
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch
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
            f"chessism:job_progress_attempt:{self.job_id}",
        ]
        async def delete_progress():
            await self.redis.delete(*self.keys)

        self.addAsyncCleanup(delete_progress)
        self.service = stockfish_analysis_module()
        client = patch.object(self.service, "redis_client", self.redis)
        client.start()
        self.addCleanup(client.stop)
        attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
        self.request = self.service.AnalysisRequest(
            fens=["first", "second"], progress_job_id=self.job_id,
            progress_attempt_id=attempt,
            progress_total=20, progress_detail_prefix="pass 1",
        )

    async def publish(self, phase, processed=0, failed=0):
        await analysis._write_job_progress(
            {"redis": self.redis}, self.job_id,
            attempt_id=self.request.progress_attempt_id,
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
            pipeline.get(self.keys[3])
            for key in self.keys:
                pipeline.pexpiretime(key)
            return await pipeline.execute()

    async def assert_terminal_unchanged(self, phase):
        await self.redis.sadd(self.keys[1], "committed", "old-failure")
        await self.redis.sadd(self.keys[2], "old-failure")
        for key in self.keys[1:3]:
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
            self.service.PROGRESS_UPDATE_SCRIPT, 4, *self.keys,
            "late-fen", "1", 20, self.job_id, "late result", 123, 86400,
            self.request.progress_attempt_id,
        )
        self.assertEqual(count, 7)
        self.assertEqual(await self.redis.exists(*self.keys[1:3]), 0)
        self.assertEqual(await self.snapshot(), before)

    async def test_ordinary_progress_deduplicates_and_preserves_payload_contract(self):
        await self.publish("queued")
        for fen, failed in (("first", False), ("first", False), ("second", True), ("second", True)):
            await self.service._write_progress(self.request, fen, failed, "batch result")
        payload = await self.payload()
        self.assertEqual(payload["job_id"], self.job_id)
        self.assertEqual(payload["attempt_id"], self.request.progress_attempt_id)
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
        old_attempt = self.request.progress_attempt_id
        attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
        self.assertNotEqual(attempt, old_attempt)
        self.assertEqual(await self.redis.get(self.keys[3]), attempt)
        self.assertEqual(await self.redis.exists(*self.keys[:3]), 0)
        self.request = self.request.model_copy(update={"progress_attempt_id": attempt})

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

    async def assert_old_update_rejected_after_reset(self, stage):
        old_request = self.request
        await self.service._write_progress(old_request, "old-fen", True, "old attempt")
        await self.publish("failed", processed=12, failed=2)
        attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
        self.request = self.service.AnalysisRequest(
            fens=["new-fen"], progress_job_id=self.job_id,
            progress_attempt_id=attempt, progress_total=4,
            progress_detail_prefix="new attempt",
        )
        if stage != "reset":
            await self.publish("queued")
            await self.service._write_progress(self.request, "new-fen", False, "new result")
        if stage == "complete":
            await self.publish("complete", processed=1)
        before = await self.snapshot()
        await self.service._write_progress(old_request, "stale-success", False, "stale result")
        await self.service._write_progress(old_request, "stale-failure", True, "stale error")
        self.assertEqual(await self.snapshot(), before)

    async def test_old_update_immediately_after_reset_cannot_recreate_progress(self):
        await self.assert_old_update_rejected_after_reset("reset")

    async def test_old_update_after_new_progress_cannot_contaminate_attempt(self):
        await self.assert_old_update_rejected_after_reset("started")

    async def test_old_update_after_new_completion_cannot_change_terminal_progress(self):
        await self.assert_old_update_rejected_after_reset("complete")

    async def test_reset_rotates_identity_and_clears_state_with_one_atomic_operation(self):
        await self.publish("failed", processed=7, failed=2)
        await self.redis.sadd(self.keys[1], "old-fen")
        await self.redis.sadd(self.keys[2], "old-fen")
        old_attempt = self.request.progress_attempt_id
        with patch.object(self.redis, "eval", wraps=self.redis.eval) as evaluate, \
             patch.object(self.redis, "delete", wraps=self.redis.delete) as delete:
            attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
        evaluate.assert_called_once()
        delete.assert_not_called()
        self.assertEqual(await self.redis.exists(*self.keys[:3]), 0)
        self.assertEqual(await self.redis.get(self.keys[3]), attempt)
        self.assertNotEqual(attempt, old_attempt)
        self.assertEqual(len(attempt), 32)

    async def test_stale_coordinator_updates_cannot_recreate_or_change_new_progress(self):
        old_attempt = self.request.progress_attempt_id
        for stage in ("reset", "started", "complete", "failed"):
            with self.subTest(stage=stage):
                attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
                self.request = self.request.model_copy(update={"progress_attempt_id": attempt})
                if stage != "reset":
                    await self.publish("queued" if stage == "started" else stage)
                before = await self.snapshot()
                for phase in ("committed", "failed", "complete"):
                    await analysis._write_job_progress(
                        {"redis": self.redis}, self.job_id, attempt_id=old_attempt,
                        total=999, processed=99, failed=9, phase=phase, detail="stale coordinator",
                    )
                    self.assertEqual(await self.snapshot(), before)

    async def test_expired_identity_rejects_both_writers_without_resurrecting_state(self):
        for expire_all in (False, True):
            with self.subTest(expire_all=expire_all):
                attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
                self.request = self.request.model_copy(update={"progress_attempt_id": attempt})
                await self.publish("queued")
                for key in (self.keys if expire_all else self.keys[3:]):
                    await self.redis.pexpireat(key, 1)
                before = await self.snapshot()
                await self.service._write_progress(self.request, "late-fen", False, "expired")
                await self.publish("complete", processed=10)
                self.assertEqual(await self.snapshot(), before)

    async def test_lua_rejects_missing_or_mismatched_identity_before_any_mutation(self):
        await self.publish("queued")
        before = await self.snapshot()
        for token in (None, "", "previous-attempt"):
            with self.subTest(token=token):
                arguments = ["stale-fen", "1", 999, self.job_id, "stale", 123, 86400]
                if token is not None:
                    arguments.append(token)
                self.assertEqual(await self.redis.eval(
                    self.service.PROGRESS_UPDATE_SCRIPT, 4, *self.keys, *arguments), 0)
                payload = dict(job_id=self.job_id, total=999, processed=99, failed=9, phase="failed")
                if token is not None:
                    payload["attempt_id"] = token
                self.assertEqual(await self.redis.eval(
                    analysis.JOB_PROGRESS_UPDATE_SCRIPT, 2, self.keys[0], self.keys[3],
                    json.dumps(payload), 86400), 0)
                self.assertEqual(await self.snapshot(), before)

    async def test_valid_updates_refresh_attempt_retention(self):
        await self.redis.expire(self.keys[3], 60)
        await self.publish("queued")
        self.assertGreater(await self.redis.ttl(self.keys[3]), 60)
        await self.redis.expire(self.keys[3], 60)
        await self.service._write_progress(self.request, "current-fen", False, "current")
        self.assertGreater(await self.redis.ttl(self.keys[3]), 60)
        self.assertEqual((await self.payload())["attempt_id"], self.request.progress_attempt_id)

    async def test_failed_reset_does_not_authorize_new_writers_to_use_old_identity(self):
        await self.publish("failed", processed=5)
        before = await self.snapshot()
        old_attempt = self.request.progress_attempt_id
        with patch.object(self.redis, "eval", side_effect=OSError("Redis unavailable")):
            attempt = await analysis._reset_job_progress({"redis": self.redis}, self.job_id)
        self.assertNotEqual(attempt, old_attempt)
        self.request = self.request.model_copy(update={"progress_attempt_id": attempt})
        await self.publish("queued")
        await self.service._write_progress(self.request, "new-fen", False, "unauthorized")
        self.assertEqual(await self.snapshot(), before)


class ProgressAttemptContractTests(unittest.IsolatedAsyncioTestCase):
    def test_progress_request_requires_both_identifiers_and_live_requests_remain_valid(self):
        request_type = stockfish_analysis_module().AnalysisRequest
        self.assertIsNone(request_type(fens=["fen"]).progress_job_id)
        request_type(fens=["fen"], progress_job_id="job", progress_attempt_id="attempt")
        for identity in ({"progress_job_id": "job"}, {"progress_attempt_id": "attempt"},
                         {"progress_job_id": "job", "progress_attempt_id": ""},
                         {"progress_job_id": "", "progress_attempt_id": "attempt"}):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                request_type(fens=["fen"], **identity)

    async def test_engine_client_transmits_the_attempt_without_changing_request_policy(self):
        response = Mock()
        response.json.return_value = [{"fen": "fen"}]
        client = Mock(post=AsyncMock(return_value=response))
        result = await analysis._call_engine_service(
            client, analysis.ENGINE_URL, ["fen"], 100,
            progress_job_id="job", progress_attempt_id="attempt", progress_total=20,
            progress_detail_prefix="run 2",
        )
        self.assertEqual(result, [{"fen": "fen"}])
        client.post.assert_awaited_once_with(analysis.ENGINE_URL, timeout=None, json={
            "fens": ["fen"], "nodes_limit": 100, "progress_job_id": "job",
            "progress_attempt_id": "attempt", "progress_total": 20, "progress_detail_prefix": "run 2",
        })

    async def test_missing_redis_does_not_prevent_distinct_attempts(self):
        first = await analysis._reset_job_progress({}, "same-job")
        second = await analysis._reset_job_progress({}, "same-job")
        self.assertNotEqual(first, second)
        self.assertEqual(len(first), 32)
        self.assertEqual(len(second), 32)
