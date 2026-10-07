"""Analyze All accepts 1,000 without changing the regular player job limit."""

import unittest
from unittest.mock import AsyncMock, patch

from chessism_api.operations import analysis
from chessism_api.routers.analysis import (
    AnalysisJobRequest,
    AnalysisLoopJobRequest,
    FenAnalysisRequest,
    PlayerAnalysisJobRequest,
    PlayerGameAnalysisConfirmRequest,
    api_run_analysis_job,
)


class AnalysisBatchLimitTests(unittest.IsolatedAsyncioTestCase):
    def test_all_local_analysis_requests_default_to_one_hundred_thousand_nodes(self):
        requests = [AnalysisJobRequest(), AnalysisLoopJobRequest(),
                    FenAnalysisRequest(fens=["8/8/8/8/8/8/4k3/6K1 w - - 0 1"]),
                    PlayerAnalysisJobRequest(player_name="test"),
                    PlayerGameAnalysisConfirmRequest(plan_id="test")]
        for request in requests:
            with self.subTest(request=type(request).__name__):
                self.assertEqual(request.nodes_limit, 100_000)

    def test_all_request_accepts_up_to_one_thousand_and_defaults_to_five_hundred(self):
        self.assertEqual(AnalysisJobRequest().batch_size, 500)
        for size in (1, 500, 501, 1000):
            self.assertEqual(AnalysisJobRequest(batch_size=size).batch_size, size)
        for size in (0, -1, 1001, 1000.5):
            with self.subTest(size=size), self.assertRaises(ValueError):
                AnalysisJobRequest(batch_size=size)

    def test_player_request_retains_five_hundred_limit(self):
        self.assertEqual(PlayerAnalysisJobRequest(player_name="test").batch_size, 500)
        with self.assertRaises(ValueError):
            PlayerAnalysisJobRequest(player_name="test", batch_size=501)

    async def test_all_route_enqueues_requested_batch_unchanged(self):
        redis = AsyncMock()
        redis.enqueue_job.return_value = "test-job"
        response = await api_run_analysis_job(
            AnalysisJobRequest(total_fens_to_process=5000, batch_size=1000), redis,
        )
        self.assertEqual(response.status_code, 202)
        args, kwargs = redis.enqueue_job.call_args
        self.assertEqual(args, ("run_analysis_job",))
        self.assertEqual(kwargs["batch_size"], 1000)
        self.assertEqual(kwargs["total_fens_to_process"], 5000)
        self.assertEqual(kwargs["nodes_limit"], 100_000)
        self.assertEqual(kwargs["_queue_name"], "analysis_queue")

    async def test_all_worker_passes_thousand_limit_to_balanced_dispatcher(self):
        with patch.object(analysis, "_run_analysis_job", new_callable=AsyncMock) as run:
            await analysis.run_analysis_job({}, 5000, 1000, 1_000_000)
        self.assertEqual(run.await_args.args, ({}, 5000, 1000, 1_000_000))
        self.assertEqual(run.await_args.kwargs["max_batch_size"], 1000)
        self.assertIs(run.await_args.kwargs["fetch_batch"], analysis.get_fens_for_analysis)


if __name__ == "__main__":
    unittest.main()
