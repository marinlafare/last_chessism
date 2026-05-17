import asyncio
import time
from testing_chessism import test_api_run_analysis_job_with_perf


async def run_three(run_id: str):
    await asyncio.gather(
        test_api_run_analysis_job_with_perf(
            total_fens=2000,
            batch_size=500,
            nodes=1_000_000,
            log_path=f"perf_log_job1_{run_id}.txt",
            interval_sec=2,
        ),
        test_api_run_analysis_job_with_perf(
            total_fens=2000,
            batch_size=500,
            nodes=1_000_000,
            log_path=f"perf_log_job2_{run_id}.txt",
            interval_sec=2,
        ),
        test_api_run_analysis_job_with_perf(
            total_fens=2000,
            batch_size=500,
            nodes=1_000_000,
            log_path=f"perf_log_job3_{run_id}.txt",
            interval_sec=2,
        ),
    )


if __name__ == "__main__":
    for run in range(8):
        run_id = time.strftime("%Y%m%d_%H%M%S")
        asyncio.run(run_three(run_id))
        time.sleep(120)
