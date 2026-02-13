import asyncio

from testing_chessism import test_api_run_analysis_job_with_perf


async def run_one():
    await test_api_run_analysis_job_with_perf(
        total_fens=40000,
        batch_size=500,
        nodes=1_000_000,
        log_path="perf_log_four.txt",
        interval_sec=2,
    )


if __name__ == "__main__":
    asyncio.run(run_one())
