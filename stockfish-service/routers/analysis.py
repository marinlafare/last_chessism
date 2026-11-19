import asyncio
import os
import time
from typing import Any

import chess
import chess.engine
import redis.asyncio as redis
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, model_validator
from stockfish_core.analysis import analyse, board_from_fen
from stockfish_core.http_policy import QUEUE_SECONDS, execution_seconds

from operations.engine import (
    engine_pool,
    ANALYSE_TIMEOUT_SEC,
    clean_engine_result,
    finish_engine_cleanup,
)

router = APIRouter()
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
PROGRESS_TTL_SECONDS = 60 * 60 * 24
MAX_ANALYSIS_BATCH_SIZE = 1000
redis_client: redis.Redis | None = None

PROGRESS_UPDATE_SCRIPT = """
if not ARGV[8] or ARGV[8] == '' or redis.call('GET', KEYS[4]) ~= ARGV[8] then
    return 0
end
local previous = nil
local current = redis.call('GET', KEYS[1])
if current then
    local ok, decoded = pcall(cjson.decode, current)
    if ok then
        -- Engine requests can outlive the job that finalized this progress.
        if decoded['phase'] == 'failed' or decoded['phase'] == 'complete' then
            return tonumber(decoded['processed']) or 0
        end
        previous = decoded
    end
end

redis.call('SADD', KEYS[2], ARGV[1])
if ARGV[2] == '1' then
    redis.call('SADD', KEYS[3], ARGV[1])
end

local processed = redis.call('SCARD', KEYS[2])
local failed = redis.call('SCARD', KEYS[3])
if previous then
    processed = math.max(processed, tonumber(previous['processed']) or 0)
    failed = math.max(failed, tonumber(previous['failed']) or 0)
end

local total = tonumber(ARGV[3])
local payload = cjson.encode({
    job_id = ARGV[4],
    attempt_id = ARGV[8],
    total = total,
    processed = math.min(total, processed),
    failed = failed,
    phase = 'analyzing',
    detail = ARGV[5],
    updated_at = tonumber(ARGV[6])
})
local ttl = tonumber(ARGV[7])
redis.call('SET', KEYS[1], payload, 'EX', ttl)
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('EXPIRE', KEYS[3], ttl)
redis.call('EXPIRE', KEYS[4], ttl)
return processed
"""


class AnalysisRequest(BaseModel):
    fens: list[str] = Field(
        ...,
        min_length=1,
        max_length=MAX_ANALYSIS_BATCH_SIZE,
        description="List of FEN strings to analyze.",
    )
    # Bulk database requests are fixed by the API research schemas. The live
    # board uses the same engine but keeps its existing non-persisted budget.
    nodes_limit: int = Field(100000, ge=1, le=100_000_000)
    multipv: int = Field(4, ge=1, le=10)
    progress_job_id: str | None = Field(None, min_length=1, description="Optional ARQ job id for live progress.")
    progress_attempt_id: str | None = Field(None, min_length=1, description="Execution attempt that owns this progress.")
    progress_total: int | None = Field(None, ge=1, description="Total FEN count for the job.")
    progress_detail_prefix: str | None = Field(None, description="Optional loop label for live progress.")

    @model_validator(mode="after")
    def require_progress_identity(self):
        if bool(self.progress_job_id) != bool(self.progress_attempt_id):
            raise ValueError("Progress requires both job and attempt IDs")
        return self


def _board_from_fen(fen: str) -> chess.Board | None:
    try:
        return board_from_fen(fen)
    except ValueError:
        return None


def _terminal_result(board: chess.Board, fen: str) -> dict[str, Any]:
    if board.is_checkmate():
        score = chess.engine.PovScore(chess.engine.Mate(0), board.turn)
    else:
        score = chess.engine.PovScore(chess.engine.Cp(0), board.turn)
    return clean_engine_result(
        result={"pv": [], "score": score},
        original_fen=fen,
        is_valid=True
    )


async def _get_redis() -> redis.Redis:
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True,
                                   socket_connect_timeout=2, socket_timeout=2)
    return redis_client


async def close_redis() -> None:
    """Close the lazily-created progress client during service shutdown."""
    global redis_client
    if redis_client is None:
        return
    close = getattr(redis_client, "aclose", redis_client.close)
    await close()
    redis_client = None


async def _write_progress(
    request: AnalysisRequest,
    fen: str,
    current_failed: bool,
    detail: str,
) -> None:
    if not request.progress_job_id:
        return

    total = int(request.progress_total or len(request.fens))
    full_detail = (
        f"{request.progress_detail_prefix} | {detail}"
        if request.progress_detail_prefix
        else detail
    )
    progress_key = f"chessism:job_progress:{request.progress_job_id}"
    processed_key = f"chessism:job_progress_fens:{request.progress_job_id}"
    failed_key = f"chessism:job_progress_failed_fens:{request.progress_job_id}"
    attempt_key = f"chessism:job_progress_attempt:{request.progress_job_id}"
    try:
        client = await _get_redis()
        await client.eval(
            PROGRESS_UPDATE_SCRIPT,
            4,
            progress_key,
            processed_key,
            failed_key,
            attempt_key,
            fen,
            "1" if current_failed else "0",
            total,
            request.progress_job_id,
            full_detail,
            time.time(),
            PROGRESS_TTL_SECONDS,
            request.progress_attempt_id,
        )
    except Exception as exc:
        print(f"Failed to write analysis progress: {exc!r}", flush=True)


async def _analyze(request: AnalysisRequest) -> list[dict[str, Any]]:
    """
    Accepts a list of FENs and returns a list of raw analysis results.
    """
    validated_boards = [_board_from_fen(fen) for fen in request.fens]
    final_results: list[dict[str, Any]] = []

    async with engine_pool.acquire() as engine_slot:
        try:
            engine = engine_slot.engine
            if engine is None:
                raise RuntimeError(f"Engine {engine_slot.number} is unavailable")
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Engine failed to start: {e}")

        for original_fen, board in zip(request.fens, validated_boards):
            if board is None:
                final_results.append(clean_engine_result(result={}, original_fen=original_fen, is_valid=False))
                await _write_progress(request, original_fen, True, f"batch fen {len(final_results)}/{len(request.fens)}")
                continue

            if board.is_game_over():
                final_results.append(_terminal_result(board, original_fen))
                await _write_progress(request, original_fen, False, f"batch fen {len(final_results)}/{len(request.fens)}")
                continue

            try:
                result = await asyncio.wait_for(
                    analyse(engine, board, request.nodes_limit, request.multipv),
                    timeout=ANALYSE_TIMEOUT_SEC)
            except asyncio.TimeoutError as error:
                raise HTTPException(status_code=504, detail="Stockfish position timed out") from error
            except chess.engine.EngineTerminatedError as error:
                raise HTTPException(status_code=503, detail="Stockfish process disconnected") from error

            if isinstance(result, list):
                analysis_list = [
                    clean_engine_result(result=item, original_fen=original_fen, is_valid=True)["analysis"]
                    for item in result
                ]
                final_results.append({
                    "fen": original_fen,
                    "is_valid": True,
                    "analysis": analysis_list
                })
            else:
                final_results.append(clean_engine_result(result=result, original_fen=original_fen, is_valid=True))

            await _write_progress(request, original_fen, False, f"batch fen {len(final_results)}/{len(request.fens)}")

    return final_results


async def _disconnected(request: Request):
    while not await request.is_disconnected():
        await asyncio.sleep(.1)


@router.post("/analyze")
async def analyze_fens_endpoint(request: AnalysisRequest, connection: Request = None) -> list[dict[str, Any]]:
    """Bound queue/search time and drain a disconnected request before releasing engines."""
    work = asyncio.create_task(_analyze(request))
    tasks = [work]
    if connection is not None:
        tasks.append(asyncio.create_task(_disconnected(connection)))
    try:
        done, _ = await asyncio.wait(tasks, timeout=QUEUE_SECONDS + execution_seconds(len(request.fens), request.nodes_limit),
                                     return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            try:
                return work.result()
            except asyncio.TimeoutError as error:
                raise HTTPException(status_code=503, detail="Stockfish queue/startup timed out") from error
        if done:
            raise HTTPException(status_code=499, detail="Analysis client disconnected")
        raise HTTPException(status_code=504, detail="Stockfish batch deadline exceeded")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await finish_engine_cleanup(asyncio.gather(*tasks, return_exceptions=True))
