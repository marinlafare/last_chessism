import asyncio
import os
import time
from typing import Any

import chess
import chess.engine
import redis.asyncio as redis
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from operations.engine import (
    engine_pool,
    ANALYSE_TIMEOUT_SEC,
    uci_newgame,
    clean_engine_result,
)

router = APIRouter()
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
PROGRESS_TTL_SECONDS = 60 * 60 * 24
MAX_ANALYSIS_BATCH_SIZE = 1000
redis_client: redis.Redis | None = None

PROGRESS_UPDATE_SCRIPT = """
redis.call('SADD', KEYS[2], ARGV[1])
if ARGV[2] == '1' then
    redis.call('SADD', KEYS[3], ARGV[1])
end

local processed = redis.call('SCARD', KEYS[2])
local failed = redis.call('SCARD', KEYS[3])
local current = redis.call('GET', KEYS[1])
if current then
    local ok, decoded = pcall(cjson.decode, current)
    if ok then
        processed = math.max(processed, tonumber(decoded['processed']) or 0)
        failed = math.max(failed, tonumber(decoded['failed']) or 0)
    end
end

local total = tonumber(ARGV[3])
local payload = cjson.encode({
    job_id = ARGV[4],
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
return processed
"""


class AnalysisRequest(BaseModel):
    fens: list[str] = Field(
        ...,
        min_length=1,
        max_length=MAX_ANALYSIS_BATCH_SIZE,
        description="List of FEN strings to analyze.",
    )
    nodes_limit: int = Field(1_000_000, ge=1, le=100_000_000, description="Nodes limit per position.")
    multipv: int = Field(4, ge=1, le=10, description="Number of principal variations to return.")
    progress_job_id: str | None = Field(None, description="Optional ARQ job id for live progress.")
    progress_total: int | None = Field(None, ge=1, description="Total FEN count for the job.")
    progress_detail_prefix: str | None = Field(None, description="Optional loop label for live progress.")


def _board_from_fen(fen: str) -> chess.Board | None:
    try:
        return chess.Board(fen)
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
        redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
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
    try:
        client = await _get_redis()
        await client.eval(
            PROGRESS_UPDATE_SCRIPT,
            3,
            progress_key,
            processed_key,
            failed_key,
            fen,
            "1" if current_failed else "0",
            total,
            request.progress_job_id,
            full_detail,
            time.time(),
            PROGRESS_TTL_SECONDS,
        )
    except Exception as exc:
        print(f"Failed to write analysis progress: {exc!r}", flush=True)


@router.post("/analyze")
async def analyze_fens_endpoint(request: AnalysisRequest) -> list[dict[str, Any]]:
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

            limit = chess.engine.Limit(nodes=request.nodes_limit)

            result = None
            last_error = None
            for _ in range(2):
                try:
                    await uci_newgame(engine)
                    result = await asyncio.wait_for(
                        engine.analyse(
                            board,
                            limit=limit,
                            info=chess.engine.Info.ALL,
                            multipv=request.multipv
                        ),
                        timeout=ANALYSE_TIMEOUT_SEC
                    )
                    last_error = None
                    break
                except asyncio.TimeoutError as e:
                    last_error = e
                    await engine_pool.restart(engine_slot, "timeout")
                    engine = engine_slot.engine
                except Exception as e:
                    last_error = e
                    await engine_pool.restart(engine_slot, "error")
                    engine = engine_slot.engine

                if engine is None:
                    break

            if result is None:
                print(f"Error during analysis of FEN {original_fen}: {last_error}", flush=True)
                final_results.append(clean_engine_result(result={"error": str(last_error)}, original_fen=original_fen, is_valid=True))
                await _write_progress(request, original_fen, True, f"batch fen {len(final_results)}/{len(request.fens)}")
                continue

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
