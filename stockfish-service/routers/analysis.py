import asyncio
import json
import os
import time
from typing import Any

import chess
import chess.engine
import redis.asyncio as redis
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from operations.engine import (
    engine_manager,
    ANALYSE_TIMEOUT_SEC,
    uci_newgame,
    clean_engine_result,
)

router = APIRouter()
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
PROGRESS_TTL_SECONDS = 60 * 60 * 24
redis_client: redis.Redis | None = None


class AnalysisRequest(BaseModel):
    fens: list[str] = Field(..., min_length=1, max_length=500, description="List of FEN strings to analyze.")
    nodes_limit: int = Field(1_000_000, ge=1, le=100_000_000, description="Nodes limit per position.")
    multipv: int = Field(4, ge=1, le=10, description="Number of principal variations to return.")
    progress_job_id: str | None = Field(None, description="Optional ARQ job id for live progress.")
    progress_total: int | None = Field(None, ge=1, description="Total FEN count for the job.")
    progress_offset: int = Field(0, ge=0, description="FENs already processed before this batch.")


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


async def _write_progress(request: AnalysisRequest, processed_in_batch: int, failed: int, detail: str) -> None:
    if not request.progress_job_id:
        return

    total = int(request.progress_total or len(request.fens))
    payload = {
        "job_id": request.progress_job_id,
        "total": total,
        "processed": min(total, int(request.progress_offset) + int(processed_in_batch)),
        "failed": int(failed),
        "phase": "analyzing",
        "detail": detail,
        "updated_at": time.time(),
    }
    try:
        client = await _get_redis()
        await client.set(
            f"chessism:job_progress:{request.progress_job_id}",
            json.dumps(payload),
            ex=PROGRESS_TTL_SECONDS,
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
    failed_count = 0

    async with engine_manager.lock:
        try:
            engine = await engine_manager.get_engine()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Engine failed to start: {e}")

        for original_fen, board in zip(request.fens, validated_boards):
            if board is None:
                final_results.append(clean_engine_result(result={}, original_fen=original_fen, is_valid=False))
                failed_count += 1
                await _write_progress(request, len(final_results), failed_count, f"batch fen {len(final_results)}/{len(request.fens)}")
                continue

            if board.is_game_over():
                final_results.append(_terminal_result(board, original_fen))
                await _write_progress(request, len(final_results), failed_count, f"batch fen {len(final_results)}/{len(request.fens)}")
                continue

            await uci_newgame(engine)
            limit = chess.engine.Limit(nodes=request.nodes_limit)

            result = None
            last_error = None
            for _ in range(2):
                try:
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
                    await engine_manager.restart("timeout")
                    engine = await engine_manager.get_engine()
                except Exception as e:
                    last_error = e
                    await engine_manager.restart("error")
                    engine = await engine_manager.get_engine()

            if result is None:
                print(f"Error during analysis of FEN {original_fen}: {last_error}", flush=True)
                final_results.append(clean_engine_result(result={"error": str(last_error)}, original_fen=original_fen, is_valid=True))
                failed_count += 1
                await _write_progress(request, len(final_results), failed_count, f"batch fen {len(final_results)}/{len(request.fens)}")
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

            await _write_progress(request, len(final_results), failed_count, f"batch fen {len(final_results)}/{len(request.fens)}")

    return final_results
