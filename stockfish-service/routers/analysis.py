import asyncio
from typing import Any

import chess
import chess.engine
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from operations.engine import (
    engine_manager,
    ANALYSE_TIMEOUT_SEC,
    uci_newgame,
    clean_engine_result,
)

router = APIRouter()


class AnalysisRequest(BaseModel):
    fens: list[str] = Field(..., min_length=1, max_length=500, description="List of FEN strings to analyze.")
    nodes_limit: int = Field(1_000_000, ge=1, le=100_000_000, description="Nodes limit per position.")
    multipv: int = Field(4, ge=1, le=10, description="Number of principal variations to return.")


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


@router.post("/analyze")
async def analyze_fens_endpoint(request: AnalysisRequest) -> list[dict[str, Any]]:
    """
    Accepts a list of FENs and returns a list of raw analysis results.
    """
    validated_boards = [_board_from_fen(fen) for fen in request.fens]
    final_results: list[dict[str, Any]] = []

    async with engine_manager.lock:
        try:
            engine = await engine_manager.get_engine()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Engine failed to start: {e}")

        for original_fen, board in zip(request.fens, validated_boards):
            if board is None:
                final_results.append(clean_engine_result(result={}, original_fen=original_fen, is_valid=False))
                continue

            if board.is_game_over():
                final_results.append(_terminal_result(board, original_fen))
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

    return final_results
