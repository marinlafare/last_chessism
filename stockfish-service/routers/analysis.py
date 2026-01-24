# stockfish-service/routers/analysis.py

import asyncio
from typing import List, Dict, Any, Union

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
    fens: List[str] = Field(..., description="List of FEN strings to analyze.")
    nodes_limit: int = Field(1000000, description="Nodes limit per position.")
    multipv: int = Field(4, description="Number of principal variations to return.")


@router.post("/analyze")
async def analyze_fens_endpoint(request: AnalysisRequest) -> List[Dict[str, Any]]:
    """
    Accepts a list of FENs and returns a list of raw analysis results.
    Creates a NEW engine for this request to ensure a clean state.
    """
    async def validate_fen(fen_str: str) -> Union[chess.Board, None]:
        try:
            return await asyncio.to_thread(chess.Board, fen_str)
        except ValueError:
            return None

    validation_tasks = [validate_fen(fen) for fen in request.fens]
    validated_boards = await asyncio.gather(*validation_tasks)

    final_results: List[Dict[str, Any]] = []

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
                synthetic_result = {"pv": []}
                if board.is_checkmate():
                    score = chess.engine.PovScore(chess.engine.Mate(0), board.turn)
                else:
                    score = chess.engine.PovScore(chess.engine.Cp(0), board.turn)
                synthetic_result["score"] = score
                final_results.append(clean_engine_result(result=synthetic_result, original_fen=original_fen, is_valid=True))
                continue

            await uci_newgame(engine)
            limit = chess.engine.Limit(nodes=request.nodes_limit)

            result = None
            last_error = None
            for attempt in range(2):
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
