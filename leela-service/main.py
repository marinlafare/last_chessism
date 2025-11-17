# leela-service/main.py

import asyncio
import os
from typing import List, Dict, Any, Union
from enum import Enum 

import chess
import chess.engine
import subprocess
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# --- CORE CONFIGURATION ---

LC0_PATH = "/usr/local/bin/lc0"
WEIGHTS_PATH = "/usr/local/bin/network.gz"
THREADS = 4
NNCACHE = 200000

# --- REMOVED: No longer using a global engine ---
# LC0_ENGINE: Union[chess.engine.UciProtocol, None] = None


# --- Pydantic Models for API ---

class AnalysisRequest(BaseModel):
    fens: List[str] = Field(..., description="List of FEN strings to analyze.")
    nodes_limit: int = Field(200000, description="Nodes limit per position.")


# --- Helper Functions (Unchanged) ---

def convert_to_serializable(value: Any) -> Any:
    """
    Recursively converts complex chess.engine objects (like PovScore, Move) 
    into JSON-serializable types (int, str, list).
    """
    if isinstance(value, chess.engine.PovScore):
        absolute_score = value.white()
        mate_val = absolute_score.mate()
        if mate_val is not None:
            return 10000 + mate_val
        return absolute_score.score()
        
    if isinstance(value, chess.Move):
        return value.uci()
        
    if isinstance(value, list):
        return [convert_to_serializable(item) for item in value]

    if isinstance(value, dict):
        return {k: convert_to_serializable(v) for k, v in value.items()}

    return value


def clean_engine_result(result: Dict[Any, Any], original_fen: str, is_valid: bool) -> Dict[str, Any]:
    """
    Cleans the raw engine result dictionary by converting all non-serializable keys and values.
    """
    output = {
        "fen": original_fen,
        "is_valid": is_valid,
        "analysis": {}
    }
    
    if not is_valid:
        return output
    
    cleaned_info = {}
    for key, value in result.items():
        if isinstance(key, Enum):
            new_key = key.name.lower()
        else:
            new_key = str(key)
        cleaned_info[new_key] = convert_to_serializable(value)
            
    output["analysis"] = cleaned_info
    return output


# --- Engine Initialization Function (Now called by the endpoint) ---

async def initialize_lc0_engine() -> chess.engine.UciProtocol:
    """
    Initializes and configures a NEW Lc0 engine instance.
    """
    try:
        gpu_id = int(os.environ.get("LC0_TARGET_GPU", 0))
    except ValueError:
        gpu_id = 0 

    BACKEND = os.environ.get("LC0_BACKEND", "cuda-fp16")
    
    print(f"--- [NEW ENGINE] Using Leela Backend: {BACKEND} ---", flush=True)
    
    command = [
        LC0_PATH,
        f"--backend={BACKEND}",
        f"--backend-opts=gpu={gpu_id}",
        f"--weights={WEIGHTS_PATH}",
        f"--threads={THREADS}",
        f"--nncache={NNCACHE}"
    ]
    
    try:
        # 1. Start the engine process
        transport, engine_uci = await chess.engine.popen_uci(command)
        
        # 2. Configure it
        await engine_uci.configure({
            "WeightsFile": WEIGHTS_PATH,
            "Backend": BACKEND,
            "BackendOptions": f"gpu={gpu_id}",
            "Threads": THREADS,
            "MinibatchSize": 1024
        })
        
        # 3. Force weights to load by analyzing 1 node
        print("--- [NEW ENGINE] Warming up (loading weights)... ---", flush=True)
        board = chess.Board()
        await engine_uci.analyse(board, chess.engine.Limit(nodes=1))
        
        print("--- [NEW ENGINE] Ready (Weights Loaded) ---", flush=True)
        return engine_uci
    
    except Exception as e:
        error_msg = f"ERROR: Failed to initialize Lc0 engine: {e}"
        print(error_msg, flush=True)
        raise HTTPException(status_code=500, detail=error_msg)


# --- FastAPI Application Setup ---

app = FastAPI(title="LC0 GPU Analysis API", version="1.0.0")

# --- REMOVED: No more global engine startup ---
# @app.on_event("startup")
# async def startup_event(): ...
#
# @app.on_event("shutdown")
# async def shutdown_event(): ...


# --- API Endpoint (MODIFIED) ---

@app.post("/analyze")
async def analyze_fens_endpoint(request: AnalysisRequest) -> List[Dict[str, Any]]:
    """
    Accepts a list of FENs and returns a list of raw analysis results.
    Creates a NEW engine for this request to ensure a clean state.
    """
    
    # --- 1. Initialize a new engine for this request ---
    engine: Union[chess.engine.UciProtocol, None] = None
    try:
        engine = await initialize_lc0_engine()
    except Exception as e:
        # If engine fails to start, we can't continue
        raise HTTPException(status_code=503, detail=f"Engine failed to start: {e}")

    # --- 2. Validate FENs (Unchanged) ---
    async def validate_fen(fen_str: str) -> Union[chess.Board, None]:
        try:
            return await asyncio.to_thread(chess.Board, fen_str)
        except ValueError:
            return None

    validation_tasks = [validate_fen(fen) for fen in request.fens]
    validated_boards = await asyncio.gather(*validation_tasks)
    
    final_results: List[Dict[str, Any]] = []
    
    # --- 3. Run Analysis Loop (Unchanged) ---
    try:
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

            limit = chess.engine.Limit(nodes=request.nodes_limit)
            
            try:
                result = await engine.analyse(
                    board, 
                    limit=limit, 
                    info=chess.engine.Info.ALL
                )
                final_results.append(clean_engine_result(result=result, original_fen=original_fen, is_valid=True))
            except Exception as e:
                print(f"Error during analysis of FEN {original_fen}: {e}", flush=True)
                final_results.append(clean_engine_result(result={"error": str(e)}, original_fen=original_fen, is_valid=True))

    finally:
        # --- 4. CRITICAL: Quit the engine process ---
        if engine:
            await engine.quit()
            print("--- [ENGINE] Process shut down. ---", flush=True)

    return final_results