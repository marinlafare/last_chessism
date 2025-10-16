# main.py (Save this file as main.py)

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

LC0_ENGINE: Union[chess.engine.UciProtocol, None] = None


# --- Pydantic Models for API ---

class AnalysisRequest(BaseModel):
    fens: List[str] = Field(..., description="List of FEN strings to analyze.")
    nodes_limit: int = Field(200000, description="Nodes limit per position.")


# --- Helper Functions ---

def convert_to_serializable(value: Any) -> Any:
    """
    Recursively converts complex chess.engine objects (like PovScore, Move) 
    into JSON-serializable types (int, str, list).
    """
    if isinstance(value, chess.engine.PovScore):
        # Convert PovScore to centipawn integer
        absolute_score = value.white()
        if absolute_score.is_mate():
            return 10000 + absolute_score.mate
        return absolute_score.cp
        
    if isinstance(value, chess.Move):
        # Convert Move object to UCI string
        return value.uci()
        
    if isinstance(value, list):
        # Recursively process lists (e.g., the PV list)
        return [convert_to_serializable(item) for item in value]

    if isinstance(value, dict):
        # Recursively process dictionaries
        return {k: convert_to_serializable(v) for k, v in value.items()}

    # If already a basic serializable type (int, str, float, etc.), return as is
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
        # 1. Handle Key Conversion (Enum to String)
        if isinstance(key, Enum):
            new_key = key.name.lower()
        else:
            new_key = str(key)
            
        # 2. Handle Value Conversion (PovScore, Move, List, etc. to serializable types)
        cleaned_info[new_key] = convert_to_serializable(value)
            
    output["analysis"] = cleaned_info
    return output


# --- Engine Initialization Functions (No changes needed here) ---

async def initialize_lc0_engine() -> chess.engine.UciProtocol:
    """
    Initializes and configures the Lc0 engine using backend and GPU options
    determined by environment variables.
    """
    # 1. Determine GPU Index and Backend
    try:
        gpu_id = int(os.environ.get("LC0_TARGET_GPU", 0))
    except ValueError:
        gpu_id = 0 

    BACKEND = "cuda-fp16"
    
    # Lc0 command line arguments
    command = [
        LC0_PATH,
        f"--backend={BACKEND}",
        f"--backend-opts=gpu={gpu_id}",
        f"--weights={WEIGHTS_PATH}",
        f"--threads={THREADS}",
        f"--nncache={NNCACHE}"
    ]
    
    engine_uci = None
    try:
        # Code to start engine... (details omitted for brevity)
        transport, engine_uci = await chess.engine.popen_uci(command)
        engine_id_info = engine_uci.id
        print(f"Engine ID loaded: {engine_id_info.get('name', 'Unknown Engine')}")

        # Configure the engine 
        await engine_uci.configure({
            "WeightsFile": WEIGHTS_PATH,
            "Backend": BACKEND,
            "BackendOptions": f"gpu={gpu_id}",
            "Threads": THREADS,
            "MinibatchSize": 1024
        })
        print("Engine configuration sent.")
        print("--- Leela engine ready ---")
        return engine_uci
    
    except Exception as e:
        error_msg = f"ERROR: Failed to initialize Lc0 engine: {e}"
        if engine_uci:
            await engine_uci.quit()
        raise HTTPException(status_code=500, detail=error_msg)


# --- FastAPI Application Setup ---

app = FastAPI(title="LC0 GPU Analysis API", version="1.0.0")

@app.on_event("startup")
async def startup_event():
    global LC0_ENGINE
    LC0_ENGINE = await initialize_lc0_engine()

@app.on_event("shutdown")
async def shutdown_event():
    global LC0_ENGINE
    if LC0_ENGINE:
        print("--- Shutting down Lc0 engine ---")
        await LC0_ENGINE.quit()


# --- API Endpoint ---

@app.post("/analyze")
async def analyze_fens_endpoint(request: AnalysisRequest) -> List[Dict[str, Any]]:
    """
    Accepts a list of FENs and returns a list of raw analysis results.
    """
    if LC0_ENGINE is None:
        raise HTTPException(status_code=503, detail="Engine not yet initialized or failed to start.")

    # 1. Concurrently validate all FENs using threads (CPU bound task)
    async def validate_fen(fen_str: str) -> Union[chess.Board, None]:
        try:
            return await asyncio.to_thread(chess.Board, fen_str)
        except ValueError:
            return None

    validation_tasks = [validate_fen(fen) for fen in request.fens]
    validated_boards = await asyncio.gather(*validation_tasks)
    
    final_results: List[Dict[str, Any]] = []
    
    # 2. Sequentially run analysis on valid boards (GPU bound task)
    for original_fen, board in zip(request.fens, validated_boards):
        
        if board is None:
            final_results.append(clean_engine_result(result={}, original_fen=original_fen, is_valid=False))
            continue
        
        # We only use nodes limit, as requested
        limit = chess.engine.Limit(nodes=request.nodes_limit)
        
        try:
            result = await LC0_ENGINE.analyse(
                board, 
                limit=limit, 
                info=chess.engine.Info.ALL
            )

            # Clean and format the raw result dictionary
            final_results.append(clean_engine_result(result=result, original_fen=original_fen, is_valid=True))

        except Exception as e:
            # Handle analysis-specific errors
            print(f"Error during analysis of FEN {original_fen}: {e}")
            # Ensure the structure remains valid even on error
            final_results.append(clean_engine_result(result={"error": str(e)}, original_fen=original_fen, is_valid=True))

    return final_results