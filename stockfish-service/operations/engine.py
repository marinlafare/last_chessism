# stockfish-service/operations/engine.py

import asyncio
import os
from typing import Any, Dict
from enum import Enum

import chess
import chess.engine
from fastapi import HTTPException

# --- CORE CONFIGURATION ---

STOCKFISH_PATH = os.environ.get("STOCKFISH_PATH", "/usr/local/bin/stockfish")
THREADS = int(os.environ.get("STOCKFISH_THREADS", "1"))
HASH_MB = int(os.environ.get("STOCKFISH_HASH_MB", "256"))
ANALYSE_TIMEOUT_SEC = float(os.environ.get("STOCKFISH_ANALYSE_TIMEOUT_SEC", "30"))


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

    if isinstance(value, chess.engine.PovWdl):
        return _normalize_wdl(value)

    if isinstance(value, chess.engine.Wdl):
        return _normalize_wdl(value)

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


def _normalize_wdl(wdl: Any) -> list[int]:
    if isinstance(wdl, chess.engine.PovWdl):
        w = wdl.white()
        return [int(w.wins), int(w.draws), int(w.losses)]
    if isinstance(wdl, chess.engine.Wdl):
        return [int(wdl.wins), int(wdl.draws), int(wdl.losses)]
    try:
        return [int(wdl[0]), int(wdl[1]), int(wdl[2])]
    except Exception:
        return [0, 0, 0]


async def _start_engine() -> tuple[Any, chess.engine.UciProtocol]:
    """
    Initializes and configures a NEW Stockfish engine instance.
    """
    try:
        transport, engine_uci = await chess.engine.popen_uci([STOCKFISH_PATH])

        if "Threads" in engine_uci.options:
            await engine_uci.configure({"Threads": THREADS})
        if "Hash" in engine_uci.options:
            await engine_uci.configure({"Hash": HASH_MB})

        extra_options = {}
        if "UCI_ShowWDL" in engine_uci.options:
            extra_options["UCI_ShowWDL"] = True
        if "Analysis Contempt" in engine_uci.options:
            extra_options["Analysis Contempt"] = "Off"
        if extra_options:
            await engine_uci.configure(extra_options)

        return transport, engine_uci

    except Exception as e:
        error_msg = f"ERROR: Failed to initialize Stockfish engine: {e}"
        print(error_msg, flush=True)
        raise HTTPException(status_code=500, detail=error_msg)


class EngineManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._engine: chess.engine.UciProtocol | None = None
        self._transport: Any | None = None

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    async def initialize(self) -> None:
        if self._engine is not None:
            return
        self._transport, self._engine = await _start_engine()

    async def get_engine(self) -> chess.engine.UciProtocol:
        if self._engine is None:
            await self.initialize()
        return self._engine

    async def restart(self, reason: str) -> None:
        print(f"--- [ENGINE] Restarting engine ({reason}) ---", flush=True)
        await self.shutdown()
        await self.initialize()

    async def shutdown(self) -> None:
        if self._engine:
            try:
                await asyncio.wait_for(self._engine.quit(), timeout=5)
            except Exception:
                pass
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._engine = None
        self._transport = None


engine_manager = EngineManager()


async def uci_newgame(engine: chess.engine.UciProtocol) -> None:
    """
    Best-effort UCI new game signal across python-chess versions.
    """
    if hasattr(engine, "ucinewgame"):
        result = engine.ucinewgame()
        if asyncio.iscoroutine(result):
            await result
        return
    if hasattr(engine, "uci_newgame"):
        result = engine.uci_newgame()
        if asyncio.iscoroutine(result):
            await result
        return
    send = getattr(engine, "send_line", None)
    if send:
        result = send("ucinewgame")
        if asyncio.iscoroutine(result):
            await result
        return
    send = getattr(engine, "_send_line", None)
    if send:
        result = send("ucinewgame")
        if asyncio.iscoroutine(result):
            await result
