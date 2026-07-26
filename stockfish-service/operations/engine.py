# stockfish-service/operations/engine.py

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Dict
from enum import Enum

import chess
import chess.engine
from fastapi import HTTPException

# --- CORE CONFIGURATION ---

STOCKFISH_PATH = os.environ.get("STOCKFISH_PATH", "/usr/local/bin/stockfish")
THREADS = int(os.environ.get("STOCKFISH_THREADS", "1"))
HASH_MB = int(os.environ.get("STOCKFISH_HASH_MB", "256"))
ENGINE_COUNT = max(1, int(os.environ.get("STOCKFISH_ENGINE_COUNT", "4")))
ANALYSE_TIMEOUT_SEC = float(os.environ.get("STOCKFISH_ANALYSE_TIMEOUT_SEC", "30"))
SYZYGY_PATH = os.environ.get("STOCKFISH_SYZYGY_PATH", "")
SYZYGY_PROBE_DEPTH = int(os.environ.get("STOCKFISH_SYZYGY_PROBE_DEPTH", "1"))


def convert_to_serializable(value: Any) -> Any:
    """
    Recursively converts complex chess.engine objects (like PovScore, Move)
    into JSON-serializable types (int, str, list).
    """
    if isinstance(value, chess.engine.PovScore):
        absolute_score = value.white()
        return absolute_score.score(mate_score=10000)

    if isinstance(value, chess.engine.PovWdl):
        return _normalize_wdl(value)

    if isinstance(value, chess.engine.Wdl):
        return _normalize_wdl(value)

    if isinstance(value, chess.Move):
        return value.uci()

    if isinstance(value, list):
        return [convert_to_serializable(item) for item in value]

    if isinstance(value, dict):
        return {
            _convert_key_to_string(key): convert_to_serializable(item)
            for key, item in value.items()
        }

    return value


def _convert_key_to_string(key: Any) -> str:
    if isinstance(key, Enum):
        return key.name.lower()
    if isinstance(key, chess.Move):
        return key.uci()
    return str(key)


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

    output["analysis"] = convert_to_serializable(result)
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


async def _start_engine(engine_number: int | None = None) -> tuple[Any, chess.engine.UciProtocol]:
    """
    Initializes and configures a NEW Stockfish engine instance.
    """
    transport = None
    engine_uci = None
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
        if SYZYGY_PATH and os.path.isdir(SYZYGY_PATH) and "SyzygyPath" in engine_uci.options:
            extra_options["SyzygyPath"] = SYZYGY_PATH
        if "SyzygyProbeDepth" in engine_uci.options:
            extra_options["SyzygyProbeDepth"] = SYZYGY_PROBE_DEPTH
        if extra_options:
            await engine_uci.configure(extra_options)
            if "SyzygyPath" in extra_options:
                print(f"--- [ENGINE] Syzygy tablebases enabled: {SYZYGY_PATH} ---", flush=True)

        label = f" {engine_number}" if engine_number is not None else ""
        print(
            f"--- [ENGINE{label}] Ready: Threads={THREADS}, Hash={HASH_MB} MiB ---",
            flush=True,
        )
        return transport, engine_uci

    except Exception as e:
        if engine_uci is not None:
            try:
                await asyncio.wait_for(engine_uci.quit(), timeout=5)
            except Exception:
                pass
        if transport is not None:
            try:
                transport.close()
            except Exception:
                pass
        label = f" {engine_number}" if engine_number is not None else ""
        error_msg = f"ERROR: Failed to initialize Stockfish engine{label}: {e}"
        print(error_msg, flush=True)
        raise HTTPException(status_code=500, detail=error_msg)


@dataclass
class EngineSlot:
    number: int
    transport: Any | None = None
    engine: chess.engine.UciProtocol | None = None
    busy: bool = False


class EnginePool:
    """Own and lease independent single-threaded Stockfish processes."""

    def __init__(self, size: int = ENGINE_COUNT) -> None:
        self._size = max(1, int(size))
        self._slots: list[EngineSlot] = []
        self._available: asyncio.Queue[EngineSlot] = asyncio.Queue(maxsize=self._size)
        self._lifecycle_lock = asyncio.Lock()
        self._initialized = False

    def status(self) -> dict[str, Any]:
        busy = sum(1 for slot in self._slots if slot.busy)
        ready = sum(1 for slot in self._slots if slot.engine is not None)
        return {
            "ok": self._initialized and ready == self._size,
            "workers": {
                "total": self._size,
                "busy": busy,
                "idle": max(0, ready - busy),
            },
            "config": {
                "threads": THREADS,
                "hash_mb": HASH_MB,
                "engine_count": self._size,
            },
        }

    async def initialize(self) -> None:
        if self._initialized:
            return

        async with self._lifecycle_lock:
            if self._initialized:
                return

            slots: list[EngineSlot] = []
            try:
                for number in range(1, self._size + 1):
                    transport, engine = await _start_engine(number)
                    slots.append(
                        EngineSlot(number=number, transport=transport, engine=engine)
                    )
            except Exception:
                await asyncio.gather(
                    *(self._shutdown_slot(slot) for slot in slots),
                    return_exceptions=True,
                )
                raise

            self._slots = slots
            self._available = asyncio.Queue(maxsize=self._size)
            for slot in slots:
                self._available.put_nowait(slot)
            self._initialized = True
            print(f"--- [ENGINE POOL] {self._size} engines ready ---", flush=True)

    @asynccontextmanager
    async def acquire(self):
        await self.initialize()
        slot = await self._available.get()
        slot.busy = True
        try:
            if slot.engine is None:
                await self.restart(slot, "unavailable")
            yield slot
        finally:
            slot.busy = False
            if self._initialized:
                self._available.put_nowait(slot)

    async def restart(self, slot: EngineSlot, reason: str) -> None:
        print(
            f"--- [ENGINE {slot.number}] Restarting ({reason}) ---",
            flush=True,
        )
        await self._shutdown_slot(slot)
        slot.transport, slot.engine = await _start_engine(slot.number)

    async def shutdown(self) -> None:
        async with self._lifecycle_lock:
            if not self._slots:
                self._initialized = False
                return

            self._initialized = False
            slots = self._slots
            self._slots = []
            await asyncio.gather(
                *(self._shutdown_slot(slot) for slot in slots),
                return_exceptions=True,
            )
            self._available = asyncio.Queue(maxsize=self._size)
            print("--- [ENGINE POOL] Shut down ---", flush=True)

    @staticmethod
    async def _shutdown_slot(slot: EngineSlot) -> None:
        if slot.engine:
            try:
                await asyncio.wait_for(slot.engine.quit(), timeout=5)
            except Exception:
                pass
        if slot.transport:
            try:
                slot.transport.close()
            except Exception:
                pass
        slot.engine = None
        slot.transport = None


engine_pool = EnginePool()


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
