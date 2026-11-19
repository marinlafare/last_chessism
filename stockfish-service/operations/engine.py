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
from stockfish_core import THREADS, HASH_MIB, STALL_SECONDS
from stockfish_core.analysis import configure, verify_binary

# --- CORE CONFIGURATION ---

STOCKFISH_PATH = os.environ.get("STOCKFISH_PATH", "/usr/local/bin/stockfish")
HASH_MB = HASH_MIB
ENGINE_COUNT = max(1, int(os.environ.get("STOCKFISH_ENGINE_COUNT", "4")))
ANALYSE_TIMEOUT_SEC = STALL_SECONDS
from stockfish_core.http_policy import ENGINE_START_SECONDS, ENGINE_CLOSE_SECONDS, QUEUE_SECONDS


async def finish_engine_cleanup(operation):
    """Drain engine/request cleanup despite repeated cancellation, then propagate it."""
    task = asyncio.ensure_future(operation)
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = cancellation or error
        except Exception:
            break
    try:
        task.result()
    except BaseException:
        if cancellation is None:
            raise
    if cancellation is not None:
        raise cancellation



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

    async def open_engine():
        nonlocal transport, engine_uci
        verify_binary(STOCKFISH_PATH)
        transport, engine_uci = await chess.engine.popen_uci([STOCKFISH_PATH])
        await configure(engine_uci, THREADS, HASH_MB)

    try:
        await asyncio.wait_for(open_engine(), timeout=ENGINE_START_SECONDS)
        return transport, engine_uci
    except BaseException:
        try:
            await finish_engine_cleanup(EnginePool._shutdown_slot(
                EngineSlot(engine_number or 0, transport, engine_uci)))
        except BaseException as error:
            print(f"Engine startup cleanup failed: {error!r}", flush=True)
        raise


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
            except BaseException:
                await finish_engine_cleanup(asyncio.gather(
                    *(self._shutdown_slot(slot) for slot in slots), return_exceptions=True))
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
        slot = await asyncio.wait_for(self._available.get(), timeout=QUEUE_SECONDS)
        slot.busy = True
        try:
            if slot.engine is None:
                await self.restart(slot, "unavailable")
            yield slot
        except BaseException:
            # A cancelled/timed-out UCI command must never share its process
            # with the next lease, even if python-chess has already sent stop.
            try:
                await finish_engine_cleanup(self._shutdown_slot(slot))
            except BaseException as error:
                print(f"Engine lease cleanup failed: {error!r}", flush=True)
            raise
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
            await finish_engine_cleanup(asyncio.gather(
                *(self._shutdown_slot(slot) for slot in slots), return_exceptions=True))
            self._available = asyncio.Queue(maxsize=self._size)
            print("--- [ENGINE POOL] Shut down ---", flush=True)

    @staticmethod
    async def _shutdown_slot(slot: EngineSlot) -> None:
        try:
            if slot.engine is not None:
                await asyncio.wait_for(slot.engine.quit(), timeout=ENGINE_CLOSE_SECONDS)
        except Exception:
            pass
        finally:
            transport = slot.transport
            slot.engine = None
            slot.transport = None
            if transport is not None:
                transport.close()


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
