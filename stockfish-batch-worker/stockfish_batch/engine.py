"""Persistent asynchronous Stockfish processes; one independent engine per worker."""
import asyncio
from enum import Enum

import chess
import chess.engine
from .watchdog import progress_timeout


def serializable(value):
    # Preserve the existing stockfish-service result convention: White's score and WDL.
    if isinstance(value, chess.engine.PovScore):
        return value.white().score(mate_score=10000)
    if isinstance(value, chess.engine.PovWdl):
        value = value.white()
    if isinstance(value, chess.engine.Wdl):
        return list(value)
    if isinstance(value, chess.Move):
        return value.uci()
    if isinstance(value, list):
        return [serializable(item) for item in value]
    if isinstance(value, dict):
        return {(key.name.lower() if isinstance(key, Enum) else str(key)): serializable(item)
                for key, item in value.items()}
    return value


class Engine:
    def __init__(self, config):
        self.config, self.transport, self.protocol = config, None, None
        self.progress_callback = lambda: None

    async def __aenter__(self):
        try:
            async with asyncio.timeout(20):
                self.transport, self.protocol = await chess.engine.popen_uci([self.config.engine])
                if self.protocol.id.get("name") != "Stockfish 16.1":
                    raise ValueError(f"Expected Stockfish 16.1, got {self.protocol.id.get('name')!r}")
                await self.protocol.configure({
                    "Threads": self.config.threads, "Hash": self.config.hash_mb,
                    "UCI_ShowWDL": True, "SyzygyPath": "", "SyzygyProbeLimit": 0,
                })
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *_):
        try:
            if self.protocol is not None:
                await asyncio.wait_for(self.protocol.quit(), timeout=5)
        except (Exception, asyncio.CancelledError):
            pass
        finally:
            if self.transport is not None:
                self.transport.close()

    async def analyse(self, row):
        board = chess.Board(row["fen"])
        if board.is_game_over():
            score = chess.engine.Mate(0) if board.is_checkmate() else chess.engine.Cp(0)
            info = {"pv": [], "score": chess.engine.PovScore(score, board.turn)}
        elif self.config.stall_timeout:
            # Streaming UCI info distinguishes a long search from a stuck engine.
            # Repeated messages/CPU activity alone are not evidence of progress.
            async with progress_timeout(self.config.stall_timeout) as advanced:
                analysis = await self.protocol.analysis(
                    board, chess.engine.Limit(nodes=self.config.nodes), multipv=self.config.multipv,
                    game=object(), info=chess.engine.INFO_BASIC | chess.engine.INFO_SCORE | chess.engine.INFO_PV,
                )
                last_nodes = 0
                with analysis:
                    async for update in analysis:
                        nodes = update.get("nodes", 0)
                        if nodes > last_nodes:
                            last_nodes = nodes
                            advanced()
                            self.progress_callback()
                    await analysis.wait()
                info = analysis.multipv
        else:
            # A distinct game token sends ucinewgame and resets hash between independent FENs.
            # SCORE includes WDL. Avoid parsing unused refutations/current-line data.
            info = await asyncio.wait_for(self.protocol.analyse(
                board, chess.engine.Limit(nodes=self.config.nodes), multipv=self.config.multipv,
                game=object(), info=chess.engine.INFO_BASIC | chess.engine.INFO_SCORE | chess.engine.INFO_PV,
            ), timeout=self.config.position_timeout)
        return {"fen": row["fen"], "is_valid": True, "analysis": serializable(info)}
