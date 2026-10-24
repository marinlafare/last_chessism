"""Only the execution wrapper differs between HTTP and cloud workers."""
import hashlib
import chess
import chess.engine

from . import ENGINE_SHA256


def verify_binary(path):
    with open(path, "rb") as stream:
        observed = hashlib.file_digest(stream, "sha256").hexdigest()
    if observed != ENGINE_SHA256:
        raise ValueError("Stockfish binary differs from the research profile")


def board_from_fen(fen):
    # Database keys have four fields. python-chess supplies identical 0/1
    # counters on all backends; retain counters when six fields were supplied.
    if len(fen.split()) not in (4, 6):
        raise ValueError("Expected four- or six-field FEN")
    board = chess.Board(fen)
    if not board.is_valid():
        raise ValueError("Invalid chess position")
    return board


async def configure(engine, threads, hash_mib):
    if engine.id.get("name") != "Stockfish 16.1":
        raise ValueError("The research profile requires Stockfish 16.1")
    await engine.configure({"Threads": threads, "Hash": hash_mib,
                            "UCI_ShowWDL": True, "SyzygyPath": "", "SyzygyProbeLimit": 0})


async def analyse(engine, board, nodes, multipv, progress=None):
    # python-chess sends ucinewgame and synchronizes with isready. Independent
    # positions never inherit another position's transposition/history tables.
    options = dict(multipv=multipv, game=object(), info=chess.engine.INFO_BASIC |
                   chess.engine.INFO_SCORE | chess.engine.INFO_PV)
    if progress is None:
        return await engine.analyse(board, chess.engine.Limit(nodes=nodes), **options)
    result = await engine.analysis(board, chess.engine.Limit(nodes=nodes), **options)
    last_nodes = 0
    with result:
        async for update in result:
            if update.get("nodes", 0) > last_nodes:
                last_nodes = update["nodes"]
                progress()
        await result.wait()
    return result.multipv
