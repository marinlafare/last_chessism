"""Pure Syzygy probes, safe to import in spawned processes without DB/Redis."""
from typing import Any
import chess
import chess.syzygy

TABLEBASE_MAX_PIECES = 5


def _move_rank(
    outcome: int,
    child_dtz: int | None,
) -> tuple[int, int]:
    distance = abs(int(child_dtz or 0))
    if outcome > 0:
        return outcome, -distance
    if outcome < 0:
        return outcome, distance
    return outcome, 0


def _best_tablebase_move(
    tablebase: chess.syzygy.Tablebase,
    board: chess.Board,
) -> str | None:
    # WDL is the first ranking key. DTZ cannot rescue a worse outcome and
    # never breaks ties between draws. Keep probes for each child together
    # (tablebase locality), UCI tie-breaking, and the caller's board unchanged.
    child = board.copy(stack=False)
    best_move = None
    best_rank = None
    for move in sorted(board.legal_moves, key=lambda candidate: candidate.uci()):
        child.push(move)
        try:
            child_wdl = tablebase.get_wdl(child)
            if child_wdl is None:
                continue
            outcome = -int(child_wdl)
            if best_rank is not None and outcome < best_rank[0]:
                continue
            rank = _move_rank(outcome, tablebase.get_dtz(child) if outcome else None)
        finally:
            child.pop()
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_move = move.uci()
    return best_move


def _tablebase_score_payload(board: chess.Board, raw_wdl: int) -> dict[str, Any]:
    white_wdl = int(raw_wdl) if board.turn == chess.WHITE else -int(raw_wdl)
    if white_wdl == 2:
        return {
            "score": 1_000.0,
            "wdl_win": 1_000.0,
            "wdl_draw": 0.0,
            "wdl_loss": 0.0,
        }
    if white_wdl == -2:
        return {
            "score": -1_000.0,
            "wdl_win": 0.0,
            "wdl_draw": 0.0,
            "wdl_loss": 1_000.0,
        }
    # Cursed wins and blessed losses are draws under the fifty-move rule.
    return {
        "score": 0.0,
        "wdl_win": 0.0,
        "wdl_draw": 1_000.0,
        "wdl_loss": 0.0,
    }


def _probe_batch_sync(
    tablebase: chess.syzygy.Tablebase,
    rows: list[dict[str, Any]],
    *, best_move=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    solved = []
    unavailable = []
    for row in rows:
        fen = str(row["fen"])
        piece_count = int(row.get("piece_count") or 0)
        try:
            board = chess.Board(fen)
            if piece_count > TABLEBASE_MAX_PIECES or board.castling_rights:
                raise KeyError("Position is not compatible with the installed tablebases")
            raw_wdl = tablebase.get_wdl(board)
            raw_dtz = tablebase.get_dtz(board)
            if raw_wdl is None or raw_dtz is None:
                raise KeyError("Position is missing from the installed tablebases")
            solved.append({
                "fen": fen,
                "piece_count": piece_count,
                "tablebase_wdl": int(raw_wdl),
                "tablebase_dtz": int(raw_dtz),
                "next_moves": (best_move or _best_tablebase_move)(tablebase, board),
                **_tablebase_score_payload(board, int(raw_wdl)),
            })
        except (ValueError, KeyError, chess.syzygy.MissingTableError):
            unavailable.append({"fen": fen, "piece_count": piece_count})
    return solved, unavailable
