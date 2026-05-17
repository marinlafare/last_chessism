# stockfish_test.py
import httpx


def analyze_fen(fen: str, nodes_limit: int = 1000000, multipv: int = 4) -> dict:
    """
    Sends a single FEN to the stockfish-service and returns the analysis result.
    """
    url = "http://localhost:9999/analyze"
    payload = {
        "fens": [fen],
        "nodes_limit": nodes_limit,
        "multipv": multipv
    }
    response = httpx.post(url, json=payload, timeout=None)
    response.raise_for_status()
    results = response.json()
    return results[0] if results else {}


def analyze_fens(fens: list[str], nodes_limit: int = 1000000, multipv: int = 4) -> list[dict]:
    """
    Sends multiple FENs to the stockfish-service in one request.
    """
    url = "http://localhost:9999/analyze"
    payload = {
        "fens": fens,
        "nodes_limit": nodes_limit,
        "multipv": multipv
    }
    response = httpx.post(url, json=payload, timeout=None)
    response.raise_for_status()
    return response.json()
