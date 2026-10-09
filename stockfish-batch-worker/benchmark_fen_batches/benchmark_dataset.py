"""Prepare the FEN-batch benchmark's fixture with a bounded read-only database sample."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import random
import subprocess

import chess

SQL = "SELECT fen FROM public.fen TABLESAMPLE SYSTEM (0.05) REPEATABLE (106) LIMIT 30000;"
TARGETS = {"many_pieces": 300, "middle_material": 400, "few_pieces": 300}


def select(candidates, targets=TARGETS):
    groups = {key: [] for key in targets}
    seen = set()
    for fen in candidates:
        fields = fen.strip().split()
        if len(fields) == 4:
            fields += ["0", "1"]
        if len(fields) != 6:
            continue
        normalized = " ".join(fields)
        if normalized in seen:
            continue
        seen.add(normalized)
        try:
            board = chess.Board(normalized)
        except ValueError:
            continue
        if not board.is_valid() or board.is_game_over():
            continue
        pieces = len(board.piece_map())
        key = "many_pieces" if pieces >= 25 else "middle_material" if pieces >= 13 else "few_pieces"
        groups[key].append(normalized)
    rng = random.Random(106)
    chosen = []
    for key, count in targets.items():
        if len(groups[key]) < count:
            raise ValueError(f"Insufficient {key} positions: {len(groups[key])} < {count}; no automatic resampling")
        chosen.extend(rng.sample(sorted(groups[key]), count))
    rng.shuffle(chosen)
    return [{"id": f"fen-{i:04d}", "fen": fen} for i, fen in enumerate(chosen)], {k: len(v) for k, v in groups.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Use a new directory; never replace a benchmark fixture")
    result = subprocess.run([
        "docker", "exec", "-i", "last_chessism-db-1", "sh", "-c",
        'PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=10000" '
        'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atq -v ON_ERROR_STOP=1',
    ], input=SQL, text=True, capture_output=True, timeout=20, check=True)
    candidates = result.stdout.splitlines()
    rows, available = select(candidates)
    raw = b"".join((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode() for row in rows)
    report = {"positions": len(rows), "distinct_positions": len({r["fen"] for r in rows}),
              "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "source": "local fen table, read only",
              "sql": SQL, "candidate_count": len(candidates), "valid_nonterminal_candidates": available,
              "chosen_material_groups": TARGETS, "seed": 106,
              "note": "Material-stratified comparison fixture, not a population estimate. Four-field FENs get counters 0 1; no production import.",
              "side_to_move": dict(Counter(row["fen"].split()[1] for row in rows))}
    args.output.mkdir(parents=True)
    (args.output / "input.jsonl").write_bytes(raw)
    (args.output / "dataset.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
