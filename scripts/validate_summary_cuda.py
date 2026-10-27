"""Explicit offline CUDA check; accepts exported JSON rows or synthetic fixtures.

No credentials, database connection, cloud access, worker control or promotion.
Use --output to save a report that Algorithm creation can open locally.
"""
import argparse
import json
from pathlib import Path

from chessism_api.operations.research_algorithms.summary_validation import compare_score_distribution
from chessism_api.operations.mate_distance import mate_distance


def synthetic_rows():
    scores = [0, -20000, 20000, 9000, 9000.5, 9998, -9996, -10000, 10000]
    for boundary in range(-550, 551, 100):
        scores.extend([boundary - 0.0001, boundary, boundary + 0.0001])
    scores.extend(range(-1200, 1201, 7))
    return [dict(player=f'fixture-{i % 3}', mode=('bullet', 'blitz', 'rapid')[i % 3],
                 player_score=score, score_kind='tablebase' if source == 'tablebase' else 'mate' if abs(score) >= 9000 else 'cp',
                 mate_moves=mate_distance(score, source))
            for i, (score, source) in enumerate((score, source) for score in scores
                for source in (None, 'stockfish', 'stockfish_backup', 'Stockfish', 'tablebase'))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--synthetic', action='store_true')
    source.add_argument('--input', type=Path, help='Frozen JSON array of matrix rows; at most 32 MiB / 100,000 rows.')
    parser.add_argument('--device', type=int, default=0)
    parser.add_argument('--batch-rows', type=int, default=8192)
    parser.add_argument('--output', type=Path, help='New report file; existing files are never overwritten.')
    args = parser.parse_args()
    if args.output and args.output.exists():
        parser.error('Report destination already exists. Choose a new name.')
    if args.input and args.input.stat().st_size > 32 * 1024 * 1024:
        parser.error('Frozen input exceeds the 32 MiB validation limit.')
    rows = synthetic_rows() if args.synthetic else json.loads(args.input.read_text())
    report = compare_score_distribution(rows, device=args.device, batch_rows=args.batch_rows)
    report['input_source'] = 'synthetic' if args.synthetic else 'frozen_export'
    rendered = json.dumps(report, indent=2, allow_nan=False)
    if args.output:
        with args.output.open('x') as file:
            file.write(rendered + '\n')
    print(rendered)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
