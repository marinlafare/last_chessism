"""Offline, bounded diagnostics of saved benchmark results; no production writes.

Run inside the existing worker image with the repository mounted read-only.
Uses one Stockfish process at a time, two hash settings, two repeats, at most
40 FENs (37 changed plus three controls). Prints JSON; never contacts Google.
"""
import asyncio
import cProfile
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import statistics
import time

import chess
from stockfish_batch.checkpoints import BatchCheckpoints, encode, semantic_digest
from stockfish_batch.config import Config
from stockfish_batch.engine import Engine

ROOT = Path('/evidence')
OLD = ROOT / 'platform-20261006ab01'
NEW = ROOT / 'vm16-20261006c016'


def load_records(directory, names):
    values = {}
    for index in range(50):
        for value in json.loads((directory / f'batches/{index:06d}.json').read_bytes())['records']:
            if value['id'] in names:
                values[value['id']] = value
    assert set(values) == set(names)
    return values


def signature(row, result):
    return semantic_digest([row], {row['id']: {'engine_result': result}})


def san(fen, move):
    return chess.Board(fen).san(chess.Move.from_uci(move))


def summarize_changes(rows, old, new):
    examples, score_deltas, sign_changes = [], [], 0
    for row in rows:
        a, b = old[row['id']]['engine_result']['analysis'], new[row['id']]['engine_result']['analysis']
        if not isinstance(a, list):
            continue
        first, second = a[0], b[0]
        if first['score'] != second['score']:
            score_deltas.append(abs(first['score'] - second['score']))
            sign_changes += first['score'] * second['score'] < 0
        if first['pv'][0] == second['pv'][0]:
            continue
        examples.append({'id': row['id'], 'fen': row['fen'],
            'old_move': san(row['fen'], first['pv'][0]), 'new_move': san(row['fen'], second['pv'][0]),
            'old_score_cp_white': first['score'], 'new_score_cp_white': second['score'],
            'old_lines': [{'move': san(row['fen'], v['pv'][0]), 'score': v['score']} for v in a],
            'new_lines': [{'move': san(row['fen'], v['pv'][0]), 'score': v['score']} for v in b]})
    return {'best_move_examples': examples, 'changed_top_scores': len(score_deltas),
        'top_score_delta_cp': {'min': min(score_deltas), 'median': statistics.median(score_deltas),
                              'max': max(score_deltas)}, 'strict_score_sign_changes': sign_changes}


def profile_results():
    payloads = [json.loads((NEW / f'download/batches/{i:06d}.json').read_bytes()) for i in range(2)]
    values = [r for p in payloads for r in p['records']]
    rows = [{'id': v['id'], 'fen': v['engine_result']['fen']} for v in values]
    contract = json.loads((NEW / 'download/contract.json').read_bytes())
    class Sink:
        def create(self, *args):
            return True
    checkpoints = BatchCheckpoints(Sink(), '/unused', contract, rows, 500)
    profiler = cProfile.Profile()
    prepared = {}
    started = time.monotonic()
    profiler.enable()
    for row, value in zip(rows, values):
        prepared[row['id']] = checkpoints.prepare(row, value['engine_result'], value['elapsed_ms'])
    for i in range(2):
        checkpoints.save_batch(i, prepared)
    profiler.disable()
    elapsed = time.monotonic() - started
    entries = []
    for stat in profiler.getstats():
        if not isinstance(stat.code, str):
            entries.append({'function': stat.code.co_name, 'file': Path(stat.code.co_filename).name,
                            'calls': stat.callcount, 'self_seconds': stat.inlinetime,
                            'cumulative_seconds': stat.totaltime})
    return {'positions': len(rows), 'wall_seconds_including_profiler': elapsed,
        'network_io': False, 'hotspots': sorted(entries, key=lambda v: v['cumulative_seconds'], reverse=True)[:15],
        'note': 'Local profiled CPU-only checkpoint path. Not a measurement of full cloud pipeline capacity.'}


async def main():
    changed = json.loads((NEW / 'changed-results.json').read_bytes())['ids']
    assert len(changed) == 37
    controls = []
    for line in (NEW / 'input.jsonl').read_bytes().splitlines():
        row = json.loads(line)
        if row['id'] not in changed and not chess.Board(row['fen']).is_game_over():
            controls.append(row['id'])
        if len(controls) == 3:
            break
    names = changed + controls
    old = load_records(OLD / 'download/batch', names)
    new = load_records(NEW / 'download', names)
    rows = [{'id': name, 'fen': new[name]['engine_result']['fen']} for name in names]
    old_contract = json.loads((OLD / 'download/batch/contract.json').read_bytes())
    new_contract = json.loads((NEW / 'download/contract.json').read_bytes())
    engine = '/usr/local/bin/stockfish'
    with open(engine, 'rb') as stream:
        engine_sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert engine_sha == old_contract['engine']['binary_sha256'] == new_contract['engine']['binary_sha256']
    changed_settings = {key: [old_contract['settings'][key], value]
                        for key, value in new_contract['settings'].items() if old_contract['settings'][key] != value}
    assert changed_settings == {'hash_mb': [2048, 256]}
    config = Config(input='/unused/input', output='/unused/output', workers=1, threads=1,
                    nodes=100000, multipv=4, memory_mib=4096, run_timeout=300, stall_timeout=60)
    runs, elapsed = {}, {}
    started = time.monotonic()
    for hash_mb in (2048, 256):
        for repeat in range(2):
            before = time.monotonic()
            results = {}
            async with Engine(replace(config, hash_mb=hash_mb).validate()) as instance:
                for row in rows:
                    results[row['id']] = await instance.analyse(row)
            runs[hash_mb, repeat] = results
            elapsed[f'{hash_mb}-{repeat}'] = time.monotonic() - before
    replay = {}
    for hash_mb, baseline in ((2048, old), (256, new)):
        expected = {row['id']: signature(row, baseline[row['id']]['engine_result']) for row in rows}
        mismatches = [[row['id'] for row in rows if signature(row, runs[hash_mb, repeat][row['id']])
                       != expected[row['id']]] for repeat in range(2)]
        nonrepeatable = [row['id'] for row in rows
            if signature(row, runs[hash_mb, 0][row['id']]) != signature(row, runs[hash_mb, 1][row['id']])]
        replay[str(hash_mb)] = {'matches_saved_cloud_each_repeat': [len(rows)-len(v) for v in mismatches],
                               'mismatches': mismatches, 'nonrepeatable_ids': nonrepeatable}
    print(json.dumps({'engine_sha': engine_sha, 'changed_settings': changed_settings,
        'positions_per_replay': len(rows), 'replay': replay, 'replay_seconds': time.monotonic() - started,
        'timings': elapsed, 'difference_details': summarize_changes(rows, old, new),
        'results_cpu_profile': profile_results()}, indent=2), flush=True)


if __name__ == '__main__':
    asyncio.run(main())
