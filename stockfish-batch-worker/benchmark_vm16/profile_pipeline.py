"""Offline memory-growth and checkpoint CPU probes. No engines or cloud calls.

Use the worker image for comparable Python/runtime versions. Input FENs are
repeated from a saved real sample with unique 64-character IDs. Memory numbers
cover Python bookkeeping, NOT the engines, OS, agents, or upload pipeline.
"""
import argparse
import asyncio
import gc
import io
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

from stockfish_batch import checkpoints as module
from stockfish_batch.checkpoints import BatchCheckpoints, encode, make_contract, parse_input
from stockfish_batch.config import Config


class Sink:
    def create(self, uri, raw):
        return True


def snapshot():
    with open('/proc/self/status') as handle:
        rss = next(int(line.split()[1]) * 1024 for line in handle if line.startswith('VmRSS:'))
    return {'rss_mib': rss / 2**20, 'peak_rss_mib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024}


def memory_case(source, count):
    baseline = snapshot()
    sample = [json.loads(line)['fen'] for line in source.read_bytes().splitlines()]
    stream = io.BytesIO()
    for i in range(count):
        stream.write(encode({'id': f'{i:064x}', 'fen': sample[i % len(sample)]}))
    raw = stream.getvalue()
    del stream, sample
    rows = parse_input(raw, count)
    config = Config('/unused/input', '/unused/output', batch_size=500)
    contract = make_contract(raw, rows, config, 'memory-probe')
    del raw
    writer = BatchCheckpoints(Sink(), '/unused', contract, rows, 500)
    queue = asyncio.Queue()
    for row in rows:
        queue.put_nowait(row)
    gc.collect()
    startup = snapshot()
    refs = {}
    while not queue.empty():
        row = queue.get_nowait()
        refs[row['id']] = writer.reference(row, {'result_sha256': f"{int(row['id'], 16):064x}"})
    complete = snapshot()
    manifest = writer.finish_references(rows, refs)
    final = snapshot()
    assert manifest['position_count'] == count
    return {'positions': count, 'baseline': baseline, 'input_and_work_queue': startup,
            'compact_completed_references': complete, 'manifest_serialization': final}


def cpu_case(directory, mode):
    batches = [json.loads((directory / f'batches/{index:06d}.json').read_bytes()) for index in range(2)]
    contract = json.loads((directory / 'contract.json').read_bytes())
    rows = [{'id': record['id'], 'fen': record['engine_result']['fen']}
            for batch in batches for record in batch['records']]
    times, calls, actual = [], [], []
    for _ in range(3):
        writer = BatchCheckpoints(Sink(), '/unused', contract, rows, 500)
        start = time.perf_counter()
        with patch.object(module, 'validate_result', wraps=module.validate_result) as validate:
            for index, batch in enumerate(batches):
                if mode == 'legacy':
                    values = {record['id']: writer.prepare(row, record['engine_result'], record['elapsed_ms'])
                              for row, record in zip(writer.groups[index], batch['records'])}
                    saved = writer.save_batch(index, values)
                else:
                    items = {record['id']: (record['engine_result'], record['elapsed_ms']) for record in batch['records']}
                    prepared = writer.prepare_batch(index, items)
                    saved = writer.commit_prepared_batch(prepared)
                assert saved == {record['id']: record for record in batch['records']}
            calls.append(validate.call_count)
        times.append(time.perf_counter() - start)
        actual.append(snapshot())
    return {'mode': mode, 'records': len(rows), 'seconds': times,
            'median_seconds': statistics.median(times), 'validation_calls': calls,
            'records_identical': True, 'memory': actual}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--cpu-mode', choices=('legacy', 'pipeline'), default='pipeline')
    parser.add_argument('--count', type=int, choices=(25000, 100000, 200000))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.count:
        result = memory_case(args.source / 'input.jsonl', args.count)
    else:
        cases = []
        for count in (25000, 100000, 200000):
            raw = subprocess.check_output([sys.executable, __file__, '--source', str(args.source), '--count', str(count)])
            cases.append(json.loads(raw))
        result = {'python': sys.version, 'scope': 'Offline bookkeeping only; not a full VM RAM measurement',
                  'memory_cases': cases, 'checkpoint_cpu': cpu_case(args.source / 'download', args.cpu_mode)}
    raw = json.dumps(result, indent=2) + '\n'
    if args.output:
        args.output.write_text(raw)
    print(raw, end='', flush=True)


if __name__ == '__main__':
    main()
