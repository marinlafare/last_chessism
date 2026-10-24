"""Bounded completion-artifact reads shared by all cloud execution formats.

Missing artifacts are not successful completion. Callers decide whether to wait
or fail, and still validate manifests against committed receipts before cleanup.
Storage/permission/JSON errors must propagate, never masquerade as missing data.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json

from stockfish_batch.storage import MAX_MANIFEST_BYTES

PERFORMANCE_MAX_BYTES = 128 * 1024


def read_json(storage, output, filename, limit):
    """Synchronous bounded read, also usable from a dedicated reader pool."""
    raw = storage.read(output + '/' + filename, limit)
    if raw is None:
        return None
    document = json.loads(raw)
    if not isinstance(document, dict):
        raise ValueError(f'{filename} must contain a JSON object')
    return document


async def read_manifest(storage, output):
    return await asyncio.to_thread(read_json, storage, output, 'manifest.json', MAX_MANIFEST_BYTES)


async def read_performance(storage, output):
    return await asyncio.to_thread(read_json, storage, output, 'performance.json', PERFORMANCE_MAX_BYTES)


def read_completion(storage_factory, output):
    """Read a bounded work unit's manifest/report together, with separate clients.

    Called off the event loop. A failure drains both readers; it must never
    allow cleanup or leave a database writer running in the background.
    """
    def read(filename, limit):
        return read_json(storage_factory(), output, filename, limit)

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='cloud-completion') as pool:
        manifest = pool.submit(read, 'manifest.json', MAX_MANIFEST_BYTES)
        performance = pool.submit(read, 'performance.json', PERFORMANCE_MAX_BYTES)
        return manifest.result(), performance.result()
