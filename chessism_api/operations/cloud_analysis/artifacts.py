"""Bounded completion-artifact reads shared by all cloud execution formats.

Missing artifacts are not successful completion. Callers decide whether to wait
or fail, and still validate manifests against committed receipts before cleanup.
Storage/permission/JSON errors must propagate, never masquerade as missing data.
"""
import asyncio
import json

from stockfish_batch.storage import MAX_MANIFEST_BYTES

PERFORMANCE_MAX_BYTES = 128 * 1024


async def _read_json(storage, output, filename, limit):
    raw = await asyncio.to_thread(storage.read, output + '/' + filename, limit)
    if raw is None:
        return None
    document = json.loads(raw)
    if not isinstance(document, dict):
        raise ValueError(f'{filename} must contain a JSON object')
    return document


async def read_manifest(storage, output):
    return await _read_json(storage, output, 'manifest.json', MAX_MANIFEST_BYTES)


async def read_performance(storage, output):
    return await _read_json(storage, output, 'performance.json', PERFORMANCE_MAX_BYTES)
