"""Small, disposable per-FEN label cache; never reuse across source snapshots.

Descriptions outlive the one-hour page cache, and survive a partial page failure.
Only requested/prefetched FENs are stored. No corpus-wide cache or DB migration.
"""
import hashlib
import json
import logging

LOG = logging.getLogger(__name__)
TTL_SECONDS = 86_400
FIELDS = ("n_move", "move_color", "san", "label_occurrences", "example_game_link", "example_line")


def description_keys(positions, snapshot, layout):
    namespace = [layout.core, layout.version] if layout else ["legacy"]
    return ["fen-commonness-description-v1:" + hashlib.sha256(json.dumps(
        [namespace, snapshot, row["fen"], row["appearances"]], separators=(",", ":")
    ).encode()).hexdigest() if snapshot else None for row in positions]


async def load_descriptions(redis, keys):
    empty = [None] * len(keys)
    if redis is None or not keys or any(key is None for key in keys):
        return empty
    try:
        values = await redis.mget(keys)
        output = []
        for value in values:
            try:
                item = json.loads(value) if value else None
                output.append({field: item[field] for field in FIELDS} if isinstance(item, dict) else None)
            except (ValueError, TypeError, KeyError):
                output.append(None)
        return output if len(output) == len(keys) else empty
    except Exception:
        LOG.warning("Position description cache unavailable; reading database", exc_info=True)
        return empty


async def save_description(redis, key, row):
    if redis is None or key is None:
        return
    try:
        await redis.set(key, json.dumps({field: row[field] for field in FIELDS}), ex=TTL_SECONDS)
    except Exception:
        LOG.warning("Could not cache position description", exc_info=True)
