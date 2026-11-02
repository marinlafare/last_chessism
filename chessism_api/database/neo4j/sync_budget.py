"""Memory/time bounded graph batches, independent of durable event chunk size."""
from dataclasses import dataclass
from pathlib import Path
import re
from neo4j import Query

MIB = 1024 ** 2
# Includes source rows, old/new paths, delta maps, Bolt packing, graph writes and
# indexes. These are conservative estimates, not claims of exact allocation.
BYTES_PER_PLY = 16 * 1024
GAME_OVERHEAD_PLIES = 4
MAX_EVENTS = 16  # Bound queue metadata; this is not a 16-game batch limit.


def available_memory():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    available = int(values['MemAvailable'].split()[0]) * 1024
    for limit, current in [('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.current'),
                           ('/sys/fs/cgroup/memory/memory.limit_in_bytes', '/sys/fs/cgroup/memory/memory.usage_in_bytes')]:
        try:
            available = min(available, max(0, int(Path(limit).read_text()) - int(Path(current).read_text())))
        except (OSError, ValueError):
            pass
    return available


def memory_bytes(value):
    match = re.fullmatch(r'\s*([\d.]+)\s*(B|KiB|MiB|GiB|TiB)\s*', str(value))
    if not match:
        raise ValueError(f'Unexpected Neo4j memory size: {value}')
    return int(float(match[1]) * 1024 ** ['B', 'KiB', 'MiB', 'GiB', 'TiB'].index(match[2]))


@dataclass
class BatchPolicy:
    graph_bytes: int
    target_seconds: float = 15.0
    adaptive_plies: int | None = None

    def capacity(self, host_bytes=None):
        host_bytes = available_memory() if host_bytes is None else host_bytes
        budget = min(self.graph_bytes, max(0, host_bytes - 64 * MIB) * 0.60)
        if budget < 32 * MIB:
            raise MemoryError('Insufficient memory for graph publication; retrying without losing progress')
        return int(budget) // BYTES_PER_PLY

    def limit(self, host_bytes=None, attempts=0):
        capacity = self.capacity(host_bytes)
        adaptive = self.adaptive_plies or capacity
        return max(1, min(capacity, adaptive, capacity // (2 ** min(attempts, 4))))

    def observe(self, cost, seconds):
        if seconds > self.target_seconds * 1.25:
            self.adaptive_plies = max(1, int(cost * self.target_seconds / seconds))
        elif self.adaptive_plies and seconds < self.target_seconds * 0.75:
            self.adaptive_plies = max(self.adaptive_plies + 1, int(self.adaptive_plies * 1.25))

    def shrink(self, cost):
        self.adaptive_plies = max(1, cost // 2)


async def from_server(driver, database):
    async with driver.session(database=database, default_access_mode='READ') as session:
        rows = await (await session.run(Query('''SHOW SETTINGS YIELD name,value
          WHERE name IN ['server.memory.heap.max_size','dbms.memory.transaction.total.max','db.memory.transaction.max']
          RETURN name,value''', timeout=3))).data()
    sizes = {r['name']: memory_bytes(r['value']) for r in rows}
    heap = sizes['server.memory.heap.max_size']
    # Leave the rest for existing graph state, other requests and GC.
    limits = [int(heap * 0.35)] + [int(value * 0.60) for key, value in sizes.items()
                                  if key != 'server.memory.heap.max_size' and value > 0]
    return BatchPolicy(min(limits))


def select_prefix(events, lengths, limit, capacity):
    """Select ordered event prefixes, charging each unique game's path once."""
    ids, seen, updates, cost = [], set(), [], 0
    for event in events:
        offset = event['offset_items']
        for game_id in event['ids'][offset:]:
            charge = 0 if game_id in seen else max(0, 2 * int(lengths.get(game_id) or 0)) + GAME_OVERHEAD_PLIES
            if not ids and charge > capacity:
                raise MemoryError('One game exceeds the available graph batch memory budget')
            if ids and cost + charge > limit:
                break
            if game_id not in seen:
                seen.add(game_id)
                ids.append(game_id)
            cost += charge
            offset += 1
        if offset > event['offset_items']:
            updates.append({'id': event['id'], 'offset': offset, 'done': offset == len(event['ids'])})
        if offset < len(event['ids']):
            break
    return ids, updates, cost
