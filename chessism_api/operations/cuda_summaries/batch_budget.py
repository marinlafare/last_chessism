"""Transport budgets, independent of the versioned chess equations.

Leave space for CUDA/runtime overhead, not for a desktop on the dedicated GPU.
Host memory accounts for input/merge buffers, packing copies, Python metadata
and output serialization. These are conservative peak estimates,
not a promise that every byte of VRAM will be allocated by every calculation.
"""
from dataclasses import dataclass
from pathlib import Path

MIB = 1024**2
DEVICE_ROW = 256  # Measured accuracy peak ~205 bytes/row, plus group allowance below.
DEVICE_GAME = 4096  # Includes worst-case distinct owner/day histogram groups.
HOST_ROW = 192
HOST_GAME = 8192
RESERVE = 512*MIB


def host_available():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    available = int(values['MemAvailable'].split()[0])*1024
    for limit_path, used_path in (
        ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.current'),
        ('/sys/fs/cgroup/memory/memory.limit_in_bytes', '/sys/fs/cgroup/memory/memory.usage_in_bytes'),
    ):
        try:
            limit = int(Path(limit_path).read_text().strip())
            used = int(Path(used_path).read_text().strip())
            if limit_path.endswith('memory.max'):
                try:
                    stats = dict(line.split() for line in Path('/sys/fs/cgroup/memory.stat').read_text().splitlines())
                    used -= min(used,int(stats.get('inactive_file',0)))
                except (OSError,ValueError): pass
        except (OSError, ValueError):
            continue
        available = min(available, max(0, limit-used))
    return available


@dataclass(frozen=True)
class BatchBudget:
    device_bytes: int
    host_bytes: int  # One of two bounded input slots.

    def fits(self, positions, games):
        return (positions*DEVICE_ROW + games*DEVICE_GAME <= self.device_bytes
                and positions*HOST_ROW + games*HOST_GAME <= self.host_bytes)

    def smaller(self):
        return BatchBudget(self.device_bytes//2, self.host_bytes//2)

    def describe(self):
        return {'batch_policy': 'memory_budget_v1', 'device_budget_bytes': self.device_bytes,
                'host_slot_bytes': self.host_bytes, 'input_slots': 2}


def plan(free_device, total_device, available_host=None):
    device = min(free_device, total_device)-RESERVE
    host = ((host_available() if available_host is None else available_host)-RESERVE)//2
    if device < 256*MIB or host < 128*MIB:
        raise RuntimeError('Insufficient memory for bounded CUDA summaries; no CPU fallback.')
    return BatchBudget(device, host)
