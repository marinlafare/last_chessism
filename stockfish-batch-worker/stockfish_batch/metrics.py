"""Lightweight, optional benchmark sampling. No monitoring service or new dependency."""
import asyncio
from collections import defaultdict
import math
from pathlib import Path
import threading
import time


class MemorySeries:
    """Bounded histogram: 16 MiB bins, capped at 1 TiB. No growing trace."""
    step = 16 * 1024 * 1024
    ceiling = 1024 ** 4

    def __init__(self):
        self.count = self.total = self.maximum = 0
        self.minimum = None
        self.histogram = defaultdict(int)

    def add(self, value):
        if value < 0 or value > self.ceiling:
            return
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        self.histogram[(value + self.step - 1) // self.step] += 1

    def summary(self):
        if not self.count:
            return {"samples": 0}
        cumulative = 0
        for bucket, count in sorted(self.histogram.items()):
            cumulative += count
            if cumulative >= math.ceil(self.count * .95):
                p95 = bucket * self.step
                break
        return {"samples": self.count, "min_bytes": self.minimum,
                "mean_bytes": self.total / self.count, "max_bytes": self.maximum,
                "p95_upper_bound_bytes": p95, "histogram_bin_bytes": self.step}


class StorageStats:
    def __init__(self):
        self.values = defaultdict(float)
        self.lock = threading.Lock()

    def add(self, name, value=1):
        with self.lock:
            self.values[name] += value

    def snapshot(self):
        with self.lock:
            return dict(self.values)


class Sampler:
    def __init__(self):
        self.peak_memory = 0
        self.samples = 0
        self.start_cpu = self.cpu()
        self.worker_memory = MemorySeries()
        self.host_used = MemorySeries()
        self.host_available = MemorySeries()
        self.host_swap = MemorySeries()
        self.host_total = None
        self.start_oom = self.oom_kills()

    @staticmethod
    def read_integer(*filenames):
        for filename in filenames:
            try:
                return int(Path(filename).read_text())
            except (OSError, ValueError):
                continue
        return None

    @staticmethod
    def oom_kills():
        try:
            values = dict(line.split() for line in Path('/sys/fs/cgroup/memory.events').read_text().splitlines())
            return int(values['oom_kill'])
        except (OSError, ValueError, KeyError):
            return None

    def sample_memory(self):
        value = self.read_integer('/sys/fs/cgroup/memory.current',
                                  '/sys/fs/cgroup/memory/memory.usage_in_bytes')
        if value is not None:
            self.peak_memory = max(self.peak_memory, value)
            self.worker_memory.add(value)
        try:
            values = {parts[0].rstrip(':'): int(parts[1]) * 1024
                      for line in Path('/proc/meminfo').read_text().splitlines()
                      if len(parts := line.split()) == 3 and parts[2] == 'kB'}
            total, available = values['MemTotal'], values['MemAvailable']
            if not 0 <= available <= total:
                return
            self.host_total = total
            self.host_available.add(available)
            self.host_used.add(total - available)
            self.host_swap.add(values['SwapTotal'] - values['SwapFree'])
        except (OSError, ValueError, KeyError):
            pass

    @staticmethod
    def cpu():
        try:
            values = [int(n) for n in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
            return sum(values), values[3] + values[4], values[7]
        except (OSError, ValueError, IndexError):
            return None

    async def sample(self, stop):
        while not stop.is_set():
            self.sample_memory()
            self.samples += 1
            try:
                await asyncio.wait_for(stop.wait(), 0.25)
            except TimeoutError:
                pass

    def finish(self):
        self.sample_memory()
        end = self.cpu()
        cpu = steal = None
        if end and self.start_cpu:
            total, idle, stolen = [b - a for a, b in zip(self.start_cpu, end)]
            if total > 0:
                cpu, steal = 100 * (total - idle - stolen) / total, 100 * stolen / total
        oom = self.oom_kills()
        return {"vm_cpu_busy_percent": cpu, "vm_cpu_steal_percent": steal,
                "container_memory_peak_sampled_bytes": self.peak_memory, "resource_samples": self.samples,
                "memory": {"sampling_interval_seconds": .25,
                    "worker": self.worker_memory.summary(),
                    "cgroup_peak_bytes": self.read_integer('/sys/fs/cgroup/memory.peak',
                        '/sys/fs/cgroup/memory/memory.max_usage_in_bytes'),
                    "cgroup_oom_kills_delta": (oom - self.start_oom
                        if oom is not None and self.start_oom is not None else None),
                    "host_total_bytes": self.host_total,
                    "host_used_excluding_reclaimable": self.host_used.summary(),
                    "host_available": self.host_available.summary(),
                    "host_swap_used": self.host_swap.summary(),
                    "scope_note": "Host values use /proc/meminfo; valid for dedicated Batch VM sizing, "
                                  "not Cloud Run allocation. Samples cover worker processing; kernel "
                                  "cgroup peak covers container lifetime so far. P95 rounds up by <=16 MiB."}}
