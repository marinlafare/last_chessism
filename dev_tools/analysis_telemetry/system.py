"""Small Linux read-only collectors; no process enumeration or shell commands."""

from pathlib import Path
import time


def read(path):
    try:
        return Path(path).read_text().strip()
    except (OSError, ValueError):
        return None


def numeric(path, scale=1):
    value = read(path)
    try:
        return float(value) / scale if value is not None else None
    except ValueError:
        return None


def counters(path):
    result = {}
    for line in (read(path) or '').splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                result[parts[0].rstrip(':')] = int(parts[1])
            except ValueError:
                pass
    return result


class SystemSampler:
    def __init__(self, groups, *, cpu_root='/host-cpu', group_root='/host-cgroup', proc_root='/proc', hwmon_root='/sys/class/hwmon'):
        self.groups = groups
        self.cpu_root, self.group_root = Path(cpu_root), Path(group_root)
        self.proc, self.hwmon = Path(proc_root), Path(hwmon_root)
        self.previous_cpus, self.previous_groups = {}, {}
        self.previous_at = None

    def sample(self):
        now = time.monotonic()
        elapsed = now - self.previous_at if self.previous_at is not None else None
        cpu_usage = {}
        for line in (read(self.proc / 'stat') or '').splitlines():
            parts = line.split()
            if not parts or not parts[0].startswith('cpu'):
                continue
            values = list(map(int, parts[1:9]))  # guest time already included in user/nice
            if len(values) < 5:
                continue
            total, idle, wait = sum(values), values[3], values[4]
            previous = self.previous_cpus.get(parts[0])
            if previous and total > previous[0]:
                delta = total - previous[0]
                cpu_usage[parts[0]] = {'busy_percent': 100 * (delta - (idle - previous[1]) - (wait - previous[2])) / delta,
                                     'iowait_percent': 100 * (wait - previous[2]) / delta}
            self.previous_cpus[parts[0]] = (total, idle, wait)
        temperatures, fans = {}, {}
        for device in sorted(self.hwmon.glob('hwmon*')):
            name = read(device / 'name') or device.name
            for sensor in device.glob('temp*_input'):
                label = read(sensor.with_name(sensor.name.replace('_input', '_label'))) or sensor.stem
                temperatures[f'{name}/{device.name}/{label}'] = numeric(sensor, 1000)
            for sensor in device.glob('fan*_input'):
                fans[f'{name}/{device.name}/{sensor.stem}'] = numeric(sensor)
        package = [value for key, value in temperatures.items() if 'coretemp/' in key and '/Package' in key and value is not None]
        cpu_temps = [value for key, value in temperatures.items() if 'coretemp/' in key and value is not None]
        frequencies, throttle = {}, {}
        for cpu in sorted(self.cpu_root.glob('cpu[0-9]*')):
            frequencies[cpu.name] = numeric(cpu / 'cpufreq/scaling_cur_freq', 1000)
            throttle[cpu.name] = {key: numeric(cpu / 'thermal_throttle' / key) for key in ('core_throttle_count', 'package_throttle_count')}
        groups = {}
        for label, group in self.groups.items():
            path = self.group_root / 'system.slice' / f'docker-{group}.scope'
            stats = counters(path / 'cpu.stat')
            usage = stats.get('usage_usec')
            previous = self.previous_groups.get(label)
            percent = None
            if elapsed and usage is not None and previous is not None and usage >= previous:
                percent = (usage - previous) / (elapsed * 10000)
            self.previous_groups[label] = usage
            groups[label] = {'cpu_percent': percent, 'cpu_stat': stats, 'memory_bytes': numeric(path / 'memory.current'),
                             'swap_bytes': numeric(path / 'memory.swap.current'), 'memory_events': counters(path / 'memory.events')}
        self.previous_at = now
        mem = counters(self.proc / 'meminfo')
        vm = counters(self.proc / 'vmstat')
        return {'cpu_usage': cpu_usage, 'temperatures_c': temperatures,
                'package_c': max(package) if package else None, 'cpu_max_c': max(cpu_temps) if cpu_temps else None,
                'fans_rpm': fans, 'frequencies_mhz': frequencies, 'hardware_throttle_counts': throttle,
                'memory_kib': {key: mem.get(key) for key in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree', 'Dirty')},
                'swap_io_pages': {key: vm.get(key) for key in ('pswpin', 'pswpout')},
                'load_average': read(self.proc / 'loadavg'),
                'pressure': {key: read(self.proc / 'pressure' / key) for key in ('cpu', 'io', 'memory')}, 'containers': groups}
