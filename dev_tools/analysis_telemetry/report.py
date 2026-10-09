"""Incremental summaries and flat CSV rows for later comparisons."""

import math


def stats(values):
    if not values:
        return {'samples': 0, 'min': None, 'mean': None, 'max': None, 'p95': None}
    ordered = sorted(values)
    return {'samples': len(values), 'min': ordered[0], 'mean': sum(values) / len(values),
            'max': ordered[-1], 'p95': ordered[max(0, math.ceil(len(values) * .95) - 1)]}


def flat(sample):
    system, job = sample.get('system', {}), sample.get('job', {})
    progress, pool = job.get('progress') or {}, sample.get('engine', {})
    workers = pool.get('workers') or {}
    cpu = system.get('cpu_usage', {}).get('cpu', {})
    return {'timestamp_utc': sample['timestamp_utc'], 'elapsed_seconds': sample['elapsed_seconds'],
            'status': job.get('status'), 'phase': progress.get('phase'), 'detail': progress.get('detail'),
            'processed': progress.get('processed'), 'total': progress.get('total'), 'failed': progress.get('failed'),
            'fens_per_second': sample.get('fens_per_second'), 'rate_interval_mixed_phase': sample.get('rate_interval_mixed_phase'),
            'engines_busy': workers.get('busy'), 'engines_total': workers.get('total'),
            'package_c': system.get('package_c'), 'cpu_max_c': system.get('cpu_max_c'),
            'host_cpu_percent': cpu.get('busy_percent'), 'host_iowait_percent': cpu.get('iowait_percent'),
            'stockfish_cpu_percent': system.get('containers', {}).get('stockfish', {}).get('cpu_percent'),
            'database_cpu_percent': system.get('containers', {}).get('database', {}).get('cpu_percent'),
            'available_memory_kib': system.get('memory_kib', {}).get('MemAvailable'),
            'errors': '; '.join(sample.get('errors', []))}


class Summary:
    def __init__(self):
        self.rows, self.cooldowns = [], []
        self.first_throttle, self.last_throttle = {}, {}

    def add(self, sample):
        row = flat(sample)
        previous = self.rows[-1] if self.rows else None
        self.rows.append(row)
        for cpu, counts in sample.get('system', {}).get('hardware_throttle_counts', {}).items():
            for name, value in counts.items():
                if value is not None:
                    key = f'{cpu}/{name}'
                    self.first_throttle.setdefault(key, value)
                    self.last_throttle[key] = value
        if row['phase'] == 'cooling':
            if not previous or previous['phase'] != 'cooling':
                self.cooldowns.append({'first_seen_utc': row['timestamp_utc'], 'first_seen_seconds': row['elapsed_seconds'],
                                      'start_package_c': row['package_c'], 'minimum_package_c': row['package_c'],
                                      'last_seen_utc': row['timestamp_utc']})
            cooldown = self.cooldowns[-1]
            cooldown['last_seen_utc'] = row['timestamp_utc']
            if row['package_c'] is not None:
                old = cooldown['minimum_package_c']
                cooldown['minimum_package_c'] = min(old, row['package_c']) if old is not None else row['package_c']
        elif previous and previous['phase'] == 'cooling':
            self.cooldowns[-1].update(next_phase_seen_utc=row['timestamp_utc'],
                                      observed_seconds=row['elapsed_seconds'] - self.cooldowns[-1]['first_seen_seconds'],
                                      next_phase_package_c=row['package_c'])

    def payload(self):
        def values(key, subset=None):
            return [row[key] for row in (self.rows if subset is None else subset) if row.get(key) is not None]
        fully_busy = [row for row in self.rows if row['engines_total'] and row['engines_busy'] == row['engines_total']]
        thermal = {key: max(0, value - self.first_throttle[key]) for key, value in self.last_throttle.items()}
        rates = [row for row in self.rows if row['phase'] in ('analyzing', 'committed') and not row['rate_interval_mixed_phase']]
        return {'sample_count': len(self.rows), 'latest': self.rows[-1] if self.rows else None,
                'sampled_package_c': stats(values('package_c')), 'sampled_cpu_max_c': stats(values('cpu_max_c')),
                'package_c_all_engines_busy': stats(values('package_c', fully_busy)),
                'stockfish_cpu_percent': stats(values('stockfish_cpu_percent', fully_busy)),
                'host_cpu_percent': stats(values('host_cpu_percent')),
                'sample_interval_fens_per_second': stats(values('fens_per_second', rates)),
                'samples_at_or_above_85c': sum(value >= 85 for value in values('cpu_max_c')),
                'samples_at_or_above_90c': sum(value >= 90 for value in values('cpu_max_c')),
                'hardware_throttle_counter_deltas': thermal,
                'hardware_thermal_throttling_observed': any(thermal.values()) if thermal else None,
                'cooldowns': self.cooldowns, 'collection_error_samples': sum(bool(row['errors']) for row in self.rows),
                'notes': ['Read-only recording; no automatic temperature-based pause or shutdown.',
                          'Temperatures are sampled, not guaranteed peak values; phase timing is approximate.',
                          'CPU percentages per container use 100% per logical CPU; host total is 0–100%.',
                          'FEN rate is progress-counter throughput, not per-position engine speed.',
                          'Data before recording started is not included; exact final job duration comes from ARQ.',
                          'Frequency changes can have causes other than heat; hardware throttling counters are separate from cgroup CPU quota counters.']}
