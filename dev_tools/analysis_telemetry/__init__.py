"""Read-only, bounded overnight Stockfish telemetry, independent of app workers.

Run recorder as a one-off container on the app network. Mount this package
read-only, host CPU sysfs at /host-cpu and cgroups at /host-cgroup read-only,
and a writable logs directory at /recordings. No Docker socket or DB access is
needed. Uses existing arq/redis/httpx dependencies; never controls analysis.

Outputs: timestamped samples.jsonl, metrics.csv and an atomic summary.json.
Flushes every sample; stops two minutes after the target job ends or at the
configured time limit. A sensor/API outage is recorded, not treated as zero.
Sampling misses short temperature spikes. CPU frequency is not proof of
thermal throttling: hardware throttle counters are recorded separately from
container CPU-quota counters. Cooldown duration is the observed sampled span,
not an exact measurement. Recording does not reconstruct earlier job history.

The analysis-telemetry Compose service runs watcher.py automatically and records
new analysis jobs under research_data/analysis_telemetry/. It uses host CPU
usage, temperatures, frequencies, and hardware throttle counters without a
Docker socket. Container-specific CPU metrics are only available in one-off
recordings supplied with explicit container IDs. Neither mode controls jobs.
"""
