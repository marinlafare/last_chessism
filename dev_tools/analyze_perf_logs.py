#!/usr/bin/env python3
import glob
import re
from typing import Dict, List, Tuple


LINE_RE = re.compile(
    r"^(?P<name>\S+)\s+"
    r"cpu=(?P<cpu>[0-9.]+)%\s+"
    r"mem=(?P<mem_val>[0-9.]+)(?P<mem_unit>[A-Za-z]+)\s*/\s*(?P<mem_total>[0-9.]+)(?P<mem_total_unit>[A-Za-z]+)\s+"
    r"mem%=(?P<mem_pct>[0-9.]+)%\s+"
    r"net=(?P<net_in>[0-9.]+)(?P<net_in_unit>[A-Za-z]+)\s*/\s*(?P<net_out>[0-9.]+)(?P<net_out_unit>[A-Za-z]+)\s+"
    r"block=(?P<blk_in>[0-9.]+)(?P<blk_in_unit>[A-Za-z]+)\s*/\s*(?P<blk_out>[0-9.]+)(?P<blk_out_unit>[A-Za-z]+)\s+"
    r"pids=(?P<pids>\d+)\s*$"
)


def to_mib(value: float, unit: str) -> float:
    unit = unit.lower().replace("ib", "b")
    if unit == "b":
        return value / (1024 * 1024)
    if unit == "kb":
        return value / 1024
    if unit == "mb":
        return value
    if unit == "gb":
        return value * 1024
    if unit == "tb":
        return value * 1024 * 1024
    return value


def to_mb(value: float, unit: str) -> float:
    unit = unit.lower().replace("ib", "b")
    if unit == "b":
        return value / 1_000_000
    if unit == "kb":
        return value / 1_000
    if unit == "mb":
        return value
    if unit == "gb":
        return value * 1_000
    if unit == "tb":
        return value * 1_000_000
    return value


def summarize(values: List[float]) -> Tuple[float, float, float]:
    if not values:
        return (0.0, 0.0, 0.0)
    return (min(values), sum(values) / len(values), max(values))


def parse_file(path: str) -> Dict[str, Dict[str, List[float]]]:
    stats: Dict[str, Dict[str, List[float]]] = {}

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("stockfish_replicas=") or line.startswith("analysis_concurrency="):
                continue
            m = LINE_RE.match(line)
            if not m:
                continue
            name = m.group("name")
            cpu = float(m.group("cpu"))
            mem_mib = to_mib(float(m.group("mem_val")), m.group("mem_unit"))
            mem_pct = float(m.group("mem_pct"))
            net_in_mb = to_mb(float(m.group("net_in")), m.group("net_in_unit"))
            net_out_mb = to_mb(float(m.group("net_out")), m.group("net_out_unit"))
            blk_in_mb = to_mb(float(m.group("blk_in")), m.group("blk_in_unit"))
            blk_out_mb = to_mb(float(m.group("blk_out")), m.group("blk_out_unit"))
            pids = float(m.group("pids"))

            entry = stats.setdefault(name, {
                "cpu": [], "mem_mib": [], "mem_pct": [],
                "net_in_mb": [], "net_out_mb": [],
                "blk_in_mb": [], "blk_out_mb": [],
                "pids": []
            })
            entry["cpu"].append(cpu)
            entry["mem_mib"].append(mem_mib)
            entry["mem_pct"].append(mem_pct)
            entry["net_in_mb"].append(net_in_mb)
            entry["net_out_mb"].append(net_out_mb)
            entry["blk_in_mb"].append(blk_in_mb)
            entry["blk_out_mb"].append(blk_out_mb)
            entry["pids"].append(pids)

    if not stats:
        return {}

    return stats


def merge_stats(all_stats: List[Dict[str, Dict[str, List[float]]]]) -> Dict[str, Dict[str, List[float]]]:
    merged: Dict[str, Dict[str, List[float]]] = {}
    for stats in all_stats:
        for name, entry in stats.items():
            target = merged.setdefault(name, {k: [] for k in entry.keys()})
            for k, v in entry.items():
                target[k].extend(v)
    return merged


if __name__ == "__main__":
    files = sorted(glob.glob("perf_log_job*.txt")) or ["perf_log.txt"]
    collected: List[Dict[str, Dict[str, List[float]]]] = []
    for path in files:
        stats = parse_file(path)
        if stats:
            collected.append(stats)

    if collected:
        merged = merge_stats(collected)
        print("== merged perf logs ==")
        for name, entry in merged.items():
            cpu_min, cpu_avg, cpu_max = summarize(entry["cpu"])
            mem_min, mem_avg, mem_max = summarize(entry["mem_mib"])
            memp_min, memp_avg, memp_max = summarize(entry["mem_pct"])
            net_in_min, net_in_avg, net_in_max = summarize(entry["net_in_mb"])
            net_out_min, net_out_avg, net_out_max = summarize(entry["net_out_mb"])
            blk_in_min, blk_in_avg, blk_in_max = summarize(entry["blk_in_mb"])
            blk_out_min, blk_out_avg, blk_out_max = summarize(entry["blk_out_mb"])
            pids_min, pids_avg, pids_max = summarize(entry["pids"])

            print(f"- {name}:")
            print(f"  cpu% min/avg/max: {cpu_min:.2f}/{cpu_avg:.2f}/{cpu_max:.2f}")
            print(f"  mem MiB min/avg/max: {mem_min:.2f}/{mem_avg:.2f}/{mem_max:.2f}")
            print(f"  mem% min/avg/max: {memp_min:.2f}/{memp_avg:.2f}/{memp_max:.2f}")
            print(f"  net_in MB min/avg/max: {net_in_min:.2f}/{net_in_avg:.2f}/{net_in_max:.2f}")
            print(f"  net_out MB min/avg/max: {net_out_min:.2f}/{net_out_avg:.2f}/{net_out_max:.2f}")
            print(f"  blk_in MB min/avg/max: {blk_in_min:.2f}/{blk_in_avg:.2f}/{blk_in_max:.2f}")
            print(f"  blk_out MB min/avg/max: {blk_out_min:.2f}/{blk_out_avg:.2f}/{blk_out_max:.2f}")
            print(f"  pids min/avg/max: {pids_min:.0f}/{pids_avg:.1f}/{pids_max:.0f}")
