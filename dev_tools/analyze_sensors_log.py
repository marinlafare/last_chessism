#!/usr/bin/env python3
import re
from typing import Dict, List, Tuple


TEMP_RE = re.compile(r"^(?P<label>[^:]+):\s+\+?(?P<temp>[0-9.]+)°C")
PACKAGE_RE = re.compile(r"^Package id 0:\s+\+?(?P<temp>[0-9.]+)°C")
CORE_RE = re.compile(r"^Core (\d+):\s+\+?(?P<temp>[0-9.]+)°C")
NVME_RE = re.compile(r"^Composite:\s+\+?(?P<temp>[0-9.]+)°C")


def summarize(values: List[float]) -> Tuple[float, float, float]:
    if not values:
        return (0.0, 0.0, 0.0)
    return (min(values), sum(values) / len(values), max(values))


def main(path: str = "sensors_log_current.txt") -> None:
    package_temps: List[float] = []
    core_temps: Dict[int, List[float]] = {}
    nvme_temps: Dict[str, List[float]] = {}

    current_section = ""
    current_nvme = ""

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line == "----":
                continue
            if line.endswith("Adapter: ISA adapter") or line.endswith("Adapter: PCI adapter") or line.endswith("Adapter: ACPI interface"):
                continue
            if line.startswith("coretemp-") or line.startswith("acpitz-"):
                current_section = line
                current_nvme = ""
                continue
            if line.startswith("nvme-pci-"):
                current_section = line
                current_nvme = line
                nvme_temps.setdefault(current_nvme, [])
                continue

            m = PACKAGE_RE.match(line)
            if m:
                package_temps.append(float(m.group("temp")))
                continue
            m = CORE_RE.match(line)
            if m:
                core = int(m.group(1))
                core_temps.setdefault(core, []).append(float(m.group("temp")))
                continue
            m = NVME_RE.match(line)
            if m and current_nvme:
                nvme_temps[current_nvme].append(float(m.group("temp")))
                continue

    print(f"== {path} ==")
    if package_temps:
        pmin, pavg, pmax = summarize(package_temps)
        print(f"Package id 0 °C min/avg/max: {pmin:.1f}/{pavg:.1f}/{pmax:.1f}")
    for core, temps in sorted(core_temps.items()):
        if temps:
            cmin, cavg, cmax = summarize(temps)
            print(f"Core {core} °C min/avg/max: {cmin:.1f}/{cavg:.1f}/{cmax:.1f}")
    for dev, temps in sorted(nvme_temps.items()):
        if temps:
            nmin, navg, nmax = summarize(temps)
            print(f"{dev} Composite °C min/avg/max: {nmin:.1f}/{navg:.1f}/{nmax:.1f}")


if __name__ == "__main__":
    import sys
    log_path = sys.argv[1] if len(sys.argv) > 1 else "sensors_log_current.txt"
    main(log_path)
