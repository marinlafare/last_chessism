"""Small, non-authoritative performance report; never a substitute for result receipts."""
import math


def validate_performance(report, contract, max_workers=16):
    if (not isinstance(report, dict) or report.get("schema_version") != 1
            or report.get("fingerprint") != contract["fingerprint"]
            or report.get("position_count") != contract["position_count"]):
        raise ValueError("Performance report does not match this run")
    count = report["position_count"]
    for name in ("resumed", "analyzed_this_attempt"):
        if type(report.get(name)) is not int or not 0 <= report[name] <= count:
            raise ValueError("Invalid performance count")
    if report["resumed"] + report["analyzed_this_attempt"] != count:
        raise ValueError("Incomplete performance counts")
    workers = report.get("workers")
    if not isinstance(workers, list) or len(workers) > max_workers:
        raise ValueError("Invalid worker summary")
    seen, total = set(), 0
    for worker in workers:
        ident, positions = worker.get("worker"), worker.get("positions")
        if (type(ident) is not int or not 0 <= ident < max_workers or ident in seen
                or type(positions) is not int or positions < 0):
            raise ValueError("Invalid worker count")
        seen.add(ident)
        total += positions
        for name in ("analysis_seconds", "upload_wait_seconds"):
            value = worker.get(name)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError("Invalid worker timing")
    if total != report["analyzed_this_attempt"]:
        raise ValueError("Worker counts do not match analyzed count")
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("Missing performance metrics")
    for name in ("worker_seconds", "fen_per_second"):
        value = metrics.get(name)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("Invalid performance metric")
    return report
