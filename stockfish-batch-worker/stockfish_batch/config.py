"""Explicit runtime limits. Defaults are for the first 20-position smoke test."""
import argparse
from dataclasses import dataclass
import math
import os
from urllib.parse import urlsplit


def gcs_parts(uri):
    parsed = urlsplit(uri)
    if parsed.scheme != "gs" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("Expected gs://bucket/object without query or fragment")
    key = parsed.path.lstrip("/")
    if not key or any(part in {"", ".", ".."} for part in key.rstrip("/").split("/")):
        raise ValueError("A nonempty, unambiguous object path is required")
    return parsed.netloc, key


@dataclass(frozen=True)
class Config:
    input: str
    output: str
    engine: str = "/usr/local/bin/stockfish"
    workers: int = 4
    threads: int = 1
    hash_mb: int = 2048
    nodes: int = 1_000_000
    multipv: int = 4
    max_positions: int = 20
    memory_mib: int = 12288
    position_timeout: float = 60
    run_timeout: float = 840
    stall_timeout: float = 0
    upload_mode: str = "blocking"
    batch_size: int = 1
    upload_queue_size: int = 64
    compact_results: bool = False

    def validate(self):
        for name, low, high in (
            ("workers", 1, 16), ("threads", 1, 16), ("hash_mb", 16, 8192),
            ("nodes", 1, 100_000_000), ("multipv", 1, 10),
            ("max_positions", 1, 200000), ("memory_mib", 512, 131072),
            ("batch_size", 1, 500), ("upload_queue_size", 1, 128),
        ):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer between {low} and {high}")
        if self.upload_mode not in {"blocking", "background"}:
            raise ValueError("upload_mode must be blocking or background")
        if self.upload_mode == "blocking" and self.batch_size != 1:
            raise ValueError("Batch uploads require background mode")
        if self.compact_results and (self.upload_mode != "background" or self.batch_size < 2):
            raise ValueError("Compact results require background batch uploads")
        if self.max_positions > 1000 and not self.compact_results:
            raise ValueError("Large jobs require compact results")
        for name in ("position_timeout", "run_timeout", "stall_timeout"):
            minimum = 1 if name == "position_timeout" else 0
            if not math.isfinite(getattr(self, name)) or not minimum <= getattr(self, name) <= 86400:
                raise ValueError(f"{name} must be finite and between {minimum} and 86400 seconds")
        if not self.run_timeout and self.stall_timeout < 1:
            raise ValueError("Disabling the runtime limit requires a positive stall timeout")
        if self.run_timeout and not self.stall_timeout and self.position_timeout > self.run_timeout:
            raise ValueError("position_timeout must not exceed run_timeout")
        # Include Python, the Batch agent, embedded NNUEs, and non-hash engine memory.
        if self.workers * (self.hash_mb + 256) + 512 > self.memory_mib:
            raise ValueError("Memory budget is too small for all engines plus overhead")
        if not self.input or not self.output or self.input == self.output:
            raise ValueError("Input and output must be nonempty and different")
        for uri in (self.input, self.output):
            if "://" in uri:
                gcs_parts(uri)
        if self.output.startswith("gs://"):
            _, key = gcs_parts(self.output)
            if not key.startswith("results/") or key.rstrip("/") == "results":
                raise ValueError("Cloud output must be inside results/<unique-run-name>/")
        return self

    def analysis_settings(self):
        return {"nodes": self.nodes, "multipv": self.multipv, "threads": self.threads,
                "hash_mb": self.hash_mb, "wdl": True, "tablebases": False,
                "score_perspective": "white", "mate_score": 10000,
                "new_game_per_position": True}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="JSONL path or gs:// URI")
    parser.add_argument("--output", required=True, help="Local directory or gs://bucket/results/run/")
    parser.add_argument("--engine", default=os.environ.get("STOCKFISH_PATH", Config.engine))
    parser.add_argument("--upload-mode", choices=("blocking", "background"), default=Config.upload_mode)
    parser.add_argument("--compact-results", action="store_true")
    for name in ("workers", "threads", "hash_mb", "nodes", "multipv", "max_positions", "memory_mib",
                 "batch_size", "upload_queue_size"):
        parser.add_argument("--" + name.replace("_", "-"), type=int, default=getattr(Config, name))
    for name in ("position_timeout", "run_timeout", "stall_timeout"):
        parser.add_argument("--" + name.replace("_", "-"), type=float, default=getattr(Config, name))
    try:
        return Config(**vars(parser.parse_args(argv))).validate()
    except ValueError as exc:
        parser.error(str(exc))
